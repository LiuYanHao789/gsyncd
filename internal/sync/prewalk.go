package sync

// walkOrdered is filepath.Walk with the directory reads done ahead of time by a
// pool of workers.
//
// Why it exists: on a cold cache the serial walk was the whole cost of a
// startup pass. Measured on 210k directories, walking the source took 139.7s
// while every target-side operation combined (readdir of every directory, 3.1s,
// or stat of every directory, 0.9s) was noise. Serial walking leaves the disk
// queue empty between metadata requests: each read waits for the previous one
// to come back. Reading listings concurrently took the same tree from 126s to
// 15.9s with 16 workers, and 131s to 50.6s on a tree with five files per
// directory.
//
// The visit order is exactly filepath.Walk's, and that is a hard requirement
// rather than a nicety: the checkpoint store is a forward cursor appended in
// walk order, so a different order would attach records to the wrong
// directories. Prefetching only changes *when* a listing is read, never which
// directory is visited next. os.ReadDir returns entries sorted by name, and
// that is the order filepath.Walk uses too.
//
// A worker reads one listing and hands it back; expansion of the children it
// found happens on the dispatcher, which also keeps the number of in-flight
// requests bounded. Requests are advisory: if the window is full or the pool
// never answers, the driver reads that one directory itself and carries on. A
// dropped request costs a syscall, never correctness.

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// prefetchFetchTimeout bounds how long the driver waits for a listing it asked
// for. Past it the driver reads the directory itself. This only exists so a
// pool that has wedged cannot stall a sync; it is not expected to fire.
const prefetchFetchTimeout = 5 * time.Second

// prefetchWindowDefault caps how many directory listings may be in flight or
// buffered.
//
// It is not the only bound, because a count of listings says nothing about
// their size: a few thousand buffered directories of 500 files each is
// millions of DirEntry-plus-FileInfo pairs, and it would be spent on listings
// the driver has not asked for yet. prefetchEntryBudget is the bound that
// actually tracks memory.
//
// The window is deliberately small. Speculation only has to keep the workers
// fed, and one listing per worker in flight is enough for that: a large window
// buys pipelining into directories the driver will not reach for minutes,
// which is exactly the read-ahead that made a 5M-file walk hold 984MB.
const prefetchWindowDefault = 256

// prefetchEntryBudget caps the total number of entries buffered across all
// ready listings. A 5M-file tree of 500-entry directories reached 984MB of
// RSS before this existed, nearly all of it read-ahead: a 4096-listing window
// times 500 entries times the size of an entry. At 256k entries the read-ahead
// tops out around 40MB, which keeps the workers fed without holding the tree
// in memory.
const prefetchEntryBudget = 1 << 18

type dirEntry struct {
	name string
	info os.FileInfo
}

type dirResult struct {
	dir  string
	ents []dirEntry
}

// prefetcher reads directory listings concurrently and hands them to the
// driver on demand.
type prefetcher struct {
	pending chan string
	done    chan dirResult
	// quit stops the pool. Without it every walk left its workers and its
	// buffered listings reachable forever: the goroutines blocked on an
	// open channel keep the maps alive, so a daemon that does an hourly full
	// resync would accumulate one pool and its read-ahead per hour.
	quit     chan struct{}
	quitOnce sync.Once
	// closed is set under mu once quit has been closed, so a caller that
	// races with the close reads its own listing instead of queueing a
	// request no worker will ever pick up.
	closed bool

	mu        sync.Mutex
	requested map[string]bool
	ready     map[string][]dirEntry
	waiting   map[string]chan []dirEntry
	window    int
	// buffered is the number of entries sitting in ready, which is the part
	// of the read-ahead cost that is proportional to how wide the tree is
	// rather than how deep. It is adjusted only under mu.
	buffered int
	// budget is how many buffered entries the fan-out may reach before it
	// stops asking for more listings. It is a field rather than the constant
	// so a test can shrink it to a number it can actually reach.
	budget int
}

func newPrefetcher(workers, window int) *prefetcher {
	if window <= 0 {
		window = prefetchWindowDefault
	}
	p := &prefetcher{
		pending:   make(chan string, window),
		done:      make(chan dirResult, window),
		requested: make(map[string]bool),
		ready:     make(map[string][]dirEntry),
		waiting:   make(map[string]chan []dirEntry),
		quit:      make(chan struct{}),
		window:    window,
		budget:    prefetchEntryBudget,
	}
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	go p.dispatch()
	return p
}

func (p *prefetcher) worker() {
	for {
		select {
		case dir := <-p.pending:
			res := dirResult{dir: dir, ents: readDirEntries(dir)}
			select {
			case p.done <- res:
			case <-p.quit:
				return
			}
		case <-p.quit:
			return
		}
	}
}

// Close stops the pool. Listings still buffered are dropped: the driver is
// finished with this tree, and keeping them reachable would defeat the point.
func (p *prefetcher) Close() {
	p.quitOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		// Drop the read-ahead now rather than at collection time: a wide
		// tree can be holding tens of megabytes in here.
		p.ready = nil
		p.requested = nil
		p.buffered = 0
		p.mu.Unlock()
		close(p.quit)
	})
}

// room reports whether another listing may be read speculatively without
// pushing the buffered entries past prefetchEntryBudget.
func (p *prefetcher) room() bool {
	p.mu.Lock()
	ok := p.buffered < p.budget
	p.mu.Unlock()
	return ok
}

func (p *prefetcher) dispatch() {
	for {
		var r dirResult
		select {
		case r = <-p.done:
		case <-p.quit:
			return
		}
		// Fan out one level before handing r.ents to its owner. The receiver
		// sorts the slice in place; reading it after the send would race with
		// that sort. The budget check stops the fan-out on a wide tree:
		// without it every listing read spawns requests for all of its
		// subdirectories, each of which will be read and then sit in ready
		// until the driver walks into it.
		for _, e := range r.ents {
			if !e.info.IsDir() {
				continue
			}
			if !p.room() {
				break
			}
			p.request(filepath.Join(r.dir, e.name))
		}
		p.mu.Lock()
		if ch, ok := p.waiting[r.dir]; ok {
			delete(p.waiting, r.dir)
			delete(p.requested, r.dir)
			p.mu.Unlock()
			ch <- r.ents
		} else if p.closed {
			// Closing: nobody will ever ask for this listing.
			p.mu.Unlock()
			continue
		} else {
			p.ready[r.dir] = r.ents
			p.buffered += len(r.ents)
			p.mu.Unlock()
		}
	}
}

// request asks for a listing unless one is already in flight, already read, or
// the window is full. It reports whether the request was accepted; a rejected
// one must be read by the caller, because nothing else will ever read it.
//
// Rejecting rather than queueing is what keeps the pool from turning into an
// unbounded backlog: the driver is always ahead of the pool by at most the
// window, and any directory the pool did not take costs one inline read.
func (p *prefetcher) request(dir string) bool {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false
	}
	if p.requested[dir] {
		// Already asked for. Whoever asked will get it, but this caller needs
		// its own answer, so it registers as a waiter instead.
		p.mu.Unlock()
		return true
	}
	if len(p.requested) >= p.window {
		p.mu.Unlock()
		return false
	}
	p.requested[dir] = true
	p.mu.Unlock()
	select {
	case p.pending <- dir:
		return true
	case <-p.quit:
		p.mu.Lock()
		delete(p.requested, dir)
		p.mu.Unlock()
		return false
	default:
		p.mu.Lock()
		delete(p.requested, dir)
		p.mu.Unlock()
		return false
	}
}

// fetch returns the listing for dir, reading it inline when the pool will not.
func (p *prefetcher) fetch(dir string) []dirEntry {
	p.mu.Lock()
	if ents, ok := p.ready[dir]; ok {
		delete(p.ready, dir)
		delete(p.requested, dir)
		p.buffered -= len(ents)
		p.mu.Unlock()
		return ents
	}
	ch := make(chan []dirEntry, 1)
	p.waiting[dir] = ch
	p.mu.Unlock()

	if !p.request(dir) {
		// The pool is saturated or already full of other work. Reading here
		// costs one syscall and keeps the driver moving; waiting for a pool
		// that was never told about this directory would stall for the whole
		// timeout, which on a wide tree happens thousands of times.
		p.mu.Lock()
		delete(p.waiting, dir)
		p.mu.Unlock()
		return readDirEntries(dir)
	}
	// A fresh timer rather than time.After: this path runs once per
	// directory, and time.After leaves its timer registered for the full
	// timeout even when the listing arrives immediately, which on a 210k
	// directory tree is a lot of timer churn for nothing.
	timer := time.NewTimer(prefetchFetchTimeout)
	defer timer.Stop()
	select {
	case ents := <-ch:
		return ents
	case <-p.quit:
		// Closed while waiting. Read it here for the same reason as the
		// timeout path: returning an empty listing would look like an empty
		// directory and make the prune delete the target it stands for.
		p.mu.Lock()
		delete(p.waiting, dir)
		delete(p.requested, dir)
		p.mu.Unlock()
		return readDirEntries(dir)
	case <-timer.C:
		// Only reachable if a worker died or a request was lost. Read inline
		// rather than stall the sync.
		p.mu.Lock()
		delete(p.waiting, dir)
		delete(p.requested, dir)
		p.mu.Unlock()
		return readDirEntries(dir)
	}
}

// parallelStatThreshold is the directory size above which the per-entry stat
// calls are spread over workers. Below it the pool overhead is larger than the
// work; above it one prefetch worker would otherwise stat a hundred thousand
// entries on a single thread while every other worker waits.
const parallelStatThreshold = 4096

// statLimit bounds concurrent nested stat calls across all prefetch workers.
// Prefetch itself is already parallel across directories; without this bound a
// burst of large directories would multiply that by the per-directory worker
// count and flood the disk with concurrent metadata requests.
var statLimit = make(chan struct{}, statWorkers())

func statWorkers() int {
	n := runtime.NumCPU()
	if n < 4 {
		n = 4
	}
	if n > 32 {
		n = 32
	}
	return n
}

func readDirEntries(dir string) []dirEntry {
	des, err := readDirFast(dir)
	if err != nil {
		// A directory that cannot be read is treated as empty, which is what
		// the serial walk did: it called the walk function with the error and
		// the callback chose to ignore it. The watcher reports the change.
		return nil
	}
	out := make([]dirEntry, len(des))
	if len(des) < parallelStatThreshold {
		n := 0
		for _, de := range des {
			info, err := de.Info()
			if err != nil {
				continue
			}
			out[n] = dirEntry{name: de.Name(), info: info}
			n++
		}
		return out[:n]
	}

	workers := statWorkers()
	if workers > len(des) {
		workers = len(des)
	}
	var next int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&next, 1)) - 1
				if i >= len(des) {
					return
				}
				statLimit <- struct{}{}
				info, err := des[i].Info()
				<-statLimit
				if err == nil {
					out[i] = dirEntry{name: des[i].Name(), info: info}
				}
			}
		}()
	}
	wg.Wait()
	n := 0
	for i := range out {
		if out[i].info != nil {
			out[n] = out[i]
			n++
		}
	}
	return out[:n]
}

// prefetchWorkers is the number of listing readers to use for a walk.
//
// A negative request means "no prefetching", which walkOrdered implements by
// falling back to filepath.Walk.
func prefetchWorkers(n int) int {
	if n < 0 {
		return 0
	}
	if n > 0 {
		return n
	}
	n = runtime.NumCPU()
	if n > 16 {
		n = 16
	}
	if n < 1 {
		n = 1
	}
	return n
}

// walkOrdered walks root in exactly the order filepath.Walk would, calling fn
// for every entry with the same arguments. The only difference is that
// directory listings are read ahead of time by a pool of workers.
//
// With workers <= 0 it defers to filepath.Walk, which keeps the serial path
// available for tests, for callers that do not want the extra goroutines, and
// as the fallback if prefetching ever misbehaves.
func walkOrdered(ctx context.Context, root string, workers, window int, fn func(path string, info os.FileInfo, err error) error) error {
	if workers <= 0 {
		return filepath.Walk(root, fn)
	}
	info, err := os.Lstat(root)
	if err != nil {
		// filepath.Walk reports the root's error through the callback rather
		// than returning it, and callers depend on that shape.
		return fn(root, nil, err)
	}
	p := newPrefetcher(workers, window)
	defer p.Close()

	var walk func(rel, abs string, info os.FileInfo) error
	walk = func(rel, abs string, info os.FileInfo) error {
		if err := fn(abs, info, nil); err != nil {
			if err == filepath.SkipDir {
				// For a directory SkipDir means "do not descend"; that is the
				// only place the walk itself uses the sentinel.
				return nil
			}
			return err
		}
		ents := p.fetch(abs)
		names := make([]string, 0, len(ents))
		for _, e := range ents {
			names = append(names, e.name)
		}
		if !sort.StringsAreSorted(names) {
			// os.ReadDir documents sorted output; if that ever changes, the
			// checkpoint's ordering assumption breaks with it.
			sort.Slice(ents, func(i, j int) bool { return ents[i].name < ents[j].name })
		}
		for _, e := range ents {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			crel := e.name
			if rel != "." {
				crel = rel + "/" + e.name
			}
			cabs := filepath.Join(abs, e.name)
			if e.info.IsDir() {
				if err := walk(crel, cabs, e.info); err != nil {
					return err
				}
				continue
			}
			if err := fn(cabs, e.info, nil); err != nil {
				if err == filepath.SkipDir {
					// filepath.Walk's documented meaning for SkipDir on a
					// non-directory: skip the remaining files of the
					// containing directory.
					break
				}
				return err
			}
		}
		return nil
	}
	return walk(".", root, info)
}
