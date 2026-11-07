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
	for dir := range p.pending {
		p.done <- dirResult{dir: dir, ents: readDirEntries(dir)}
	}
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
	for r := range p.done {
		p.mu.Lock()
		if ch, ok := p.waiting[r.dir]; ok {
			delete(p.waiting, r.dir)
			delete(p.requested, r.dir)
			p.mu.Unlock()
			ch <- r.ents
		} else {
			p.ready[r.dir] = r.ents
			p.buffered += len(r.ents)
			p.mu.Unlock()
		}
		// Fan out one level. A chain of single-child directories gains nothing
		// here, but each child is exactly what the driver asks for next, so it
		// is worth requesting. The budget check stops the fan-out on a wide
		// tree: without it every listing read spawns requests for all of its
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
	select {
	case ents := <-ch:
		return ents
	case <-time.After(prefetchFetchTimeout):
		// Only reachable if a worker died or a request was lost. Read inline
		// rather than stall the sync.
		p.mu.Lock()
		delete(p.waiting, dir)
		delete(p.requested, dir)
		p.mu.Unlock()
		return readDirEntries(dir)
	}
}

func readDirEntries(dir string) []dirEntry {
	des, err := os.ReadDir(dir)
	if err != nil {
		// A directory that cannot be read is treated as empty, which is what
		// the serial walk did: it called the walk function with the error and
		// the callback chose to ignore it. The watcher reports the change.
		return nil
	}
	out := make([]dirEntry, 0, len(des))
	for _, de := range des {
		info, err := de.Info()
		if err != nil {
			continue
		}
		out = append(out, dirEntry{name: de.Name(), info: info})
	}
	return out
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
