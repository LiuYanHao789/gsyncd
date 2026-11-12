// Package watcher turns kernel file notifications into a stream of events.
//
// The recursive part lives here so both the inotify and the polling backend
// share one implementation. Compared to Lua lsyncd this layer fixes:
//
//   - rename events are paired by kernel cookie, not by adjacency (#225);
//   - a lost event batch no longer restarts the process, it emits Overflow and
//     the engine re-synchronises only the affected roots (#202, #284);
//   - watch registration is parallel and bounded, so a 10M directory tree does
//     not pin one core for days (#202).
package watcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"gsyncd/internal/event"
)

// ErrClosed is returned after Close.
var ErrClosed = errors.New("watcher: closed")

// ErrWatchLimit marks "the kernel ran out of watch descriptors". It is not a
// fatal condition: the directories registered so far keep working, the rest of
// the tree falls back to periodic full resyncs. Treating it as fatal is what
// made the original unusable on trees with more directories than
// /proc/sys/fs/inotify/max_user_watches allows (#65).
var ErrWatchLimit = errors.New("watcher: out of watch descriptors")

// Backend is the low level kernel interface.
type Backend interface {
	// Add starts watching a single directory (non recursively).
	Add(path string) error
	// Remove stops watching a single directory.
	Remove(path string) error
	// Events returns the channel the backend pushes raw events into.
	Events() <-chan event.Event
	// Close releases all resources.
	Close() error
}

// CloseReporter is implemented by backends that can tell a finished write from
// an in-progress one.
//
// close_write depends on it: a backend that never reports a close leaves every
// file looking permanently open, so the window would only ever flush at
// max_hold. Saying so up front beats silently degrading a setting the operator
// asked for.
type CloseReporter interface {
	ReportsClose() bool
}

// rootSetter is implemented by backends that watch whole trees and can report
// the disappearance of a root itself. Without it a backend can only forget the
// watch silently, and the tree goes dark until the process restarts.
type rootSetter interface {
	SetRootPaths(isRoot func(path string) bool)
}

// Options configures the recursive layer.
type Options struct {
	// Workers is the number of goroutines used to register watches.
	Workers int
	// Ignore decides whether a directory is skipped entirely.
	Ignore func(path string) bool
	// Logf receives diagnostics.
	Logf func(format string, args ...any)
}

// Tree keeps a recursive watch on a set of roots.
type Tree struct {
	be   Backend
	opts Options
	out  chan event.Event
	done chan struct{}
	once sync.Once
	// emitMu/outClosed let pump close out after the backend dies without
	// racing a concurrent emit from an adopter.
	emitMu    sync.RWMutex
	outClosed bool
	outOnce   sync.Once
	mu        sync.Mutex
	roots     map[string]bool
	// lost marks roots whose watch the kernel dropped (the directory was
	// removed, unmounted, or the watch table was pruned). The path stays in
	// roots because the operator asked for it to be watched; it is simply
	// not being watched right now. Reattach is the only way back.
	lost map[string]bool
	// rootIdent remembers the identity each root had when its watch was
	// installed. Bookkeeping alone cannot tell that a directory was removed
	// and recreated under the same name: the path is identical, the watch is
	// not. One identity read per root answers that without a tree walk.
	rootIdent map[string]fileIdentity
	// dirs holds every watched directory, mapped to its slot in the parent's
	// entry of kids. The slot makes detaching O(1); a stale slot falls back to
	// scanning one parent's children, which is bounded by the fan-out.
	dirs map[string]int32
	// kids indexes the children of each recorded directory so a subtree can be
	// enumerated without scanning the whole table. Without it, removing a
	// directory was linear in the number of watched directories, and since
	// "rm -rf" raises one Delete per directory the total was quadratic - the
	// single worst hot spot of a batch delete (see bookkeeping.go).
	kids map[string][]string
	// unwatched counts directories that could not be registered because the
	// kernel watch table was full.
	unwatched int
	// newDirs feeds directories that appeared at runtime to a small fixed
	// pool of adopters. A goroutine per new directory looks harmless until a
	// bulk copy creates a million of them at once: that is a million
	// walkAdd calls, each spawning its own worker pool.
	newDirs chan string
	addWG   sync.WaitGroup
}

// adoptWorkers bounds how many subtrees are scanned for new watches at the same
// time. Each scan is internally parallel, so two is plenty.
const adoptWorkers = 2

// New wraps a backend with the recursive bookkeeping layer.
func New(be Backend, opts Options) *Tree {
	if opts.Workers <= 0 {
		opts.Workers = runtime.NumCPU()
		if opts.Workers > 8 {
			opts.Workers = 8
		}
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	t := &Tree{
		be:   be,
		opts: opts,
		// Same reasoning as the inotify backend's buffer: this channel is the
		// last place a burst can be absorbed before the kernel queue starts
		// dropping events, and a full resync costs far more than 64k event
		// structs of memory.
		out:       make(chan event.Event, 65536),
		done:      make(chan struct{}),
		roots:     make(map[string]bool),
		lost:      make(map[string]bool),
		rootIdent: make(map[string]fileIdentity),
		dirs:      make(map[string]int32),
		kids:      make(map[string][]string),
		newDirs:   make(chan string, 1024),
	}
	// Tell backends which paths are roots. A backend that watches whole trees
	// can then report "the root itself is gone" instead of one anonymous
	// notice per forgotten directory.
	if rs, ok := be.(rootSetter); ok {
		rs.SetRootPaths(t.isRoot)
	}
	for i := 0; i < adoptWorkers; i++ {
		t.addWG.Add(1)
		go t.adoptLoop()
	}
	go t.pump()
	return t
}

// adoptLoop installs watches for directories that appeared after startup.
func (t *Tree) adoptLoop() {
	defer t.addWG.Done()
	for {
		select {
		case <-t.done:
			return
		case p := <-t.newDirs:
			if t.opts.Ignore != nil && t.opts.Ignore(p) {
				continue
			}
			if err := t.adoptTree(p); err != nil && !errors.Is(err, ErrClosed) {
				t.opts.Logf("watcher: cannot watch %s: %v", p, err)
			}
		}
	}
}

// Events returns the merged event stream.
func (t *Tree) Events() <-chan event.Event { return t.out }

// Roots returns the configured roots in sorted order.
func (t *Tree) Roots() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.roots))
	for r := range t.roots {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// WatchDirCount reports how many directories are currently watched.
func (t *Tree) WatchDirCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.dirs)
}

// Unwatched reports how many directories could not be registered because the
// kernel watch table was full. A non-zero value means events under those
// subtrees are invisible and the caller must schedule periodic full resyncs.
func (t *Tree) Unwatched() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.unwatched
}

// isRoot reports whether path is one of the registered roots. Backends use it
// to tell "a directory below a root disappeared" (an ordinary event) from "the
// root itself disappeared" (the whole tree is now invisible).
func (t *Tree) isRoot(path string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.roots[path]
}

// IsWatched reports whether the directory is currently registered. It is a
// bookkeeping answer, not proof: the kernel can drop a watch without telling
// anyone, which is why the supervisor also stats the root.
func (t *Tree) IsWatched(path string) bool {
	path = filepath.Clean(path)
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.dirs[path]
	return ok
}

// Reattach re-registers a root whose watch disappeared. It does nothing when
// the root is already watched, so a supervisor can call it every tick. The
// directory must exist again; AddRoot reports the error otherwise and the
// caller retries on its next tick.
func (t *Tree) Reattach(root string) error {
	root = filepath.Clean(root)
	t.mu.Lock()
	prev, known := t.rootIdent[root]
	t.mu.Unlock()
	if known {
		if now, err := identityOf(root); err == nil && prev == now && t.IsWatched(root) {
			// Same directory, watch still recorded: nothing to repair.
			return nil
		}
	}
	// Either the root is a different inode than the one that was registered
	// (removed and recreated), or the watch was lost outright. Both mean the
	// recorded directories below are stale: drop them from the bookkeeping and
	// from the kernel before walking, or the walk reports "already watched"
	// about paths the kernel has never heard of.
	t.mu.Lock()
	t.lost[root] = true
	victims := t.takeBelowLocked(root)
	t.mu.Unlock()
	for _, d := range victims {
		_ = t.be.Remove(d)
	}
	return t.AddRoot(root)
}

// Lost reports the roots whose watch disappeared and that have not been
// re-attached yet, sorted for stable output.
func (t *Tree) Lost() []string {
	t.mu.Lock()
	out := make([]string, 0, len(t.lost))
	for p := range t.lost {
		out = append(out, p)
	}
	t.mu.Unlock()
	sort.Strings(out)
	return out
}

// Verify reports whether the directory at root is still the one that was
// registered.
//
// It answers a question the bookkeeping cannot: "rm -rf data && cp -a new data"
// leaves the path identical and the watch pointing at a deleted inode, and if
// the kernel dropped the notice while its queue was overflowing, every other
// signal says the tree is being watched while it is in fact blind. One stat
// per root, no directory walk.
func (t *Tree) Verify(root string) (bool, error) {
	root = filepath.Clean(root)
	t.mu.Lock()
	prev, ok := t.rootIdent[root]
	t.mu.Unlock()
	if !ok {
		// No identity was recorded (AddRoot could not read it). Reporting
		// "different" made the supervisor call Reattach + a full resync every
		// ten seconds forever; an error makes it skip until the next restart.
		return false, fmt.Errorf("%s: no recorded identity", root)
	}
	now, err := identityOf(root)
	if err != nil {
		return false, err
	}
	return prev == now, nil
}

// AddRoot registers a directory tree recursively.
func (t *Tree) AddRoot(root string) error {
	root = filepath.Clean(root)
	st, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return fmt.Errorf("%s: not a directory", root)
	}
	t.mu.Lock()
	if t.roots[root] && !t.lost[root] {
		t.mu.Unlock()
		return nil
	}
	// A root that is coming back after its watch disappeared must not keep
	// the "lost" mark, or the next Reattach would try to add it twice.
	delete(t.lost, root)
	t.roots[root] = true
	id, ierr := identityOf(root)
	if ierr != nil {
		delete(t.roots, root)
		t.mu.Unlock()
		return fmt.Errorf("%s: cannot read identity: %w", root, ierr)
	}
	t.rootIdent[root] = id
	t.mu.Unlock()
	if !t.perDirectory() {
		// A snapshot backend watches whole roots, so the root itself is the
		// only thing to register: walking the tree to record names would be a
		// second full pass with no effect on what is reported, and on a
		// hundred million files that pass is not free.
		if err := t.be.Add(root); err != nil {
			return err
		}
		t.mu.Lock()
		t.addRef(root)
		t.mu.Unlock()
		return nil
	}
	return t.walkAdd(root)
}

// walkAdd registers root and every subdirectory below it using a worker pool.
func (t *Tree) walkAdd(root string) error { return t.scanTree(root, false) }

// adoptTree registers a directory that appeared at runtime and reports every
// file already inside it.
//
// The backfill is not optional. mkdir and the first write into the new directory
// are microseconds apart, while add_watch happens on an adopter goroutine after
// the directory event has been forwarded: a file created in that window raises
// no inotify event at all, because the kernel only reports changes made under a
// watch that already existed. Without the rescan the file would never be copied
// until the next full resync, which on a real workload is the difference
// between "cp -a" working and silently losing files (issue #284).
func (t *Tree) adoptTree(root string) error { return t.scanTree(root, true) }

// scanTree is the recursive part shared by both entry points.
//
// The queue is fed by the workers themselves, so the dispatcher must not close
// the channel before the last outstanding job reports back. The loop below
// counts dispatched jobs instead: it exits when the queue is empty and nothing
// is in flight, which keeps the tree walk parallel without the "send on closed
// channel" race a naive close-after-seed has.
func (t *Tree) scanTree(root string, backfill bool) error {
	type scanResult struct {
		children []string
		// dirs and files are only filled for an adoption: they are the
		// content that already existed when the watch went in.
		dirs  []string
		files []string
		err   error
	}
	work := make(chan string)
	results := make(chan scanResult, t.opts.Workers*2)
	var wg sync.WaitGroup

	for i := 0; i < t.opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range work {
				// Only a directory whose watch was installed just now has
				// content the kernel never reported; one that was already
				// watched has had every change delivered as an event.
				fresh, err := t.addOne(p)
				bf := backfill && fresh
				if err != nil {
					select {
					case results <- scanResult{err: err}:
					case <-t.done:
						return
					}
					continue
				}
				ents, err := os.ReadDir(p)
				if err != nil {
					select {
					case results <- scanResult{}:
					case <-t.done:
						return
					}
					continue
				}
				var children, dirs, files []string
				for _, e := range ents {
					child := filepath.Join(p, e.Name())
					if !e.IsDir() {
						// Symlinks are not followed: the original lsyncd
						// ignored them to avoid loops and double counting.
						if bf && e.Type()&os.ModeSymlink == 0 {
							files = append(files, child)
						}
						continue
					}
					if t.opts.Ignore != nil && t.opts.Ignore(child) {
						continue
					}
					children = append(children, child)
					if bf {
						dirs = append(dirs, child)
					}
				}
				select {
				case results <- scanResult{children: children, dirs: dirs, files: files}:
				case <-t.done:
					return
				}
			}
		}()
	}

	queue := []string{root}
	pending := 0
	var firstErr error
	for len(queue) > 0 || pending > 0 {
		var send chan string
		var next string
		if len(queue) > 0 {
			send = work
			next = queue[0]
		}
		select {
		case send <- next:
			queue = queue[1:]
			pending++
		case r := <-results:
			pending--
			if r.err != nil && firstErr == nil {
				firstErr = r.err
			}
			if len(r.children) > 0 {
				queue = append(queue, r.children...)
			}
			for _, d := range r.dirs {
				// Empty directories are invisible to inotify too: they hold no
				// files to backfill and no event was ever raised for them.
				t.emit(event.Event{Path: d, Op: event.Create, IsDir: true, At: time.Now()})
			}
			for _, f := range r.files {
				// The watch on f's parent may be brand new, so these changes
				// are synthesised rather than observed: they exist to make the
				// engine copy what already landed in a fresh directory.
				//
				// Close is part of the mask on purpose. These files were listed
				// after the watch was installed, so no IN_CLOSE_WRITE will ever
				// arrive for them; without the bit a close_write sync would
				// count every backfilled file as still open and could only copy
				// them when max_hold expired.
				t.emit(event.Event{Path: f, Op: event.Create | event.Write | event.Close, At: time.Now()})
			}
		case <-t.done:
			close(work)
			wg.Wait()
			return firstErr
		}
	}
	close(work)
	wg.Wait()
	return firstErr
}

// perDirectoryBackend is implemented by backends that keep one kernel watch per
// directory. The polling fallback is not one of them: it snapshots whole roots,
// so registering a subdirectory as another root would make every scan re-walk
// the same tree once per directory, which is quadratic and on a ten million
// directory tree never finishes.
type perDirectoryBackend interface {
	PerDirectory() bool
}

// perDirectory reports whether subdirectories must be registered individually.
func (t *Tree) perDirectory() bool {
	if b, ok := t.be.(perDirectoryBackend); ok {
		return b.PerDirectory()
	}
	return true
}

// addOne installs a watch on one directory and reports whether the watch is
// new. A false return covers both "already watched" (a second root that
// contains this directory) and "kernel refused it": in neither case is there
// anything to backfill.
func (t *Tree) addOne(p string) (bool, error) {
	if !t.perDirectory() {
		// Nothing to register, but the path still belongs in the bookkeeping
		// so WatchDirCount keeps counting what we cover.
		t.mu.Lock()
		t.addRef(p)
		t.mu.Unlock()
		return false, nil
	}
	fresh, err := t.watchOne(p)
	if err != nil {
		if errors.Is(err, ErrWatchLimit) {
			// Keep going: one exhausted watch table must not abort a
			// twenty million directory walk. Count it and let the caller
			// decide to fall back to periodic resyncs.
			t.mu.Lock()
			t.unwatched++
			first := t.unwatched == 1
			t.mu.Unlock()
			if first {
				t.opts.Logf("watcher: %v; further directories under %s stay unwatched, "+
					"falling back to periodic full resyncs", err, p)
			}
			return false, nil
		}
		return false, err
	}
	return fresh, nil
}

// inodeWatcher is implemented by backends whose watch follows the inode and
// which can therefore tell a fresh watch from a redundant one.
type inodeWatcher interface {
	AddWatch(path string) (bool, error)
}

// watchOne registers one directory and records it.
//
// The recorded set is bookkeeping, never proof. A path can outlive its inode -
// "rm -rf x && cp -a y x" is routine for a mirror - and the entry that remains
// then says "already watched" about a directory the kernel has long forgotten.
// Only the backend can answer that, so the decision is delegated to it.
func (t *Tree) watchOne(p string) (bool, error) {
	fresh := true
	if iw, ok := t.be.(inodeWatcher); ok {
		f, err := iw.AddWatch(p)
		if err != nil {
			return false, err
		}
		fresh = f
	} else if err := t.be.Add(p); err != nil {
		return false, err
	}
	t.mu.Lock()
	t.addRef(p)
	t.mu.Unlock()
	return fresh, nil
}

// pump forwards backend events and extends the watch set for new directories.
func (t *Tree) pump() {
	defer t.closeOut()
	in := t.be.Events()
	for {
		select {
		case <-t.done:
			return
		case ev, ok := <-in:
			if !ok {
				return
			}
			t.handle(ev)
		}
	}
}

func (t *Tree) handle(ev event.Event) {
	if ev.Op&event.RootGone != 0 {
		// The backend lost the watch on a root itself: the directory was
		// removed, unmounted, or its watch was dropped. Record that, drop the
		// bookkeeping of everything below (those watches went with it), and
		// pass it on - the engine has to re-transfer this tree once the root
		// is back, and until then every event under it is invisible.
		t.mu.Lock()
		if t.roots[ev.Path] {
			t.lost[ev.Path] = true
		}
		t.dropPath(ev.Path)
		t.mu.Unlock()
		t.forgetBelow(ev.Path)
		t.emit(ev)
		return
	}
	if ev.Op == event.Overflow {
		// The kernel dropped events: tell the engine, keep the watch set.
		t.emit(ev)
		return
	}
	// Adopt a directory that is new to the watched set. A Move whose source was
	// not recorded (it lived under an ignored path, or the watch table was
	// full) is such a case: rekey has nothing to move, and without adoption the
	// destination and everything already inside it stay invisible.
	newToTree := ev.Op&event.Create != 0 ||
		(ev.Op&event.Move != 0 && (ev.From == "" || !t.IsWatched(ev.From)))
	if t.perDirectory() && ev.IsDir && newToTree {
		// A new directory appeared: watch it, then re-scan it so files created
		// between mkdir and add_watch are not lost. Queueing instead of
		// spawning keeps the goroutine count constant when a bulk copy lands.
		select {
		case t.newDirs <- ev.Path:
		case <-t.done:
			return
		}
	}
	if ev.IsDir && ev.Op&event.Delete != 0 {
		t.mu.Lock()
		t.dropPath(ev.Path)
		t.mu.Unlock()
		// A directory that is gone must not stay in the bookkeeping of the
		// paths below it either. inotify only reports the top of a removed
		// subtree, so "rm -rf big" leaves one event but leaves every child
		// path in t.dirs: on a churning dataset that map grows without bound
		// and a later directory that happens to get the same name is counted
		// as already watched.
		t.forgetBelow(ev.Path)
	}
	if ev.IsDir && ev.Op&event.Move != 0 && ev.From != "" {
		t.rekey(ev.From, ev.Path)
	}
	t.emit(ev)
}

// forgetBelow drops every recorded directory strictly below root.
//
// It walks the child index instead of the table, so the cost is the subtree
// that is actually going away. That distinction is the whole point: "rm -rf"
// raises one Delete per directory, and a table-wide scan per event made a batch
// delete quadratic (measured: 44ms per delete at one million watched
// directories).
func (t *Tree) forgetBelow(root string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.forgetBelowLocked(root)
}

// rekey follows a directory rename.
//
// inotify watches belong to the inode, so "mv old new" keeps the same watch
// descriptors while the names they map back to are gone. Every later event
// under the renamed tree would then be delivered with a stale path, the file at
// that path is missing, and the engine would faithfully mirror the result as a
// delete: renaming one directory would wipe its copy on the target. Both the
// backend's wd tables and the recorded directory set have to follow.
//
// The recorded subtree is computed once, here, and handed to the backend: it
// used to be computed twice, once by scanning the tree index and once by
// scanning the backend's own table, and the second scan was the expensive one
// (1.1s and 122MB per rename at a million watched directories, measured on the
// test machine, with the event reader blocked behind the same mutex).
func (t *Tree) rekey(from, to string) {
	t.mu.Lock()
	moved := t.rekeyDirsLocked(from, to)
	t.mu.Unlock()
	if r, ok := t.be.(renameFollower); ok && len(moved) > 0 {
		r.Rekey(from, to, moved)
	}
	if len(moved) > 0 {
		t.opts.Logf("watcher: %d directories follow rename %s -> %s", len(moved), from, to)
	}
}

// renameFollower is implemented by backends whose watch descriptors survive a
// directory rename and therefore have to be told about it. The paths are the
// ones the caller already collected from the tree index, parents first.
type renameFollower interface {
	Rekey(from, to string, moved []string)
}

// replacePrefix rewrites p from the from subtree to the to subtree.
func replacePrefix(p, from, to string) (string, bool) {
	if p == from {
		return to, true
	}
	if !IsUnder(from, p) {
		return "", false
	}
	return to + p[len(from):], true
}

func (t *Tree) emit(ev event.Event) {
	t.emitMu.RLock()
	defer t.emitMu.RUnlock()
	if t.outClosed {
		return
	}
	select {
	case t.out <- ev:
	case <-t.done:
	}
}

// closeOut closes the consumer channel exactly once. Without it a backend
// whose read loop died left the engine waiting forever: Run selects on the
// channel and the context, and the context is only cancelled on shutdown.
func (t *Tree) closeOut() {
	t.outOnce.Do(func() {
		t.emitMu.Lock()
		t.outClosed = true
		close(t.out)
		t.emitMu.Unlock()
	})
}

// Close stops the tree and the backend.
func (t *Tree) Close() error {
	var err error
	t.once.Do(func() {
		close(t.done)
		err = t.be.Close()
		t.addWG.Wait()
		t.closeOut()
	})
	return err
}

// IsUnder reports whether path sits inside root. Both the platform separator
// and '/' are accepted: paths coming from inotify are always slash separated,
// while Windows paths are not, and the same code runs on both.
func IsUnder(root, path string) bool {
	if path == root {
		return true
	}
	root = strings.TrimSuffix(root, string(os.PathSeparator))
	root = strings.TrimSuffix(root, "/")
	if !strings.HasPrefix(path, root) {
		return false
	}
	rest := path[len(root):]
	if rest == "" {
		return false
	}
	return rest[0] == '/' || rest[0] == os.PathSeparator
}

// MoveGrace is how long an unpaired MOVED_FROM is kept before it is turned
// into a Delete. The original code gave up as soon as the read buffer was
// drained, which produced spurious delete+retransfer pairs under load.
const MoveGrace = 750 * time.Millisecond
