//go:build !linux

package watcher

// This file provides a portable fallback backend for platforms without
// inotify (macOS, Windows, BSD). It polls the tree and diffs snapshots.
//
// It is deliberately simple: it keeps the same event semantics as the inotify
// backend so the engine above it does not need to care. Two details matter for
// correctness and both were bugs in naive implementations of this idea:
//
//   - a newly added root is indexed first and only later diffed, otherwise the
//     whole tree is reported as created and gets copied twice;
//   - entries are keyed by the walk path, so events carry exactly the paths the
//     engine expects.

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"gsyncd/internal/event"
)

type fileState struct {
	size  int64
	mtime int64
	mode  os.FileMode
	isDir bool
	// gen is the scan generation that last saw this entry. It replaces the
	// "build a second map, then swap" pattern: on a tree with tens of millions
	// of entries that pattern doubles peak memory once per tick and hands the
	// collector a multi-gigabyte map to free every few seconds.
	gen uint64
}

type pollBackend struct {
	interval time.Duration
	out      chan event.Event
	done     chan struct{}
	once     sync.Once
	wake     chan struct{}
	logf     func(string, ...any)

	mu    sync.Mutex
	roots map[string]bool
	// seed marks roots whose current content must be recorded without
	// emitting events.
	seed map[string]bool
	snap map[string]fileState
	gen  uint64

	// scanMu serialises scans: the loop runs them on a ticker while Prime may
	// run one from the main goroutine.
	scanMu sync.Mutex
	// slowWarned keeps the "scan overran the interval" message to one line.
	slowWarned bool
}

// NewPoll creates a polling backend.
func NewPoll(interval time.Duration, logf func(string, ...any)) Backend {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	b := &pollBackend{
		interval: interval,
		out:      make(chan event.Event, 16384),
		done:     make(chan struct{}),
		wake:     make(chan struct{}, 1),
		logf:     logf,
		roots:    make(map[string]bool),
		seed:     make(map[string]bool),
		snap:     make(map[string]fileState),
	}
	go b.loop()
	return b
}

func (b *pollBackend) Events() <-chan event.Event { return b.out }

// ReportsClose is false: a snapshot diff sees that a file changed, never that a
// writer closed it. Every write would look permanently open, so close_write is
// rejected at startup on this backend instead of degrading to "flush at
// max_hold only".
func (b *pollBackend) ReportsClose() bool { return false }

// PerDirectory tells the recursive layer that this backend watches whole roots
// and needs no per-directory registration. Without it every directory of a tree
// would be added as another root: the snapshot then re-walks the same subtree
// once per directory it contains, which is quadratic and hopeless on the trees
// this tool exists for.
func (b *pollBackend) PerDirectory() bool { return false }

func (b *pollBackend) Add(path string) error {
	path = filepath.Clean(path)
	b.mu.Lock()
	if !b.roots[path] {
		b.roots[path] = true
		b.seed[path] = true
	}
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
	return nil
}

func (b *pollBackend) Remove(path string) error {
	path = filepath.Clean(path)
	b.mu.Lock()
	delete(b.roots, path)
	delete(b.seed, path)
	for p := range b.snap {
		if IsUnder(path, p) {
			delete(b.snap, p)
		}
	}
	b.mu.Unlock()
	return nil
}

// Prime records the current state of every root without emitting events. It
// must be called once after the roots are registered and before the initial
// synchronisation starts; otherwise a modification made while the first
// snapshot is being taken could be folded into the baseline and lost.
func (b *pollBackend) Prime() { b.scan() }

func (b *pollBackend) Close() error {
	b.once.Do(func() { close(b.done) })
	return nil
}

func (b *pollBackend) loop() {
	defer close(b.out)
	t := time.NewTicker(b.interval)
	defer t.Stop()
	b.scan()
	for {
		select {
		case <-b.done:
			return
		case <-t.C:
			b.scan()
		case <-b.wake:
			b.scan()
		}
	}
}

// scan diffs the tree against the previous snapshot.
//
// The snapshot is updated in place, stamped with the current generation, and
// entries that the walk did not touch are swept afterwards. The obvious
// alternative - build a brand new map, then swap - doubles the resident
// snapshot once per tick and hands the collector a multi-gigabyte map to free
// every few seconds, which on a tree with tens of millions of entries is the
// difference between a steady RSS and a sawtooth that eventually wins.
//
// The walk runs under b.mu so that the sweep is exact. Add/Remove block for the
// duration of one scan, which is acceptable: they only happen at startup.
func (b *pollBackend) scan() {
	b.scanMu.Lock()
	defer b.scanMu.Unlock()

	start := time.Now()
	b.mu.Lock()
	roots := make([]string, 0, len(b.roots))
	for r := range b.roots {
		roots = append(roots, r)
	}
	seed := make(map[string]bool, len(b.seed))
	for r := range b.seed {
		seed[r] = true
	}
	b.seed = make(map[string]bool, len(b.roots))
	b.gen++
	gen := b.gen
	b.mu.Unlock()
	sort.Strings(roots)

	now := time.Now()
	var evs []event.Event
	// gone collects roots whose walk failed at the root itself. Polling has no
	// way to notice a vanished root otherwise: the walk reports one error, the
	// snapshot sweep below turns every recorded path into a Delete, and the
	// tree looks empty rather than unwatched. On a producer that does
	// "rm -rf data && cp -a new data" that reads as "everything was deleted",
	// which is the wrong mirror and the wrong log line.
	var gone []string
	b.mu.Lock()
	for _, root := range roots {
		_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				if p == root {
					gone = append(gone, root)
				}
				return nil
			}
			size, mtime, mode, isDir := info.Size(), info.ModTime().UnixNano(), info.Mode(), info.IsDir()
			old, seen := b.snap[p]
			if !seen {
				// A tree we just started watching is recorded silently,
				// otherwise everything in it is reported as created and
				// copied a second time.
				if !inFresh(seed, p) {
					evs = append(evs, event.Event{Path: p, Op: event.Create, IsDir: isDir, At: now})
				}
			} else if !inFresh(seed, p) && !isDir {
				if old.size != size || old.mtime != mtime {
					evs = append(evs, event.Event{Path: p, Op: event.Write, At: now})
				} else if old.mode != mode {
					evs = append(evs, event.Event{Path: p, Op: event.Attrib, At: now})
				}
			}
			b.snap[p] = fileState{size: size, mtime: mtime, mode: mode, isDir: isDir, gen: gen}
			return nil
		})
	}
	for p, st := range b.snap {
		if st.gen == gen {
			continue
		}
		if !inFresh(seed, p) {
			evs = append(evs, event.Event{Path: p, Op: event.Delete, IsDir: st.isDir, At: now})
		}
		delete(b.snap, p)
	}
	b.mu.Unlock()

	// Delivery happens outside the lock: push blocks on the event channel when
	// the engine is behind, and holding the snapshot mutex across that would
	// stall every other operation.
	for _, ev := range evs {
		b.push(ev)
	}
	for _, root := range gone {
		b.push(event.Event{Path: root, Op: event.RootGone, IsDir: true, At: now})
	}

	// A scan that takes longer than the interval means the tree is too big for
	// polling: the ticker fires again immediately and the process spends all
	// its time walking. Warn once so the user can raise watch_interval instead
	// of wondering why the CPU is pinned (issue #202 is exactly this shape).
	if d := time.Since(start); d > b.interval && !b.slowWarned {
		b.slowWarned = true
		b.logf("watcher: one scan took %s, longer than the %s interval; raise watch_interval",
			d.Round(time.Millisecond), b.interval)
	}
}

func inFresh(fresh map[string]bool, p string) bool {
	for r := range fresh {
		if IsUnder(r, p) {
			return true
		}
	}
	return false
}

func (b *pollBackend) push(ev event.Event) {
	select {
	case b.out <- ev:
	case <-b.done:
	}
}
