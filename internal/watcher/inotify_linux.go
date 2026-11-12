//go:build linux

package watcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"gsyncd/internal/event"
)

// inotifyBackend implements Backend on top of inotify(7).
type inotifyBackend struct {
	f    *os.File
	fd   int
	out  chan event.Event
	done chan struct{}
	once sync.Once

	mu       sync.RWMutex
	wdToDir  map[int32]string
	dirToWd  map[string]int32
	dirIdent map[string]fileIdentity

	// pending pairs MOVED_FROM events waiting for MOVED_TO, keyed by cookie.
	pending map[uint32]pendingMove

	// isRoot tells whether a path is a sync root. It is what lets the
	// backend distinguish "a directory below a root vanished" from "the root
	// itself vanished", and only the latter has to be reported as RootGone:
	// the first is an ordinary delete the engine already handles.
	isRoot func(path string) bool

	// lastLostOverflow throttles the non-root watch-loss notice: a bulk
	// delete+recreate can retire thousands of watches, and one full resync is
	// enough for all of them.
	lastLostOverflow time.Time

	logf func(format string, args ...any)
}

type pendingMove struct {
	ev   event.Event
	when time.Time
}

// inotifyMask is the base set of events we care about. IN_CREATE is kept
// because a plain touch(1) only produces IN_ATTRIB/IN_CREATE on some kernels.
//
// IN_MODIFY is what makes a long-lived writer visible at all. Without it a file
// that stays open for hours - an append-only log, a WAL, a database growing in
// place - produces exactly one event, the IN_CREATE at open time, and the mirror
// keeps a snapshot of the first few megabytes until the file is finally closed
// or rotated (issues #77, #458). With it, every write refreshes that path's
// pending entry, and the ordinary delay/debounce logic turns into "copy the file
// once it has been quiet for delay" - a file still being appended to is not
// recopied over and over, and one that stops changing is picked up without
// waiting for a close that may never come.
const inotifyMask = syscall.IN_ATTRIB |
	syscall.IN_CLOSE_WRITE |
	syscall.IN_CREATE |
	syscall.IN_MODIFY |
	syscall.IN_DELETE |
	syscall.IN_DELETE_SELF |
	syscall.IN_MOVED_FROM |
	syscall.IN_MOVED_TO |
	syscall.IN_MOVE_SELF |
	syscall.IN_ONLYDIR |
	syscall.IN_DONT_FOLLOW

// NewInotify creates an inotify backend.
func NewInotify(logf func(string, ...any)) (Backend, error) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	// IN_NONBLOCK|IN_CLOEXEC in one call, unlike the original
	// inotify_init() + separate fcntl dance.
	fd, err := syscall.InotifyInit1(syscall.IN_NONBLOCK | syscall.IN_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("inotify_init1: %w", err)
	}
	b := &inotifyBackend{
		f:  os.NewFile(uintptr(fd), "inotify"),
		fd: fd,
		// Generous relative to the kernel queue (fs.inotify.max_queued_events,
		// 16384 by default): every event this buffer cannot hold becomes
		// backpressure on the kernel queue, and what the kernel cannot hold is
		// dropped outright. Absorbing a burst here is far cheaper than the full
		// resync an overflow triggers.
		out:      make(chan event.Event, 32768),
		done:     make(chan struct{}),
		wdToDir:  make(map[int32]string),
		dirToWd:  make(map[string]int32),
		dirIdent: make(map[string]fileIdentity),
		pending:  make(map[uint32]pendingMove),
		logf:     logf,
	}
	go b.read()
	return b, nil
}

func (b *inotifyBackend) Events() <-chan event.Event { return b.out }

// ReportsClose is true: IN_CLOSE_WRITE is the kernel's own "this writer is
// done" signal, and dispatch turns it into event.Close.
func (b *inotifyBackend) ReportsClose() bool { return true }

// SetRootPaths installs the predicate that recognises sync roots.
func (b *inotifyBackend) SetRootPaths(isRoot func(path string) bool) {
	b.mu.Lock()
	b.isRoot = isRoot
	b.mu.Unlock()
}

// rootOf reports the watched directory for wd and whether it is a sync root.
func (b *inotifyBackend) rootOf(wd int32) (string, bool) {
	b.mu.RLock()
	dir, ok := b.wdToDir[wd]
	isRoot := b.isRoot
	b.mu.RUnlock()
	if !ok || isRoot == nil {
		return "", false
	}
	return dir, isRoot(dir)
}

func (b *inotifyBackend) Add(path string) error {
	_, err := b.AddWatch(path)
	return err
}

// AddWatch installs a watch and reports whether it landed on a different inode
// than the one recorded for that path.
//
// The "fresh" answer is what tells the caller whether the directory's existing
// content has to be backfilled, and the syscall is the only trustworthy source
// for it. inotify_add_watch is idempotent for an inode that is already watched,
// so calling it unconditionally costs one syscall per directory and removes a
// whole class of silent failures: the recorded path outliving its inode.
//
// That case is not exotic. Delete a directory and recreate it under the same
// name - rsync --delete followed by a re-copy, or "rm -rf x && cp -a y x" - and
// the stale entry makes a cache-trusting Add() return "already watched" while
// the kernel watches nothing. Every later change below that path is then
// invisible until the next full resync, with no error and no log line.
func (b *inotifyBackend) AddWatch(path string) (bool, error) {
	ident, identErr := identityOf(path)
	// Hold b.mu across the syscall and the map update. The kernel can recycle a
	// wd between InotifyAddWatch and the map write; retireMovedSubtree holds
	// the same lock while it re-checks and removes, so serialising here is what
	// makes its invariant real.
	b.mu.Lock()
	wd, err := syscall.InotifyAddWatch(b.fd, path, inotifyMask)
	if err != nil {
		b.mu.Unlock()
		if err == syscall.ENOSPC {
			// The kernel watch table is full. Report it as a degradable
			// condition so a huge tree keeps working instead of aborting
			// the whole daemon (issue #65).
			return false, fmt.Errorf("%s: raise /proc/sys/fs/inotify/max_user_watches: %w", path, ErrWatchLimit)
		}
		return false, fmt.Errorf("inotify_add_watch %s: %w", path, err)
	}
	prev, known := b.dirToWd[path]
	if known && prev == int32(wd) {
		// The same wd number is not proof of the same inode: after a lost
		// IN_IGNORED the kernel can hand the number to a recreated directory.
		// Compare the recorded identity and treat a different inode as fresh,
		// so its existing content is backfilled.
		if identErr == nil {
			if old, ok := b.dirIdent[path]; ok && old == ident {
				b.mu.Unlock()
				return false, nil
			}
		} else {
			// Cannot prove it is new; avoid a needless rescan.
			b.mu.Unlock()
			return false, nil
		}
	}
	if known {
		// The recorded descriptor is a leftover from an inode that no longer
		// lives at this path. Drop it so a later Remove or Rekey cannot touch
		// an unrelated directory, and report the watch as fresh so the caller
		// rescans what the new inode already holds.
		if other, ok := b.wdToDir[prev]; !ok || other == path {
			delete(b.wdToDir, prev)
		}
		delete(b.dirIdent, path)
	}
	b.wdToDir[int32(wd)] = path
	b.dirToWd[path] = int32(wd)
	if identErr == nil {
		b.dirIdent[path] = ident
	} else {
		delete(b.dirIdent, path)
	}
	b.mu.Unlock()
	return true, nil
}

func (b *inotifyBackend) Remove(path string) error {
	b.mu.Lock()
	wd, ok := b.dirToWd[path]
	if ok {
		delete(b.dirToWd, path)
		delete(b.dirIdent, path)
		// The wd may have been recycled for another directory; only remove the
		// reverse mapping (and the kernel watch) when it still names this path.
		if cur, found := b.wdToDir[wd]; found && cur == path {
			delete(b.wdToDir, wd)
		} else {
			ok = false
		}
	}
	b.mu.Unlock()
	if !ok {
		return nil
	}
	_, err := syscall.InotifyRmWatch(b.fd, uint32(wd))
	return err
}

// rawEvent mirrors struct inotify_event from the kernel headers.
type rawEvent struct {
	Wd     int32
	Mask   uint32
	Cookie uint32
	Len    uint32
}

func (b *inotifyBackend) read() {
	defer close(b.out)
	buf := make([]byte, 128*1024)
	failures := 0
	for {
		select {
		case <-b.done:
			return
		default:
		}
		_ = b.f.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := b.f.Read(buf)
		if err != nil {
			if os.IsTimeout(err) {
				b.flushExpired()
				continue
			}
			select {
			case <-b.done:
				return
			default:
			}
			// A read error is not automatically fatal: EINTR and a momentary
			// EAGAIN happen, and treating them as the end would leave the
			// daemon running with a dead watch stream, still reporting a
			// healthy status while nothing is ever mirrored again. Retry a few
			// times with a growing pause, and only then give up - loudly, and
			// with an overflow event so the engine schedules a full resync
			// instead of trusting a mirror it can no longer keep current.
			failures++
			if failures <= 3 {
				b.logf("watcher: inotify read failed (%d/3), retrying: %v", failures, err)
				time.Sleep(time.Duration(failures) * 100 * time.Millisecond)
				continue
			}
			b.logf("watcher: inotify read failed for good, event stream is dead: %v", err)
			b.push(event.Event{Op: event.Overflow, At: time.Now()})
			return
		}
		failures = 0
		b.parse(buf[:n])
		b.flushExpired()
	}
}

func (b *inotifyBackend) parse(buf []byte) {
	const hdr = int(unsafe.Sizeof(rawEvent{}))
	for off := 0; off+hdr <= len(buf); {
		ev := (*rawEvent)(unsafe.Pointer(&buf[off]))
		nameLen := int(ev.Len)
		if off+hdr+nameLen > len(buf) {
			return
		}
		var name string
		if nameLen > 0 {
			raw := buf[off+hdr : off+hdr+nameLen]
			end := 0
			for end < len(raw) && raw[end] != 0 {
				end++
			}
			name = string(raw[:end])
		}
		off += hdr + nameLen
		b.dispatch(ev, name)
	}
}

func (b *inotifyBackend) dispatch(ev *rawEvent, name string) {
	now := time.Now()
	if ev.Mask&syscall.IN_IGNORED != 0 && ev.Mask&(syscall.IN_DELETE_SELF|syscall.IN_MOVE_SELF) == 0 {
		// The kernel dropped this watch on its own (the directory was removed
		// or unmounted). Forget the descriptor: leaving it registered keeps a
		// dead wd -> path entry for every directory ever deleted, which on a
		// churning dataset is a slow leak (#686) and can also mislabel a later
		// event that happens to get the recycled wd number.
		b.mu.RLock()
		mapped, known := b.wdToDir[ev.Wd]
		rec, hasRec := b.dirIdent[mapped]
		isRootFn := b.isRoot
		b.mu.RUnlock()
		if !known {
			return
		}
		isRoot := isRootFn != nil && isRootFn(mapped)
		cleanup := func() {
			b.mu.Lock()
			if cur, ok := b.wdToDir[ev.Wd]; ok && cur == mapped {
				delete(b.wdToDir, ev.Wd)
				delete(b.dirIdent, mapped)
				if w, ok := b.dirToWd[mapped]; ok && w == ev.Wd {
					delete(b.dirToWd, mapped)
				}
			}
			b.mu.Unlock()
		}
		ident, ierr := identityOf(mapped)
		switch {
		case ierr != nil:
			// The directory is gone. For a root that is the only notice its
			// own subtree will never produce; for a descendant the parent's
			// Delete already describes it, and emitting Overflow per directory
			// turned "rm -rf" into a resync storm.
			cleanup()
			if isRoot {
				b.push(event.Event{Path: mapped, Op: event.RootGone, IsDir: true, At: now})
			}
			return
		case hasRec && ident == rec:
			// Same inode, still present. This is a queued IN_IGNORED whose wd
			// was already reused for a recreated directory; deleting the live
			// mapping would make the new watch silently invisible.
			return
		default:
			// The path now names a different inode (replaced, or a nested
			// mount unmounted). The watch that belonged to the old inode is
			// gone while the subtree may still be expected at this path.
			cleanup()
			if isRoot {
				b.push(event.Event{Path: mapped, Op: event.RootGone, IsDir: true, At: now})
			} else {
				b.mu.Lock()
				notify := now.Sub(b.lastLostOverflow) >= time.Minute
				if notify {
					b.lastLostOverflow = now
				}
				b.mu.Unlock()
				if notify {
					b.push(event.Event{Op: event.Overflow, At: now})
				}
			}
			return
		}
	}
	if ev.Mask&syscall.IN_Q_OVERFLOW != 0 {
		// Never dropped: an overflow means events were already lost, and the
		// only recovery is the engine's full resync. A non-blocking send here
		// would discard the one notice that says the mirror is now stale,
		// precisely when the consumer is too slow to have room for it.
		b.push(event.Event{Op: event.Overflow, At: now})
		return
	}
	b.mu.RLock()
	dir, ok := b.wdToDir[ev.Wd]
	b.mu.RUnlock()
	if !ok {
		// No watch for this descriptor: it was removed above, or IN_IGNORED
		// already retired it. Either way the event names nothing we can map
		// back to a path, and guessing one would mirror a change onto the
		// wrong file.
		return
	}
	path := dir
	if name != "" {
		// filepath.Join, not dir+"/"+name: for a root of "/" the latter
		// produced "//name", which did not compare equal to the "/name" the
		// initial walk recorded.
		path = filepath.Join(dir, name)
	}

	var op event.Op
	var isSelf bool
	switch {
	case ev.Mask&syscall.IN_CREATE != 0:
		op |= event.Create
	case ev.Mask&syscall.IN_MODIFY != 0:
		// A write to an already-open file. The aggregator folds repeated
		// writes to the same path into a single pending change, so a writer
		// producing a stream of 4 KiB writes costs one map update each and
		// one copy per quiet period, not one copy per write.
		op |= event.Write
	case ev.Mask&syscall.IN_CLOSE_WRITE != 0:
		// The last writer closed the file. Marking it apart from a plain
		// IN_MODIFY lets the close_write option hold a flush until the file
		// is finished rather than copying a snapshot of a file that is still
		// growing (issue #458).
		op |= event.Write | event.Close
	case ev.Mask&syscall.IN_ATTRIB != 0:
		op |= event.Attrib
	case ev.Mask&syscall.IN_DELETE != 0:
		op |= event.Delete
	case ev.Mask&syscall.IN_DELETE_SELF != 0:
		// Only directories are watched with IN_ONLYDIR, so the subject of a
		// self event is necessarily a directory even though the kernel does
		// not set IN_ISDIR here. Reporting it as a file left the deleted
		// directory in the watcher's bookkeeping forever, and a later
		// recreation of the same name then looked "already watched".
		op = event.Delete
		path = dir
		isSelf = true
	case ev.Mask&syscall.IN_MOVE_SELF != 0:
		// The watched directory itself was renamed. For an ordinary
		// subdirectory the parent's MOVED_FROM/MOVED_TO pair carries the new
		// name. A sync root has no watched parent, so a root rename arrives
		// only as MOVED_SELF; without reporting it the kernel watch follows
		// the moved inode while paths keep the old prefix, and a later write
		// under the moved tree deletes the target copy. Retire the stale
		// watches and let the supervisor re-attach if the configured path
		// reappears.
		if d, wasRoot := b.rootOf(ev.Wd); wasRoot {
			b.removeWatchesBelow(d)
			b.push(event.Event{Path: d, Op: event.RootGone, IsDir: true, At: now})
		}
		return
	case ev.Mask&syscall.IN_MOVED_FROM != 0:
		// Hold it: a matching MOVED_TO may still arrive (issue #225).
		// IN_ISDIR must be carried through: hardcoding "directory" here made a
		// moved-out file look like a directory, and if the partner never
		// arrived the fallback delete dropped every pending change below a
		// path that was never a directory.
		b.mu.Lock()
		b.pending[ev.Cookie] = pendingMove{
			ev: event.Event{
				Path: path, Op: event.Move,
				IsDir:  ev.Mask&syscall.IN_ISDIR != 0,
				Cookie: ev.Cookie, At: now,
			},
			when: now,
		}
		b.mu.Unlock()
		return
	case ev.Mask&syscall.IN_MOVED_TO != 0:
		isDir := ev.Mask&syscall.IN_ISDIR != 0
		b.mu.Lock()
		p, ok := b.pending[ev.Cookie]
		delete(b.pending, ev.Cookie)
		b.mu.Unlock()
		if ok {
			b.push(event.Event{
				Path:   path,
				From:   p.ev.Path,
				Op:     event.Move,
				IsDir:  isDir,
				Cookie: ev.Cookie,
				At:     now,
			})
			return
		}
		// A move into the tree from outside: it is a create as far as the
		// destination is concerned.
		b.push(event.Event{Path: path, Op: event.Move | event.Create, IsDir: isDir, Cookie: ev.Cookie, At: now})
		return
	default:
		return
	}

	// Know whether the subject is a directory without an extra stat in the
	// common case: IN_ISDIR is set by the kernel.
	ev2 := event.Event{
		Path:  path,
		Op:    op,
		IsDir: isSelf || ev.Mask&syscall.IN_ISDIR != 0,
		At:    now,
	}
	gone := ev.Mask&syscall.IN_IGNORED != 0
	// Ask before the tables are cleared: after that the wd no longer resolves.
	_, wasRoot := b.rootOf(ev.Wd)
	if gone {
		b.mu.Lock()
		delete(b.dirToWd, dir)
		delete(b.wdToDir, ev.Wd)
		b.mu.Unlock()
	}
	b.push(ev2)
	if gone && wasRoot {
		// IN_DELETE_SELF/IN_IGNORED on a root: the watch is gone with the
		// directory. The event above tells the engine to remove the mirror;
		// this tells the supervisor the tree has to be re-attached once it
		// comes back, otherwise the root stays dark forever.
		b.push(event.Event{Path: dir, Op: event.RootGone, IsDir: true, At: now})
	}
}

// Rekey rewrites every recorded directory name below from so it reports under
// to.
//
// It is not cosmetic. inotify watches follow the inode, so after "mv old new"
// the kernel keeps reporting events for the same wd while the name that wd used
// to correspond to no longer exists. Without this rewrite every later event
// under the renamed tree arrives with a stale path: the backend hands the
// engine ".../old/x", the file is gone from that path, and the change is
// interpreted as a delete of the target copy. Renaming one directory would
// silently delete its mirror.
//
// The subtree comes from the caller, which already had it: finding the same set
// here meant scanning every watched directory on every rename, and a rename of
// a busy tree is not a rare event (rotating log directories, the temp-dir swap
// rsync does, an operator moving a dataset). Measured before the change, on a
// million watched directories: 1.15s and 122MB allocated per rename, holding
// b.mu, so the event reader could not run at all for that long.
func (b *inotifyBackend) Rekey(from, to string, moved []string) {
	// Keep this under the lock and free of syscalls: the backend reader may
	// dispatch events under the renamed directory while this runs, resolving
	// their paths from wdToDir. Identity is unchanged by a rename, so the
	// recorded value moves with the path.
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, old := range moved {
		wd, ok := b.dirToWd[old]
		if !ok {
			continue
		}
		np, ok := replacePrefix(old, from, to)
		if !ok {
			continue
		}
		delete(b.dirToWd, old)
		oldID, hadID := b.dirIdent[old]
		delete(b.dirIdent, old)
		b.dirToWd[np] = wd
		b.wdToDir[wd] = np
		if hadID {
			b.dirIdent[np] = oldID
		} else {
			delete(b.dirIdent, np)
		}
	}
}

func (b *inotifyBackend) push(ev event.Event) {
	select {
	case b.out <- ev:
	case <-b.done:
	}
}

// flushExpired converts MOVED_FROM entries that never got a partner into a
// Delete, after the grace period.
func (b *inotifyBackend) flushExpired() {
	now := time.Now()
	var dead []event.Event
	var movedOut []string
	var recreated []string
	b.mu.Lock()
	for c, p := range b.pending {
		if now.Sub(p.when) < MoveGrace {
			continue
		}
		ev := p.ev
		_, lerr := os.Lstat(ev.Path)
		if lerr != nil && !errors.Is(lerr, os.ErrNotExist) && !errors.Is(lerr, syscall.ENOTDIR) {
			// EACCES/EIO and friends: we cannot tell whether the name is gone.
			// Keep the pending move and retry on the next tick rather than
			// dropping it without any event.
			continue
		}
		delete(b.pending, c)
		if lerr == nil {
			// The name was recreated inside the grace window. Tell the engine
			// to replace the old target incarnation wholesale; a plain Delete
			// would collapse the newer Create and drop the recreated subtree.
			ev.Op = event.Delete | event.Create
			ev.Recreate = true
			ev.Subtree = ev.IsDir
			dead = append(dead, ev)
			recreated = append(recreated, ev.Path)
			continue
		}
		ev.Op = event.Delete
		dead = append(dead, ev)
		if p.ev.IsDir {
			movedOut = append(movedOut, p.ev.Path)
		}
	}
	b.mu.Unlock()
	// inotify watches follow the inode. A directory moved out of the tree
	// keeps its watches, and those stale paths would keep producing events for
	// a directory the mirror no longer contains (and consume max_user_watches
	// forever). Retire them now that the move is known to have left the tree.
	for _, root := range movedOut {
		b.removeWatchesBelow(root)
	}
	// A recreated name may still have the old inode's descendant watches; keep
	// the new AddWatch mappings and retire only the stale ones.
	for _, root := range recreated {
		b.retireMovedSubtree(root)
	}
	for _, ev := range dead {
		b.push(ev)
	}
}

// retireMovedSubtree removes the watches of a moved-out directory's old
// inodes while leaving those of a directory recreated at the same path.
func (b *inotifyBackend) retireMovedSubtree(root string) {
	type cand struct {
		path string
		wd   int32
		id   fileIdentity
		has  bool
	}
	b.mu.RLock()
	cands := make([]cand, 0, 16)
	for p, wd := range b.dirToWd {
		if p == root || IsUnder(root, p) {
			id, has := b.dirIdent[p]
			cands = append(cands, cand{p, wd, id, has})
		}
	}
	b.mu.RUnlock()
	for _, c := range cands {
		stale := false
		if _, err := os.Lstat(c.path); err != nil {
			stale = true
		} else if id, err := identityOf(c.path); err == nil && c.has && id != c.id {
			stale = true
		}
		if !stale {
			continue
		}
		b.mu.Lock()
		// Re-check identity as well as the wd: AddWatch may have re-registered
		// this path (possibly with a recycled wd) between the snapshot and here.
		if cur, ok := b.dirToWd[c.path]; ok && cur == c.wd {
			id, has := b.dirIdent[c.path]
			if has == c.has && (!c.has || id == c.id) {
				delete(b.dirToWd, c.path)
				delete(b.dirIdent, c.path)
				if cur2, ok := b.wdToDir[c.wd]; ok && cur2 == c.path {
					delete(b.wdToDir, c.wd)
					// Remove while the lock is held so AddWatch cannot recycle
					// this number before the syscall.
					_, _ = syscall.InotifyRmWatch(b.fd, uint32(c.wd))
				}
			}
		}
		b.mu.Unlock()
	}
}

// removeWatchesBelow retires every recorded watch at or below root.
func (b *inotifyBackend) removeWatchesBelow(root string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for path, wd := range b.dirToWd {
		if path == root || IsUnder(root, path) {
			delete(b.dirToWd, path)
			delete(b.dirIdent, path)
			if cur, ok := b.wdToDir[wd]; ok && cur == path {
				delete(b.wdToDir, wd)
				// Remove under the lock so a concurrent AddWatch cannot recycle
				// this wd before the syscall.
				_, _ = syscall.InotifyRmWatch(b.fd, uint32(wd))
			}
			// A stale reverse mapping (wd recycled for another directory) is
			// dropped above, but its live watch must not be removed.
		}
	}
}

func (b *inotifyBackend) Close() error {
	var err error
	b.once.Do(func() {
		close(b.done)
		err = b.f.Close()
	})
	return err
}
