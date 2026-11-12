package aggregate

import (
	"sort"
	"sync"
	"time"

	"gsyncd/internal/event"
)

// Kind classifies an aggregated change.
type Kind uint8

const (
	// KindFile is a regular file change.
	KindFile Kind = iota
	// KindDir is a directory change.
	KindDir
	// KindFull means "re-synchronise everything below Root".
	KindFull
)

// Change is one entry of the collapsed work list.
type Change struct {
	Path string
	// From is the old path for a pure rename.
	From string
	// PrevFrom holds earlier rename sources folded into the same entry (for
	// example two renames onto one destination inside one window). Every one
	// of them has to disappear from the target as well.
	PrevFrom []string
	Op       event.Op
	Kind     Kind
	// At is the first time this path was touched in the window.
	At time.Time
	// Recreate marks a directory that was deleted and created again inside the
	// same window. The target directory has to be removed before it is
	// recreated: the entries that were deleted at the source would otherwise
	// survive in the mirror, because a recreated directory carries no implied
	// removal of its old contents.
	Recreate bool
	// FromCycle marks a rename that is part of a dependency cycle (the common
	// mv a tmp; mv b a; mv tmp b swap). The backend must copy from the source
	// instead of renaming target copies, and must not remove a source that is
	// another cycle member's destination.
	FromCycle bool
	// KeepSource suppresses the post-rename removal of From. It is set for
	// cycle members whose source name is also a destination in the batch.
	KeepSource bool

	// Subtree marks a directory Create that must copy the whole source
	// subtree. It is set by the engine when a rename crosses a filter
	// boundary, where only one side is excluded and the destination has to be
	// treated as new content rather than as a rename of the target copy.
	Subtree bool

	// open records whether the newest thing that happened to this path was a
	// write whose writer has not closed the file yet. It is not derivable
	// from Op: Op accumulates the bits of every event in the window, so a
	// file that was closed and then written again carries both Write and
	// Close. Only the newest event decides, and only the newest event can
	// answer "is this file finished?" - which is the whole question the
	// close_write option asks.
	open bool

	// parent and slot locate this entry inside the directory tree used by
	// dropSubtree. They are bookkeeping, not API.
	parent string
	slot   int
}

// IsDelete reports whether the path disappeared.
func (c Change) IsDelete() bool {
	return c.Op&event.Delete != 0 && c.Op&(event.Create|event.Write) == 0
}

// Stats reports what the aggregator did, for logging and the status file.
type Stats struct {
	Accepted    uint64
	Collapsed   uint64
	Overflows   uint64
	Dropped     uint64
	PeakPending int
}

// Options configures an Aggregator.
type Options struct {
	// MaxPending is the soft cap. When exceeded, the aggregator asks for an
	// immediate flush but keeps accepting events.
	MaxPending int
	// MaxPendingHard is the true ceiling. Past it new events are dropped and
	// the window is marked overflowed, which schedules a full resync. That is
	// the safe direction: memory stays bounded, and the lost events are
	// recovered by the resync rather than by growing until the process is
	// killed.
	MaxPendingHard int
	// Now allows tests to control the clock.
	Now func() time.Time
}

// dirNode is one directory in the pending-change tree. A node exists only
// while something below it is pending; the tree is pruned bottom-up as entries
// are removed, so its size tracks the window, not the filesystem.
//
// The tree exists for one reason: absorbing the pending children of a deleted
// directory must cost the size of that subtree, not the size of the whole
// window. Without it, "rm -rf" over N pending files costs N x (window size),
// which is quadratic - measured at 58 s of CPU for a 200k window, all of it
// while holding the aggregator lock, so the watcher cannot drain the kernel
// queue and overflows into a full resync (issue #695).
type dirNode struct {
	// children holds the pending paths directly below this directory: entry
	// paths that live in Aggregator.index, and subdirectory paths that live in
	// Aggregator.nodes.
	children []string
	// childSet mirrors children for O(1) membership. A path can be both an
	// entry and a node (a directory with a pending change and pending
	// children); keeping one link for it, removed only when neither remains,
	// prevents the duplicate links that used to corrupt slots.
	childSet map[string]int
	parent   string
	slot     int
}

// maxPrevFrom bounds the stale names folded into one change. Past this point
// the window is marked overflowed: the names are recovered by the full resync,
// which is cheaper than letting a hot atomic-replace path grow without limit.
const maxPrevFrom = 512

// Aggregator collapses events. It is safe for concurrent use.
type Aggregator struct {
	mu    sync.Mutex
	opts  Options
	index map[string]int
	// fromIdx maps a rename source to one entry (by item position). Readd
	// needs to find the newer consumer of a failed change without scanning a
	// window that can hold millions of entries under the lock.
	fromIdx  map[string]int
	nodes    map[string]*dirNode
	items    []Change
	pending  int
	stats    Stats
	overflow bool
	// urgent marks "the window is at its size cap, flush it now". Unlike
	// overflow it does not mean events were lost, so the engine must not turn
	// it into a wholesale resync.
	urgent bool
	// oldest is the earliest At of anything appended since the last Flush.
	//
	// It exists to bound staleness, not to batch better. The engine flushes
	// when the tree has been quiet for delay, so a file that is written to
	// without pause - a log, a WAL, a database file - never lets that
	// condition come true and is never copied at all (issues #77, #458). Since
	// Change.At is the first time a path was touched inside the window, the
	// minimum of those timestamps says how long the oldest pending change has
	// been waiting, and the engine can use it to force a flush on a path that
	// never goes quiet.
	//
	// Entries removed from the window (collapsed create+delete, absorbed
	// subtree) leave this value behind, so it can be older than the true
	// minimum. That direction is safe: it flushes a little early, never late.
	oldest time.Time
	// openFiles counts the entries in items whose open flag is set. It is
	// maintained incrementally because the close_write flush decision asks
	// for it on every tick, and counting by scanning the window there would
	// be O(window) per tick - the same shape of cost that made the original
	// unusable on a large tree.
	openFiles int
}

// New creates an aggregator.
func New(opts Options) *Aggregator {
	if opts.MaxPending <= 0 {
		opts.MaxPending = 500_000
	}
	if opts.MaxPendingHard <= 0 {
		opts.MaxPendingHard = opts.MaxPending * 4
	}
	if opts.MaxPendingHard < opts.MaxPending {
		opts.MaxPendingHard = opts.MaxPending
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Aggregator{
		opts:    opts,
		index:   make(map[string]int, 1024),
		fromIdx: make(map[string]int, 128),
		nodes:   make(map[string]*dirNode, 256),
	}
}

// Add folds one event into the window.
func (a *Aggregator) Add(ev event.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Accepted++

	if ev.Op == event.Overflow {
		a.stats.Overflows++
		a.overflow = true
		return
	}

	// A directory delete makes every pending child irrelevant: the backend
	// removes the subtree in one go.
	if ev.IsDir && ev.Op&event.Delete != 0 && ev.Op&event.Create == 0 && !ev.Recreate {
		a.dropSubtree(ev.Path)
	}

	if ev.Op&event.Move != 0 && ev.From != "" {
		a.applyMove(ev)
		return
	}

	kind := KindFile
	if ev.IsDir {
		kind = KindDir
	}
	a.upsert(Change{Path: ev.Path, Op: ev.Op, Kind: kind, At: ev.At,
		Subtree: ev.Subtree, Recreate: ev.Recreate})
}

// upsert merges a change into the existing entry for the same path.
func (a *Aggregator) upsert(c Change) { a.upsertKnown(c, false) }

// Readd puts a change back into the window after its batch failed, keeping the
// open state that was recorded when the batch was taken.
//
// Going through Add would lose it: the re-sent change carries the Op bits but
// not the answer to "was this file still open", and a Write without Close reads
// as open again. That is not merely imprecise - a close that arrived while the
// retry was running would be undone, and a close_write sync would then wait for
// max_hold (an hour by default) before retrying a path that is already finished.
func (a *Aggregator) Readd(c Change) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.readdLocked(c, a.snapshotFromIdx())
}

// ReaddBatch re-adds one failed batch in a single critical section. Folding is
// limited to consumers that existed before the batch: two changes from the
// same failed batch are not "newer" than each other, even when Readd is called
// in Path-sorted order ("mv b a" plus a recreated b is the common case).
func (a *Aggregator) ReaddBatch(changes []Change) {
	a.mu.Lock()
	defer a.mu.Unlock()
	pre := a.snapshotFromIdx()
	for _, c := range changes {
		a.readdLocked(c, pre)
	}
}

func (a *Aggregator) snapshotFromIdx() map[string]int {
	pre := make(map[string]int, len(a.fromIdx))
	for k, v := range a.fromIdx {
		pre[k] = v
	}
	return pre
}

func (a *Aggregator) readdLocked(c Change, pre map[string]int) {
	if c.Path != "" {
		// A newer change may already consume c's path: source did f<-e, then
		// c<-f while the failed batch (f<-e) was retrying. Folding the stale
		// link/content into the consumer gives the right final source; inserting
		// both leaves a transient path that no longer exists and can be
		// resurrected by rename ordering.
		if i, ok := pre[c.Path]; ok && i < len(a.items) {
			t := &a.items[i]
			if t.From == c.Path && t.Path != c.Path {
				if c.Op&event.Move != 0 && c.From != "" && c.From != c.Path {
					if j, ok := a.fromIdx[t.From]; ok && j == i {
						delete(a.fromIdx, t.From)
					}
					t.From = c.From
					a.fromIdx[c.From] = i
					t.PrevFrom = a.addPrevFrom(t.PrevFrom, c.Path)
				} else if c.Op&event.Delete != 0 && c.Op&(event.Create|event.Write|event.Attrib) == 0 {
					// Stale delete: the target copy must be replaced from the
					// live source, not renamed.
					t.FromCycle = true
				}
				t.Op |= c.Op & (event.Create | event.Write | event.Attrib)
				if c.Kind == KindDir {
					t.Kind = KindDir
				}
				if c.Recreate {
					t.Recreate = true
				}
				if c.Subtree {
					t.Subtree = true
				}
				for _, pf := range c.PrevFrom {
					t.PrevFrom = a.addPrevFrom(t.PrevFrom, pf)
				}
				if c.At.Before(t.At) {
					t.At = c.At
				}
				a.stats.Collapsed++
				return
			}
		}
	}
	a.upsertKnown(c, true)
}

// upsertKnown is upsert with the option of trusting c.open over the Op bits.
// keepOpen is only set by Readd; ordinary events carry the truth in their mask.
func (a *Aggregator) upsertKnown(c Change, keepOpen bool) {
	if i, ok := a.index[c.Path]; ok {
		old := &a.items[i]
		if keepOpen {
			// Readd: the entry already in the window describes a newer state
			// than the failed batch, so its Op must win. Merging the stale
			// bits used to let an old Delete collapse a newer Create (or OR a
			// Delete into a newer Move|Write), leaving the path missing until
			// a restart.
			//
			// The rename source still has to be carried: the failed batch may
			// never have run, in which case the target still holds the old
			// name. Dropping From left a directory rename as a bare Attrib,
			// which only does MkdirAll and never copies the subtree.
			if c.From != "" {
				if old.Op&event.Delete != 0 && old.Op&(event.Create|event.Write) == 0 {
					// Newer state is a delete; the old rename source has to
					// disappear too, but there is nothing to rename. Let the
					// batch-level PrevFrom cleanup remove it.
					old.PrevFrom = a.addPrevFrom(old.PrevFrom, c.From)
				} else if old.From == "" {
					old.From = c.From
					a.fromIdx[c.From] = i
				} else if old.From != c.From && c.From != old.Path {
					old.PrevFrom = a.addPrevFrom(old.PrevFrom, c.From)
				}
			}
			for _, pf := range c.PrevFrom {
				old.PrevFrom = a.addPrevFrom(old.PrevFrom, pf)
			}
			// The failed batch may contain the only record that content was
			// changed. Merge it unless the newer state is a pure Delete (which
			// must not be weakened back into a write).
			if old.Op&event.Delete == 0 || old.Op&(event.Create|event.Write) != 0 {
				old.Op |= c.Op & (event.Create | event.Write | event.Attrib)
				// Deliberately do not adopt c.open: Readd trusts the newer
				// entry's open state (a close during the retry must stick).
			}
			// A stale directory Delete means the target may still contain the
			// old incarnation's children; only Recreate removes them.
			if c.Op&event.Delete != 0 &&
				(c.Kind == KindDir || old.Kind == KindDir) &&
				old.Op&(event.Create|event.Write|event.Attrib) != 0 {
				old.Recreate = true
			}
			if c.Recreate {
				old.Recreate = true
			}
			if c.Subtree {
				old.Subtree = true
			}
			if c.At.Before(old.At) {
				old.At = c.At
			}
			a.stats.Collapsed++
			return
		}
		merged := old.Op | c.Op
		// delete then create == a modify of whatever is there now. For a file
		// that is an atomic replace; for a directory it is not - the target
		// still holds the old entries, so the delete has to be preserved as a
		// recreate rather than folded away.
		if old.Op&event.Delete != 0 && c.Op&(event.Create|event.Write) != 0 {
			merged = (merged &^ event.Delete) | event.Write
			if c.Kind == KindDir || old.Kind == KindDir {
				merged &^= event.Write
				if merged&event.Create == 0 {
					merged |= event.Create
				}
				old.Recreate = true
			}
		}
		// create then delete == nothing happened. A Delete that also carries
		// Create is the "old incarnation removed, name recreated" signal and
		// must survive to set Recreate.
		if old.Op&event.Create != 0 && c.Op&event.Delete != 0 && c.Op&event.Create == 0 && old.From == "" {
			a.removeAt(i)
			a.stats.Collapsed++
			return
		}
		a.stats.Collapsed++
		old.Op = merged
		if c.Kind == KindDir {
			old.Kind = KindDir
		}
		// A subtree copy is never weakened by a later event: once the window
		// decided that a directory has to be mirrored wholesale, folding in a
		// plain Create must not reduce that to "create the entry only".
		if c.Subtree {
			old.Subtree = true
		}
		if c.Recreate {
			old.Recreate = true
		}
		for _, pf := range c.PrevFrom {
			old.PrevFrom = a.addPrevFrom(old.PrevFrom, pf)
		}
		if c.From != "" {
			// A newer move landing on this path means it exists in the final
			// state, so a pending Delete is superseded. Earlier rename
			// sources have to be deleted too.
			old.Op &^= event.Delete
			if old.From != "" && old.From != c.From {
				old.PrevFrom = a.addPrevFrom(old.PrevFrom, old.From)
			}
			if old.From != c.From {
				if old.From != "" {
					if j, ok := a.fromIdx[old.From]; ok && j == i {
						delete(a.fromIdx, old.From)
					}
				}
				old.From = c.From
				if _, ok := a.fromIdx[c.From]; !ok {
					a.fromIdx[c.From] = i
				}
			}
		}
		if c.At.Before(old.At) {
			old.At = c.At
		}
		open := nextOpen(old.open, c)
		if c.open {
			// c.open is only ever pre-set by applyMove, which carries the
			// flag of a file that was renamed onto this path. A rename does
			// not close anything, so if either side is open the merged entry
			// is open: "mv a b" while a is still being written must not
			// release the window just because b happened to have a finished
			// pending entry.
			open = true
		}
		a.setOpen(i, open)
		return
	}
	if c.From == "" && c.Op == 0 {
		return
	}
	a.appendItem(c)
}

// nextOpen reports whether an entry should count as still being written after c
// is folded into it. wasOpen is the entry's previous state; a brand new entry
// passes false.
//
// Only the newest event can answer the question. Op accumulates bits across the
// whole window, so a file that was closed and then written again carries both
// Write and Close, and "has a Close bit" would call it finished while it is
// open again.
func nextOpen(wasOpen bool, c Change) bool {
	if c.Kind == KindDir {
		// A directory is never opened for writing, so no IN_CLOSE_WRITE will
		// ever arrive for it. Counting one as open would block the window
		// until max_hold.
		return false
	}
	switch {
	case c.Op&event.Close != 0:
		// The last writer closed the file: what it wrote is complete.
		return false
	case c.Op&event.Write != 0:
		// Written and not closed yet.
		//
		// Create alone is deliberately not enough. A create can mean a file
		// that will be filled next (and then a Write follows, which marks it
		// open before the delay can elapse), but it also means a symlink, a
		// fifo, a device node, or a file moved into place by an atomic
		// rename - none of which ever get an IN_CLOSE_WRITE. Counting those
		// as open would pin the whole window, every other file included,
		// until max_hold expired, so one "ln -s" in the tree would stall the
		// sync for an hour.
		return true
	case c.Op&event.Delete != 0:
		// Removed. No close event can arrive for a path that is gone, so
		// keeping the entry open would hold the window until max_hold for
		// nothing.
		return false
	}
	// Metadata only: a chmod says nothing about writers, so it must not
	// release a file that is genuinely still open.
	return wasOpen
}

// setOpen updates one entry's open flag and keeps openFiles in step.
func (a *Aggregator) setOpen(i int, open bool) {
	c := &a.items[i]
	if c.open == open {
		return
	}
	c.open = open
	if open {
		a.openFiles++
		return
	}
	a.openFiles--
}

// appendItem adds a brand new entry and links it into the directory tree.
func (a *Aggregator) appendItem(c Change) {
	if a.pending >= a.opts.MaxPendingHard {
		// The window is already at its hard ceiling. Keeping this event would
		// trade bounded memory for an OOM kill; an overflow event makes the
		// engine re-synchronise the whole root, which recovers it.
		a.overflow = true
		a.urgent = true
		a.stats.Dropped++
		return
	}
	c.parent = parentOf(c.Path)
	a.ensureDir(c.parent)
	c.slot = a.linkChild(c.parent, c.Path)

	// c.open is pre-set only by applyMove (a rename carries the flag: renaming
	// an open file does not close it) and by Readd (a failed batch keeps the
	// state it was taken with). Both mean "this file was open when we last
	// knew", which no accumulated mask can contradict - a retried entry whose
	// window also contains an earlier Close would otherwise read as finished
	// and get copied half-written.
	if !c.open {
		c.open = nextOpen(false, c)
	}
	if c.open {
		a.openFiles++
	}
	a.items = append(a.items, c)
	idx := len(a.items) - 1
	a.index[c.Path] = idx
	if c.From != "" {
		// First consumer wins: a later rename with the same From implies the
		// name was recreated in between, and a stale Readd belongs to the
		// first incarnation.
		if _, ok := a.fromIdx[c.From]; !ok {
			a.fromIdx[c.From] = idx
		}
	}
	if a.oldest.IsZero() || (!c.At.IsZero() && c.At.Before(a.oldest)) {
		a.oldest = c.At
	}
	a.pending = len(a.items)
	if a.pending > a.stats.PeakPending {
		a.stats.PeakPending = a.pending
	}
	if a.pending > a.opts.MaxPending {
		// The window reached its cap. Nothing was lost, we just stopped
		// waiting for the delay to elapse; the engine flushes immediately.
		// The original capped its queue at 1000 events and silently dropped
		// everything beyond that (issue #202).
		a.urgent = true
	}
}

// ensureDir makes sure a node exists for dir and for every ancestor up to the
// top of the pending tree. It stops as soon as it meets an existing node,
// because an existing node already implies its whole chain.
//
// The top is the path whose parent is itself - "/" on Unix, "C:" and the
// empty string on Windows - and it is deliberately not linked into anything.
// parentOf returns the input for those, so linking would put the node into its
// own child list; a self-referential node is never pruned, never counted and
// turns any later subtree walk into an infinite recursion.
func (a *Aggregator) ensureDir(dir string) {
	if _, ok := a.nodes[dir]; ok {
		return
	}
	up := parentOf(dir)
	n := &dirNode{parent: up, childSet: make(map[string]int)}
	a.nodes[dir] = n
	if up == dir {
		n.parent = ""
		return
	}
	a.ensureDir(up)
	if a.nodes[up] == nil {
		return
	}
	n.slot = a.linkChild(up, dir)
}

// linkChild adds child to parent once and returns its slot. An entry and an
// internal node for the same path share one link; the link is removed only
// when both are gone (see unlink).
func (a *Aggregator) linkChild(parent, child string) int {
	node := a.nodes[parent]
	if node == nil {
		return -1
	}
	if node.childSet == nil {
		node.childSet = make(map[string]int, len(node.children))
		for i, c := range node.children {
			node.childSet[c] = i
		}
	}
	if slot, ok := node.childSet[child]; ok {
		return slot
	}
	slot := len(node.children)
	node.children = append(node.children, child)
	node.childSet[child] = slot
	return slot
}

func (a *Aggregator) applyMove(ev event.Event) {
	// Renaming a file does not close it, so a file that was still being
	// written keeps that state under its new name. Losing the flag here would
	// copy a half-written file exactly once - the case close_write exists to
	// prevent - and it would be the hardest one to reproduce, since it needs
	// a rename during a write.
	var wasOpen bool
	// carried keeps a pending content change attached to the file through the
	// rename. Without it "write, then mv" produces a Move-only change: the
	// target rename would move the old, stale copy to the new name and the
	// write would never reach the mirror.
	var carried event.Op
	// recreate survives a rename too: a directory that was deleted and
	// recreated before being moved must not be applied as a rename of the old
	// target copy, which still holds the deleted children.
	var recreate bool
	// from is the root of a rename chain. Windows-style "mv a b; mv b c" used
	// to become "c From b", which renamed the stale target copy of b to c and
	// left a behind. Folding the chain back to a gives the backend the path
	// that actually still exists in the target, and prev collects the
	// intermediate names that must be deleted.
	from := ev.From
	var prev []string
	if i, ok := a.index[ev.From]; ok {
		old := &a.items[i]
		if ev.IsDir {
			// A directory's pending Attrib/Create change has to survive the
			// rename. followRename removes the old entry, so capture it first.
			carried = old.Op & (event.Attrib | event.Create)
		} else {
			carried = old.Op & (event.Create | event.Write | event.Attrib)
			wasOpen = old.open
		}
		recreate = old.Recreate
		if old.From != "" {
			from = old.From
			prev = a.addPrevFrom(prev, old.Path)
		}
		for _, pf := range old.PrevFrom {
			prev = a.addPrevFrom(prev, pf)
		}
		if !ev.IsDir {
			a.removeAt(i)
		}
	}
	if ev.IsDir {
		a.followRename(ev.From, ev.Path)
	}
	// Renaming a directory renames everything below it, so the kind has to
	// survive: a KindFile entry loses the dropSubtree shortcut for every later
	// delete under that path.
	kind := KindFile
	if ev.IsDir {
		kind = KindDir
	}
	if from == ev.Path {
		// "mv a b; mv b a" folds back onto itself. Emitting Move with
		// From==Path made the backend copy the file and then remove its own
		// destination (or rename a path onto itself). Emit an ordinary
		// content/create change; PrevFrom still cleans up the intermediate name.
		if ev.IsDir {
			// The target directory may already hold content (the readded
			// rename may have been partially applied), so build it from the
			// live source instead of assuming the target copy is intact.
			a.upsert(Change{Path: ev.Path, Op: event.Create | carried, Kind: KindDir, At: ev.At,
				Recreate: recreate, Subtree: true, PrevFrom: prev})
		} else {
			a.upsert(Change{Path: ev.Path, Op: event.Write | carried, Kind: KindFile, At: ev.At, PrevFrom: prev, open: wasOpen})
		}
		return
	}
	if ev.Op&event.Create != 0 && ev.From == "" {
		a.upsert(Change{Path: ev.Path, Op: event.Create | carried, Kind: kind, At: ev.At, open: wasOpen})
		return
	}
	a.upsert(Change{
		Path: ev.Path, From: from, PrevFrom: prev,
		Op: event.Move | carried, Kind: kind, At: ev.At,
		Recreate: recreate, open: wasOpen,
	})
}

// followRename moves pending children of a renamed directory to their new
// names.
//
// A directory rename on the target only moves the directory as it exists
// there. It does not carry pending content changes made at the source, so
// every pending entry below the old name - create, write, delete or a nested
// rename - has to be retargeted at the new prefix. Dropping the non-deletes
// (the old behaviour) silently lost writes and renames that had not been
// transferred yet, leaving stale content under the new directory name.
func (a *Aggregator) followRename(from, to string) {
	// Only the renamed subtree can hold affected entries, so this walks that
	// subtree instead of the whole window. Scanning the window made "rename N
	// directories" cost N x (window size), the same quadratic shape the delete
	// path had.
	victims := a.subtreePaths(from, nil)
	var retarget []Change
	for _, p := range victims {
		i, ok := a.index[p]
		if !ok {
			continue
		}
		c := a.items[i]
		if p != from {
			suffix := p[len(from):]
			c.Path = to + suffix
			if c.From != "" && underPath(from, c.From) {
				c.From = to + c.From[len(from):]
			}
			if len(c.PrevFrom) > 0 {
				// The target directory rename moves the whole old subtree, so
				// stale names below the old prefix now live below the new one.
				// Leaving them under the old prefix made removePrevFrom delete
				// a path that no longer exists and leave the real stale file.
				var rewritten []string
				for _, pf := range c.PrevFrom {
					if underPath(from, pf) {
						pf = to + pf[len(from):]
					}
					rewritten = a.addPrevFrom(rewritten, pf)
				}
				c.PrevFrom = rewritten
			}
			retarget = append(retarget, c)
		}
		// Look the index up again: removeAt swaps the tail into i.
		if j, ok := a.index[p]; ok {
			a.removeAt(j)
		}
	}
	for _, c := range retarget {
		a.upsert(c)
	}
}

// addPrevFrom adds p once, preserving the order earlier renames were seen.
// The list is bounded: a hot atomic-replace path can otherwise grow it without
// limit (the item count stays at one, so MaxPending never fires), and the
// linear dedupe would become O(n^2) under the aggregator lock. Past the cap
// the window is marked overflowed so a full resync recovers the names.
func (a *Aggregator) addPrevFrom(list []string, p string) []string {
	if p == "" {
		return list
	}
	if len(list) >= maxPrevFrom {
		a.overflow = true
		a.urgent = true
		return list
	}
	for _, x := range list {
		if x == p {
			return list
		}
	}
	return append(list, p)
}

// underPath reports whether p is root itself or lives below it.
func underPath(root, p string) bool {
	if p == root {
		return true
	}
	if len(p) <= len(root) || p[:len(root)] != root {
		return false
	}
	return p[len(root)] == '/' || p[len(root)] == '\\'
}

// dropSubtree removes every pending entry at or below root, walking only that
// subtree.
func (a *Aggregator) dropSubtree(root string) {
	// The directory itself may be pending as an entry (a create, or a delete
	// that arrived earlier); it is one of the victims.
	victims := a.subtreePaths(root, nil)
	dropped := 0
	for _, p := range victims {
		if i, ok := a.index[p]; ok {
			a.removeAt(i)
			dropped++
		}
	}
	if dropped == 0 {
		return
	}
	// Every absorbed child is one event that will not need its own transfer.
	a.stats.Collapsed += uint64(dropped)
}

// subtreePaths collects root and every pending entry below it.
//
// A path can be both an entry (it has a pending Create/Attrib) and an internal
// node (something below it is pending). Returning at the entry used to skip
// the node's children, so a rename of the parent dropped pending writes below
// such an intermediate directory. The seen set also absorbs the duplicate
// child links older windows could contain.
func (a *Aggregator) subtreePaths(dir string, out []string) []string {
	return a.subtreePathsInto(dir, out,
		make(map[string]bool, len(out)+16), // emitted
		make(map[string]bool, len(out)+16)) // node expanded
}

func (a *Aggregator) subtreePathsInto(dir string, out []string, seen, expanded map[string]bool) []string {
	if !seen[dir] {
		seen[dir] = true
		out = append(out, dir)
	}
	if expanded[dir] {
		return out
	}
	expanded[dir] = true
	node := a.nodes[dir]
	if node == nil {
		return out
	}
	for _, child := range node.children {
		if _, isEntry := a.index[child]; isEntry && !seen[child] {
			seen[child] = true
			out = append(out, child)
		}
		if _, isNode := a.nodes[child]; isNode {
			out = a.subtreePathsInto(child, out, seen, expanded)
		}
	}
	return out
}

func (a *Aggregator) removeAt(i int) {
	c := a.items[i]
	if c.open {
		a.openFiles--
	}

	if c.From != "" {
		if j, ok := a.fromIdx[c.From]; ok && j == i {
			delete(a.fromIdx, c.From)
		}
	}
	last := len(a.items) - 1
	delete(a.index, c.Path)
	moved := a.items[last]
	a.items[i] = moved
	a.items = a.items[:last]
	if i < len(a.items) {
		// The swap only moves the entry; its position inside its parent's
		// child list is untouched, so the slot stays valid.
		a.index[moved.Path] = i
		if moved.From != "" {
			// Preserve first-consumer-wins: only retarget the index when it
			// pointed at the moved item's old slot (or was removed with c).
			if j, ok := a.fromIdx[moved.From]; !ok || j == last {
				a.fromIdx[moved.From] = i
			}
		}
	}
	a.pending = len(a.items)
	// The index entry is already gone, so unlink can tell whether a node for
	// the same path still needs the shared child link.
	a.unlink(c.parent, c.Path, c.slot)
}

// unlink removes path from the child list of dir unless an entry or an
// internal node still owns that child. Entries and nodes share one child
// link; removing it for one owner would make the other unreachable from
// dropSubtree/followRename and could corrupt the slot bookkeeping.
func (a *Aggregator) unlink(dir, path string, slot int) {
	node := a.nodes[dir]
	if node == nil {
		return
	}
	if _, ok := a.index[path]; ok {
		return
	}
	if _, ok := a.nodes[path]; ok {
		return
	}
	if node.childSet == nil {
		node.childSet = make(map[string]int, len(node.children))
		for i, c := range node.children {
			node.childSet[c] = i
		}
	}
	s, ok := node.childSet[path]
	if !ok {
		return
	}
	if slot >= 0 && slot < len(node.children) && node.children[slot] == path {
		s = slot
	}
	last := len(node.children) - 1
	node.children[s] = node.children[last]
	node.children = node.children[:last]
	delete(node.childSet, path)
	if s < len(node.children) {
		moved := node.children[s]
		node.childSet[moved] = s
		if mi, ok := a.index[moved]; ok {
			a.items[mi].slot = s
		} else if mn, ok := a.nodes[moved]; ok {
			mn.slot = s
		}
	}
	if len(node.children) == 0 {
		delete(a.nodes, dir)
		a.unlink(node.parent, dir, node.slot)
	}
}

// Len reports the number of pending changes.
func (a *Aggregator) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.items)
}

// Overflowed reports whether events were lost since the last Flush.
func (a *Aggregator) Overflowed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.overflow
}

// Urgent reports whether the window reached its size cap and should be
// flushed without waiting for the delay to elapse.
func (a *Aggregator) Urgent() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.urgent
}

// OpenFiles reports how many pending changes belong to files whose newest
// event was a write without a matching close.
//
// It is O(1) on purpose: the close_write flush decision asks on every tick, and
// counting by walking the window there would turn a 5 Hz question into an
// O(window) scan - the shape of cost that made the original's per-event queue
// check quadratic on a large tree.
func (a *Aggregator) OpenFiles() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.openFiles
}

// Overdue reports whether the oldest pending change has been waiting for at
// least hold.
//
// This is the escape hatch for paths that are never quiet. Waiting for a
// silence that never comes is not a delay, it is a permanent omission, so a
// writer that keeps a file open gets a copy every hold instead of none at all.
// A hold of zero disables the bound.
func (a *Aggregator) Overdue(now time.Time, hold time.Duration) bool {
	if hold <= 0 {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.items) == 0 || a.oldest.IsZero() {
		return false
	}
	return now.Sub(a.oldest) >= hold
}

// Stats returns a snapshot of counters.
func (a *Aggregator) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

// Flush returns the current window and resets it. The returned slice is owned
// by the caller.
func (a *Aggregator) Flush() ([]Change, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	items := a.items
	overflow := a.overflow
	a.items = nil
	a.index = make(map[string]int, 1024)
	a.fromIdx = make(map[string]int, 128)
	a.nodes = make(map[string]*dirNode, 256)
	a.pending = 0
	a.overflow = false
	a.urgent = false
	a.oldest = time.Time{}
	// Everything left the window, so nothing in it is pending on a writer any
	// more. A path that is still open gets a fresh write event the next time
	// the writer touches it, which re-arms the count.
	a.openFiles = 0
	// Stable ordering keeps the generated rsync argument list reproducible.
	sort.SliceStable(items, func(i, j int) bool { return items[i].Path < items[j].Path })
	return items, overflow
}

// MarkOverflow records that the caller lost events and a full resync of this
// aggregator's tree is required. The engine calls it when the watcher reports a
// queue overflow, so a lost inotify buffer turns into one full transfer instead
// of a silently incomplete mirror.
func (a *Aggregator) MarkOverflow() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Overflows++
	a.overflow = true
}

// parentOf returns the directory part of p, accepting both separators so tests
// can use portable literals. It does not clean the path: entries arrive already
// clean from the watcher, and comparing raw strings keeps the index consistent.
func parentOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			if i == 0 {
				return p[:1]
			}
			return p[:i]
		}
	}
	return ""
}
