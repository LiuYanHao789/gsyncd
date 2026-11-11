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
	Op   event.Op
	Kind Kind
	// At is the first time this path was touched in the window.
	At time.Time
	// Recreate marks a directory that was deleted and created again inside the
	// same window. The target directory has to be removed before it is
	// recreated: the entries that were deleted at the source would otherwise
	// survive in the mirror, because a recreated directory carries no implied
	// removal of its old contents.
	Recreate bool

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
	parent   string
	slot     int
}

// Aggregator collapses events. It is safe for concurrent use.
type Aggregator struct {
	mu       sync.Mutex
	opts     Options
	index    map[string]int
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
		opts:  opts,
		index: make(map[string]int, 1024),
		nodes: make(map[string]*dirNode, 256),
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
	if ev.IsDir && ev.Op&event.Delete != 0 {
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
	a.upsert(Change{Path: ev.Path, Op: ev.Op, Kind: kind, At: ev.At, Subtree: ev.Subtree})
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
	a.upsertKnown(c, true)
}

// upsertKnown is upsert with the option of trusting c.open over the Op bits.
// keepOpen is only set by Readd; ordinary events carry the truth in their mask.
func (a *Aggregator) upsertKnown(c Change, keepOpen bool) {
	if i, ok := a.index[c.Path]; ok {
		old := &a.items[i]
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
		// create then delete == nothing happened.
		if old.Op&event.Create != 0 && c.Op&event.Delete != 0 && old.From == "" {
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
		if c.From != "" {
			if keepOpen {
				// This is a failed batch being re-added: it predates anything
				// already in the window, so it must not clear a newer Delete.
				// Its old name still has to be cleaned up.
				if old.From == "" {
					old.From = c.From
				} else if old.From != c.From {
					old.PrevFrom = appendFrom(old.PrevFrom, c.From)
				}
			} else {
				// A newer move landing on this path means it exists in the
				// final state, so a pending Delete is superseded. Earlier
				// rename sources have to be deleted too.
				old.Op &^= event.Delete
				if old.From != "" && old.From != c.From {
					old.PrevFrom = appendFrom(old.PrevFrom, old.From)
				}
				old.From = c.From
			}
		}
		if c.At.Before(old.At) {
			old.At = c.At
		}
		if keepOpen {
			// The entry already knows the newest answer: events that arrived
			// between the failed flush and this retry were folded in as they
			// happened, so leave its flag alone.
			return
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
	node := a.nodes[c.parent]
	c.slot = len(node.children)
	node.children = append(node.children, c.Path)

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
	a.index[c.Path] = len(a.items) - 1
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
	n := &dirNode{parent: up}
	a.nodes[dir] = n
	if up == dir {
		n.parent = ""
		return
	}
	a.ensureDir(up)
	pn := a.nodes[up]
	if pn == nil {
		return
	}
	n.slot = len(pn.children)
	pn.children = append(pn.children, dir)
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
	if ev.IsDir {
		// A directory's pending Attrib/Create change has to survive the
		// rename. followRename removes the old entry, so capture it first.
		if i, ok := a.index[ev.From]; ok {
			carried = a.items[i].Op & (event.Attrib | event.Create)
			recreate = a.items[i].Recreate
		}
		a.followRename(ev.From, ev.Path)
	} else if i, ok := a.index[ev.From]; ok {
		carried = a.items[i].Op & (event.Create | event.Write | event.Attrib)
		recreate = a.items[i].Recreate
		wasOpen = a.items[i].open
		a.removeAt(i)
	}
	// Renaming a directory renames everything below it, so the kind has to
	// survive: a KindFile entry loses the dropSubtree shortcut for every later
	// delete under that path.
	kind := KindFile
	if ev.IsDir {
		kind = KindDir
	}
	if ev.Op&event.Create != 0 && ev.From == "" {
		a.upsert(Change{Path: ev.Path, Op: event.Create | carried, Kind: kind, At: ev.At, open: wasOpen})
		return
	}
	a.upsert(Change{Path: ev.Path, From: ev.From, Op: event.Move | carried, Kind: kind, At: ev.At, Recreate: recreate, open: wasOpen})
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

// appendFrom adds p once, preserving the order earlier renames were seen.
func appendFrom(list []string, p string) []string {
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
func (a *Aggregator) subtreePaths(dir string, out []string) []string {
	out = append(out, dir)
	node := a.nodes[dir]
	if node == nil {
		return out
	}
	for _, child := range node.children {
		if _, isEntry := a.index[child]; isEntry {
			out = append(out, child)
			continue
		}
		out = a.subtreePaths(child, out)
	}
	return out
}

func (a *Aggregator) removeAt(i int) {
	c := a.items[i]
	if c.open {
		a.openFiles--
	}
	a.unlink(c.parent, c.Path, c.slot)

	last := len(a.items) - 1
	delete(a.index, c.Path)
	a.items[i] = a.items[last]
	a.items = a.items[:last]
	if i < len(a.items) {
		// The swap only moves the entry; its position inside its parent's
		// child list is untouched, so the slot stays valid.
		a.index[a.items[i].Path] = i
	}
	a.pending = len(a.items)
}

// unlink removes path from the child list of dir, keeping the swapped-in
// child's slot accurate whether that child is an entry or a subdirectory.
func (a *Aggregator) unlink(dir, path string, slot int) {
	node := a.nodes[dir]
	if node == nil || slot >= len(node.children) || node.children[slot] != path {
		return
	}
	last := len(node.children) - 1
	node.children[slot] = node.children[last]
	node.children = node.children[:last]
	if slot < len(node.children) {
		moved := node.children[slot]
		if mi, ok := a.index[moved]; ok {
			a.items[mi].slot = slot
		} else if mn, ok := a.nodes[moved]; ok {
			mn.slot = slot
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
