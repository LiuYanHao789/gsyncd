// Package event defines the filesystem event vocabulary shared by the
// watcher, the aggregator and the sync engine.
package event

import (
	"strings"
	"time"
)

// Op is a bitmask of things that happened to a path since the last flush.
type Op uint8

const (
	// Create means the path did not exist in the last known state.
	Create Op = 1 << iota
	// Write means file content changed.
	Write
	// Attrib means metadata (mode/owner/mtime) changed.
	Attrib
	// Delete means the path is gone.
	Delete
	// Move means the path was renamed. From/To are set on the event.
	Move
	// Overflow means the watcher lost events and the tree must be
	// re-synchronised wholesale.
	Overflow
	// RootGone means the watch on a sync root itself disappeared: the
	// directory was removed, unmounted, or the kernel dropped the watch.
	// Path carries the root. Unlike Overflow it names exactly one tree, so
	// only that sync has to be re-transferred.
	RootGone
	// Close means the last writer closed the file (inotify IN_CLOSE_WRITE).
	// It is always set together with Write. The aggregator uses it to tell
	// "still being written" from "finished writing": a path whose newest
	// event carries Write without Close is treated as open by the
	// close_write sync option, and that option holds the flush back until
	// every such path has been closed (issue #458).
	Close
)

// String renders an op mask in a stable, log friendly form.
//
// Combinations are rendered as "Create|Write" rather than "Op(?)": the backfill
// path deliberately synthesises that pair for files that exist inside a
// directory whose watch was just installed, and seeing "Op(?)" in a log while
// chasing a missing file is exactly the wrong moment to lose information.
func (o Op) String() string {
	if o == 0 {
		return "None"
	}
	var b strings.Builder
	for _, bit := range [...]struct {
		op   Op
		name string
	}{
		{Create, "Create"}, {Write, "Write"}, {Attrib, "Attrib"},
		{Delete, "Delete"}, {Move, "Move"}, {Overflow, "Overflow"},
		{RootGone, "RootGone"}, {Close, "Close"},
	} {
		if o&bit.op == 0 {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('|')
		}
		b.WriteString(bit.name)
	}
	if b.Len() == 0 {
		return "Op(?)"
	}
	return b.String()
}

// Event is a single notification from a watcher.
//
// Path is always absolute and cleaned. From is only set when Op == Move and
// carries the previous absolute path.
type Event struct {
	Path string
	From string
	Op   Op
	// IsDir reports whether the subject is a directory. It is best effort:
	// delete events for directories are recognised by the watcher when it
	// still has the watch registered.
	IsDir bool
	// Cookie is the kernel rename cookie (inotify) used to pair
	// MOVED_FROM/MOVED_TO. Zero when not applicable.
	Cookie uint32
	// At is the wall clock time the watcher observed the event.
	At time.Time
}
