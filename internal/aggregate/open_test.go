package aggregate

import (
	"testing"

	"gsyncd/internal/event"
)

// openCount is the whole of the close_write decision in the aggregator, so each
// rule that can move it gets its own case: a wrong count either copies a
// half-written file (too low) or stalls the window until max_hold (too high).

func TestWriteWithoutCloseCountsAsOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d, want 1", got)
	}
}

func TestCloseClearsOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	a.Add(ev("/r/f", event.Write|event.Close, false))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after close, want 0", got)
	}
}

// A file that is closed and then written again is open again, even though the
// accumulated mask still carries the Close bit. Deriving the state from the mask
// instead of the newest event would call this one finished.
func TestWriteAfterCloseReopens(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write|event.Close, false))
	a.Add(ev("/r/f", event.Write, false))
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want 1 entry, got %+v", items)
	}
	if items[0].Op&event.Close == 0 {
		t.Fatal("test setup: the accumulated mask should still carry Close")
	}
	a.Add(ev("/r/f", event.Write, false))
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d, want 1", got)
	}
}

// chmod on an open file must not release it, and on a closed file must not open
// it: metadata says nothing about writers.
func TestAttribDoesNotChangeOpenState(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	a.Add(ev("/r/f", event.Attrib, false))
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d after attrib on an open file, want 1", got)
	}
	a.Add(ev("/r/f", event.Write|event.Close, false))
	a.Add(ev("/r/f", event.Attrib, false))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after attrib on a closed file, want 0", got)
	}
}

// A directory never gets IN_CLOSE_WRITE, so counting one as open would block the
// window until max_hold for nothing.
func TestDirectoriesAreNeverOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/d", event.Create, true))
	a.Add(ev("/r/d", event.Write, true))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d for a directory, want 0", got)
	}
}

// A create with no write is not evidence of a writer. It is also how a symlink,
// a fifo and a file moved into place by rename arrive - none of which ever get a
// close - so treating it as open would let one "ln -s" stall the whole window.
func TestCreateAloneIsNotOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/link", event.Create, false))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d for a bare create, want 0", got)
	}
}

// The backfill path synthesises Create|Write|Close for files it found inside a
// freshly watched directory, precisely so they do not read as open.
func TestBackfillMaskIsNotOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Create|event.Write|event.Close, false))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d for a backfilled file, want 0", got)
	}
}

// A deleted path can never receive a close, so holding it open would pin the
// window.
func TestDeleteClearsOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	a.Add(ev("/r/f", event.Delete, false))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after delete, want 0", got)
	}
}

// Renaming a file does not close it. Dropping the flag here would copy a
// half-written file exactly once, in the hardest case to reproduce: a rename
// during a write.
func TestRenameKeepsOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/a", event.Write, false))
	a.Add(event.Event{Path: "/r/b", From: "/r/a", Op: event.Move})
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d after rename, want 1", got)
	}
	items, _ := a.Flush()
	if len(items) != 1 || items[0].Path != "/r/b" {
		t.Fatalf("want the rename to /r/b, got %+v", items)
	}
}

// Deleting a directory absorbs its pending children; their open state has to go
// with them or the count drifts up until the window is flushed.
func TestDirDeleteReleasesOpenChildren(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/d/f", event.Write, false))
	a.Add(ev("/r/d/g", event.Write, false))
	if got := a.OpenFiles(); got != 2 {
		t.Fatalf("openFiles=%d, want 2", got)
	}
	a.Add(ev("/r/d", event.Delete, true))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after the parent was deleted, want 0", got)
	}
}

// Flushing empties the window, so nothing in it can be pending on a writer. A
// path that is still open re-arms on its next write.
func TestFlushResetsOpenCount(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	a.Flush()
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after flush, want 0", got)
	}
	a.Add(ev("/r/f", event.Write, false))
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d after a new write, want 1", got)
	}
}

// A rename can land on a path that already has a pending entry (write to b, then
// "mv a b" while a is still open). The rename carries the open flag, so the
// merged entry must stay open: the file being written is now at b, and flushing
// there would copy it half-written.
func TestRenameOntoPendingPathStaysOpen(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/b", event.Write|event.Close, false)) // b was written and finished
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after the closed write, want 0", got)
	}
	a.Add(ev("/r/a", event.Write, false)) // a is still being written
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d, want 1", got)
	}
	a.Add(event.Event{Path: "/r/b", From: "/r/a", Op: event.Move})
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d after the rename onto /r/b, want 1: the writer is still open", got)
	}
	// And closing at the new path releases it.
	a.Add(ev("/r/b", event.Write|event.Close, false))
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after the close at the new path, want 0", got)
	}
}

// Readd is the retry path: the batch was taken while the file was closed, the
// close arrived, then the transfer failed. Re-adding the change must not reopen
// it, or the retry waits for max_hold on a file that is already finished.
func TestReaddKeepsClosedState(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want 1 change, got %+v", items)
	}
	a.Add(ev("/r/f", event.Write|event.Close, false))
	a.Readd(items[0])
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after readd of a closed file, want 0", got)
	}
}

// Readd must not invent a close either: a change that was open when the batch
// was taken stays open.
func TestReaddKeepsOpenState(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	items, _ := a.Flush()
	a.Readd(items[0])
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d after readd of an open file, want 1", got)
	}
}
