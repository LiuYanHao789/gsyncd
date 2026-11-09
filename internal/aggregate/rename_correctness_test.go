package aggregate

import (
	"testing"
	"time"

	"gsyncd/internal/event"
)

func at() time.Time { return time.Unix(1700000000, 0) }

// A write that has not been transferred when the file is renamed must travel
// with the move. Otherwise the target renames its stale copy and the write is
// lost silently.
func TestRenameCarriesPendingWrite(t *testing.T) {
	a := New(Options{})
	a.Add(event.Event{Path: "/src/a", Op: event.Write, At: at()})
	a.Add(event.Event{Path: "/src/b", From: "/src/a", Op: event.Move, At: at()})
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want one change, got %+v", items)
	}
	c := items[0]
	if c.Path != "/src/b" || c.From != "/src/a" {
		t.Fatalf("wrong move: %+v", c)
	}
	if c.Op&event.Write == 0 {
		t.Fatalf("pending write was dropped from the move: %+v", c)
	}
}

// A directory rename must retarget every pending entry below it. Dropping the
// writes (the old behaviour) left stale content under the new directory name.
func TestDirRenameRetargetsPendingChildren(t *testing.T) {
	a := New(Options{})
	a.Add(event.Event{Path: "/src/d/f", Op: event.Write, At: at()})
	a.Add(event.Event{Path: "/src/d/g", Op: event.Delete, At: at()})
	a.Add(event.Event{Path: "/src/e", From: "/src/d", Op: event.Move, IsDir: true, At: at()})

	items, _ := a.Flush()
	got := map[string]Change{}
	for _, c := range items {
		got[c.Path] = c
	}
	if c, ok := got["/src/e/f"]; !ok || c.Op&event.Write == 0 {
		t.Fatalf("pending write under renamed dir was lost: %+v", items)
	}
	if c, ok := got["/src/e/g"]; !ok || c.Op&event.Delete == 0 {
		t.Fatalf("pending delete under renamed dir was not retargeted: %+v", items)
	}
	if _, ok := got["/src/d/f"]; ok {
		t.Fatalf("stale path survived the rename: %+v", items)
	}
	if c, ok := got["/src/e"]; !ok || c.From != "/src/d" {
		t.Fatalf("directory move missing: %+v", items)
	}
}

// A file renamed inside a directory that is renamed in the same window must
// carry both the old and the new name to the new prefix.
func TestNestedRenameFollowsDirRename(t *testing.T) {
	a := New(Options{})
	a.Add(event.Event{Path: "/src/d/x", From: "/src/d/y", Op: event.Move, At: at()})
	a.Add(event.Event{Path: "/src/e", From: "/src/d", Op: event.Move, IsDir: true, At: at()})
	items, _ := a.Flush()
	var found bool
	for _, c := range items {
		if c.Path == "/src/e/x" && c.From == "/src/e/y" {
			found = true
		}
		if c.Path == "/src/d/x" {
			t.Fatalf("nested rename was not retargeted: %+v", items)
		}
	}
	if !found {
		t.Fatalf("nested rename missing after dir rename: %+v", items)
	}
}

// delete + create of the same directory is not an atomic replace: the target
// has to remove the old contents, so the change carries Recreate.
func TestDirDeleteRecreateMarksRecreate(t *testing.T) {
	a := New(Options{})
	a.Add(event.Event{Path: "/src/d", Op: event.Delete, IsDir: true, At: at()})
	a.Add(event.Event{Path: "/src/d", Op: event.Create, IsDir: true, At: at()})
	a.Add(event.Event{Path: "/src/d/new", Op: event.Create, At: at()})
	items, _ := a.Flush()
	var found bool
	for _, c := range items {
		if c.Path == "/src/d" {
			found = true
			if !c.Recreate {
				t.Fatalf("directory recreate flag missing: %+v", c)
			}
			if c.Op&event.Create == 0 {
				t.Fatalf("recreated directory lost its create bit: %+v", c)
			}
		}
	}
	if !found {
		t.Fatalf("recreated directory missing: %+v", items)
	}
}

// Past the hard ceiling the window drops events and reports overflow; keeping
// them would trade bounded memory for an OOM kill, and the full resync that
// overflow schedules recovers them.
func TestHardCapDropsAndOverflows(t *testing.T) {
	a := New(Options{MaxPending: 10, MaxPendingHard: 20})
	for i := 0; i < 100; i++ {
		a.Add(event.Event{Path: "/p/" + string(rune('a'+i%26)) + string(rune('0'+i/26)), Op: event.Create, At: at()})
	}
	if n := a.Len(); n != 20 {
		t.Fatalf("window grew to %d, want hard cap 20", n)
	}
	if !a.Overflowed() {
		t.Fatal("dropping past the hard cap must mark overflow")
	}
	if a.Stats().Dropped == 0 {
		t.Fatal("dropped events were not counted")
	}
}
