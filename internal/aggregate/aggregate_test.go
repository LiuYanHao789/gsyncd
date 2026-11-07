package aggregate

import (
	"testing"
	"time"

	"gsyncd/internal/event"
)

func ev(path string, op event.Op, isDir bool) event.Event {
	return event.Event{Path: path, Op: op, IsDir: isDir, At: time.Unix(0, 0)}
}

func TestCollapseCreateDeleteVanishes(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/tmp", event.Create, false))
	a.Add(ev("/r/tmp", event.Delete, false))
	items, _ := a.Flush()
	if len(items) != 0 {
		t.Fatalf("want empty, got %+v", items)
	}
}

func TestCollapseDeleteThenCreateIsWrite(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Delete, false))
	a.Add(ev("/r/f", event.Create, false))
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want 1, got %+v", items)
	}
	if items[0].Op&event.Delete != 0 || items[0].Op&event.Write == 0 {
		t.Fatalf("want write without delete, got %v", items[0].Op)
	}
}

func TestManyWritesToOneEntry(t *testing.T) {
	a := New(Options{})
	for i := 0; i < 1000; i++ {
		a.Add(ev("/r/f", event.Write, false))
	}
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want 1, got %d", len(items))
	}
	if got := a.Stats().Collapsed; got != 999 {
		t.Fatalf("collapsed=%d", got)
	}
}

func TestDirDeleteDropsChildren(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/d", event.Create, true))
	a.Add(ev("/r/d/a", event.Write, false))
	a.Add(ev("/r/d/sub/b", event.Write, false))
	a.Add(ev("/r/d2", event.Write, false))
	a.Add(ev("/r/d", event.Delete, true))
	items, _ := a.Flush()
	if len(items) != 2 {
		t.Fatalf("want 2 (dir delete + d2), got %+v", items)
	}
	if items[0].Path != "/r/d" || items[0].Op != event.Delete {
		t.Fatalf("first=%+v", items[0])
	}
}

func TestMoveReplacesSource(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/old", event.Write, false))
	a.Add(event.Event{Path: "/r/new", From: "/r/old", Op: event.Move, At: time.Unix(0, 0)})
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want 1, got %+v", items)
	}
	if items[0].Path != "/r/new" || items[0].From != "/r/old" {
		t.Fatalf("got %+v", items[0])
	}
}

// A directory rename must stay a directory rename. Recording it as a file
// loses the dropSubtree shortcut for every delete that follows below it, so a
// renamed 100k file tree turns into 100k individual deletes.
func TestMoveKeepsDirectoryKind(t *testing.T) {
	a := New(Options{})
	a.Add(event.Event{Path: "/r/new", From: "/r/old", Op: event.Move, IsDir: true, At: time.Unix(0, 0)})
	a.Add(ev("/r/new/a", event.Write, false))
	a.Add(ev("/r/new", event.Delete, true))
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want 1 (move + absorbed child), got %+v", items)
	}
	if items[0].Kind != KindDir {
		t.Fatalf("move lost the directory kind: %+v", items[0])
	}
}

// Hitting the size cap is not data loss: the batch is simply sent early.
// Conflating the two would make the engine do a wholesale resync every time a
// busy tree produced more than MaxPending changes, which at 10T scale is every
// few seconds.
func TestSizeCapIsUrgentNotOverflow(t *testing.T) {
	a := New(Options{MaxPending: 4})
	for i := 0; i < 10; i++ {
		a.Add(ev("/r/f"+string(rune('a'+i)), event.Write, false))
	}
	if a.Overflowed() {
		t.Fatal("size cap must not look like data loss")
	}
	if !a.Urgent() {
		t.Fatal("want urgent")
	}
	if items, of := a.Flush(); len(items) != 10 || of {
		t.Fatalf("want 10 items and no overflow, got %d %v", len(items), of)
	}
	if a.Urgent() {
		t.Fatal("urgent must reset")
	}
}

// A pending delete below a renamed directory is not carried by the rename, so
// it has to be retargeted at the new name. Dropping it would leave a file that
// was removed just before the rename sitting on the target forever.
func TestRenameRetargetsPendingDelete(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/old/a.txt", event.Delete, false))
	a.Add(ev("/r/old/b.txt", event.Delete, false))
	// The subtree directory itself is renamed; both pending deletes follow.
	a.Add(event.Event{Path: "/r/new", From: "/r/old", Op: event.Move, IsDir: true})

	items, _ := a.Flush()
	paths := map[string]event.Op{}
	for _, c := range items {
		paths[c.Path] = c.Op
	}
	for _, want := range []string{"/r/new", "/r/new/a.txt", "/r/new/b.txt"} {
		if _, ok := paths[want]; !ok {
			t.Fatalf("missing %s in %+v", want, items)
		}
	}
	if _, ok := paths["/r/old/a.txt"]; ok {
		t.Fatal("stale path survived rename")
	}
}

// A pending create below a renamed directory is already covered by the rename:
// keeping it would ask the engine to process a path that no longer exists.
func TestRenameDropsPendingCreates(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/old/a.txt", event.Write, false))
	a.Add(event.Event{Path: "/r/new", From: "/r/old", Op: event.Move, IsDir: true})

	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("want only the rename, got %+v", items)
	}
	if items[0].Path != "/r/new" || items[0].Kind != KindDir {
		t.Fatalf("want dir rename, got %+v", items[0])
	}
}

func TestOverflowEventSetsFlag(t *testing.T) {
	a := New(Options{})
	a.Add(event.Event{Op: event.Overflow})
	if !a.Overflowed() {
		t.Fatal("want overflow")
	}
}

// The pending tree must shrink back to nothing when the window drains. It is
// pruned bottom-up, and a node that survives a drained window is a leak: on a
// long-running daemon the tree would grow with every directory ever touched and
// every later subtree walk would pay for it.
func TestPendingTreePrunesBackToEmpty(t *testing.T) {
	a := New(Options{})
	at := time.Unix(0, 0)
	a.Add(event.Event{Path: "/r/a/b/f", Op: event.Create, At: at})
	a.Add(event.Event{Path: "/r/a/b/f", Op: event.Delete, At: at})
	if items, _ := a.Flush(); len(items) != 0 {
		t.Fatalf("want an empty window, got %+v", items)
	}
	if n := len(a.nodes); n != 0 {
		for d, node := range a.nodes {
			t.Logf("leftover node %q parent=%q children=%v", d, node.parent, node.children)
		}
		t.Fatalf("%d node(s) left in the pending tree after the window drained", n)
	}
}

// The top of the tree is the path whose parent is itself, and linking it into
// its own child list makes it impossible to prune: the node keeps pointing at
// itself, so it survives every drain and any subtree walk below it recurses
// until the stack runs out.
func TestTopNodeIsNotItsOwnChild(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/x/y", event.Write, false))
	for dir, node := range a.nodes {
		for _, child := range node.children {
			if child == dir {
				t.Fatalf("node %q lists itself as a child (parent=%q)", dir, node.parent)
			}
		}
	}
	// A subtree walk from the top must terminate and must find the entry.
	found := false
	for _, p := range a.subtreePaths(a.items[0].parent, nil) {
		if p == "/x/y" {
			found = true
		}
	}
	if !found {
		t.Fatal("subtree walk from the parent of a pending entry missed the entry")
	}
}

// BenchmarkAdd measures the O(1) upsert path that replaced the O(n^2) scan.
func BenchmarkAdd(b *testing.B) {
	a := New(Options{MaxPending: 1 << 30})
	paths := make([]string, 1024)
	for i := range paths {
		paths[i] = "/r/dir/file" + string(rune('a'+i%26)) + string(rune('0'+i%10))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Add(ev(paths[i%len(paths)], event.Write, false))
	}
}
