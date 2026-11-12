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

// A pending write below a renamed directory is not carried by the target
// rename: the target only moves the copy it already has. It has to follow the
// directory to its new name, or the write is lost.
func TestRenameRetargetsPendingWrites(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/old/a.txt", event.Write, false))
	a.Add(event.Event{Path: "/r/new", From: "/r/old", Op: event.Move, IsDir: true})

	items, _ := a.Flush()
	paths := map[string]event.Op{}
	for _, c := range items {
		paths[c.Path] = c.Op
	}
	if op, ok := paths["/r/new/a.txt"]; !ok || op&event.Write == 0 {
		t.Fatalf("pending write was not retargeted: %+v", items)
	}
	if _, ok := paths["/r/old/a.txt"]; ok {
		t.Fatalf("stale path survived rename: %+v", items)
	}
	if op, ok := paths["/r/new"]; !ok || op&event.Move == 0 {
		t.Fatalf("want dir rename, got %+v", items)
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

func moveEv(from, path string, isDir bool) event.Event {
	return event.Event{Path: path, From: from, Op: event.Move, IsDir: isDir, At: time.Unix(0, 0)}
}

func findChange(items []Change, path string) *Change {
	for i := range items {
		if items[i].Path == path {
			return &items[i]
		}
	}
	return nil
}

func TestRenameChainKeepsRootSource(t *testing.T) {
	a := New(Options{})
	a.Add(moveEv("/r/a", "/r/b", false))
	a.Add(moveEv("/r/b", "/r/c", false))
	items, _ := a.Flush()
	c := findChange(items, "/r/c")
	if c == nil || c.From != "/r/a" {
		t.Fatalf("rename chain must fold to From=/r/a, got %+v", items)
	}
	if len(c.PrevFrom) != 1 || c.PrevFrom[0] != "/r/b" {
		t.Fatalf("intermediate /r/b must be deleted, got %+v", c.PrevFrom)
	}
	if findChange(items, "/r/a") != nil || findChange(items, "/r/b") != nil {
		t.Fatalf("chain sources must not remain as entries: %+v", items)
	}
}

func TestRenameOfEntryAndNodeRetargetsDescendants(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/D/sub", event.Create, true))
	a.Add(ev("/r/D/sub/f", event.Write, false))
	a.Add(moveEv("/r/D", "/r/E", true))
	items, _ := a.Flush()
	if findChange(items, "/r/E/sub/f") == nil {
		t.Fatalf("pending write under an entry+node directory was not retargeted: %+v", items)
	}
}

func TestReaddDoesNotLetStaleDeleteCollapseNewerCreate(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Delete, false))
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("setup: %+v", items)
	}
	stale := items[0]
	a.Add(ev("/r/f", event.Create|event.Write, false))
	a.Readd(stale)
	items, _ = a.Flush()
	f := findChange(items, "/r/f")
	if f == nil {
		t.Fatal("newer create was collapsed by the stale failed delete")
	}
	if f.Op&event.Delete != 0 || f.Op&event.Create == 0 {
		t.Fatalf("newer state must win, got %v", f.Op)
	}
}

func TestFollowRenameRetargetsPrevFrom(t *testing.T) {
	a := New(Options{})
	a.Add(moveEv("/r/d/x", "/r/d/y", false))
	a.Add(moveEv("/r/d/z", "/r/d/y", false))
	a.Add(moveEv("/r/d", "/r/e", true))
	items, _ := a.Flush()
	y := findChange(items, "/r/e/y")
	if y == nil {
		t.Fatalf("missing retargeted entry: %+v", items)
	}
	if len(y.PrevFrom) != 1 || y.PrevFrom[0] != "/r/e/x" {
		t.Fatalf("PrevFrom must be retargeted into /r/e, got %+v", y.PrevFrom)
	}
}

func TestPrevFromIsBounded(t *testing.T) {
	a := New(Options{})
	for i := 0; i < maxPrevFrom+100; i++ {
		a.Add(moveEv("/r/tmp"+itoa(i), "/r/dst", false))
	}
	items, overflow := a.Flush()
	if !overflow {
		t.Fatal("overflow must be signalled once PrevFrom hits its cap")
	}
	dst := findChange(items, "/r/dst")
	if dst == nil {
		t.Fatalf("missing dst: %+v", items)
	}
	if len(dst.PrevFrom) > maxPrevFrom {
		t.Fatalf("PrevFrom grew to %d, cap %d", len(dst.PrevFrom), maxPrevFrom)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

func TestRenameRoundTripCollapsesWithoutFrom(t *testing.T) {
	a := New(Options{})
	a.Add(moveEv("/r/a", "/r/b", false))
	a.Add(moveEv("/r/b", "/r/a", false))
	items, _ := a.Flush()
	x := findChange(items, "/r/a")
	if x == nil {
		t.Fatalf("round trip lost the path: %+v", items)
	}
	if x.From == x.Path || x.From != "" {
		t.Fatalf("From must not point at the path itself: %+v", x)
	}
	if len(x.PrevFrom) != 1 || x.PrevFrom[0] != "/r/b" {
		t.Fatalf("intermediate /r/b must be cleaned up: %+v", x.PrevFrom)
	}
}

func TestReaddKeepsDirectoryRenameSource(t *testing.T) {
	a := New(Options{})
	a.Add(moveEv("/r/a", "/r/b", true))
	items, _ := a.Flush()
	if len(items) != 1 {
		t.Fatalf("setup: %+v", items)
	}
	stale := items[0]
	// Newer event on the destination; the failed batch never ran, so the
	// target still has /r/a.
	a.Add(ev("/r/b", event.Attrib, true))
	a.Readd(stale)
	items, _ = a.Flush()
	b := findChange(items, "/r/b")
	if b == nil || b.From != "/r/a" {
		t.Fatalf("Readd dropped the rename source: %+v", items)
	}
}

func TestReaddStaleDirDeleteImpliesRecreate(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/d", event.Delete, true))
	items, _ := a.Flush()
	stale := items[0]
	a.Add(ev("/r/d", event.Create, true))
	a.Readd(stale)
	items, _ = a.Flush()
	d := findChange(items, "/r/d")
	if d == nil || !d.Recreate {
		t.Fatalf("stale directory delete must set Recreate: %+v", items)
	}
}

func TestRecreateSignalSurvivesNewerCreate(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/d", event.Create, true))
	a.Add(event.Event{Path: "/r/d", Op: event.Delete | event.Create, IsDir: true, Recreate: true, Subtree: true})
	items, _ := a.Flush()
	d := findChange(items, "/r/d")
	if d == nil || !d.Recreate || !d.Subtree || d.Op&event.Create == 0 {
		t.Fatalf("recreate signal was lost: %+v", items)
	}
}

func TestReaddFoldsProducerIntoNewerConsumer(t *testing.T) {
	a := New(Options{})
	// Newer window: c <- f (f already renamed to c at the source).
	a.Add(moveEv("/r/f", "/r/c", false))
	// Failed older batch: f <- e is put back.
	a.Readd(Change{Path: "/r/f", From: "/r/e", Op: event.Move, Kind: KindFile, At: time.Unix(1, 0)})
	items, _ := a.Flush()
	if findChange(items, "/r/f") != nil {
		t.Fatalf("transient rename target must be folded away: %+v", items)
	}
	c := findChange(items, "/r/c")
	if c == nil || c.From != "/r/e" {
		t.Fatalf("consumer must be folded to the chain root: %+v", items)
	}
}

func TestReaddFoldKeepsContentBits(t *testing.T) {
	a := New(Options{})
	a.Add(moveEv("/r/f", "/r/g", false))
	stale := Change{Path: "/r/f", From: "/r/e", Op: event.Move | event.Write, Kind: KindFile, At: time.Unix(1, 0)}
	a.Readd(stale)
	items, _ := a.Flush()
	g := findChange(items, "/r/g")
	if g == nil || g.From != "/r/e" {
		t.Fatalf("fold wrong: %+v", items)
	}
	if g.Op&event.Write == 0 {
		t.Fatalf("failed batch content bit was dropped: %+v", g)
	}
}

func TestReaddContentChangeConsumedByRename(t *testing.T) {
	a := New(Options{})
	a.Add(moveEv("/r/p", "/r/q", false))
	a.Readd(Change{Path: "/r/p", Op: event.Write, Kind: KindFile, At: time.Unix(1, 0)})
	items, _ := a.Flush()
	if findChange(items, "/r/p") != nil {
		t.Fatalf("stale content path should be folded away: %+v", items)
	}
	q := findChange(items, "/r/q")
	if q == nil || q.From != "/r/p" || q.Op&event.Write == 0 {
		t.Fatalf("consumer must copy from source with stale write bits: %+v", items)
	}
}

func TestReaddKeepOpenMergesContentBits(t *testing.T) {
	a := New(Options{})
	// Newer metadata change, failed batch carried the content write.
	a.Add(ev("/r/p", event.Attrib, false))
	items, _ := a.Flush()
	stale := Change{Path: "/r/p", Op: event.Write, Kind: KindFile, At: time.Unix(1, 0)}
	a.Readd(stale)
	items, _ = a.Flush()
	p := findChange(items, "/r/p")
	if p == nil || p.Op&event.Write == 0 {
		t.Fatalf("content bit from failed batch lost: %+v", items)
	}
}

func TestReaddBatchDoesNotFoldSameBatchChanges(t *testing.T) {
	a := New(Options{})
	// One failed batch: mv b a (a<-b), then b recreated and written.
	changes := []Change{
		{Path: "/r/a", From: "/r/b", Op: event.Move, Kind: KindFile, At: time.Unix(1, 0)},
		{Path: "/r/b", Op: event.Create | event.Write, Kind: KindFile, At: time.Unix(2, 0)},
	}
	a.ReaddBatch(changes)
	items, _ := a.Flush()
	if findChange(items, "/r/a") == nil || findChange(items, "/r/b") == nil {
		t.Fatalf("same-batch changes must both survive: %+v", items)
	}
}

func TestRemoveAtPreservesFirstConsumer(t *testing.T) {
	a := New(Options{})
	a.Add(moveEv("/r/x", "/r/a", false)) // first consumer of /r/x
	a.Add(ev("/r/w", event.Create, false))
	a.Add(moveEv("/r/x", "/r/b", false)) // later incarnation
	// Collapse w: removeAt swaps the tail (/r/b) into its slot.
	a.Add(ev("/r/w", event.Delete, false))
	a.Readd(Change{Path: "/r/x", Op: event.Delete, Kind: KindFile, At: time.Unix(1, 0)})
	items, _ := a.Flush()
	b := findChange(items, "/r/b")
	aa := findChange(items, "/r/a")
	if b == nil || aa == nil {
		t.Fatalf("missing entries: %+v", items)
	}
	if b.FromCycle {
		t.Fatalf("stale delete was attached to the later incarnation: %+v", items)
	}
	if !aa.FromCycle {
		t.Fatalf("first consumer must be protected: %+v", items)
	}
}
