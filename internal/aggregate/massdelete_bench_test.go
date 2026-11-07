package aggregate

import (
	"fmt"
	"testing"
	"time"

	"gsyncd/internal/event"
)

// BenchmarkMassDeleteAbsorb measures rm -rf shaped input: one Delete event per
// directory, children already pending. Absorbing the pending children of a
// deleted directory must cost the size of that subtree, not the size of the
// window. The latter is quadratic - 58 s of CPU for a 200k window before this
// was indexed - and it is the shape of issue #695 ("lsyncd get stuck when doing
// batch operations"), because the scan happens while holding the aggregator
// lock, so the watcher cannot drain the kernel queue meanwhile.
func BenchmarkMassDeleteAbsorb(b *testing.B) {
	cases := []struct{ files, perDir int }{
		{10_000, 5},
		{100_000, 5},
		{200_000, 5},
		{500_000, 5},
	}
	for _, c := range cases {
		dirs := c.files / c.perDir
		b.Run(fmt.Sprintf("files=%d dirs=%d", c.files, dirs), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				a := New(Options{MaxPending: 10_000_000})
				for j := 0; j < c.files; j++ {
					dir := fmt.Sprintf("/src/d%07d", j/c.perDir)
					a.Add(event.Event{Path: dir + "/f.dat", Op: event.Create | event.Write, At: time.Now()})
				}
				now := time.Now()
				b.StartTimer()
				for d := 0; d < dirs; d++ {
					a.Add(event.Event{Path: fmt.Sprintf("/src/d%07d", d), Op: event.Delete, IsDir: true, At: now})
				}
				b.StopTimer()
				// One entry per deleted directory remains - the backend still
				// has to be told to remove it - but every child below it is
				// gone.
				if n := a.Len(); n != dirs {
					b.Fatalf("expected %d directory deletes, got %d entries", dirs, n)
				}
				b.StartTimer()
			}
		})
	}
}

// TestMassDeleteOfNestedTreeDeletesEveryEntry is the correctness half of the
// benchmark above: after deleting the root, nothing may be left pending, or the
// backend would be asked to transfer paths that no longer exist.
func TestMassDeleteOfNestedTreeDeletesEveryEntry(t *testing.T) {
	a := New(Options{MaxPending: 1_000_000})
	const dirs, perDir = 2000, 5
	for d := 0; d < dirs; d++ {
		for f := 0; f < perDir; f++ {
			a.Add(ev(fmt.Sprintf("/src/a%04d/b%02d/f%d.dat", d, f%7, f), event.Create|event.Write, false))
		}
	}
	for d := 0; d < dirs; d++ {
		a.Add(ev(fmt.Sprintf("/src/a%04d", d), event.Delete, true))
	}
	// Exactly one entry per deleted directory survives: the backend still has
	// to be told to remove each of them. Every pending child below must be
	// gone, or the backend would be asked to transfer paths that no longer
	// exist.
	if n := a.Len(); n != dirs {
		t.Fatalf("want %d directory deletes, %d entries survived", dirs, n)
	}
	items, _ := a.Flush()
	if len(items) != dirs {
		t.Fatalf("want %d changes, got %d", dirs, len(items))
	}
	for _, c := range items {
		if c.Kind != KindDir || !c.IsDelete() {
			t.Fatalf("want a directory delete, got %+v", c)
		}
	}
}

// BenchmarkMassRenameDir measures the rename path with the same shape. A
// directory rename retargets the pending entries below it; doing that with a
// full window scan is quadratic for the same reason the delete path was.
func BenchmarkMassRenameDir(b *testing.B) {
	for _, files := range []int{10_000, 100_000, 500_000} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				a := New(Options{MaxPending: 10_000_000})
				for j := 0; j < files; j++ {
					a.Add(event.Event{Path: fmt.Sprintf("/src/tree/d%05d/f.dat", j/5), Op: event.Create | event.Write, At: time.Now()})
				}
				// A pending delete below the renamed tree must follow it.
				a.Add(event.Event{Path: "/src/tree/d00001/gone.dat", Op: event.Delete, At: time.Now()})
				b.StartTimer()
				a.Add(event.Event{Path: "/src/moved", From: "/src/tree", Op: event.Move, IsDir: true, At: time.Now()})
				b.StopTimer()
				b.StartTimer()
			}
		})
	}
}

// TestDeleteOfSiblingKeepsOthers guards the index against over-collection: one
// directory delete must not disturb pending entries outside it.
func TestDeleteOfSiblingKeepsOthers(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/src/a/1.txt", event.Write, false))
	a.Add(ev("/src/ab/2.txt", event.Write, false))
	a.Add(ev("/src/a/deep/3.txt", event.Write, false))
	// "ab" shares a string prefix with "a" but is not below it.
	a.Add(ev("/src/a", event.Delete, true))

	items, _ := a.Flush()
	got := map[string]bool{}
	for _, c := range items {
		got[c.Path] = true
	}
	if len(items) != 2 || !got["/src/ab/2.txt"] || !got["/src/a"] {
		t.Fatalf("want /src/a and /src/ab/2.txt only, got %+v", items)
	}
}

// TestDeleteThenRecreateKeepsSubtreeCorrect covers the ordering where a
// directory is deleted while its children are pending and a new file appears
// under it afterwards: the fresh entry must be visible, not absorbed.
func TestDeleteThenRecreateKeepsSubtreeCorrect(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/src/d/old.txt", event.Write, false))
	a.Add(ev("/src/d", event.Delete, true))
	a.Add(ev("/src/d/new.txt", event.Create, false))

	items, _ := a.Flush()
	got := map[string]event.Op{}
	for _, c := range items {
		got[c.Path] = c.Op
	}
	if _, ok := got["/src/d/old.txt"]; ok {
		t.Fatalf("stale child survived the delete: %+v", items)
	}
	if op, ok := got["/src/d/new.txt"]; !ok || op&event.Create == 0 {
		t.Fatalf("recreated file missing: %+v", items)
	}
}
