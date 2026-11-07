package aggregate

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"gsyncd/internal/event"
)

// openFiles is maintained incrementally and is the only thing standing between
// close_write and either a half-written copy (count too low) or a window pinned
// until max_hold (count too high). A drift of one would be invisible in every
// other test, so this one recomputes the answer from the entries after each step
// and compares. It runs one long history rather than a fresh aggregator per
// step: a leak or a double decrement only shows up after many folds.
func TestOpenCountStaysConsistent(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	dirs := []string{"/r/a", "/r/b", "/r/a/c", "/r/b/d"}
	files := []string{"/r/a/f1", "/r/a/f2", "/r/b/f3", "/r/a/c/f4", "/r/b/d/f5", "/r/f6"}

	a := New(Options{MaxPending: 64})
	check := func(t *testing.T, step int) {
		t.Helper()
		a.mu.Lock()
		want := 0
		for _, c := range a.items {
			if c.open {
				want++
			}
		}
		got := a.openFiles
		a.mu.Unlock()
		if got != want {
			t.Fatalf("step %d: openFiles=%d but %d entries carry the flag", step, got, want)
		}
	}

	ops := []event.Op{
		event.Write, event.Write | event.Close, event.Create, event.Attrib,
		event.Delete, event.Create | event.Write, event.Write | event.Attrib,
	}
	for step := 0; step < 3000; step++ {
		at := time.Unix(int64(step), 0)
		switch rng.Intn(12) {
		case 0:
			a.Add(event.Event{Path: dirs[rng.Intn(len(dirs))], Op: event.Delete, IsDir: true, At: at})
		case 1:
			a.Add(event.Event{Path: fmt.Sprintf("/r/moved%d", rng.Intn(3)), From: files[rng.Intn(len(files))], Op: event.Move, At: at})
		case 2:
			a.Add(event.Event{Path: "/r/renamed", From: dirs[rng.Intn(len(dirs))], Op: event.Move, IsDir: true, At: at})
		case 3:
			// A failed batch goes back with the state it was taken with.
			items, _ := a.Flush()
			for _, c := range items {
				a.Readd(c)
			}
		default:
			a.Add(event.Event{Path: files[rng.Intn(len(files))], Op: ops[rng.Intn(len(ops))], At: at})
		}
		check(t, step)
	}
}

// Flush hands the window to the backend, so nothing is waiting for a writer any
// more. A path that is still open re-marks itself on its next write.
func TestFlushClearsOpenCountWithoutStrandingIt(t *testing.T) {
	a := New(Options{})
	a.Add(ev("/r/f", event.Write, false))
	a.Add(ev("/r/g", event.Write|event.Close, false))
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("openFiles=%d, want 1", got)
	}
	a.Flush()
	if got := a.OpenFiles(); got != 0 {
		t.Fatalf("openFiles=%d after flush, want 0", got)
	}
	a.Add(ev("/r/f", event.Write, false))
	if got := a.OpenFiles(); got != 1 {
		t.Fatalf("a write after the flush must count again, openFiles=%d", got)
	}
}
