package aggregate

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"gsyncd/internal/event"
)

// makeEvent builds a synthetic write event with a realistic deep path.
func makeEvent(i int) event.Event {
	return event.Event{
		Path: fmt.Sprintf("/data/project/%03d/sub%02d/file-%07d.dat", i%512, i%97, i),
		Op:   event.Write,
		At:   time.Unix(1700000000, 0),
	}
}

// BenchmarkAddLarge measures the per event cost of folding into a large
// window. The original Delay queue scanned linearly for duplicates, which the
// maintainer described as O(n^2) (issue #202); this is the number that makes
// 10-100 TB bursts tractable.
func BenchmarkAddLarge(b *testing.B) {
	b.ReportAllocs()
	for _, n := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("pending=%d", n), func(b *testing.B) {
			a := New(Options{MaxPending: n + 1000})
			for i := 0; i < n; i++ {
				a.Add(makeEvent(i))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.Add(makeEvent(i % n))
			}
		})
	}
}

// TestMillionPathsMemory keeps a hard number on the memory the fold window
// needs, which is what decides how big MaxPending can safely be.
func TestMillionPathsMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping scale test in short mode")
	}
	// GSYNCD_AGGPATHS raises the sample so the per-path cost can be measured at
	// the size of a real backlog instead of extrapolated from a million. The
	// number matters because MaxPending is multiplied by it.
	n := 1_000_000
	if v := os.Getenv("GSYNCD_AGGPATHS"); v != "" {
		got, err := strconv.Atoi(v)
		if err != nil || got < 1000 {
			t.Fatalf("GSYNCD_AGGPATHS=%q is not a path count", v)
		}
		n = got
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	a := New(Options{MaxPending: n + 1})
	start := time.Now()
	for i := 0; i < n; i++ {
		a.Add(makeEvent(i))
	}
	elapsed := time.Since(start)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	perPath := float64(after.HeapAlloc-before.HeapAlloc) / float64(n)
	t.Logf("%d paths: %s total (%.0f ns/event), %.0f bytes/path resident",
		n, elapsed, float64(elapsed.Nanoseconds())/float64(n), perPath)

	if got := a.Len(); got != n {
		t.Fatalf("expected %d pending entries, got %d", n, got)
	}
	// 1M paths must stay well under a gigabyte so that MaxPending=500000 is a
	// safe default even on small instances.
	if perPath > 400 {
		t.Fatalf("memory per pending path too high: %.0f bytes", perPath)
	}
}

// TestBurstDeleteOfHundredThousand mirrors issue #695 (lsyncd got stuck when
// deleting more than 100k files) and checks the fold collapses the whole
// subtree into a single change.
func TestBurstDeleteOfHundredThousand(t *testing.T) {
	const n = 100_000
	a := New(Options{MaxPending: n + 10})
	for i := 0; i < n; i++ {
		a.Add(event.Event{Path: fmt.Sprintf("/data/tree/f%06d", i), Op: event.Create})
	}
	a.Add(event.Event{Path: "/data/tree", Op: event.Delete, IsDir: true})

	changes, overflow := a.Flush()
	if overflow {
		t.Fatal("unexpected overflow")
	}
	if len(changes) != 1 || changes[0].Path != "/data/tree" {
		t.Fatalf("expected the subtree to collapse into one change, got %d: %v", len(changes), changes[:min(3, len(changes))])
	}
	if st := a.Stats(); st.Collapsed < n {
		t.Fatalf("expected at least %d collapsed events, got %d", n, st.Collapsed)
	}
}
