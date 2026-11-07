package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// TestWalkOrderedMatchesFilepathWalk is the invariant the checkpoint store
// depends on: a different visit order would attach records to the wrong
// directories, and the store is a forward cursor that cannot detect that.
func TestWalkOrderedMatchesFilepathWalk(t *testing.T) {
	root := t.TempDir()
	mk := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Names chosen so that component-wise order and byte order disagree:
	// "a/b" must be visited before "a-b" even though "a-" < "a/".
	mk("a/b/deep.txt", "x")
	mk("a/b/c/deeper.txt", "x")
	mk("a/mid.txt", "x")
	mk("a-b/sibling.txt", "x")
	mk("a.b/dotted.txt", "x")
	mk("z/last.txt", "x")
	if err := os.MkdirAll(filepath.Join(root, "empty", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}

	var want, got []string
	if err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		want = append(want, p)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, workers := range []int{1, 2, 8} {
		got = got[:0]
		if err := walkOrdered(ctx, root, workers, 0, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			got = append(got, p)
			return nil
		}); err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("workers=%d: visit order differs\nwant %v\n got %v", workers, want, got)
		}
	}
}

// TestWalkOrderedReportsRootError checks the shape filepath.Walk has and the
// caller relies on: a missing root arrives as a callback error, not as a
// return value.
func TestWalkOrderedReportsRootError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	called := false
	err := walkOrdered(context.Background(), missing, 4, 0, func(p string, info os.FileInfo, werr error) error {
		called = true
		if werr == nil {
			t.Fatalf("expected an error for %s", p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walkOrdered returned %v, want the error through the callback", err)
	}
	if !called {
		t.Fatal("callback never ran")
	}
}

// TestWalkOrderedSkipDir verifies that skipping a directory stops the descent
// and leaves its siblings alone.
func TestWalkOrderedSkipDir(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"keep/a.txt", "skip/b.txt", "keep2/c.txt"} {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	err := walkOrdered(context.Background(), root, 4, 0, func(p string, info os.FileInfo, werr error) error {
		if werr != nil {
			return nil
		}
		if info.IsDir() && filepath.Base(p) == "skip" {
			return filepath.SkipDir
		}
		seen = append(seen, filepath.Base(p))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range seen {
		if name == "b.txt" {
			t.Fatalf("descended into a skipped directory: %v", seen)
		}
	}
	if len(seen) != 5 {
		t.Fatalf("visited %v, want 5 entries", seen)
	}
}

// TestWalkOrderedSerialFallback pins the escape hatch: workers <= 0 must behave
// exactly like filepath.Walk.
func TestWalkOrderedSerialFallback(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 5; i++ {
		p := filepath.Join(root, fmt.Sprintf("d%d/f.txt", i))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var want, got []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil {
			want = append(want, p)
		}
		return nil
	})
	if err := walkOrdered(context.Background(), root, 0, 0, func(p string, info os.FileInfo, err error) error {
		if err == nil {
			got = append(got, p)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("serial fallback diverged: %v vs %v", want, got)
	}
}

// TestPrefetchStopsAtBudget pins the bound that keeps read-ahead proportional
// to the work in flight instead of to the width of the tree. A 5M-file tree
// with a 4096-listing window held 984MB of RSS in listings the driver had not
// asked for yet. Here the pool has no workers, so nothing is ever consumed
// and any listing it asks for above the budget stays buffered forever; the
// fan-out must therefore stop asking.
func TestPrefetchStopsAtBudget(t *testing.T) {
	root := t.TempDir()
	for d := 0; d < 20; d++ {
		dir := filepath.Join(root, fmt.Sprintf("d%02d", d))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ents := readDirEntries(root)

	// Zero budget: the first listing may be taken (the driver asked for it),
	// but the fan-out it triggers must ask for nothing.
	p := newPrefetcher(0, 8)
	p.budget = 0
	p.done <- dirResult{dir: root, ents: ents}
	waitFor(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return len(p.ready) == 1
	})
	p.mu.Lock()
	requested := len(p.requested)
	buffered := p.buffered
	p.mu.Unlock()
	if requested != 0 {
		t.Fatalf("fan-out asked for %d listings with an exhausted budget", requested)
	}
	if buffered != len(ents) {
		t.Fatalf("buffered %d entries, want the %d of the one listing", buffered, len(ents))
	}

	// A budget that covers one listing's worth of children lets the fan-out
	// start, which is what keeps the workers fed on a normal tree.
	q := newPrefetcher(0, 8)
	q.budget = len(ents) + 1
	q.done <- dirResult{dir: root, ents: ents}
	waitFor(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.requested) > 0
	})
}

// TestPrefetcherCloseReleasesPool pins the lifetime fix: a walk used to leave
// its workers blocked on an open channel forever, with the read-ahead they had
// buffered still reachable through them. On an hourly full resync that is one
// pool per hour for the lifetime of the daemon.
func TestPrefetcherCloseReleasesPool(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := newPrefetcher(2, 8)
	p.ready[root] = readDirEntries(root)
	p.requested[root] = true
	p.buffered = 1

	p.Close()
	p.Close() // must be idempotent

	p.mu.Lock()
	if p.ready != nil || p.requested != nil || p.buffered != 0 {
		t.Fatalf("read-ahead survived Close: ready=%v requested=%v buffered=%d", p.ready, p.requested, p.buffered)
	}
	closed := p.closed
	p.mu.Unlock()
	if !closed {
		t.Fatal("Close did not mark the pool closed")
	}
	if p.request(root) {
		t.Fatal("request accepted after Close, but no worker will serve it")
	}
	// A closed pool must still answer: the caller reads the listing itself
	// rather than blocking on a worker that has already returned.
	if got := p.fetch(root); len(got) != 1 {
		t.Fatalf("fetch after Close returned %d entries, want the inline read", len(got))
	}
}

// waitFor polls a condition the dispatch goroutine sets, so the assertion does
// not depend on how fast a scheduling decision lands.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition never became true")
}
