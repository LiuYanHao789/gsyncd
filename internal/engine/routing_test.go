package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/filter"
	"gsyncd/internal/logx"
	syncpkg "gsyncd/internal/sync"
)

// captureBackend records what the engine hands it.
type captureBackend struct {
	mu      sync.Mutex
	batches []syncpkg.Batch
}

func (c *captureBackend) Name() string                      { return "capture" }
func (c *captureBackend) Startup(ctx context.Context) error { return nil }
func (c *captureBackend) Close() error                      { return nil }
func (c *captureBackend) Apply(ctx context.Context, b syncpkg.Batch) error {
	c.mu.Lock()
	c.batches = append(c.batches, b)
	c.mu.Unlock()
	return nil
}

func (c *captureBackend) snapshot() []syncpkg.Batch {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]syncpkg.Batch, len(c.batches))
	copy(out, c.batches)
	return out
}

// Two roots must not steal each other's events: routing is by longest prefix.
func TestRoutesToCorrectRoot(t *testing.T) {
	a, b := &captureBackend{}, &captureBackend{}
	eng, err := New([]Spec{
		{Name: "a", Source: "/data/a", Backend: a, Delay: time.Millisecond},
		{Name: "b", Source: "/data/b", Backend: b, Delay: time.Millisecond},
	}, Options{Log: logx.New(discard{}, logx.None), Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Handle(event.Event{Path: "/data/a/x.txt", Op: event.Write, At: time.Now()})
	eng.Handle(event.Event{Path: "/data/b/y.txt", Op: event.Write, At: time.Now()})
	eng.Handle(event.Event{Path: "/elsewhere/z.txt", Op: event.Write, At: time.Now()})
	eng.Tick(time.Now().Add(time.Hour))
	eng.Wait()

	if got := len(a.snapshot()); got != 1 {
		t.Fatalf("root a got %d batches, want 1", got)
	}
	if got := len(b.snapshot()); got != 1 {
		t.Fatalf("root b got %d batches, want 1", got)
	}
	if changes := b.snapshot()[0].Changes; len(changes) != 1 || changes[0].Path != "/data/b/y.txt" {
		t.Fatalf("root b received %+v", changes)
	}
}

// A nested root must win over its parent, otherwise the inner sync would
// never see its own events.
func TestNestedRootPrefersLongestPrefix(t *testing.T) {
	outer, inner := &captureBackend{}, &captureBackend{}
	eng, err := New([]Spec{
		{Name: "outer", Source: "/data", Backend: outer, Delay: time.Millisecond},
		{Name: "inner", Source: "/data/logs", Backend: inner, Delay: time.Millisecond},
	}, Options{Log: logx.New(discard{}, logx.None), Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Handle(event.Event{Path: "/data/logs/app.log", Op: event.Write, At: time.Now()})
	eng.Tick(time.Now().Add(time.Hour))
	eng.Wait()

	if n := len(outer.snapshot()); n != 0 {
		t.Fatalf("outer root must not receive nested events, got %d batches", n)
	}
	if n := len(inner.snapshot()); n != 1 {
		t.Fatalf("inner root got %d batches, want 1", n)
	}
}

// A burst of writes to the same tree must fold before it reaches the backend.
// This is the difference between one rsync call and one per file at 100 TB.
func TestBurstIsFoldedBeforeApply(t *testing.T) {
	be := &captureBackend{}
	eng, err := New([]Spec{{Name: "s", Source: "/data", Backend: be, Delay: 50 * time.Millisecond}},
		Options{Log: logx.New(discard{}, logx.None), Tick: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const n = 50_000
	start := time.Now()
	for i := 0; i < n; i++ {
		eng.Handle(event.Event{
			Path: fmt.Sprintf("/data/dir%03d/f%05d", i%200, i),
			Op:   event.Write,
			At:   time.Now(),
		})
	}
	ingest := time.Since(start)

	eng.Tick(time.Now().Add(time.Hour))
	eng.Wait()

	batches := be.snapshot()
	if len(batches) != 1 {
		t.Fatalf("expected one batch, got %d", len(batches))
	}
	if got := len(batches[0].Changes); got != n {
		t.Fatalf("expected %d changes, got %d", n, got)
	}
	t.Logf("folded %d events in %s (%.0f ns/event) into 1 batch", n, ingest, float64(ingest.Nanoseconds())/n)

	// Ticking again with an empty window must not produce a second batch.
	eng.Tick(time.Now().Add(2 * time.Hour))
	eng.Wait()
	if got := len(be.snapshot()); got != 1 {
		t.Fatalf("empty window produced an extra batch (%d total)", got)
	}
}

// Excluded paths never reach the backend.
func TestExcludesAreApplied(t *testing.T) {
	be := &captureBackend{}
	eng, err := New([]Spec{{
		Name:    "s",
		Source:  "/data",
		Backend: be,
		Delay:   time.Millisecond,
		Filter:  filter.New([]string{"*.tmp"}),
	}}, Options{Log: logx.New(discard{}, logx.None), Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Handle(event.Event{Path: "/data/keep.txt", Op: event.Write, At: time.Now()})
	eng.Handle(event.Event{Path: "/data/skip.tmp", Op: event.Write, At: time.Now()})
	eng.Tick(time.Now().Add(time.Hour))
	eng.Wait()

	batches := be.snapshot()
	if len(batches) != 1 || len(batches[0].Changes) != 1 || batches[0].Changes[0].Path != "/data/keep.txt" {
		t.Fatalf("unexpected batch: %+v", batches)
	}
}

// A move between two sync roots is two operations, not one: the source tree
// loses a name and the destination tree gains one. Treating it as a single
// rename would hand the destination backend a relative path starting with
// ".." and it would write outside its own target.
func TestCrossRootMoveSplitsIntoDeleteAndCreate(t *testing.T) {
	a, b := &captureBackend{}, &captureBackend{}
	eng, err := New([]Spec{
		{Name: "a", Source: "/data/a", Backend: a, Delay: time.Millisecond},
		{Name: "b", Source: "/data/b", Backend: b, Delay: time.Millisecond},
	}, Options{Log: logx.New(discard{}, logx.None), Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Handle(event.Event{
		Path: "/data/b/moved.txt", From: "/data/a/moved.txt",
		Op: event.Move, At: time.Now(),
	})
	eng.Tick(time.Now().Add(time.Hour))
	eng.Wait()

	ba, bb := a.snapshot(), b.snapshot()
	if len(ba) != 1 || len(ba[0].Changes) != 1 {
		t.Fatalf("source root got %+v, want one delete", ba)
	}
	if c := ba[0].Changes[0]; c.Path != "/data/a/moved.txt" || !c.IsDelete() || c.From != "" {
		t.Fatalf("source side must be a plain delete, got %+v", c)
	}
	if len(bb) != 1 || len(bb[0].Changes) != 1 {
		t.Fatalf("destination root got %+v, want one create", bb)
	}
	if c := bb[0].Changes[0]; c.Path != "/data/b/moved.txt" || c.Op&event.Create == 0 || c.From != "" {
		t.Fatalf("destination side must be a create, got %+v", c)
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
