package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/filter"
	"gsyncd/internal/logx"
	syncpkg "gsyncd/internal/sync"
)

// fakeBackend records batches instead of touching the filesystem.
type fakeBackend struct {
	mu      sync.Mutex
	batches []syncpkg.Batch
	fail    int
	// startupFail makes the next N Startup calls fail, so the initial sync
	// path can be tested.
	startupFail int
	// startupBlock, when non-nil, keeps Startup parked until it is closed.
	// It exists so a test can observe the engine while the initial transfer
	// is in flight, which is a state that lasts hours on a real tree.
	startupBlock chan struct{}
	calls        int32
}

func (f *fakeBackend) Name() string { return "fake" }
func (f *fakeBackend) Startup(ctx context.Context) error {
	f.mu.Lock()
	block := f.startupBlock
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startupFail > 0 {
		f.startupFail--
		return errors.New("startup boom")
	}
	return nil
}
func (f *fakeBackend) Close() error { return nil }
func (f *fakeBackend) Apply(ctx context.Context, b syncpkg.Batch) error {
	atomic.AddInt32(&f.calls, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return errors.New("boom")
	}
	f.batches = append(f.batches, b)
	return nil
}

func (f *fakeBackend) snapshot() []syncpkg.Batch {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]syncpkg.Batch, len(f.batches))
	copy(out, f.batches)
	return out
}

func newTestEngine(t *testing.T, be syncpkg.Backend, delay time.Duration, pats []string) (*Engine, *Root) {
	t.Helper()
	e, err := New([]Spec{{
		Name:    "t",
		Source:  "/src",
		Delay:   delay,
		Filter:  filter.New(pats),
		Backend: be,
	}}, Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return e, e.Roots()[0]
}

func TestEngineCollapsesAndFlushes(t *testing.T) {
	be := &fakeBackend{}
	e, _ := newTestEngine(t, be, 0, nil)
	for i := 0; i < 100; i++ {
		e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	}
	e.Handle(event.Event{Path: "/src/b", Op: event.Create})
	e.FlushAll(context.Background())
	got := be.snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 batch, got %d", len(got))
	}
	if len(got[0].Changes) != 2 {
		t.Fatalf("want 2 changes (a,b), got %d: %+v", len(got[0].Changes), got[0].Changes)
	}
}

func TestEngineRespectsDelay(t *testing.T) {
	be := &fakeBackend{}
	e, _ := newTestEngine(t, be, time.Minute, nil)
	now := time.Now()
	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.Tick(now.Add(30 * time.Second))
	if len(be.snapshot()) != 0 {
		t.Fatal("flushed before the delay elapsed")
	}
	e.Tick(now.Add(2 * time.Minute))
	e.Wait()
	if len(be.snapshot()) != 1 {
		t.Fatal("did not flush after the delay elapsed")
	}
}

func TestEngineFiltersExcluded(t *testing.T) {
	be := &fakeBackend{}
	e, r := newTestEngine(t, be, 0, []string{"*.log"})
	e.Handle(event.Event{Path: "/src/a.log", Op: event.Write})
	e.Handle(event.Event{Path: "/src/a.txt", Op: event.Write})
	e.FlushAll(context.Background())
	got := be.snapshot()
	if len(got) != 1 || len(got[0].Changes) != 1 || got[0].Changes[0].Path != "/src/a.txt" {
		t.Fatalf("filter not applied: %+v", got)
	}
	if s := r.Stats(); s.Filtered != 1 {
		t.Fatalf("Filtered=%d want 1", s.Filtered)
	}
}

func TestEngineOverflowForcesFull(t *testing.T) {
	be := &fakeBackend{}
	e, r := newTestEngine(t, be, time.Hour, nil)
	e.Handle(event.Event{Op: event.Overflow})
	if !r.agg.Overflowed() {
		t.Fatal("overflow flag not set")
	}
	e.Tick(time.Now())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got := be.snapshot()
		if len(got) == 1 {
			if !got[0].Full {
				t.Fatalf("batch is not marked Full: %+v", got[0])
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no full batch was transferred")
}

func TestEngineRetriesThenSucceeds(t *testing.T) {
	be := &fakeBackend{fail: 1}
	e, r := newTestEngine(t, be, 0, nil)
	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.FlushAll(context.Background())
	if len(be.snapshot()) != 1 {
		t.Fatalf("retry did not deliver the batch: %d", len(be.snapshot()))
	}
	if s := r.Stats(); s.Retries != 1 {
		t.Fatalf("Retries=%d want 1", s.Retries)
	}
}

func TestEngineReaddsFailedBatch(t *testing.T) {
	be := &fakeBackend{fail: 3} // exactly the retry budget
	e, r := newTestEngine(t, be, 0, nil)
	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.FlushAll(context.Background())
	if s := r.Stats(); s.Errors == 0 {
		t.Fatal("failure not recorded")
	}
	if r.Pending() != 1 {
		t.Fatalf("failed change was not put back, pending=%d", r.Pending())
	}
}

// TestEngineEndToEnd drives the real direct backend over a temp tree.
func TestEngineEndToEnd(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "a/b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a/b/f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	be := syncpkg.NewDirect(src, dst, true, 2, nil)
	e, err := New([]Spec{{Name: "e2e", Source: src, Delay: 0, Backend: be}}, Options{
		Log: logx.NewStderr(logx.Error),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := e.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a/b/f.txt")); err != nil || string(b) != "hello" {
		t.Fatalf("initial copy failed: %v %q", err, b)
	}

	// Modify, add and delete, then let the engine apply the window.
	if err := os.WriteFile(filepath.Join(src, "a/b/f.txt"), []byte("hello world"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "new.txt"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(src, "a")); err != nil {
		t.Fatal(err)
	}
	e.Handle(event.Event{Path: filepath.Join(src, "a/b/f.txt"), Op: event.Write})
	e.Handle(event.Event{Path: filepath.Join(src, "new.txt"), Op: event.Create})
	e.Handle(event.Event{Path: filepath.Join(src, "a"), Op: event.Delete, IsDir: true})
	e.FlushAll(ctx)
	e.Wait()

	if _, err := os.Stat(filepath.Join(dst, "a")); !os.IsNotExist(err) {
		t.Fatalf("deleted directory still present on target: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "new.txt")); err != nil || string(b) != "new" {
		t.Fatalf("new file not copied: %v %q", err, b)
	}
}

// countingBackend is a fakeBackend that also reports backend counters, the way
// the real direct/rsync backends do.
type countingBackend struct {
	fakeBackend
	files   uint64
	deletes uint64
}

func (c *countingBackend) Stats() syncpkg.Stats {
	return syncpkg.Stats{Files: c.files, Deletes: c.deletes}
}

// The status file has to report what actually reached the target. The engine
// only counts batches and collapsed changes; files written and entries removed
// live in the backend. If they are not merged, "deletes" reads zero forever and
// an operator watching a batch delete drain sees a frozen field and concludes
// the run has stalled.
func TestStatsMergeBackendCounters(t *testing.T) {
	be := &countingBackend{files: 120, deletes: 7}
	e, r := newTestEngine(t, be, 0, nil)
	if err := e.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.FlushAll(context.Background())
	e.Wait()

	st := r.Stats()
	if st.Files != 120 || st.Deletes != 7 {
		t.Fatalf("backend counters not merged: files=%d deletes=%d", st.Files, st.Deletes)
	}
	if st.Batches == 0 || st.Applied == 0 {
		t.Fatalf("engine counters lost: %+v", st)
	}
}

// A backend that does not report counters must not break Stats, and must leave
// the engine counters intact.
func TestStatsWithoutBackendCounters(t *testing.T) {
	be := &fakeBackend{}
	e, r := newTestEngine(t, be, 0, nil)
	if err := e.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.FlushAll(context.Background())
	e.Wait()

	st := r.Stats()
	if st.Files != 0 || st.Deletes != 0 {
		t.Fatalf("unexpected backend counters: %+v", st)
	}
	if st.Applied != 1 {
		t.Fatalf("applied=%d, want 1", st.Applied)
	}
}
