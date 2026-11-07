package engine

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/logx"
)

// newRootEngine builds an engine whose source is a real path.
//
// The source must be a clean absolute path from this platform: Reattach cleans
// what the caller passes, and on Windows filepath.Clean("/src") is "\\src",
// which no longer matches a root recorded as "/src". Production sources come
// from the config and are cleaned once, so this only matters for the fixture.
func newRootEngine(t *testing.T, be *fakeBackend, delay time.Duration) (*Engine, *Root) {
	t.Helper()
	src := filepath.Join(t.TempDir(), "src")
	e, err := New([]Spec{{Name: "t", Source: src, Delay: delay, Backend: be}}, Options{
		Log:  logx.NewStderr(logx.Error),
		Tick: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e, e.Roots()[0]
}

// A root whose watch disappeared is not a "resync this tree" event like an
// overflow: the daemon has to remember that it is blind there, because that
// state is what the operator sees in the status file. Measured on a real kernel:
// rm -rf of the source root left the counters frozen and the log clean, which is
// indistinguishable from an idle system.
func TestRootGoneMarksSyncLost(t *testing.T) {
	buf := &syncBuffer{}
	be := &fakeBackend{}
	e, r := newRootEngine(t, be, time.Minute)
	e.log = logx.New(buf, logx.Warn)

	e.Handle(event.Event{Path: r.Source, Op: event.RootGone, IsDir: true})

	if lost := e.LostRoots(); len(lost) != 1 || lost[0] != r.Source {
		t.Fatalf("LostRoots() = %v, want [%s]", lost, r.Source)
	}
	if got := r.Stats().RootGone; got != 1 {
		t.Fatalf("RootGone counter = %d, want 1", got)
	}
	if !strings.Contains(buf.String(), "the tree is invisible until it is re-attached") {
		t.Fatalf("the lost root was not reported: %q", buf.String())
	}

	// A second report for the same root must not inflate the counter: the
	// kernel emits one per dropped watch, and the supervisor retries until it
	// succeeds, so repeats are normal.
	e.Handle(event.Event{Path: r.Source, Op: event.RootGone, IsDir: true})
	if got := r.Stats().RootGone; got != 1 {
		t.Fatalf("RootGone counter = %d after a repeated report, want 1", got)
	}
}

// Reattaching is the other half: the sync must stop being reported as lost and
// must be re-transferred, because whatever changed while the root was gone left
// no events at all.
func TestReattachForcesFullResync(t *testing.T) {
	be := &fakeBackend{}
	e, r := newRootEngine(t, be, time.Minute)
	e.Handle(event.Event{Path: r.Source, Op: event.RootGone, IsDir: true})

	e.Reattach(r.Source)
	e.Wait()

	if lost := e.LostRoots(); len(lost) != 0 {
		t.Fatalf("LostRoots() = %v after Reattach, want empty", lost)
	}
	batches := be.snapshot()
	if len(batches) == 0 {
		t.Fatal("Reattach did not flush anything")
	}
	full := false
	for _, b := range batches {
		if b.Full {
			full = true
		}
	}
	if !full {
		t.Fatalf("Reattach must force a full transfer, got %+v", batches)
	}
}

// A failure that happened while the root was missing says nothing about the
// target, so re-attaching must clear the retry gate. Otherwise the resync waits
// for the backoff of an error that is already fixed.
func TestReattachClearsRetryGate(t *testing.T) {
	be := &fakeBackend{fail: 100}
	e, r := newRootEngine(t, be, 0)
	e.Handle(event.Event{Path: filepath.Join(r.Source, "a"), Op: event.Write})
	e.Tick(time.Now())
	e.Wait()

	r.mu.Lock()
	gated := !r.retryAt.IsZero()
	r.mu.Unlock()
	if !gated {
		t.Fatal("the failed transfer did not arm the retry gate")
	}

	e.Reattach(r.Source)
	r.mu.Lock()
	cleared := r.retryAt.IsZero() && r.fails == 0
	r.mu.Unlock()
	if !cleared {
		t.Fatal("Reattach must clear the retry gate and the failure count")
	}
}

// Reattach for a path that is not a sync root must be a no-op instead of a
// panic or a silent extra root: the supervisor only ever passes configured
// sources, and anything else is a bug worth not crashing over.
func TestReattachIgnoresUnknownSource(t *testing.T) {
	e, r := newRootEngine(t, &fakeBackend{}, time.Minute)
	e.Reattach(filepath.Join(t.TempDir(), "not-a-root"))
	if lost := e.LostRoots(); len(lost) != 0 {
		t.Fatalf("LostRoots() = %v, want empty", lost)
	}
	if r.Source != e.Roots()[0].Source {
		t.Fatalf("the root changed to %s", r.Source)
	}
}
