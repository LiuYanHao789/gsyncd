package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/logx"
)

// A failed full resync must stay a full resync. Flush clears the overflow flag,
// so without re-marking it the retry would run as an ordinary incremental batch
// and the tree would stay inconsistent forever.
func TestFailedFullResyncStaysFull(t *testing.T) {
	be := &fakeBackend{fail: 1}
	e, r := newTestEngine(t, be, 0, nil)
	r.Retries = 1

	e.Handle(event.Event{Op: event.Overflow})
	if !r.agg.Overflowed() {
		t.Fatal("overflow flag not set")
	}

	e.Tick(time.Now())
	e.Wait()

	if !r.agg.Overflowed() {
		t.Fatal("a failed full resync must re-arm the overflow flag")
	}
	if got := be.snapshot(); len(got) != 0 {
		t.Fatalf("a failed batch must not be recorded as applied: %+v", got)
	}
	r.mu.Lock()
	errs, last := r.st.Errors, r.st.LastError
	r.mu.Unlock()
	if errs == 0 || last == "" {
		t.Fatalf("expected a recorded error, got %d %q", errs, last)
	}

	// The next round retries the full resync and succeeds. The tick time is
	// pushed past the backoff the failure armed: without that the engine
	// deliberately declines to try again yet.
	e.Tick(time.Now().Add(time.Minute))
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 || !got[0].Full {
		t.Fatalf("want one successful full batch, got %+v", got)
	}
}

// A failed incremental batch goes back into the window, so the next round
// retries exactly those paths instead of rescanning the whole tree.
func TestFailedBatchIsReadded(t *testing.T) {
	be := &fakeBackend{fail: 1}
	e, r := newTestEngine(t, be, 0, nil)
	r.Retries = 1

	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.Handle(event.Event{Path: "/src/b", Op: event.Write})

	e.Tick(time.Now())
	e.Wait()

	if n := len(be.snapshot()); n != 0 {
		t.Fatalf("the batch should have failed, got %d", n)
	}
	if r.agg.Len() != 2 {
		t.Fatalf("failed paths must return to the window, len=%d", r.agg.Len())
	}

	r.Retries = 3
	e.Tick(time.Now().Add(time.Minute))
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 || len(got[0].Changes) != 2 {
		t.Fatalf("want one successful batch of 2, got %+v", got)
	}
	if got[0].Full {
		t.Fatal("a retried incremental batch must not become a full resync")
	}
}

// A target that is down must not be retried at the speed of the delay loop.
// Each round can be a whole-tree rescan, so the engine backs off before the
// next attempt and a healthy tick in between changes nothing.
func TestFailedRoundBacksOff(t *testing.T) {
	be := &fakeBackend{}
	now := time.Unix(1700000000, 0)
	e, err := New([]Spec{{
		Name:    "t",
		Source:  "/src",
		Delay:   0,
		Backend: be,
		Retries: 1,
	}}, Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	r := e.Roots()[0]

	be.mu.Lock()
	be.fail = 1
	be.mu.Unlock()
	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.Tick(now)
	e.Wait()
	if n := atomic.LoadInt32(&be.calls); n != 1 {
		t.Fatalf("want one attempt, got %d", n)
	}

	// Immediately again: the failed round succeeded at re-adding the change,
	// but the gate must hold the next attempt back.
	e.Tick(now)
	e.Wait()
	if n := atomic.LoadInt32(&be.calls); n != 1 {
		t.Fatalf("retried inside the backoff window: %d attempts", n)
	}
	if r.gateOpen(now) {
		t.Fatal("the gate should be shut right after a failure")
	}

	// The backend recovers; once the backoff elapses the change goes out.
	be.mu.Lock()
	be.fail = 0
	be.mu.Unlock()
	now = now.Add(5 * time.Minute)
	e.Tick(now)
	e.Wait()
	if !r.gateOpen(now) {
		t.Fatal("the gate must reopen after a success")
	}
	got := be.snapshot()
	if len(got) != 1 || len(got[0].Changes) != 1 {
		t.Fatalf("want the retried change to land, got %+v", got)
	}
}

// A failed initial sync must not leave the tree silently half mirrored. The
// files that failed are already on disk at the source, so no later event will
// ever repair them: the engine has to arm a full resync behind the retry gate.
func TestFailedStartupArmsFullResync(t *testing.T) {
	be := &fakeBackend{startupFail: 1}
	e, r := newTestEngine(t, be, 0, nil)
	r.Retries = 1

	if err := e.Startup(context.Background()); err == nil {
		t.Fatal("the initial sync should have failed")
	}
	if !r.agg.Overflowed() {
		t.Fatal("a failed initial sync must arm a full resync")
	}
	r.mu.Lock()
	fails, errs, retryAt := r.fails, r.st.Errors, r.retryAt
	r.mu.Unlock()
	if fails != 1 || errs != 1 || retryAt.IsZero() {
		t.Fatalf("expected one recorded failure and a retry gate, got fails=%d errs=%d retryAt=%v", fails, errs, retryAt)
	}

	// Before the gate opens the engine deliberately does nothing.
	e.Tick(time.Now())
	e.Wait()
	if got := be.snapshot(); len(got) != 0 {
		t.Fatalf("the retry gate should hold the round back, got %+v", got)
	}

	// After it opens the repair is a full resync, not an incremental batch.
	e.Tick(time.Now().Add(time.Minute))
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 || !got[0].Full {
		t.Fatalf("want one successful full batch, got %+v", got)
	}
}

// While the initial transfer runs, the status document must say so. On a large
// tree that phase lasts hours and every other counter reads zero, so without
// this flag a daemon copying at full speed looks identical to a hung one.
func TestStartingFlagVisibleDuringInitialSync(t *testing.T) {
	be := &fakeBackend{}
	be.startupBlock = make(chan struct{})
	eng, _ := newTestEngine(t, be, 0, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = eng.Startup(context.Background())
	}()

	// Wait for Startup to publish the flag rather than sleeping a fixed time.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if r := eng.Roots()[0]; r.Stats().Starting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Starting was never reported while the initial sync was blocked")
		}
		time.Sleep(2 * time.Millisecond)
	}

	close(be.startupBlock)
	<-done
	if eng.Roots()[0].Stats().Starting {
		t.Error("Starting still set after the initial sync finished")
	}
}
