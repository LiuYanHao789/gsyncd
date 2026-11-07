package engine

import (
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/filter"
	"gsyncd/internal/logx"
)

func newResyncEngine(t *testing.T, be *fakeBackend, delay time.Duration, maxPending int) *Engine {
	t.Helper()
	e, err := New([]Spec{{
		Name:       "t",
		Source:     "/src",
		Delay:      delay,
		Filter:     filter.New(nil),
		Backend:    be,
		MaxPending: maxPending,
	}}, Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// The periodic resync is the only safety net for subtrees the kernel cannot
// watch (watch table full, or a platform without recursive notification). It
// has to fire on schedule and turn into exactly one full batch.
func TestPeriodicFullResyncFires(t *testing.T) {
	be := &fakeBackend{}
	e := newResyncEngine(t, be, time.Hour, 0)
	e.SetFullResync(time.Minute)
	if d := e.FullResync(); d != time.Minute {
		t.Fatalf("FullResync()=%v want 1m", d)
	}

	now := time.Now()
	e.Tick(now) // arms the timer, must not transfer
	if len(be.snapshot()) != 0 {
		t.Fatal("resync fired immediately")
	}
	e.Tick(now.Add(30 * time.Second))
	if len(be.snapshot()) != 0 {
		t.Fatal("resync fired before the interval elapsed")
	}

	e.Tick(now.Add(2 * time.Minute))
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 batch, got %d", len(got))
	}
	if !got[0].Full {
		t.Fatalf("periodic batch must be a full transfer: %+v", got[0])
	}
}

func TestFullResyncDisabledByZero(t *testing.T) {
	be := &fakeBackend{}
	e := newResyncEngine(t, be, time.Hour, 0)
	e.SetFullResync(0)
	now := time.Now()
	e.Tick(now)
	e.Tick(now.Add(100 * time.Hour))
	e.Wait()
	if n := len(be.snapshot()); n != 0 {
		t.Fatalf("disabled resync transferred %d batches", n)
	}
}

// Reaching the size cap must send the batch early instead of waiting out the
// delay; the whole point is to bound memory on a 100T tree.
func TestSizeCapFlushesEarly(t *testing.T) {
	be := &fakeBackend{}
	e := newResyncEngine(t, be, time.Hour, 2)
	e.Handle(event.Event{Path: "/src/a", Op: event.Write})
	e.Handle(event.Event{Path: "/src/b", Op: event.Write})
	e.Tick(time.Now())
	if n := len(be.snapshot()); n != 0 {
		t.Fatalf("flushed below the cap: %d batches", n)
	}

	e.Handle(event.Event{Path: "/src/c", Op: event.Write})
	e.Tick(time.Now())
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 || len(got[0].Changes) != 3 {
		t.Fatalf("want one batch of 3, got %+v", got)
	}
	if got[0].Full {
		t.Fatal("reaching the size cap is not data loss and must not force a full transfer")
	}
}
