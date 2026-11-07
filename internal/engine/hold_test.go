package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/logx"
)

// A file that is written to continuously never lets the delay expire, so
// without a hold bound it is never copied at all (issues #77, #458). This drives
// the clock by hand: writes keep arriving, the tree is never quiet, and only the
// age of the oldest pending change can close the window.
func TestMaxHoldFlushesAFileThatIsNeverQuiet(t *testing.T) {
	var ns int64
	t0 := time.Unix(1_700_000_000, 0)
	atomic.StoreInt64(&ns, t0.UnixNano())
	now := func() time.Time { return time.Unix(0, atomic.LoadInt64(&ns)) }

	be := &fakeBackend{}
	e, err := New([]Spec{{
		Name:    "t",
		Source:  "/src",
		Delay:   5 * time.Minute,
		MaxHold: 30 * time.Second,
		Backend: be,
	}}, Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour, Now: now})
	if err != nil {
		t.Fatal(err)
	}

	e.Handle(event.Event{Path: "/src/log", Op: event.Write, At: now()})
	if n := len(be.snapshot()); n != 0 {
		t.Fatalf("nothing should have been copied yet, got %d batches", n)
	}

	// Writes keep coming, so lastEvent is never old enough for the delay.
	atomic.StoreInt64(&ns, t0.Add(10*time.Second).UnixNano())
	e.Handle(event.Event{Path: "/src/log", Op: event.Write, At: now()})
	e.Tick(now())
	e.Wait()
	if n := len(be.snapshot()); n != 0 {
		t.Fatalf("delay has not elapsed, got %d batches", n)
	}

	// The oldest pending change is now 40s old: past the hold, still well
	// inside the delay. The hold is what has to fire.
	atomic.StoreInt64(&ns, t0.Add(40*time.Second).UnixNano())
	e.Tick(now())
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 {
		t.Fatalf("want exactly one batch from the hold bound, got %+v", got)
	}
	if got[0].Full {
		t.Fatal("a hold must not escalate to a full resync")
	}
	if len(got[0].Changes) != 1 || got[0].Changes[0].Path != "/src/log" {
		t.Fatalf("want the busy path, got %+v", got[0].Changes)
	}
}

// The bound is opt-out: a negative value restores "wait for silence forever".
func TestMaxHoldDefaultsAndOptOut(t *testing.T) {
	be := &fakeBackend{}
	e, err := New([]Spec{{Name: "t", Source: "/src", Backend: be}},
		Options{Log: logx.NewStderr(logx.Error)})
	if err != nil {
		t.Fatal(err)
	}
	if got := e.Roots()[0].MaxHold; got != DefaultMaxHold {
		t.Fatalf("zero must select the default, got %v", got)
	}

	e2, err := New([]Spec{{Name: "t", Source: "/src", Backend: be, MaxHold: -time.Second}},
		Options{Log: logx.NewStderr(logx.Error)})
	if err != nil {
		t.Fatal(err)
	}
	if got := e2.Roots()[0].MaxHold; got != 0 {
		t.Fatalf("a negative value must disable the bound, got %v", got)
	}
}
