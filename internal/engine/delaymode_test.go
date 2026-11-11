package engine

import (
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/logx"
)

// deadline mode is the lsyncd semantic: the oldest pending change flushes
// once it is Delay old, even while new events keep arriving. quiet mode waits
// for the whole tree to fall silent and would not flush here.
func TestDeadlineModeFlushesWhileEventsKeepArriving(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }

	be := &fakeBackend{}
	e, err := New([]Spec{{
		Name:      "t",
		Source:    "/src",
		Delay:     10 * time.Second,
		DelayMode: "deadline",
		Backend:   be,
	}}, Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour, Now: clock})
	if err != nil {
		t.Fatal(err)
	}

	e.Handle(event.Event{Path: "/src/a", Op: event.Write, At: now})
	now = now.Add(9 * time.Second)
	e.Handle(event.Event{Path: "/src/b", Op: event.Write, At: now})
	e.Tick(now)
	e.Wait()
	if n := len(be.snapshot()); n != 0 {
		t.Fatalf("flushed before the oldest change was Delay old: %d batches", n)
	}

	// The newest event is only 2s old, but the oldest is 11s old.
	now = now.Add(2 * time.Second)
	e.Tick(now)
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 {
		t.Fatalf("deadline mode did not flush the busy tree: %+v", got)
	}
	if len(got[0].Changes) != 2 {
		t.Fatalf("expected both pending changes, got %+v", got[0].Changes)
	}
}
