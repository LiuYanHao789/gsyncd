package aggregate

import (
	"testing"
	"time"

	"gsyncd/internal/event"
)

// A path that is written to forever never produces a quiet window. Overdue is
// the only thing that turns its stream of events into periodic copies, so it
// must fire on the age of the oldest pending change and not on the newest.
func TestOverdueFiresOnOldestPending(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	a := New(Options{Now: func() time.Time { return base }})

	a.Add(event.Event{Path: "/r/log", Op: event.Write, At: base})
	if a.Overdue(base.Add(59*time.Minute), time.Hour) {
		t.Fatal("must not be overdue before the hold elapsed")
	}
	if !a.Overdue(base.Add(time.Hour), time.Hour) {
		t.Fatal("must be overdue once the oldest change reached the hold")
	}

	// A later write to the same path keeps the original timestamp: the change
	// has been pending since the first write, and resetting the clock on every
	// write is exactly the bug that made a busy file never sync.
	a.Add(event.Event{Path: "/r/log", Op: event.Write, At: base.Add(30 * time.Minute)})
	if !a.Overdue(base.Add(time.Hour), time.Hour) {
		t.Fatal("a later write must not push the deadline back")
	}
}

func TestOverdueDisabledAndEmpty(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	a := New(Options{})
	if a.Overdue(base.Add(24*time.Hour), time.Hour) {
		t.Fatal("an empty window is never overdue")
	}
	a.Add(event.Event{Path: "/r/f", Op: event.Write, At: base})
	if a.Overdue(base.Add(24*time.Hour), 0) {
		t.Fatal("hold=0 must disable the bound")
	}
	if a.Overdue(base.Add(24*time.Hour), -time.Second) {
		t.Fatal("a negative hold must disable the bound")
	}
}

// Flush resets the window, so the next window must start its own clock.
func TestOverdueResetsOnFlush(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	a := New(Options{})
	a.Add(event.Event{Path: "/r/f", Op: event.Write, At: base})
	a.Flush()
	if a.Overdue(base.Add(48*time.Hour), time.Hour) {
		t.Fatal("a flushed window must not report overdue")
	}
	a.Add(event.Event{Path: "/r/g", Op: event.Write, At: base.Add(48 * time.Hour)})
	if a.Overdue(base.Add(48*time.Hour+time.Minute), time.Hour) {
		t.Fatal("the new window must measure from its own oldest entry")
	}
}
