package engine

import (
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/logx"
)

// Nested roots follow the longest-prefix ownership rule: an event is routed
// by its source path, and the destination is not fanned out to another root.
// A move from the outer tree into a nested tree is therefore applied as a
// rename in the outer mirror; the nested mirror does not observe it. This is
// the same contract TestNestedRootPrefersLongestPrefix pins for ordinary
// events.
func TestNestedRootMoveStaysInSourceRoot(t *testing.T) {
	outer, inner := &captureBackend{}, &captureBackend{}
	eng, err := New([]Spec{
		{Name: "outer", Source: "/data", Backend: outer, Delay: time.Millisecond},
		{Name: "inner", Source: "/data/critical", Backend: inner, Delay: time.Millisecond},
	}, Options{Log: logx.New(discard{}, logx.None), Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Handle(event.Event{
		Path: "/data/critical/f", From: "/data/tmp/f",
		Op: event.Move, At: time.Now(),
	})
	eng.Tick(time.Now().Add(time.Hour))
	eng.Wait()

	ba := outer.snapshot()
	if len(ba) != 1 || len(ba[0].Changes) != 1 ||
		ba[0].Changes[0].Path != "/data/critical/f" ||
		ba[0].Changes[0].From != "/data/tmp/f" ||
		ba[0].Changes[0].Op&event.Move == 0 {
		t.Fatalf("outer root did not get the rename: %+v", ba)
	}
	if got := inner.snapshot(); len(got) != 0 {
		t.Fatalf("nested root must not receive an event routed by the outer source: %+v", got)
	}
}
