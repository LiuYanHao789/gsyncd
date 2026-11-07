package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/filter"
	"gsyncd/internal/logx"
)

func newCWEngine(t *testing.T, delay, maxHold time.Duration, closeWrite bool) (*Engine, *Root) {
	t.Helper()
	e, err := New([]Spec{{
		Name:       "cw",
		Source:     "/src",
		Delay:      delay,
		MaxHold:    maxHold,
		CloseWrite: closeWrite,
		Filter:     filter.New(nil),
		Backend:    &fakeBackend{},
	}}, Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return e, e.Roots()[0]
}

// A write whose writer has not closed the file is not copied yet: that is the
// whole point of the option (issue #458).
func TestCloseWriteHoldsFlushWhileFileIsOpen(t *testing.T) {
	be := &fakeBackend{}
	e, _ := New([]Spec{{Name: "cw", Source: "/src", CloseWrite: true, Filter: filter.New(nil), Backend: be}},
		Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	now := time.Now()
	e.Handle(event.Event{Path: "/src/big", Op: event.Write, At: now})
	e.Tick(now.Add(time.Minute))
	e.Wait()
	if got := be.snapshot(); len(got) != 0 {
		t.Fatalf("copied a file that is still open: %+v", got)
	}

	// The close arrives: now the delay decides, and with delay 0 that is at once.
	e.Handle(event.Event{Path: "/src/big", Op: event.Write | event.Close, At: now})
	e.Tick(now.Add(2 * time.Minute))
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 batch after the close, got %d", len(got))
	}
	if len(got[0].Changes) != 1 || got[0].Changes[0].Path != "/src/big" {
		t.Fatalf("batch: %+v", got[0].Changes)
	}
}

// One open file must not hold back the others' copy either way round: the window
// is flushed as a whole, so the closed file waits too, but it waits for the
// close, not for max_hold.
func TestCloseWriteWaitsForEveryOpenFile(t *testing.T) {
	be := &fakeBackend{}
	e, _ := New([]Spec{{Name: "cw", Source: "/src", CloseWrite: true, Filter: filter.New(nil), Backend: be}},
		Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	now := time.Now()
	e.Handle(event.Event{Path: "/src/a", Op: event.Write | event.Close, At: now})
	e.Handle(event.Event{Path: "/src/b", Op: event.Write, At: now})
	e.Tick(now.Add(time.Minute))
	e.Wait()
	if got := be.snapshot(); len(got) != 0 {
		t.Fatalf("flushed while /src/b was still open: %+v", got)
	}
	e.Handle(event.Event{Path: "/src/b", Op: event.Write | event.Close, At: now})
	e.Tick(now.Add(2 * time.Minute))
	e.Wait()
	if got := be.snapshot(); len(got) != 1 {
		t.Fatalf("want 1 batch once both files closed, got %d", len(got))
	}
}

// delay keeps its meaning: a close restarts the quiet timer instead of flushing
// immediately, so a writer that touches the file again is folded into the same
// batch.
func TestCloseWriteStillWaitsForDelayAfterClose(t *testing.T) {
	be := &fakeBackend{}
	e, _ := New([]Spec{{Name: "cw", Source: "/src", Delay: time.Minute, CloseWrite: true, Filter: filter.New(nil), Backend: be}},
		Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	now := time.Now()
	e.Handle(event.Event{Path: "/src/f", Op: event.Write, At: now})
	e.Tick(now.Add(10 * time.Second))
	e.Handle(event.Event{Path: "/src/f", Op: event.Write | event.Close, At: now})
	e.Tick(now.Add(30 * time.Second))
	e.Wait()
	if got := be.snapshot(); len(got) != 0 {
		t.Fatal("flushed before the delay elapsed after the close")
	}
	e.Tick(now.Add(2 * time.Minute))
	e.Wait()
	if got := be.snapshot(); len(got) != 1 {
		t.Fatal("did not flush once the delay elapsed")
	}
}

// max_hold is the escape hatch: a writer that never closes still gets a copy.
// Without it close_write is a mirror that silently stops updating.
func TestCloseWriteMaxHoldForcesFlush(t *testing.T) {
	be := &fakeBackend{}
	e, _ := New([]Spec{{Name: "cw", Source: "/src", MaxHold: 30 * time.Second, CloseWrite: true, Filter: filter.New(nil), Backend: be}},
		Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	now := time.Now()
	e.Handle(event.Event{Path: "/src/held", Op: event.Write, At: now})
	e.Tick(now.Add(10 * time.Second))
	e.Wait()
	if got := be.snapshot(); len(got) != 0 {
		t.Fatalf("flushed before max_hold: %+v", got)
	}
	e.Tick(now.Add(time.Minute))
	e.Wait()
	if got := be.snapshot(); len(got) != 1 {
		t.Fatalf("max_hold did not release the window, got %d batches", len(got))
	}
}

// Lost events are not negotiable: the mirror is already inconsistent, so waiting
// for a close would only extend the window in which it is wrong.
func TestCloseWriteDoesNotHoldBackOverflow(t *testing.T) {
	be := &fakeBackend{}
	e, _ := New([]Spec{{Name: "cw", Source: "/src", CloseWrite: true, Filter: filter.New(nil), Backend: be}},
		Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	now := time.Now()
	e.Handle(event.Event{Path: "/src/open", Op: event.Write, At: now})
	e.Handle(event.Event{Op: event.Overflow, At: now})
	e.Tick(now.Add(time.Minute))
	e.Wait()
	got := be.snapshot()
	if len(got) != 1 || !got[0].Full {
		t.Fatalf("want one full resync despite the open file, got %+v", got)
	}
}

// Off by default: the historical behaviour must survive untouched.
func TestCloseWriteIsOffByDefault(t *testing.T) {
	be := &fakeBackend{}
	e, r := newTestEngine(t, be, 0, nil)
	if r.CloseWrite {
		t.Fatal("close_write is on by default")
	}
	now := time.Now()
	e.Handle(event.Event{Path: "/src/f", Op: event.Write, At: now})
	e.Tick(now.Add(time.Minute))
	e.Wait()
	if got := be.snapshot(); len(got) != 1 {
		t.Fatalf("default behaviour changed: %d batches", len(got))
	}
}

// A configuration that cannot converge is refused at construction time, not
// discovered months later as one file that never made it.
func TestCloseWriteRejectsDisabledMaxHold(t *testing.T) {
	_, err := New([]Spec{{
		Name: "cw", Source: "/src", CloseWrite: true, MaxHold: -1,
		Filter: filter.New(nil), Backend: &fakeBackend{},
	}}, Options{Log: logx.NewStderr(logx.Error)})
	if err == nil {
		t.Fatal("want an error for close_write with max_hold disabled")
	}
	if !strings.Contains(err.Error(), "max_hold") {
		t.Fatalf("error should name the missing setting: %v", err)
	}
}

// A directory create carries no writer, so it must not be able to stall the
// window until max_hold.
func TestCloseWriteIgnoresDirectories(t *testing.T) {
	be := &fakeBackend{}
	e, _ := New([]Spec{{Name: "cw", Source: "/src", CloseWrite: true, Filter: filter.New(nil), Backend: be}},
		Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	now := time.Now()
	e.Handle(event.Event{Path: "/src/d", Op: event.Create, IsDir: true, At: now})
	e.Tick(now.Add(time.Minute))
	e.Wait()
	if got := be.snapshot(); len(got) != 1 {
		t.Fatalf("a directory create stalled the window: %d batches", len(got))
	}
}

// Shutdown must not wait for a close that never comes.
func TestCloseWriteDoesNotBlockShutdown(t *testing.T) {
	be := &fakeBackend{}
	e, _ := New([]Spec{{Name: "cw", Source: "/src", CloseWrite: true, Filter: filter.New(nil), Backend: be}},
		Options{Log: logx.NewStderr(logx.Error), Tick: time.Hour})
	e.Handle(event.Event{Path: "/src/f", Op: event.Write, At: time.Now()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.FlushAll(ctx)
	e.Wait()
	if got := be.snapshot(); len(got) != 1 {
		t.Fatalf("FlushAll did not drain the window on shutdown: %d batches", len(got))
	}
}
