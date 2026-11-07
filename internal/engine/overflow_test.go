package engine

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"gsyncd/internal/event"
	"gsyncd/internal/logx"
)

// syncBuffer is a bytes.Buffer that survives concurrent writes from the
// engine while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A burst that overflows the kernel queue emits one Overflow event per dropped
// batch. Nineteen identical "scheduling a full resync" lines in three seconds
// is noise, not information: the operator needs to know the mirror fell behind,
// once. Everything suppressed is still counted, so nothing disappears silently.
func TestOverflowWarningIsThrottled(t *testing.T) {
	buf := &syncBuffer{}
	e, err := New([]Spec{{
		Name:    "a",
		Source:  "/tmp/a",
		Backend: &fakeBackend{},
	}}, Options{Log: logx.New(buf, logx.Warn), Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		e.Handle(event.Event{Op: event.Overflow, At: time.Now()})
	}
	got := strings.Count(buf.String(), "scheduling a full resync")
	if got != 1 {
		t.Fatalf("want exactly 1 overflow warning, got %d", got)
	}
	if n := e.roots[0].Stats().Overflows; n == 0 {
		t.Fatalf("overflow must still be recorded, Overflows=%d", n)
	}
}

// Two separate bursts are two episodes, and the status counter has to show
// that: counting raw Overflow events would report dozens for one burst, and
// counting only flushes would hide a second burst entirely.
func TestOverflowCountsEpisodesNotEvents(t *testing.T) {
	be := &captureBackend{}
	e, err := New([]Spec{{
		Name:    "a",
		Source:  "/tmp/a",
		Delay:   time.Millisecond,
		Backend: be,
	}}, Options{Log: logx.New(discard{}, logx.None), Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		e.Handle(event.Event{Op: event.Overflow, At: time.Now()})
	}
	if n := e.roots[0].Stats().Overflows; n != 1 {
		t.Fatalf("first burst: want 1 episode, got %d", n)
	}
	// Flush consumes the flag, so the next burst is a new episode.
	e.Tick(time.Now())
	e.Wait()
	if n := e.roots[0].Stats().FullSyncs; n != 1 {
		t.Fatalf("want 1 full resync, got %d", n)
	}
	for i := 0; i < 5; i++ {
		e.Handle(event.Event{Op: event.Overflow, At: time.Now()})
	}
	if n := e.roots[0].Stats().Overflows; n != 2 {
		t.Fatalf("second burst: want 2 episodes, got %d", n)
	}
}
