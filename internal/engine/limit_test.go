package engine

import (
	"context"
	"testing"
	"time"

	"gsyncd/internal/logx"
)

// A per-sync cap has to actually block: the global max_processes alone lets one
// busy sync take every slot while a small urgent one waits behind it.
func TestPerSyncTransferCapBlocks(t *testing.T) {
	e, err := New([]Spec{{
		Name:         "busy",
		Source:       "/src",
		Delay:        time.Millisecond,
		MaxProcesses: 1,
	}}, Options{Log: logx.New(discard{}, logx.None), MaxProcesses: 8, Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	r := e.Roots()[0]
	ctx := context.Background()

	if !e.acquire(ctx, r) {
		t.Fatal("first acquire must succeed")
	}

	got := make(chan bool, 1)
	go func() { got <- e.acquire(ctx, r) }()
	select {
	case <-got:
		t.Fatal("second acquire must wait for the per-sync slot")
	case <-time.After(80 * time.Millisecond):
	}

	e.release(r)
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("acquire failed after the slot was released")
		}
		e.release(r)
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not proceed after release")
	}
}

// Without a per-sync cap the engine must still work, using only the global one.
func TestNoPerSyncCapUsesGlobalOnly(t *testing.T) {
	e, err := New([]Spec{{Name: "s", Source: "/src", Delay: time.Millisecond}},
		Options{Log: logx.New(discard{}, logx.None), MaxProcesses: 2, Tick: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	r := e.Roots()[0]
	if r.limit != nil {
		t.Fatal("no per-sync cap was configured, limit must be nil")
	}
	if !e.acquire(context.Background(), r) {
		t.Fatal("acquire failed")
	}
	e.release(r)
}
