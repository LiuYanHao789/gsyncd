package main

import (
	"runtime"
	"sync"
)

func runtimeVersion() string { return runtime.Version() }

// phase records what the daemon is doing right now, for the status file.
//
// The status document has three phases that all report near-zero counters: the
// watch installation, the initial transfer and an idle steady state. On a large
// tree the first two last for hours, and without a label they are
// indistinguishable from a hung process - which is exactly how a ten-terabyte
// run was read the first time it was measured.
type phase struct {
	mu sync.Mutex
	v  string
}

func (p *phase) set(s string) {
	p.mu.Lock()
	p.v = s
	p.mu.Unlock()
}

func (p *phase) get() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.v
}
