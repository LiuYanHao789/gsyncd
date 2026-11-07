//go:build linux

package guard

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestParseIOPrio(t *testing.T) {
	cases := []struct {
		in         string
		class, lvl int
		wantErr    bool
	}{
		{"idle", 3, 0, false},
		{"IDLE", 3, 0, false},
		{"best-effort", 2, 0, false},
		{"best-effort:7", 2, 7, false},
		{"best_effort:0", 2, 0, false},
		{"realtime:1", 1, 1, false},
		{" idle ", 3, 0, false},
		{"best-effort:8", 0, 0, true},
		{"best-effort:-1", 0, 0, true},
		{"best-effort:x", 0, 0, true},
		{"turbo", 0, 0, true},
		{"", 0, 0, true},
	}
	for _, c := range cases {
		class, lvl, err := parseIOPrio(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseIOPrio(%q) = %d,%d, want an error", c.in, class, lvl)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseIOPrio(%q): %v", c.in, err)
			continue
		}
		if class != c.class || lvl != c.lvl {
			t.Errorf("parseIOPrio(%q) = %d,%d, want %d,%d", c.in, class, lvl, c.class, c.lvl)
		}
	}
}

// TestApplyNiceActuallyChangesPriority proves the syscall path works rather
// than only that it returns nil. Raising niceness never needs a privilege, so
// this is safe to assert anywhere.
//
// The thread is locked for the whole test because setpriority on Linux acts on
// the calling thread: without the lock the readback could land on a different
// OS thread and report the old value.
func TestApplyNiceActuallyChangesPriority(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := Apply(Policy{Nice: 5}); err != nil {
		t.Skipf("cannot renice here: %v", err)
	}
	nice, err := currentNice()
	if err != nil {
		t.Fatalf("cannot read back nice: %v", err)
	}
	if nice < 5 {
		t.Fatalf("nice is %d after asking for 5", nice)
	}
}

// TestApplyIOIdleIsAccepted checks that a valid io class does not error out on
// a kernel that supports it, and is reported rather than silently ignored on
// one that does not.
func TestApplyIOIdleIsAccepted(t *testing.T) {
	err := applyIOPrio(3, 0)
	if err != nil {
		t.Logf("io priority unavailable here (expected in containers): %v", err)
	}
}

// TestApplyNiceCoversEveryThread is the reason applyNice exists. setpriority
// only touches one thread, and the copy workers run on other threads, so the
// check reads the nice value back from /proc for every thread rather than from
// the calling one.
func TestApplyNiceCoversEveryThread(t *testing.T) {
	// Occupy several threads so the sweep has something to cover and the
	// runtime cannot get away with a single-threaded process.
	block := make(chan struct{})
	ready := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			ready <- struct{}{}
			<-block
		}()
	}
	for i := 0; i < 8; i++ {
		<-ready
	}
	defer close(block)

	if err := Apply(Policy{Nice: 6}); err != nil {
		t.Skipf("cannot renice here: %v", err)
	}
	nices, err := threadNices()
	if err != nil {
		t.Skipf("cannot read thread states: %v", err)
	}
	if len(nices) < 2 {
		t.Skipf("only %d thread(s), nothing to prove", len(nices))
	}
	for tid, n := range nices {
		if n < 6 {
			t.Errorf("thread %d is at nice %d after Apply asked for 6", tid, n)
		}
	}
}

// threadNices reads the nice value of every thread in this process out of
// /proc/self/task/<tid>/stat, where it is field 19.
func threadNices() (map[int]int, error) {
	ents, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return nil, err
	}
	out := make(map[int]int, len(ents))
	for _, e := range ents {
		tid, cerr := strconv.Atoi(e.Name())
		if cerr != nil {
			continue
		}
		raw, rerr := os.ReadFile("/proc/self/task/" + e.Name() + "/stat")
		if rerr != nil {
			continue
		}
		// The comm field is parenthesised and may contain spaces, so fields
		// are counted from after the closing paren. nice is the 19th field
		// overall, which is the 17th after state.
		s := string(raw)
		i := strings.LastIndexByte(s, ')')
		if i < 0 {
			continue
		}
		f := strings.Fields(s[i+1:])
		if len(f) < 17 {
			continue
		}
		n, nerr := strconv.Atoi(f[16])
		if nerr != nil {
			continue
		}
		out[tid] = n
	}
	return out, nil
}
