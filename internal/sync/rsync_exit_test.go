package sync

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

// Exit 24 means "some source files vanished before rsync read them". During a
// mass delete that is the expected outcome of every batch, not a failure:
// treating it as fatal backs off the retry gate and schedules a full rescan for
// a transfer that actually delivered everything still present (issue #695).
func TestBenignExitCodes(t *testing.T) {
	r := NewRSync("/src", "/dst", true, nil, nil)
	cases := []struct {
		code  int
		benig bool
	}{
		{0, false}, // not an error at all
		{1, false}, // syntax or usage error: must stay fatal
		{23, true}, // partial transfer
		{24, true}, // source files vanished
		{12, false},
		{255, false},
		{-1, false}, // timeout / killed child: no exit status
	}
	for _, c := range cases {
		err := error(nil)
		if c.code >= 0 {
			err = &ExitError{Code: c.code, Tail: "boom"}
		}
		if got := r.benignExit(err); got != c.benig {
			t.Fatalf("benignExit(exit %d) = %v, want %v", c.code, got, c.benig)
		}
	}
}

func TestExitCodeUnwraps(t *testing.T) {
	if got := ExitCode(&ExitError{Code: 24}); got != 24 {
		t.Fatalf("ExitCode = %d, want 24", got)
	}
	if got := ExitCode(nil); got != -1 {
		t.Fatalf("ExitCode(nil) = %d, want -1", got)
	}
	if got := ExitCode(ErrTimeout); got != -1 {
		t.Fatalf("ExitCode(ErrTimeout) = %d, want -1", got)
	}
}

// The real thing: a child that exits 24 must leave Apply returning nil, so the
// engine sees a delivered batch and does not enter the retry gate.
func TestApplyTreatsExit24AsDelivered(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell to fake an rsync exit status")
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "rsync")
	script := "#!/bin/sh\nexit 24\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GSYNCD_RSYNC", fake)

	r := NewRSync(dir, filepath.Join(dir, "dst"), true, nil, nil)
	r.Timeout = 30 * time.Second
	changes := []aggregate.Change{
		{Path: filepath.Join(dir, "a.txt"), Op: event.Write | event.Create},
		{Path: filepath.Join(dir, "b.txt"), Op: event.Write | event.Create},
	}
	if err := r.Apply(context.Background(), Batch{Changes: changes}); err != nil {
		t.Fatalf("Apply returned %v; exit 24 during a mass delete must not fail the batch", err)
	}
	if got := r.Stats().Files; got != 2 {
		t.Fatalf("Files = %d, want 2: a partial transfer still moved the paths that survived", got)
	}
}

// Exit 1 stays fatal: it is a usage error and retrying it forever is exactly
// the busy loop of issue #250.
func TestApplyKeepsExit1Fatal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell to fake an rsync exit status")
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "rsync")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GSYNCD_RSYNC", fake)

	r := NewRSync(dir, filepath.Join(dir, "dst"), true, nil, nil)
	r.Timeout = 30 * time.Second
	err := r.Apply(context.Background(), Batch{Changes: []aggregate.Change{
		{Path: filepath.Join(dir, "a.txt"), Op: event.Write | event.Create},
	}})
	if err == nil {
		t.Fatal("Apply swallowed exit 1")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("ExitCode(err) = %d, want 1", got)
	}
	if r.Stats().Errors != 1 {
		t.Fatalf("Errors = %d, want 1", r.Stats().Errors)
	}
}

// Exit 23 means some paths were not transferred. The batch must be reported so
// the engine re-adds it; swallowing it leaves those paths missing until a full
// resync.
func TestApplyReportsExit23ForRetry(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("needs a POSIX shell to fake an rsync exit status")
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "rsync")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 23\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GSYNCD_RSYNC", fake)
	r := NewRSync(dir, filepath.Join(dir, "dst"), true, nil, nil)
	r.Timeout = 30 * time.Second
	err := r.Apply(context.Background(), Batch{Changes: []aggregate.Change{
		{Path: filepath.Join(dir, "a.txt"), Op: event.Write | event.Create},
	}})
	if err == nil {
		t.Fatal("Apply swallowed exit 23; the batch would never be retried")
	}
	if got := ExitCode(err); got != 23 {
		t.Fatalf("ExitCode(err) = %d, want 23", got)
	}
}
