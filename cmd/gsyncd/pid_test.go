package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestWritePidFileReplacesStale(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gsyncd.pid")
	// A pid that is certain not to be running: a number far above any real pid.
	stale := 4_000_000
	if pidAlive(stale) {
		t.Skipf("pid %d happens to be in use", stale)
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(stale)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePidFile(path); err != nil {
		t.Fatalf("a stale pid file must not block startup: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("pid file holds %q, want this process", got)
	}
}

func TestWritePidFileRejectsLiveInstance(t *testing.T) {
	// A real second process, so the test proves the check rather than the
	// absence of one: this binary re-executes itself in sleep mode.
	cmd := exec.Command(os.Args[0], "-test.run=TestPidHelperProcess")
	cmd.Env = append(os.Environ(), "GSYNCD_PID_HELPER=1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start helper: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !pidAlive(cmd.Process.Pid) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	path := filepath.Join(t.TempDir(), "gsyncd.pid")
	if err := os.WriteFile(path, []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := writePidFile(path)
	if err == nil {
		t.Fatal("starting next to a live process must be refused")
	}
	if !strings.Contains(err.Error(), "already running") {
		t.Fatalf("unhelpful error: %v", err)
	}
}

// TestPidHelperProcess is not a test: it is the body of the live process the
// test above needs. Without the environment marker it does nothing.
func TestPidHelperProcess(t *testing.T) {
	if os.Getenv("GSYNCD_PID_HELPER") != "1" {
		return
	}
	time.Sleep(30 * time.Second)
}

func TestWritePidFileCreatesDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "dir", "gsyncd.pid")
	if err := writePidFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}
