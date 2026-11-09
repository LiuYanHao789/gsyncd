//go:build linux

package sync

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Opening a FIFO blocks until a writer appears. The initial walk must skip
// special files, not hand them to copyFile, or one FIFO wedges a worker and
// the whole pass forever.
func TestFIFOIsSkippedNotOpened(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "regular.txt"), "ok")
	if err := syscall.Mkfifo(filepath.Join(src, "pipe"), 0o644); err != nil {
		t.Skipf("mkfifo unsupported: %v", err)
	}
	d := NewDirect(src, dst, true, 2, nil)
	done := make(chan error, 1)
	go func() { done <- d.Startup(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Startup hung on a FIFO")
	}
	if _, err := os.Lstat(filepath.Join(dst, "pipe")); !os.IsNotExist(err) {
		t.Fatalf("FIFO was mirrored: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "regular.txt")); err != nil {
		t.Fatalf("regular file missing: %v", err)
	}
}
