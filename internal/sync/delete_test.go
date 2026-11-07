package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

// delete = "running": the target may be a deliberate superset, so the first
// pass must not prune it, but deletes seen while running are still propagated.
func TestDeleteModeRunningKeepsTargetOnStartup(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a.txt"), "a")
	write(t, filepath.Join(dst, "precious.txt"), "keep me")
	write(t, filepath.Join(dst, "gone.txt"), "delete me")

	d := NewDirect(src, dst, true, 2, nil)
	d.DeleteOnStartup, d.DeleteOnRun = false, true
	ctx := context.Background()
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "precious.txt")); err != nil {
		t.Fatalf("startup pruned the target: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "gone.txt")); err != nil {
		t.Fatalf("startup pruned the target: %v", err)
	}

	batch := Batch{Changes: []aggregate.Change{{Path: filepath.Join(src, "gone.txt"), Op: event.Delete}}}
	if err := d.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("running delete was not propagated: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "precious.txt")); err != nil {
		t.Fatalf("unrelated target entry vanished: %v", err)
	}
}

// delete = "startup": clean up once, then never remove, so a target that gained
// files from elsewhere keeps them.
func TestDeleteModeStartupIgnoresLaterDeletes(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a.txt"), "a")
	write(t, filepath.Join(dst, "stale.txt"), "old")

	d := NewDirect(src, dst, true, 2, nil)
	d.DeleteOnStartup, d.DeleteOnRun = true, false
	ctx := context.Background()
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("startup should have pruned: %v", err)
	}

	write(t, filepath.Join(dst, "a.txt"), "a")
	batch := Batch{Changes: []aggregate.Change{{Path: filepath.Join(src, "a.txt"), Op: event.Delete}}}
	if err := d.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "a.txt")); err != nil {
		t.Fatalf("delete = startup must not remove at runtime: %v", err)
	}
}
