package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

// Two renames onto the same destination inside one window: the fast
// os.Rename path must still delete the earlier source's target copy.
func TestRenameFastPathRemovesPrevFrom(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 1, nil)
	ctx := context.Background()

	write(t, filepath.Join(src, "m1"), "m1")
	write(t, filepath.Join(src, "m3"), "m3")
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	// Source final state: mv m1 m2; mv m3 m2.
	if err := os.Rename(filepath.Join(src, "m1"), filepath.Join(src, "m2")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(src, "m3"), filepath.Join(src, "m2")); err != nil {
		t.Fatal(err)
	}
	c := aggregate.Change{
		Path: filepath.Join(src, "m2"), From: filepath.Join(src, "m3"),
		PrevFrom: []string{filepath.Join(src, "m1")},
		Op:       event.Move, Kind: aggregate.KindFile,
	}
	if err := d.Apply(ctx, Batch{Changes: []aggregate.Change{c}}); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "m2")); err != nil || string(b) != "m3" {
		t.Fatalf("m2 = %q, %v; want m3", b, err)
	}
	for _, gone := range []string{"m1", "m3"} {
		if _, err := os.Stat(filepath.Join(dst, gone)); !os.IsNotExist(err) {
			t.Fatalf("%s must be removed from the target: %v", gone, err)
		}
	}
}

// mv a b && rm b: both names are gone at the source, so both target copies
// must disappear even though the change carries Move.
func TestMoveDeleteRemovesBothNames(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 1, nil)
	ctx := context.Background()

	write(t, filepath.Join(src, "a"), "a")
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(src, "a"), filepath.Join(src, "b")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(src, "b")); err != nil {
		t.Fatal(err)
	}
	c := aggregate.Change{
		Path: filepath.Join(src, "b"), From: filepath.Join(src, "a"),
		Op: event.Move | event.Delete, Kind: aggregate.KindFile,
	}
	if err := d.Apply(ctx, Batch{Changes: []aggregate.Change{c}}); err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{"a", "b"} {
		if _, err := os.Stat(filepath.Join(dst, gone)); !os.IsNotExist(err) {
			t.Fatalf("%s must be removed from the target: %v", gone, err)
		}
	}
}
