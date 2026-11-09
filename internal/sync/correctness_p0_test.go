package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

func TestMoveWithPendingWriteCopiesContent(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a"), "v1")
	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(src, "a"), "v2")
	if err := os.Rename(filepath.Join(src, "a"), filepath.Join(src, "b")); err != nil {
		t.Fatal(err)
	}
	b := Batch{Changes: []aggregate.Change{{
		Path: filepath.Join(src, "b"), From: filepath.Join(src, "a"),
		Op: event.Move | event.Write,
	}}}
	if err := d.Apply(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dst, "b"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "v2" {
		t.Fatalf("moved file carries stale content: %q", body)
	}
	if _, err := os.Stat(filepath.Join(dst, "a")); !os.IsNotExist(err) {
		t.Fatalf("old name survived: %v", err)
	}
}

func TestDirRecreateRemovesStaleEntries(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "d", "old"), "old")
	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := Batch{Changes: []aggregate.Change{{
		Path: filepath.Join(src, "d"), Op: event.Create,
		Kind: aggregate.KindDir, Recreate: true,
	}}}
	if err := d.Apply(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "d", "old")); !os.IsNotExist(err) {
		t.Fatalf("stale entry survived a directory recreate: %v", err)
	}
	if st, err := os.Stat(filepath.Join(dst, "d")); err != nil || !st.IsDir() {
		t.Fatalf("recreated directory missing: %v", err)
	}
}

func TestDirRenameAppliesRetargetedWrite(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "d", "f"), "v1")
	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(src, "d", "f"), "v2")
	if err := os.Rename(filepath.Join(src, "d"), filepath.Join(src, "e")); err != nil {
		t.Fatal(err)
	}
	b := Batch{Changes: []aggregate.Change{
		{Path: filepath.Join(src, "e"), From: filepath.Join(src, "d"), Op: event.Move, Kind: aggregate.KindDir},
		{Path: filepath.Join(src, "e", "f"), Op: event.Write},
	}}
	if err := d.Apply(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(dst, "e", "f"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "v2" {
		t.Fatalf("write under renamed directory was lost: %q", body)
	}
	if _, err := os.Stat(filepath.Join(dst, "d")); !os.IsNotExist(err) {
		t.Fatalf("old directory name survived: %v", err)
	}
}

func TestSymlinkInternalMirroredExternalSkipped(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "dir", "target"), "x")
	if err := os.Symlink("dir/target", filepath.Join(src, "rel")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := os.Symlink("../../outside", filepath.Join(src, "ext")); err != nil {
		t.Fatal(err)
	}
	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	link, err := os.Readlink(filepath.Join(dst, "rel"))
	if err != nil || link != "dir/target" {
		t.Fatalf("internal symlink not mirrored: %q %v", link, err)
	}
	if _, err := os.Lstat(filepath.Join(dst, "ext")); !os.IsNotExist(err) {
		t.Fatalf("external symlink was mirrored: %v", err)
	}
}

func TestStartupTypeChangeFileToDir(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "x"), "old file")
	write(t, filepath.Join(dst, "x"), "target file")
	if err := os.Remove(filepath.Join(src, "x")); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(src, "x", "child"), "new child")
	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(dst, "x")); err != nil || !st.IsDir() {
		t.Fatalf("source dir did not replace target file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "x", "child")); err != nil {
		t.Fatalf("child missing: %v", err)
	}
}

func TestStartupTypeChangeDirToFile(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "x", "child"), "old child")
	write(t, filepath.Join(dst, "x", "stale"), "stale")
	if err := os.RemoveAll(filepath.Join(src, "x")); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(src, "x"), "new file")
	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(dst, "x"))
	if err != nil || !st.Mode().IsRegular() {
		t.Fatalf("source file did not replace target directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "x", "stale")); err == nil {
		t.Fatal("stale target child survived")
	}
	body, err := os.ReadFile(filepath.Join(dst, "x"))
	if err != nil || string(body) != "new file" {
		t.Fatalf("target file content = %q, %v", body, err)
	}
}

func TestTargetGuardRequiresMarkerOrMount(t *testing.T) {
	dir := t.TempDir()
	if mounted, err := onNonRootMount(dir); err == nil && mounted {
		t.Skip("temp dir is already on a non-root mount")
	}
	if err := CheckTarget(dir); err == nil {
		t.Fatal("unmarked target on the root filesystem was accepted")
	}
	if err := InitTarget(dir); err != nil {
		t.Fatal(err)
	}
	if err := CheckTarget(dir); err != nil {
		t.Fatalf("marked target rejected: %v", err)
	}
	if err := CheckTarget(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing target was accepted")
	}
}

func TestDirectTargetGuardRefusesMissingTarget(t *testing.T) {
	src := t.TempDir()
	write(t, filepath.Join(src, "a"), "x")
	dst := filepath.Join(t.TempDir(), "not-mounted", "target")
	d := NewDirect(src, dst, true, 1, nil)
	d.TargetGuard = true
	if err := d.Startup(context.Background()); err == nil {
		t.Fatal("Startup wrote to a target that does not exist")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("Startup created the target despite the guard: %v", err)
	}
}
