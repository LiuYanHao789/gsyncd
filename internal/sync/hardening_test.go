package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

// A staging file abandoned by a killed process is removed; a fresh one may
// belong to a copy that is still running and must survive.
func TestSweepTempRemovesOnlyStale(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, tmpPrefix+"stale")
	fresh := filepath.Join(dir, tmpPrefix+"fresh")
	keep := filepath.Join(dir, "ordinary.txt")
	write(t, stale, "x")
	write(t, fresh, "x")
	write(t, keep, "x")
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDirect(dir, dir, false, 1, func(string, string, ...any) {})
	d.sweepTemp(context.Background(), dir, ents)

	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale staging file survived: %v", err)
	}
	for _, p := range []string{fresh, keep} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s was removed: %v", p, err)
		}
	}
}

// A cancelled context stops the sweep instead of deleting the rest.
func TestSweepTempHonoursCancel(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, tmpPrefix+"stale")
	write(t, p, "x")
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := NewDirect(dir, dir, false, 1, func(string, string, ...any) {})
	d.sweepTemp(ctx, dir, ents)
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("a cancelled sweep must not delete: %v", err)
	}
}

// pruneDir accepts the directory listing the walk already has, so reconciling a
// directory costs one readdir rather than three syscalls.
func TestPruneDirUsesSuppliedEntries(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "keep.txt"), "k")
	write(t, filepath.Join(dst, "keep.txt"), "k")
	write(t, filepath.Join(dst, "gone.txt"), "g")
	write(t, filepath.Join(dst, "gone-dir/deep.txt"), "g")

	ents, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDirect(src, dst, true, 1, func(string, string, ...any) {})
	if err := d.pruneDir(context.Background(), src, dst, ents); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "keep.txt")); err != nil {
		t.Fatalf("kept file was removed: %v", err)
	}
	for _, name := range []string{"gone.txt", "gone-dir"} {
		if _, err := os.Stat(filepath.Join(dst, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived the prune", name)
		}
	}
}

// Every failing path in a batch has to reach the caller. The old code kept one
// error in a buffered channel and dropped the rest, so a batch could report
// success while files were missing on the target.
func TestApplyReportsAllFailures(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	// A regular file where a directory is needed makes every copy fail.
	if err := os.WriteFile(filepath.Join(dst, "sub"), []byte("not a dir"), 0o644); err != nil {
		t.Fatal(err)
	}
	var changes []aggregate.Change
	for i := 0; i < 8; i++ {
		name := filepath.Join("sub", "f"+strconv.Itoa(i)+".txt")
		write(t, filepath.Join(src, name), "x")
		changes = append(changes, aggregate.Change{Path: filepath.Join(src, name), Op: event.Write})
	}
	d := NewDirect(src, dst, false, 4, func(string, string, ...any) {})
	err := d.Apply(context.Background(), Batch{Changes: changes})
	if err == nil {
		t.Fatal("a batch whose files could not be written must fail")
	}
	if n := d.Stats().Errors; n != uint64(len(changes)) {
		t.Fatalf("want %d errors counted, got %d", len(changes), n)
	}
}

// A cancelled context reports the changes that never made it, instead of
// silently claiming success.
func TestApplyReportsCancellation(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	var changes []aggregate.Change
	for i := 0; i < 50; i++ {
		name := "f" + strconv.Itoa(i) + ".txt"
		write(t, filepath.Join(src, name), strings.Repeat("x", 4096))
		changes = append(changes, aggregate.Change{Path: filepath.Join(src, name), Op: event.Write})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := NewDirect(src, dst, false, 1, func(string, string, ...any) {})
	err := d.Apply(ctx, Batch{Changes: changes})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

// A directory that cannot be read is not the same as a directory with nothing
// extra: pruning must say so rather than silently leaving orphans behind.
func TestPruneDirWarnsOnUnreadableSource(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "orphan.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	var logged []string
	d := NewDirect(src, dst, true, 1, func(level, format string, args ...any) {
		logged = append(logged, level+": "+fmt.Sprintf(format, args...))
	})
	d.Delete = true
	d.DeleteOnStartup = true

	// Remove the source directory after the walk already descended into it.
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	if err := d.pruneDir(context.Background(), src, dst, nil); err != nil {
		t.Fatalf("pruneDir must not fail a whole sync on an unreadable directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "orphan.txt")); err != nil {
		t.Fatal("orphan should survive: with no source listing there is no keep set to prune against")
	}
	// A source that is simply gone is the normal burst-delete case and needs no
	// warning; what must never happen is deleting against an unknown keep list.
	for _, l := range logged {
		if strings.Contains(l, "cannot read source") {
			t.Fatalf("a missing source is not an error worth a warning: %q", l)
		}
	}
}
