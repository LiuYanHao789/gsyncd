package sync

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

func write(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDirectStartupMirrors(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a/b/c.txt"), "c")
	write(t, filepath.Join(src, "top.txt"), "t")
	// An extra file on the target must be pruned by delete=true.
	write(t, filepath.Join(dst, "stale.txt"), "old")

	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a/b/c.txt")); err != nil || string(b) != "c" {
		t.Fatalf("nested file: %v %q", err, b)
	}
	if _, err := os.Stat(filepath.Join(dst, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale file survived: %v", err)
	}
}

// A source root that does not exist must fail the sync instead of reporting
// success over an untouched target, and it must never be mistaken for an empty
// tree - that would let pruneDir wipe the whole mirror when a filesystem is not
// mounted.
func TestStartupRefusesMissingSourceRoot(t *testing.T) {
	src := filepath.Join(t.TempDir(), "gone")
	dst := t.TempDir()
	write(t, filepath.Join(dst, "keep.txt"), "k")

	d := NewDirect(src, dst, true, 2, nil)
	err := d.Startup(context.Background())
	if err == nil {
		t.Fatal("Startup over a missing source root reported success")
	}
	if !strings.Contains(err.Error(), "refusing to sync") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, serr := os.Stat(filepath.Join(dst, "keep.txt")); serr != nil {
		t.Fatalf("target was modified despite the missing source: %v", serr)
	}
}

// The same guard has to hold for a full resync triggered by an event, because
// that path goes through Apply -> Startup with no operator watching.
func TestFullBatchRefusesMissingSourceRoot(t *testing.T) {
	src := filepath.Join(t.TempDir(), "gone")
	dst := t.TempDir()
	write(t, filepath.Join(dst, "keep.txt"), "k")

	d := NewDirect(src, dst, true, 2, nil)
	if err := d.Apply(context.Background(), Batch{Full: true}); err == nil {
		t.Fatal("full resync over a missing source root reported success")
	}
	if _, serr := os.Stat(filepath.Join(dst, "keep.txt")); serr != nil {
		t.Fatalf("target was wiped by a full resync with a missing source: %v", serr)
	}
}

// Excluded paths must be absent from the initial copy as well, not just from
// the incremental event stream.
func TestDirectStartupHonoursIgnore(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "keep.txt"), "k")
	write(t, filepath.Join(src, "a.tmp"), "t")
	write(t, filepath.Join(src, "cache/x.bin"), "x")

	d := NewDirect(src, dst, false, 2, nil)
	d.Ignore = func(rel string, dir bool) bool { return rel == "a.tmp" || rel == "cache" }
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "keep.txt")); err != nil {
		t.Fatalf("kept file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "a.tmp")); !os.IsNotExist(err) {
		t.Fatalf("excluded file was copied: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "cache")); !os.IsNotExist(err) {
		t.Fatalf("excluded directory was descended into: %v", err)
	}
}

func TestDirectApply(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 2, nil)
	ctx := context.Background()

	write(t, filepath.Join(src, "keep.txt"), "v1")
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}

	// modify
	write(t, filepath.Join(src, "keep.txt"), "v2")
	// create
	write(t, filepath.Join(src, "new.txt"), "n")
	// delete
	write(t, filepath.Join(src, "gone.txt"), "g")

	batch := Batch{Changes: []aggregate.Change{
		{Path: filepath.Join(src, "keep.txt"), Op: event.Write},
		{Path: filepath.Join(src, "new.txt"), Op: event.Create},
		{Path: filepath.Join(src, "gone.txt"), Op: event.Delete},
	}}
	if err := d.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "keep.txt")); string(b) != "v2" {
		t.Fatalf("modify not applied: %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "new.txt")); string(b) != "n" {
		t.Fatalf("create not applied: %q", b)
	}
	if _, err := os.Stat(filepath.Join(dst, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("delete not applied: %v", err)
	}
}

// TestSameTickWriteStillCopies pins the bug found on the real Linux target: the
// filesystem there has 1 ms timestamp resolution, so two writes of equal length
// inside one tick leave size and mtime identical. A stat-based quick skip then
// decided the file had not changed and the target kept the old content forever.
func TestSameTickWriteStillCopies(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 1, nil)
	ctx := context.Background()

	p := filepath.Join(src, "tick.txt")
	write(t, p, "v1")
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}

	// Rewrite immediately: same length, very likely the same mtime tick.
	write(t, p, "v2")
	srcInfo, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	dstInfo, err := os.Stat(filepath.Join(dst, "tick.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if srcInfo.ModTime().Equal(dstInfo.ModTime()) && srcInfo.Size() == dstInfo.Size() {
		t.Log("source and destination still look identical to stat; the copy must happen anyway")
	}

	batch := Batch{Changes: []aggregate.Change{{Path: p, Op: event.Write}}}
	if err := d.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "tick.txt")); string(b) != "v2" {
		t.Fatalf("a write event must always be copied, got %q", b)
	}
}

// TestAttribOnlySkipsIdenticalFile keeps the fast path honest: an event that
// carries no content change may be skipped when stat says nothing differs.
func TestAttribOnlySkipsIdenticalFile(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 1, nil)
	ctx := context.Background()
	p := filepath.Join(src, "attr.txt")
	write(t, p, "body")
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	before := d.Stats().Files
	if err := d.Apply(ctx, Batch{Changes: []aggregate.Change{{Path: p, Op: event.Attrib}}}); err != nil {
		t.Fatal(err)
	}
	if after := d.Stats().Files; after != before {
		t.Fatalf("an attribute-only event on an identical file should not recopy, files %d -> %d", before, after)
	}
}

func TestDirectRename(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 1, nil)
	ctx := context.Background()
	write(t, filepath.Join(src, "old.txt"), "payload")
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	// Simulate the local mv: source renamed, event carries From.
	if err := os.Rename(filepath.Join(src, "old.txt"), filepath.Join(src, "new.txt")); err != nil {
		t.Fatal(err)
	}
	batch := Batch{Changes: []aggregate.Change{{
		Path: filepath.Join(src, "new.txt"),
		From: filepath.Join(src, "old.txt"),
		Op:   event.Move,
	}}}
	if err := d.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("rename left the old name behind: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "new.txt")); err != nil || string(b) != "payload" {
		t.Fatalf("rename result: %v %q", err, b)
	}
}

func TestDirectApplyNoTempLeftovers(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, false, 4, nil)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		write(t, filepath.Join(src, "f"+string(rune('a'+i%26))+".txt"), "x")
	}
	if err := d.Apply(ctx, Batch{Full: true}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".gsyncd-tmp-") {
			t.Fatalf("temporary file left behind: %s", e.Name())
		}
	}
}

// A path outside the source must never be joined onto the target: filepath.Join
// happily turns "../../etc" into something outside the destination.
func TestDirectRejectsPathsOutsideSource(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 1, nil)
	ctx := context.Background()
	outside := filepath.Join(filepath.Dir(src), "not-in-source.txt")
	write(t, outside, "x")

	err := d.Apply(ctx, Batch{Changes: []aggregate.Change{{Path: outside, Op: event.Write}}})
	if err != nil {
		t.Fatalf("outside paths must be skipped, not reported as a transfer error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "not-in-source.txt")); !os.IsNotExist(err) {
		t.Fatalf("a path outside the source was written into the target: %v", err)
	}
	if b, err := os.ReadFile(outside); err != nil || string(b) != "x" {
		t.Fatalf("the file outside the source was damaged: %v %q", err, b)
	}
}

// A directory moved in from outside the tree reaches applyRename with no
// counterpart on the target, so the whole subtree is copied by copyTree. That
// fallback must apply the same exclusions as the initial walk: a single mv of a
// parent directory would otherwise smuggle every *.tmp and cache/ below it onto
// the target.
func TestRenameFallbackCopyHonoursIgnore(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	d := NewDirect(src, dst, true, 2, nil)
	d.Ignore = func(rel string, dir bool) bool {
		if dir && (rel == "incoming/cache" || rel == "incoming/nested/cache") {
			return true
		}
		return filepath.Base(rel) == "drop.tmp"
	}
	ctx := context.Background()

	incoming := filepath.Join(src, "incoming")
	write(t, filepath.Join(incoming, "keep.txt"), "k")
	write(t, filepath.Join(incoming, "drop.tmp"), "x")
	write(t, filepath.Join(incoming, "cache/big.bin"), "x")
	write(t, filepath.Join(incoming, "nested/cache/deep.bin"), "x")
	write(t, filepath.Join(incoming, "nested/keep2.txt"), "k2")

	batch := Batch{Changes: []aggregate.Change{{
		Path: incoming,
		From: filepath.Join(src, "was-incoming"),
		Op:   event.Move,
		Kind: aggregate.KindDir,
	}}}
	if err := d.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"incoming/keep.txt", "incoming/nested/keep2.txt"} {
		if _, err := os.Stat(filepath.Join(dst, want)); err != nil {
			t.Fatalf("%s should have been copied: %v", want, err)
		}
	}
	for _, gone := range []string{"incoming/drop.tmp", "incoming/cache", "incoming/nested/cache"} {
		if _, err := os.Stat(filepath.Join(dst, gone)); !os.IsNotExist(err) {
			t.Fatalf("%s should have been excluded from the fallback copy: %v", gone, err)
		}
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	prev := time.Duration(0)
	for i := 0; i < 12; i++ {
		d := Backoff(i, time.Second, 30*time.Second)
		if d > 40*time.Second {
			t.Fatalf("attempt %d: %s exceeds the cap", i, d)
		}
		if i < 5 && d < prev {
			t.Fatalf("attempt %d went backwards: %s < %s", i, d, prev)
		}
		prev = d
	}
}

// A fresh target lets the initial walk skip the per-file "already identical"
// stat and the per-directory "already exists" lstat. That is only sound if the
// copy is still complete, so pin both halves: everything arrives, and a second
// pass over the now populated target copies nothing more.
func TestFreshTargetCopiesAllThenResumes(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	for i := 0; i < 20; i++ {
		name := "f" + strconv.Itoa(i) + ".txt"
		write(t, filepath.Join(src, "d", strconv.Itoa(i%7), name), "body"+strconv.Itoa(i))
	}
	d := NewDirect(src, dst, true, 4, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().Files; got != 20 {
		t.Fatalf("first pass copied %d files, want 20", got)
	}

	// Second pass: the target is no longer empty, so the stat path is active
	// again and every file matches by size+mtime.
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().Files; got != 20 {
		t.Fatalf("resume copied %d files total, want 20 (nothing new on an unchanged tree)", got)
	}
}

// The resume path must still notice a file that changed while nothing was
// watching - the case that makes a restarted mirror catch up.
func TestStartupResumeCopiesChangedFile(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "keep.txt"), "v1")
	write(t, filepath.Join(src, "other.txt"), "o")

	d := NewDirect(src, dst, false, 2, nil)
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := d.Stats().Files

	write(t, filepath.Join(src, "keep.txt"), "v2-longer")
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().Files - before; got != 1 {
		t.Fatalf("resume copied %d files, want exactly 1", got)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "keep.txt")); err != nil || string(b) != "v2-longer" {
		t.Fatalf("changed file not refreshed: %v %q", err, b)
	}
}

func TestMkdirOnce(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "x", "y", "z")
	if err := mkdirOnce(nested, 0o755); err != nil {
		t.Fatalf("missing parents: %v", err)
	}
	// Second call must be a no-op rather than an error.
	if err := mkdirOnce(nested, 0o755); err != nil {
		t.Fatalf("existing dir: %v", err)
	}
	if st, err := os.Stat(nested); err != nil || !st.IsDir() {
		t.Fatalf("dir not created: %v", err)
	}
}

func TestTargetIsEmpty(t *testing.T) {
	dir := t.TempDir()
	if !targetIsEmpty(dir) {
		t.Fatal("empty dir reported as non-empty")
	}
	write(t, filepath.Join(dir, "f"), "x")
	if targetIsEmpty(dir) {
		t.Fatal("non-empty dir reported as empty")
	}
	if targetIsEmpty(filepath.Join(dir, "does-not-exist")) {
		t.Fatal("missing dir reported as empty; nothing can be skipped there")
	}
}

func TestOrderRenamesVacatesDestinationFirst(t *testing.T) {
	renames := []aggregate.Change{
		{Path: "/r/x/z", From: "/r/a"},
		{Path: "/r/y", From: "/r/x", Kind: aggregate.KindDir},
	}
	got := orderRenames(renames)
	if len(got) != 2 || got[0].From != "/r/x" || got[1].From != "/r/a" {
		t.Fatalf("x->y must run first so a->x/z lands in the new x: %+v", got)
	}
}

func TestOrderRenamesAncestorFirst(t *testing.T) {
	renames := []aggregate.Change{
		{Path: "/r/e/f", From: "/r/d/f"},
		{Path: "/r/e", From: "/r/d", Kind: aggregate.KindDir},
	}
	got := orderRenames(renames)
	if got[0].From != "/r/d" {
		t.Fatalf("ancestor directory rename must be first: %+v", got)
	}
}

func TestOrderRenamesMarksSwapCycle(t *testing.T) {
	renames := []aggregate.Change{
		{Path: "/r/a", From: "/r/b"},
		{Path: "/r/b", From: "/r/a"},
	}
	got := orderRenames(renames)
	for _, c := range got {
		if !c.FromCycle || !c.KeepSource {
			t.Fatalf("swap cycle must copy from source and keep both names: %+v", got)
		}
	}
}

func TestOrderRenamesChainContinuation(t *testing.T) {
	// Readd can produce c<-f and f<-e in one batch; f must be filled before c
	// consumes it, otherwise the stale f survives on the target.
	renames := []aggregate.Change{
		{Path: "/r/c", From: "/r/f"},
		{Path: "/r/f", From: "/r/e"},
	}
	got := orderRenames(renames)
	if got[0].Path != "/r/f" {
		t.Fatalf("f<-e must run before c<-f: %+v", got)
	}
}

func TestOrderRenamesAtomicReplaceCopiesConsumer(t *testing.T) {
	// mv x y && mv z x: y must be copied from source (old x), not renamed from
	// a target x that z has already overwritten.
	renames := []aggregate.Change{
		{Path: "/r/y", From: "/r/x", At: time.Unix(1, 0)},
		{Path: "/r/x", From: "/r/z", At: time.Unix(2, 0)},
	}
	got := orderRenames(renames)
	for _, c := range got {
		if c.Path == "/r/y" {
			if !c.FromCycle || !c.KeepSource {
				t.Fatalf("consumer of a replaced source must copy: %+v", got)
			}
		}
		if c.Path == "/r/x" && c.FromCycle {
			t.Fatalf("producer should remain a rename: %+v", got)
		}
	}
}
