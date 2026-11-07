package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gsyncd/internal/scanstate"
)

// scanLog collects the checkpoint summary line so a test can assert on what the
// walk actually did, not just on the end state of the tree.
type scanLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *scanLog) logf(level, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *scanLog) find(sub string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			return s
		}
	}
	return ""
}

func checkpointDirect(t *testing.T, src, dst, cp string, logf func(string, string, ...any)) *Direct {
	t.Helper()
	d := NewDirect(src, dst, true, 2, logf)
	fp := scanstate.Fingerprint(src, dst, nil)
	d.UseCheckpoint(scanstate.Open(cp, fp, logf))
	return d
}

// The second run over an unchanged tree must not touch the target at all, and
// it must say so: zero files copied, every directory reused.
func TestCheckpointSecondRunCopiesNothing(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a/b/c.txt"), "c")
	write(t, filepath.Join(src, "a/d.txt"), "d")
	write(t, filepath.Join(src, "top.txt"), "t")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()

	first := &scanLog{}
	if err := checkpointDirect(t, src, dst, cp, first.logf).Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cp); err != nil {
		t.Fatalf("first run did not write a checkpoint: %v", err)
	}

	second := &scanLog{}
	d := checkpointDirect(t, src, dst, cp, second.logf)
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().Files; got != 0 {
		t.Fatalf("second run copied %d files, want 0", got)
	}
	line := second.find("scan state:")
	if line == "" {
		t.Fatal("the second run did not report its scan state")
	}
	// "reused" is the number of files whose directory digest still matched, so
	// on an unchanged tree every one of them must have hit. A zero here means
	// the checkpoint was read but never trusted.
	if strings.Contains(line, " 0 reused") {
		t.Fatalf("second run reused nothing: %q", line)
	}
	// The mirror is still correct.
	if b, err := os.ReadFile(filepath.Join(dst, "a/b/c.txt")); err != nil || string(b) != "c" {
		t.Fatalf("nested file: %v %q", err, b)
	}
}

// A modified file changes its directory's digest, so that directory is
// reconciled again and the file is copied. Everything else stays untouched.
func TestCheckpointDetectsModifiedFile(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "keep/a.txt"), "one")
	write(t, filepath.Join(src, "other/b.txt"), "two")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}

	write(t, filepath.Join(src, "keep/a.txt"), "changed")
	d := checkpointDirect(t, src, dst, cp, nil)
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "keep/a.txt")); err != nil || string(b) != "changed" {
		t.Fatalf("modified file not mirrored: %v %q", err, b)
	}
	if got := d.Stats().Files; got != 1 {
		t.Fatalf("copied %d files, want exactly the one that changed", got)
	}
}

// A file created while the process was down changes the directory mtime and the
// digest, so the next run must notice even though it never saw an event.
func TestCheckpointDetectsFileAddedWhileDown(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a.txt"), "a")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}

	write(t, filepath.Join(src, "b.txt"), "b")
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "b.txt")); err != nil || string(b) != "b" {
		t.Fatalf("file added while down was not copied: %v %q", err, b)
	}
}

// A deletion on the source while the process was down must be mirrored too:
// this is the case where trusting the checkpoint without checking the digest
// would leave a stale file on the target forever.
func TestCheckpointDetectsFileDeletedWhileDown(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "gone.txt"), "g")
	write(t, filepath.Join(src, "stay.txt"), "s")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(filepath.Join(src, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("file deleted on the source survived on the target: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "stay.txt")); err != nil {
		t.Fatalf("untouched file was lost: %v", err)
	}
}

// The checkpoint only describes the source. If someone edits the target by
// hand, the target directory's mtime no longer matches the record, and the
// directory is reconciled again instead of being trusted.
func TestCheckpointDetectsTargetTampering(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a.txt"), "a")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}

	// An operator deletes a file from the mirror.
	if err := os.Remove(filepath.Join(dst, "a.txt")); err != nil {
		t.Fatal(err)
	}
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a.txt")); err != nil || string(b) != "a" {
		t.Fatalf("hand deleted file was not restored: %v %q", err, b)
	}
}

// A checkpoint from a different source or target must be ignored outright.
func TestCheckpointIgnoresForeignFingerprint(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a.txt"), "a")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}

	// Same file, different sync: the records describe another tree.
	d := NewDirect(src, filepath.Join(t.TempDir(), "dst2"), true, 2, nil)
	fp := scanstate.Fingerprint(src, "/somewhere/else", nil)
	d.UseCheckpoint(scanstate.Open(cp, fp, nil))
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().Files; got != 1 {
		t.Fatalf("copied %d files, want 1 (records must not be reused)", got)
	}
}

// A corrupt checkpoint must never make the walk skip work: the worst allowed
// outcome is a full re-verification.
func TestCheckpointCorruptFileStillMirrors(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a/b.txt"), "b")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cp, []byte("not a checkpoint at all\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Wipe the target so a skip would be visible.
	if err := os.RemoveAll(dst); err != nil {
		t.Fatal(err)
	}

	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a/b.txt")); err != nil || string(b) != "b" {
		t.Fatalf("corrupt checkpoint caused a skip: %v %q", err, b)
	}
}

// A checkpoint that was never written (unwritable location) must not change
// behaviour at all.
func TestCheckpointUnwritableStillMirrors(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a.txt"), "a")
	bad := filepath.Join(t.TempDir(), "cp")
	if err := os.MkdirAll(bad+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}
	d := NewDirect(src, dst, true, 2, nil)
	d.UseCheckpoint(scanstate.Open(bad, scanstate.Fingerprint(src, dst, nil), nil))
	if err := d.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a.txt")); err != nil || string(b) != "a" {
		t.Fatalf("unwritable checkpoint broke the sync: %v %q", err, b)
	}
}

// Deep nesting is where the record ordering matters: a child record may not be
// written before its parent's, and the parent's digest is not final until the
// whole subtree is done.
func TestCheckpointDeepTreeRoundTrip(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	p := "l1/l2/l3/l4/l5"
	write(t, filepath.Join(src, p, "deep.txt"), "deep")
	write(t, filepath.Join(src, "l1", "mid.txt"), "mid")
	cp := filepath.Join(t.TempDir(), "scan")
	ctx := context.Background()
	if err := checkpointDirect(t, src, dst, cp, nil).Startup(ctx); err != nil {
		t.Fatal(err)
	}

	d := checkpointDirect(t, src, dst, cp, nil)
	if err := d.Startup(ctx); err != nil {
		t.Fatal(err)
	}
	if got := d.Stats().Files; got != 0 {
		t.Fatalf("second run copied %d files, want 0", got)
	}
	if _, err := os.Stat(filepath.Join(dst, p, "deep.txt")); err != nil {
		t.Fatalf("deep file missing: %v", err)
	}
}
