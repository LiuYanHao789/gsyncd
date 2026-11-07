package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gsyncd/internal/scanstate"
)

func benchCtx(tb testing.TB) context.Context { return context.Background() }

// buildBenchTree creates fan^depth directories, each holding files regular
// files of size bytes. Depth 0 is a single leaf directory.
func buildBenchTree(tb testing.TB, root string, fan, depth, files, size int) int {
	tb.Helper()
	payload := make([]byte, size)
	n := 0
	var rec func(dir string, d int)
	rec = func(dir string, d int) {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			tb.Fatal(err)
		}
		n++
		for i := 0; i < files; i++ {
			p := filepath.Join(dir, fmt.Sprintf("f%02d.dat", i))
			if err := os.WriteFile(p, payload, 0o644); err != nil {
				tb.Fatal(err)
			}
		}
		if d == 0 {
			return
		}
		for i := 0; i < fan; i++ {
			rec(filepath.Join(dir, fmt.Sprintf("d%02d", i)), d-1)
		}
	}
	rec(root, depth)
	return n
}

type benchLog struct{ b *testing.B }

func (l benchLog) logf(level, format string, args ...any) {
	if level == "info" {
		l.b.Logf(format, args...)
	}
}

// BenchmarkWarmWalkNoCheckpoint is the baseline: the target is already
// identical, so every file is skipped on a size+mtime stat and every
// directory is read once to answer "exists" and "what must go".
func BenchmarkWarmWalkNoCheckpoint(b *testing.B) {
	dir := b.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	dirs := buildBenchTree(b, src, 10, 2, 5, 2048)
	b.Logf("tree: %d directories", dirs)

	first := NewDirect(src, dst, true, 4, benchLog{b}.logf)
	if err := first.Startup(benchCtx(b)); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := NewDirect(src, dst, true, 4, benchLog{b}.logf)
		if err := d.Startup(benchCtx(b)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWarmWalkCheckpoint is the same tree with a checkpoint from the
// previous run: no target directory is read and no file is stat'ed.
func BenchmarkWarmWalkCheckpoint(b *testing.B) {
	dir := b.TempDir()
	src := filepath.Join(dir, "src")
	dst := filepath.Join(dir, "dst")
	cp := filepath.Join(dir, "scan")
	dirs := buildBenchTree(b, src, 10, 2, 5, 2048)
	b.Logf("tree: %d directories", dirs)

	fp := scanstate.Fingerprint(src, dst, nil)
	logf := benchLog{b}.logf

	first := NewDirect(src, dst, true, 4, logf)
	first.UseCheckpoint(scanstate.Open(cp, fp, logf))
	if err := first.Startup(benchCtx(b)); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := NewDirect(src, dst, true, 4, logf)
		d.UseCheckpoint(scanstate.Open(cp, fp, logf))
		if err := d.Startup(benchCtx(b)); err != nil {
			b.Fatal(err)
		}
	}
}
