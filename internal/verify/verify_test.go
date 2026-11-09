package verify

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestTreeIdentical(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a", "b.txt"), "x")
	write(t, filepath.Join(dst, "a", "b.txt"), "x")
	write(t, filepath.Join(dst, ".gsyncd-target"), "marker")
	res, err := Tree(src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK() {
		t.Fatalf("identical trees reported differences: %+v", res)
	}
}

func TestTreeMissingExtraType(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "dir", "keep.txt"), "x")
	write(t, filepath.Join(src, "only-src.txt"), "x")
	write(t, filepath.Join(dst, "dir", "keep.txt"), "x")
	write(t, filepath.Join(dst, "only-dst.txt"), "x")
	write(t, filepath.Join(dst, "type.txt"), "file")
	if err := os.MkdirAll(filepath.Join(src, "type.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
	res, err := Tree(src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Missing != 1 || res.Extra != 1 || res.TypeMismatch != 1 {
		t.Fatalf("want missing=1 extra=1 type=1, got %+v", res)
	}
}

func TestTreeMissingSubtreeCountsAll(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "d", "a"), "a")
	write(t, filepath.Join(src, "d", "sub", "b"), "b")
	res, err := Tree(src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Missing != 4 { // d, d/a, d/sub, d/sub/b
		t.Fatalf("want 4 missing, got %+v", res)
	}
}

func TestTreeIgnoresExcludedAndStaging(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "cache", "x"), "x")
	write(t, filepath.Join(src, "keep"), "k")
	write(t, filepath.Join(dst, "keep"), "k")
	write(t, filepath.Join(dst, ".gsyncd-tmp-123"), "tmp")
	ignore := func(rel string, dir bool) bool { return rel == "cache" || (len(rel) > 6 && rel[:6] == "cache/") }
	res, err := Tree(src, dst, Options{Ignore: ignore})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK() {
		t.Fatalf("excluded/staging paths should not count: %+v", res)
	}
}

func sameMtime(t *testing.T, src, dst string) {
	t.Helper()
	st, err := os.Stat(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(dst, st.ModTime(), st.ModTime()); err != nil {
		t.Fatal(err)
	}
}

func TestContentMatchesAndMismatches(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a"), "same")
	write(t, filepath.Join(dst, "a"), "same")
	sameMtime(t, filepath.Join(src, "a"), filepath.Join(dst, "a"))
	write(t, filepath.Join(src, "b"), "left")
	write(t, filepath.Join(dst, "b"), "right")
	sameMtime(t, filepath.Join(src, "b"), filepath.Join(dst, "b"))
	write(t, filepath.Join(src, "missing"), "gone")
	res, err := Content(src, dst, ContentOptions{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.Checked != 2 || res.Mismatch != 1 || res.Missing != 1 {
		t.Fatalf("want checked=2 mismatch=1 missing=1, got %+v", res)
	}
}

func TestContentDetectsMtimeMismatch(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a"), "same")
	write(t, filepath.Join(dst, "a"), "same")
	st, err := os.Stat(filepath.Join(src, "a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(dst, "a"), st.ModTime(), st.ModTime().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	res, err := Content(src, dst, ContentOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if res.Mismatch != 0 || res.MtimeMismatch != 1 {
		t.Fatalf("want hash=0 mtime=1, got %+v", res)
	}
}

func TestContentSampleIsDeterministic(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	for i := 0; i < 50; i++ {
		name := string(rune('a'+i%26)) + string(rune('0'+i/26))
		write(t, filepath.Join(src, name), "x")
		write(t, filepath.Join(dst, name), "x")
		sameMtime(t, filepath.Join(src, name), filepath.Join(dst, name))
	}
	res, err := Content(src, dst, ContentOptions{Workers: 2, SamplePercent: 10})
	if err != nil {
		t.Fatal(err)
	}
	if res.Checked == 0 || res.Checked >= 50 {
		t.Fatalf("sample checked %d of 50", res.Checked)
	}
}

func TestContentCoarseTimestampGranularity(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	write(t, filepath.Join(src, "a"), "same")
	write(t, filepath.Join(dst, "a"), "same")
	// Same second, different sub-second value: a coarse target cannot store
	// the difference, so it must not be reported.
	base := time.Unix(1700000000, 100_000_000)
	if err := os.Chtimes(filepath.Join(src, "a"), base, base); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(dst, "a"), base, base.Add(800*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	res, err := Content(src, dst, ContentOptions{Workers: 1, MtimeGranularity: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res.MtimeMismatch != 0 {
		t.Fatalf("coarse granularity still reported mtime mismatch: %+v", res)
	}
	res, err = Content(src, dst, ContentOptions{Workers: 1, MtimeGranularity: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.MtimeMismatch != 1 {
		t.Fatalf("nanosecond granularity missed mtime mismatch: %+v", res)
	}
}
