package sync

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// The point of copyBounded is throughput on real devices, and the difference
// only shows up when the destination is a file on disk: writing to a
// bytes.Buffer measures memory bandwidth and both implementations look equal.
// These benchmarks exist so the choice of 1 MiB can be re-measured on whatever
// machine is being tuned, instead of being taken on faith.
const benchCopySize = 64 << 20

func benchSource(tb testing.TB) []byte {
	tb.Helper()
	buf := make([]byte, benchCopySize)
	for i := range buf {
		buf[i] = byte('a' + i%26)
	}
	return buf
}

func benchWrite(tb testing.TB, dir string, copyFn func(dst io.Writer, src io.Reader, n int64) error, data []byte) {
	tb.Helper()
	dst := filepath.Join(dir, "out.bin")
	f, err := os.Create(dst)
	if err != nil {
		tb.Fatal(err)
	}
	if err := copyFn(f, bytes.NewReader(data), int64(len(data))); err != nil {
		f.Close()
		tb.Fatal(err)
	}
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := os.Remove(dst); err != nil {
		tb.Fatal(err)
	}
}

func BenchmarkCopyBounded(b *testing.B) {
	dir := b.TempDir()
	data := benchSource(b)
	b.SetBytes(benchCopySize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchWrite(b, dir, func(dst io.Writer, src io.Reader, n int64) error {
			_, err := copyBounded(dst, src, n, nil)
			return err
		}, data)
	}
}

func BenchmarkCopyNBounded(b *testing.B) {
	dir := b.TempDir()
	data := benchSource(b)
	b.SetBytes(benchCopySize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchWrite(b, dir, func(dst io.Writer, src io.Reader, n int64) error {
			_, err := io.CopyN(dst, src, n)
			return err
		}, data)
	}
}

// Short copies must not be reported as complete: a truncated source has to be
// visible to the caller, which stamps the size it stat'ed on the result.
func TestCopyBoundedStopsAtN(t *testing.T) {
	src := bytes.NewReader(bytes.Repeat([]byte("x"), 5000))
	var dst bytes.Buffer
	n, err := copyBounded(&dst, src, 3000, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3000 || dst.Len() != 3000 {
		t.Fatalf("copied %d bytes (dst %d), want 3000", n, dst.Len())
	}
}

// A file that shrank between stat and copy ends at EOF early. That is not an
// error: the next event brings whatever the file grew into.
func TestCopyBoundedToleratesShortSource(t *testing.T) {
	src := bytes.NewReader([]byte("short"))
	var dst bytes.Buffer
	n, err := copyBounded(&dst, src, 4096, nil)
	if err != nil {
		t.Fatalf("short source returned an error: %v", err)
	}
	if n != 5 || dst.String() != "short" {
		t.Fatalf("copied %d bytes %q, want 5 %q", n, dst.String(), "short")
	}
}

// Zero-length files are common (lock files, placeholders) and must not touch
// the pool or the destination at all.
func TestCopyBoundedZeroLength(t *testing.T) {
	var dst bytes.Buffer
	n, err := copyBounded(&dst, bytes.NewReader([]byte("ignored")), 0, nil)
	if err != nil || n != 0 || dst.Len() != 0 {
		t.Fatalf("n=%d err=%v dst=%d, want 0/nil/0", n, err, dst.Len())
	}
}

// The pooled buffer is handed back with its full capacity. If a small copy
// shrank it permanently, every later user of that pool entry would copy in
// tiny slices and quietly lose the throughput this whole change is about.
func TestCopyBoundedReturnsFullBufferToPool(t *testing.T) {
	var dst bytes.Buffer
	if _, err := copyBounded(&dst, bytes.NewReader([]byte("abc")), 3, nil); err != nil {
		t.Fatal(err)
	}
	bp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bp)
	if len(*bp) != copyBufferSize {
		t.Fatalf("pooled buffer came back with length %d, want %d", len(*bp), copyBufferSize)
	}
}
