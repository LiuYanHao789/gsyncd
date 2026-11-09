package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// BenchmarkReadDirEntriesLarge measures the metadata path that dominates a
// dataset with very large directories: one getdents plus one stat per entry.
func BenchmarkReadDirEntriesLarge(b *testing.B) {
	dir := b.TempDir()
	const n = 10000
	for i := 0; i < n; i++ {
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("f%06d", i)))
		if err != nil {
			b.Fatal(err)
		}
		if err := f.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ents := readDirEntries(dir)
		if len(ents) != n {
			b.Fatalf("read %d entries, want %d", len(ents), n)
		}
	}
}
