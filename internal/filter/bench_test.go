package filter

import (
	"fmt"
	"testing"
)

var benchPats = []string{"*.tmp", "*.swp", "/cache/", "node_modules/", "*.log", "/var/lib/mysql/", "*.o", "*.pyc"}

// Every path on a large tree is unique, which is the case the pattern list has
// to be fast for. An earlier version memoised decisions per path; measured here
// it was a net loss (1214 ns/op against 872 ns/op uncached) because the cache
// could never hit, and its 200k-entry cap retained 13 MiB of dead strings.
func BenchmarkMatchUniquePaths(b *testing.B) {
	f := New(benchPats)
	paths := make([]string, 4096)
	for i := range paths {
		paths[i] = fmt.Sprintf("d%03d/d%03d/f%04d.dat", i%100, (i/100)%100, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Match(paths[i%len(paths)], false)
	}
}

func BenchmarkMatchParallel(b *testing.B) {
	f := New(benchPats)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			f.Match(fmt.Sprintf("d%03d/f%04d.dat", i%100, i), false)
			i++
		}
	})
}

// A path with no separator at all is the cheapest possible miss and still has
// to go through the same decision.
func BenchmarkMatchShortPath(b *testing.B) {
	f := New(benchPats)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Match("file.dat", false)
	}
}

// Deep paths exercise the unanchored window scan, which is the worst case for
// the pattern matcher.
func BenchmarkMatchDeepPath(b *testing.B) {
	f := New(benchPats)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Match("a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/file.dat", false)
	}
}

// The realistic exclude list: all one-segment rules, which is what people
// actually write. This is the case that must not allocate.
var singlePats = []string{"*.tmp", "*.swp", "*.log", "*.o", "*.pyc", "*~", "#*#", ".DS_Store"}

func BenchmarkMatchSingleSegmentRules(b *testing.B) {
	f := New(singlePats)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Match("src/internal/engine/engine.go", false)
	}
}

func BenchmarkMatchSingleSegmentParallel(b *testing.B) {
	f := New(singlePats)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			f.Match("src/internal/engine/engine.go", false)
		}
	})
}
