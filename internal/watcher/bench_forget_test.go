package watcher

import (
	"fmt"
	"strconv"
	"testing"

	"gsyncd/internal/event"
)

// buildTree fills the watch bookkeeping with a synthetic tree so the maps can
// be exercised without touching a real filesystem. That is exactly what a
// directory deletion pays for.
func buildTree(t testing.TB, roots, fan, depth int) (*Tree, []string) {
	t.Helper()
	tree := New(newLimitBackend(1<<30), Options{Workers: 1})
	t.Cleanup(func() { tree.Close() })
	tree.roots["/r"] = true
	level := make([]string, 0, roots)
	for i := 0; i < roots; i++ {
		level = append(level, "/r/d"+strconv.Itoa(i))
	}
	for d := 0; d < depth; d++ {
		next := make([]string, 0, len(level)*fan)
		for _, p := range level {
			tree.addRef(p)
			for i := 0; i < fan; i++ {
				next = append(next, p+"/d"+strconv.Itoa(i))
			}
		}
		level = next
	}
	for _, p := range level {
		tree.addRef(p)
	}
	return tree, level
}

// BenchmarkLeafDelete measures one directory deletion on a watch table of the
// given size. rm -rf raises one such event per directory, so any cost that is
// linear in the table multiplies by the number of directories removed.
func BenchmarkLeafDelete(b *testing.B) {
	for _, size := range []int{10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("dirs=%d", size), func(b *testing.B) {
			tree, leaves := buildTree(b, size, 1, 0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := leaves[i%len(leaves)]
				tree.handle(event.Event{Path: p, Op: event.Delete, IsDir: true})
			}
		})
	}
}

// BenchmarkMassDeleteTree deletes a whole tree one event at a time, the way the
// kernel reports an rm -rf: ten top level directories, a hundred below them and
// a thousand leaves, so 1110 deletions.
func BenchmarkMassDeleteTree(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree, _ := buildTree(b, 10, 10, 2) // 1110 directories
		all := make([]string, 0, len(tree.dirs))
		for p := range tree.dirs {
			all = append(all, p)
		}
		b.StartTimer()
		for _, p := range all {
			tree.handle(event.Event{Path: p, Op: event.Delete, IsDir: true})
		}
	}
}
