//go:build linux

package watcher

import (
	"fmt"
	"testing"
)

// benchRekeyBackend builds a backend with n watched directories laid out as a
// wide tree, then renames one subtree of it.
//
// The question it answers: inotifyBackend.Rekey rewrites every recorded
// directory name below the old path, and the only way it knows which names
// those are is to scan the whole table. Directory renames are not rare in the
// workloads this tool is built for (rotating log directories, rsync-style
// temp-dir swaps, "mv" of a whole tree), so the cost has to be known rather
// than assumed.
func benchRekeyBackend(n int) *inotifyBackend {
	b := &inotifyBackend{
		wdToDir: make(map[int32]string, n),
		dirToWd: make(map[string]int32, n),
		pending: make(map[uint32]pendingMove),
	}
	// Half the directories under /r/old, the other half under /r/other, so the
	// rename has a real subtree to walk and a real remainder to skip.
	for i := 0; i < n; i++ {
		var p string
		if i%2 == 0 {
			p = fmt.Sprintf("/r/old/d%03d/d%04d", (i/2)%50, i/2)
		} else {
			p = fmt.Sprintf("/r/other/d%03d/d%04d", (i/2)%50, i/2)
		}
		b.dirToWd[p] = int32(i + 1)
		b.wdToDir[int32(i+1)] = p
	}
	return b
}

func BenchmarkRekeyLargeTree(b *testing.B) {
	for _, n := range []int{10000, 100000, 1000000} {
		b.Run(fmt.Sprintf("dirs=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				be := benchRekeyBackend(n)
				moved := make([]string, 0, n/2)
				for p := range be.dirToWd {
					if IsUnder("/r/old", p) {
						moved = append(moved, p)
					}
				}
				b.StartTimer()
				be.Rekey("/r/old", "/r/new", moved)
			}
		})
	}
}

// BenchmarkRekeyWide measures the same thing when the renamed subtree is only
// one directory deep but very wide, which is the shape a flat data directory
// has.
func BenchmarkRekeyWide(b *testing.B) {
	const n = 200000
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		be := &inotifyBackend{
			wdToDir: make(map[int32]string, n),
			dirToWd: make(map[string]int32, n),
			pending: make(map[uint32]pendingMove),
		}
		for j := 0; j < n; j++ {
			p := fmt.Sprintf("/data/f%06d", j)
			be.dirToWd[p] = int32(j + 1)
			be.wdToDir[int32(j+1)] = p
		}
		moved := make([]string, 0, n)
		for p := range be.dirToWd {
			moved = append(moved, p)
		}
		b.StartTimer()
		be.Rekey("/data", "/data2", moved)
	}
}
