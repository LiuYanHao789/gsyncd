package watcher

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gsyncd/internal/event"
)

// The child index exists to make subtree removal cost the subtree instead of
// the whole table. An index that is merely fast is worthless, so every
// operation below is checked against a reference implementation that keeps a
// plain set and answers questions by scanning it.

type refSet map[string]bool

func (r refSet) below(root string) []string {
	var out []string
	for p := range r {
		if p != root && IsUnder(root, p) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func (r refSet) list() []string {
	out := make([]string, 0, len(r))
	for p := range r {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// buildRandomTree creates a random nested tree under /r and records it in both
// the Tree and the reference set.
func buildRandomTree(t *testing.T, rnd *rand.Rand, n int) (*Tree, refSet) {
	t.Helper()
	tree := New(newLimitBackend(1<<30), Options{Workers: 1})
	t.Cleanup(func() { tree.Close() })
	tree.roots["/r"] = true
	ref := refSet{}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	tree.addRef("/r")
	ref["/r"] = true
	paths := []string{"/r"}
	for i := 0; i < n; i++ {
		parent := paths[rnd.Intn(len(paths))]
		p := parent + "/d" + strconv.Itoa(i)
		tree.addRef(p)
		ref[p] = true
		paths = append(paths, p)
	}
	return tree, ref
}

func TestIndexMatchesReference(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	for round := 0; round < 30; round++ {
		tree, ref := buildRandomTree(t, rnd, 200)

		// Random removals, some of them subtrees.
		for step := 0; step < 40; step++ {
			all := ref.list()
			if len(all) <= 1 {
				break
			}
			victim := all[rnd.Intn(len(all))]
			switch rnd.Intn(3) {
			case 0:
				// dropPath forgets one directory and leaves its descendants
				// alone, which is only a sane request for a directory that has
				// none. Production pairs it with forgetBelow for directories
				// that do (see the delete case in handle).
				var leaves []string
				for _, p := range all {
					if len(ref.below(p)) == 0 {
						leaves = append(leaves, p)
					}
				}
				if len(leaves) == 0 {
					continue
				}
				victim = leaves[rnd.Intn(len(leaves))]
				tree.mu.Lock()
				tree.dropPath(victim)
				tree.mu.Unlock()
				delete(ref, victim)
			case 1:
				// Production always drops the directory and then forgets what
				// was below it, so the reference does the same.
				below := ref.below(victim)
				tree.mu.Lock()
				tree.dropPath(victim)
				tree.forgetBelowLocked(victim)
				tree.mu.Unlock()
				delete(ref, victim)
				for _, p := range below {
					delete(ref, p)
				}
			default:
				tree.mu.Lock()
				got := tree.takeBelowLocked(victim)
				tree.mu.Unlock()
				want := append([]string{victim}, ref.below(victim)...)
				if _, ok := ref[victim]; !ok {
					want = want[1:]
				}
				sort.Strings(got)
				sort.Strings(want)
				if strings.Join(got, ",") != strings.Join(want, ",") {
					t.Fatalf("round %d step %d: takeBelow(%s) = %v, want %v", round, step, victim, got, want)
				}
				delete(ref, victim)
				for _, p := range ref.below(victim) {
					delete(ref, p)
				}
			}

			tree.mu.Lock()
			got := make([]string, 0, len(tree.dirs))
			for p := range tree.dirs {
				got = append(got, p)
			}
			tree.mu.Unlock()
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(ref.list(), ",") {
				t.Fatalf("round %d step %d: table = %v, want %v", round, step, got, ref.list())
			}
			if n := tree.OrphanCount(); n != 0 {
				t.Fatalf("round %d step %d: %d entries lost their parent link", round, step, n)
			}
		}
	}
}

// A rename must move the whole subtree in the index, not just the entry itself:
// otherwise a later delete of the new name stops finding the children, they stay
// in the table forever, and WatchDirCount drifts up on a long-running daemon.
func TestRenameRebuildsIndex(t *testing.T) {
	tree := New(newLimitBackend(1<<30), Options{Workers: 1})
	defer tree.Close()
	tree.mu.Lock()
	for _, p := range []string{"/r", "/r/old", "/r/old/a", "/r/old/a/b", "/r/old/c", "/r/keep"} {
		tree.addRef(p)
	}
	tree.mu.Unlock()

	tree.handle(event.Event{Path: "/r/new", From: "/r/old", Op: event.Move, IsDir: true})

	tree.mu.Lock()
	defer tree.mu.Unlock()
	want := []string{"/r", "/r/keep", "/r/new", "/r/new/a", "/r/new/a/b", "/r/new/c"}
	got := make([]string, 0, len(tree.dirs))
	for p := range tree.dirs {
		got = append(got, p)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("after rename: %v, want %v", got, want)
	}
	// The index has to agree: removing the new subtree must take the children.
	n := tree.forgetBelowLocked("/r/new")
	if n != 3 {
		t.Fatalf("forgetBelow after rename removed %d entries, want 3", n)
	}
	if _, ok := tree.dirs["/r/new/a/b"]; ok {
		t.Fatal("a child of the renamed directory is still recorded")
	}
}

// A directory recorded by hand, or one whose slot went stale, must still be
// removable: the fallback scans one parent's children instead of silently
// leaving the entry behind.
func TestDropPathToleratesStaleSlot(t *testing.T) {
	tree := New(newLimitBackend(1<<30), Options{Workers: 1})
	defer tree.Close()
	tree.mu.Lock()
	tree.addRef("/r")
	tree.addRef("/r/a")
	tree.addRef("/r/b")
	tree.dirs["/r/a"] = 99 // stale slot
	tree.mu.Unlock()

	tree.mu.Lock()
	tree.dropPath("/r/a")
	left := len(tree.dirs)
	tree.mu.Unlock()
	if left != 2 {
		t.Fatalf("after dropping the stale entry %d directories remain, want 2", left)
	}
	tree.mu.Lock()
	if _, ok := tree.dirs["/r/b"]; !ok {
		tree.mu.Unlock()
		t.Fatal("the sibling was lost while detaching a stale entry")
	}
	tree.mu.Unlock()
}

// TestMemoryPerDirectory measures the user-space cost of one tracked directory.
// It is the number that decides whether a 100T tree (millions of directories)
// can be held in memory at all, so it is asserted rather than left to a
// comment. The kernel side is separate: one inotify watch is about 100 bytes of
// unswappable memory plus roughly 900 bytes of reclaimable inode/dentry
// (cmd/probemark, DESIGN §6.2), which is why the watch count, not this table,
// is the real ceiling.
func TestMemoryPerDirectory(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a million paths")
	}
	// The default is the number the docs quote. GSYNCD_MEMDIRS raises it so the
	// same assertion can be run at the size of a real 100T tree (tens of
	// millions of directories) on a machine with the memory to hold it; an
	// extrapolation is only worth as much as the point it was fitted to.
	n := 1_000_000
	if v := os.Getenv("GSYNCD_MEMDIRS"); v != "" {
		got, err := strconv.Atoi(v)
		if err != nil || got < 1000 {
			t.Fatalf("GSYNCD_MEMDIRS=%q is not a directory count", v)
		}
		n = got
	}
	// Fan out 100 children per parent, which is what real trees look like and
	// what makes the kids slices grow.
	paths := make([]string, n)
	for i := 0; i < n; i++ {
		paths[i] = fmt.Sprintf("/data/d%03d/d%03d/d%06d", i/10000, (i/100)%100, i)
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	tree := New(newLimitBackend(1<<30), Options{Workers: 1})
	tree.mu.Lock()
	for _, p := range paths {
		tree.addRef(p)
	}
	tree.mu.Unlock()

	runtime.GC()
	runtime.ReadMemStats(&after)

	perDir := float64(after.HeapAlloc-before.HeapAlloc) / float64(n)
	t.Logf("%d directories: heap %d -> %d, %.0f bytes/directory",
		n, before.HeapAlloc, after.HeapAlloc, perDir)
	if got := tree.WatchDirCount(); got != n {
		t.Fatalf("index lost entries: got %d want %d", got, n)
	}
	if o := tree.OrphanCount(); o != 0 {
		t.Fatalf("%d orphans in the index", o)
	}
	// 400 bytes is the budget the docs promise for 100T arithmetic. Going over
	// it means the per-directory representation has to change before the tree
	// size does.
	if perDir > 400 {
		t.Errorf("%.0f bytes/directory exceeds the 400-byte budget", perDir)
	}
}
