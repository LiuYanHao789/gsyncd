//go:build linux

package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gsyncd/internal/event"
)

// TestRecreatedSubtreeStillReportsEvents reproduces the failure observed on a
// real kernel: a whole subtree is deleted and immediately recreated (rsync
// --delete style, or "rm -rf x && cp -a y x"). The directory inodes are new,
// their old watch descriptors are dead, and the tree must end up watching the
// new inodes. If it silently reuses the stale descriptor, nothing under the
// recreated tree is ever reported again and the target diverges forever.
func TestRecreatedSubtreeStillReportsEvents(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	deep := filepath.Join(sub, "deep")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deep, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}

	be, err := NewInotify(t.Logf)
	if err != nil {
		t.Skipf("inotify unavailable: %v", err)
	}
	defer be.Close()
	tree := New(be, Options{Workers: 2, Logf: t.Logf})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	if err := os.RemoveAll(sub); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	drain(tree.Events(), 200*time.Millisecond)

	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	// Written immediately, before the adopter goroutine can have installed the
	// watch: the kernel raises no event for a change made under a directory it
	// was not yet watching, so the only thing that can deliver it is the
	// backfill scan.
	if err := os.WriteFile(filepath.Join(deep, "f.txt"), []byte("f"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for !got["f.txt"] {
		select {
		case ev := <-tree.Events():
			t.Logf("event %s %s", ev.Op, ev.Path)
			got[filepath.Base(ev.Path)] = true
		case <-deadline:
			t.Fatalf("no event for files written into the recreated subtree; saw %v", got)
		}
	}

	// And a write that happens well after the watch should have settled must
	// also arrive: this is the part that fails when a stale wd was reused.
	if err := os.WriteFile(filepath.Join(deep, "late.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline = time.After(3 * time.Second)
	for {
		select {
		case ev := <-tree.Events():
			if filepath.Base(ev.Path) == "late.txt" {
				return
			}
		case <-deadline:
			t.Fatalf("no event for late.txt written into the recreated subtree")
		}
	}
}

func drain(ch <-chan event.Event, d time.Duration) {
	timer := time.After(d)
	for {
		select {
		case <-ch:
		case <-timer:
			return
		}
	}
}

// Removing the root of a sync is not an ordinary "directory deleted" event: the
// kernel first reports IN_DELETE_SELF and then IN_IGNORED, and once the watch is
// gone nothing below that path raises an event again. Measured on a real kernel,
// the daemon kept running, watched_dirs stayed plausible, and a file created
// after the tree was rebuilt never reached the target. The backend has to say
// "this root is gone" so the supervisor can re-attach it.
func TestRootRemovalReportsRootGone(t *testing.T) {
	root := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	be, err := NewInotify(t.Logf)
	if err != nil {
		t.Skipf("inotify unavailable: %v", err)
	}
	defer be.Close()
	tree := New(be, Options{Workers: 2, Logf: t.Logf})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-tree.Events():
			if ev.Op&event.RootGone != 0 && ev.Path == root {
				if tree.IsWatched(root) {
					t.Fatal("the tree still reports the removed root as watched")
				}
				if lost := tree.Lost(); len(lost) != 1 || lost[0] != root {
					t.Fatalf("Lost() = %v, want [%s]", lost, root)
				}
				return
			}
		case <-deadline:
			t.Fatal("removing the root never produced a RootGone event")
		}
	}
}

// TestRekeyFollowsRenameOnRealKernel pins the backend half of a directory
// rename: the watch descriptors stay attached to the inodes, so after "mv old
// new" the kernel keeps reporting through the same wd while the name that wd
// maps to no longer exists. Without the rewrite every later event arrives with
// a stale path, the file is missing there, and the engine mirrors that as a
// delete - renaming one directory would silently wipe its copy on the target.
func TestRekeyFollowsRenameOnRealKernel(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "old")
	deep := filepath.Join(old, "deep")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}

	be, err := NewInotify(t.Logf)
	if err != nil {
		t.Skipf("inotify unavailable: %v", err)
	}
	defer be.Close()
	tree := New(be, Options{Workers: 2, Logf: t.Logf})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	neu := filepath.Join(root, "new")
	if err := os.Rename(old, neu); err != nil {
		t.Fatal(err)
	}
	// Let the rename event reach the tree, which is what drives the rekey.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-tree.Events():
			if ev.Op&event.Move != 0 && ev.From == old {
				goto renamed
			}
		case <-deadline:
			t.Fatal("the rename event never arrived")
		}
	}
renamed:
	drain(tree.Events(), 300*time.Millisecond)

	// A write deep inside the renamed tree must be reported under the new
	// name. That only happens if both the tree index and the wd table moved.
	if err := os.WriteFile(filepath.Join(neu, "deep", "after.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(neu, "deep", "after.txt")
	timeout := time.After(3 * time.Second)
	for {
		select {
		case ev := <-tree.Events():
			if ev.Path == want {
				return
			}
			if ev.Path == filepath.Join(old, "deep", "after.txt") {
				t.Fatalf("event still reported under the old name: %q", ev.Path)
			}
		case <-timeout:
			t.Fatalf("no event for %q after the rename", want)
		}
	}
}

// close_write (issue #458) rests entirely on the kernel telling us which write
// event was the last one. IN_MODIFY fires while the writer still holds the file;
// IN_CLOSE_WRITE fires once, at close. If the backend mapped both to the same
// op, the engine could not tell a half-written file from a finished one, and if
// it forgot the bit on close, every file would wait for max_hold.
func TestCloseWriteCarriesItsOwnBit(t *testing.T) {
	root := t.TempDir()

	be, err := NewInotify(t.Logf)
	if err != nil {
		t.Skipf("inotify unavailable: %v", err)
	}
	defer be.Close()
	tree := New(be, Options{Workers: 2, Logf: t.Logf})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	path := filepath.Join(root, "f.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("half")); err != nil {
		t.Fatal(err)
	}
	// Still open: the write must arrive without the close bit.
	if op := waitOp(t, tree, path, event.Write, 3*time.Second); op&event.Close != 0 {
		t.Fatalf("a write from an open file reported %s, which claims the writer closed it", op)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// And closing must say so, on the same path.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-tree.Events():
			if ev.Path == path && ev.Op&event.Close != 0 {
				if ev.Op&event.Write == 0 {
					t.Fatalf("close arrived without the write bit: %s", ev.Op)
				}
				return
			}
		case <-deadline:
			t.Fatal("closing the file never produced an event carrying Close")
		}
	}
}

// waitOp blocks until an event for path carrying op arrives, and returns its op.
func waitOp(t *testing.T, tree *Tree, path string, op event.Op, d time.Duration) event.Op {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case ev := <-tree.Events():
			if ev.Path == path && ev.Op&op != 0 {
				return ev.Op
			}
		case <-deadline:
			t.Fatalf("no %s event for %q", op, path)
		}
	}
}
