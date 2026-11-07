package watcher

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gsyncd/internal/event"
)

// limitBackend accepts the first n directories and then behaves like a kernel
// whose watch table is full.
type limitBackend struct {
	mu     sync.Mutex
	n      int
	added  []string
	events chan event.Event
}

func newLimitBackend(n int) *limitBackend {
	return &limitBackend{n: n, events: make(chan event.Event, 16)}
}

func (b *limitBackend) Add(path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.added) >= b.n {
		return ErrWatchLimit
	}
	b.added = append(b.added, path)
	return nil
}

func (b *limitBackend) Remove(string) error        { return nil }
func (b *limitBackend) Events() <-chan event.Event { return b.events }
func (b *limitBackend) Close() error               { return nil }
func (b *limitBackend) count() int                 { b.mu.Lock(); defer b.mu.Unlock(); return len(b.added) }

// A full watch table must degrade, not abort: the directories registered so
// far keep working and the caller can schedule periodic full resyncs. The
// original aborted the whole process with "No space left on device" (#65).
func TestWalkAddDegradesOnWatchLimit(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 20; i++ {
		if err := os.MkdirAll(filepath.Join(root, "d", "e", string(rune('a'+i))), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	be := newLimitBackend(5)
	tree := New(be, Options{Workers: 4})
	defer tree.Close()

	if err := tree.AddRoot(root); err != nil {
		t.Fatalf("AddRoot must not fail on a full watch table: %v", err)
	}
	if got := be.count(); got != 5 {
		t.Fatalf("registered %d watches, want 5", got)
	}
	if tree.Unwatched() == 0 {
		t.Fatal("Unwatched() must report the directories that stayed unwatched")
	}
	if tree.WatchDirCount() == 0 {
		t.Fatal("the watched directories must still be usable")
	}
}

// Any other Add error is still fatal, so a genuinely broken path is not
// silently treated as unwatched.
func TestWalkAddPropagatesRealErrors(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	be := &failingBackend{err: errors.New("boom"), events: make(chan event.Event)}
	tree := New(be, Options{Workers: 1})
	defer tree.Close()
	if err := tree.AddRoot(root); err == nil {
		t.Fatal("want an error")
	}
}

type failingBackend struct {
	mu     sync.Mutex
	err    error
	events chan event.Event
}

func (b *failingBackend) Add(string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}
func (b *failingBackend) Remove(string) error        { return nil }
func (b *failingBackend) Events() <-chan event.Event { return b.events }
func (b *failingBackend) Close() error               { return nil }

// TestRenameKeepsWatchBookkeeping pins that a directory renamed in place keeps
// its watches and only the recorded names change. Without this every later
// delete under the new name leaks an entry from the watch table and
// WatchDirCount drifts away from reality on a long-running daemon.
func TestRenameKeepsWatchBookkeeping(t *testing.T) {
	root := t.TempDir()
	be := newLimitBackend(100)
	tree := New(be, Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}

	from := filepath.Join(root, "old")
	to := filepath.Join(root, "new")
	tree.mu.Lock()
	tree.addRef(from)
	tree.addRef(filepath.Join(from, "deeper"))
	tree.addRef(filepath.Join(root, "untouched"))
	tree.mu.Unlock()

	// Drive the event through the real handler, not the helper, so the wiring
	// in handle() is covered as well.
	tree.handle(event.Event{Path: to, From: from, Op: event.Move, IsDir: true})

	tree.mu.Lock()
	defer tree.mu.Unlock()
	if _, ok := tree.dirs[from]; ok {
		t.Fatal("old path must be dropped from the watch bookkeeping")
	}
	if _, ok := tree.dirs[filepath.Join(from, "deeper")]; ok {
		t.Fatal("children of the old path must be rekeyed too")
	}
	if _, ok := tree.dirs[to]; !ok {
		t.Fatal("new path must be recorded")
	}
	if _, ok := tree.dirs[filepath.Join(to, "deeper")]; !ok {
		t.Fatal("children must follow the rename")
	}
	if _, ok := tree.dirs[filepath.Join(root, "untouched")]; !ok {
		t.Fatal("siblings must not be touched")
	}
}

// Removing a directory must also clear its descendants from the bookkeeping.
// inotify reports only the top of a removed subtree, so "rm -rf big" leaves one
// event and, without this, every path below it stays in the map forever.
func TestDeleteForgetsChildren(t *testing.T) {
	root := t.TempDir()
	be := newLimitBackend(100)
	tree := New(be, Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}

	gone := filepath.Join(root, "gone")
	tree.mu.Lock()
	tree.addRef(gone)
	tree.addRef(filepath.Join(gone, "a"))
	tree.addRef(filepath.Join(gone, "a", "b"))
	tree.addRef(filepath.Join(root, "keep"))
	tree.mu.Unlock()

	tree.handle(event.Event{Path: gone, Op: event.Delete, IsDir: true})

	tree.mu.Lock()
	defer tree.mu.Unlock()
	if _, ok := tree.dirs[gone]; ok {
		t.Fatal("the deleted directory itself must be dropped")
	}
	if _, ok := tree.dirs[filepath.Join(gone, "a", "b")]; ok {
		t.Fatal("descendants of a removed tree must not stay in the watch table")
	}
	if _, ok := tree.dirs[filepath.Join(root, "keep")]; !ok {
		t.Fatal("a sibling with a shared prefix must not be forgotten")
	}
}

// A directory that appears at runtime is watched only after the directory event
// has been handled, so anything written into it in the meantime raises no
// inotify event at all. The adopter has to rescan the subtree and synthesise
// those changes; without it a plain "cp -a" into a new directory loses files
// until the next full resync (issue #284).
func TestAdoptBackfillsExistingContent(t *testing.T) {
	root := t.TempDir()
	be := newLimitBackend(100)
	tree := New(be, Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}

	fresh := filepath.Join(root, "fresh")
	if err := os.MkdirAll(filepath.Join(fresh, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	pre := filepath.Join(fresh, "pre.txt")
	if err := os.WriteFile(pre, []byte("before the watch existed"), 0o644); err != nil {
		t.Fatal(err)
	}

	// No real change follows: everything below must come from the backfill.
	tree.handle(event.Event{Path: fresh, Op: event.Create, IsDir: true})

	got := map[string]event.Op{}
	timeout := time.After(3 * time.Second)
	for i := 0; i < 3; i++ {
		select {
		case ev := <-tree.Events():
			got[ev.Path] = ev.Op
		case <-timeout:
			t.Fatalf("timed out, collected %v", got)
		}
	}
	if op, ok := got[pre]; !ok || op&event.Write == 0 {
		t.Fatalf("existing file was not backfilled: %v", got)
	}
	if _, ok := got[filepath.Join(fresh, "empty")]; !ok {
		t.Fatalf("empty directory was not backfilled: %v", got)
	}
}

// The kernel drops the watch on a root when the directory itself is removed, and
// it reports that only as IN_IGNORED - an event no other layer can interpret.
// The backend turns it into RootGone; the tree must record that this specific
// root is now invisible while keeping it in the requested set, because the
// operator still wants it watched and only a re-attach can restore that.
func TestRootGoneMarksRootLost(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	tree := New(newLimitBackend(100), Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	if !tree.IsWatched(root) {
		t.Fatal("the root must be watched right after AddRoot")
	}

	tree.handle(event.Event{Path: root, Op: event.RootGone, IsDir: true})

	if tree.IsWatched(root) {
		t.Fatal("a root whose watch was dropped must not report itself as watched")
	}
	lost := tree.Lost()
	if len(lost) != 1 || lost[0] != root {
		t.Fatalf("Lost() = %v, want [%s]", lost, root)
	}
	if _, ok := tree.dirs[sub]; ok {
		t.Fatal("the watches below a gone root went with it and must not stay recorded")
	}

	select {
	case ev := <-tree.Events():
		if ev.Op&event.RootGone == 0 {
			t.Fatalf("got %s, want RootGone", ev.Op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RootGone was never forwarded to the engine")
	}
}

// Reattach is what turns a lost root back into a working one. It has to walk the
// tree again: the old file descriptors are dead, so every directory below needs
// a new watch, and the tree that came back may have different contents.
func TestReattachRewatchesTree(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	tree := New(newLimitBackend(100), Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	tree.handle(event.Event{Path: root, Op: event.RootGone, IsDir: true})
	if err := os.MkdirAll(filepath.Join(sub, "new"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := tree.Reattach(root); err != nil {
		t.Fatalf("Reattach: %v", err)
	}
	if !tree.IsWatched(root) {
		t.Fatal("the root must be watched again")
	}
	if _, ok := tree.dirs[filepath.Join(sub, "new")]; !ok {
		t.Fatal("directories that appeared while the root was gone must be picked up")
	}
	if left := tree.Lost(); len(left) != 0 {
		t.Fatalf("Lost() = %v after a successful re-attach, want empty", left)
	}

	// A second call must not add anything twice: the supervisor runs every
	// ten seconds and would otherwise multiply the watch table on a healthy tree.
	before := tree.WatchDirCount()
	if err := tree.Reattach(root); err != nil {
		t.Fatalf("Reattach on a healthy root: %v", err)
	}
	if after := tree.WatchDirCount(); after != before {
		t.Fatalf("re-attaching a healthy root changed the watch count %d -> %d", before, after)
	}
}

// A root removed and recreated under the same name keeps its path but not its
// identity. Verify has to notice, or "rm -rf data && cp -a new data" leaves a
// mirror that is watched by nobody and looks healthy.
func TestVerifyDetectsRecreatedRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	be := newLimitBackend(100)
	tree := New(be, Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}

	same, err := tree.Verify(root)
	if err != nil || !same {
		t.Fatalf("Verify on an untouched root = %v, %v; want true, nil", same, err)
	}

	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	same, err = tree.Verify(root)
	if err != nil {
		t.Fatalf("Verify on a recreated root: %v", err)
	}
	if same {
		t.Fatal("a recreated root must not verify as the same directory")
	}
}

// Reattach on a recreated root must replace the recorded directory set, not
// just add to it: the old children are gone, and keeping them makes the walk
// believe they are already watched.
func TestReattachReplacesStaleChildren(t *testing.T) {
	root := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(filepath.Join(root, "old"), 0o755); err != nil {
		t.Fatal(err)
	}
	be := newLimitBackend(100)
	tree := New(be, Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	if !tree.IsWatched(filepath.Join(root, "old")) {
		t.Fatal("the child must be recorded after AddRoot")
	}

	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "new"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := tree.Reattach(root); err != nil {
		t.Fatalf("Reattach: %v", err)
	}
	if !tree.IsWatched(root) {
		t.Fatal("the root must be watched again")
	}
	if !tree.IsWatched(filepath.Join(root, "new")) {
		t.Fatal("the new child must be watched")
	}
	if tree.IsWatched(filepath.Join(root, "old")) {
		t.Fatal("the child of the old inode must be dropped from the bookkeeping")
	}
}

// renameBackend records what the tree hands it on a directory rename.
//
// The contract is the point: the backend is told which paths moved instead of
// scanning its own table to find out. That scan used to be the expensive half
// of a rename (1.1s and 122MB at a million watched directories, with the event
// reader blocked behind the same mutex), and it is redundant work - the tree
// index already knows the subtree.
type renameBackend struct {
	limitBackend
	mu    sync.Mutex
	from  string
	to    string
	moved []string
	calls int
}

func newRenameBackend(n int) *renameBackend {
	return &renameBackend{limitBackend: *newLimitBackend(n)}
}

func (b *renameBackend) Rekey(from, to string, moved []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.from, b.to = from, to
	b.moved = append([]string(nil), moved...)
}

func (b *renameBackend) rename() (string, string, []string, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.from, b.to, b.moved, b.calls
}

func TestRenameTellsBackendWhichPathsMoved(t *testing.T) {
	root := t.TempDir()
	be := newRenameBackend(100)
	tree := New(be, Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}

	from := filepath.Join(root, "old")
	to := filepath.Join(root, "new")
	deep := filepath.Join(from, "a", "b")
	tree.mu.Lock()
	tree.addRef(from)
	tree.addRef(filepath.Join(from, "a"))
	tree.addRef(deep)
	tree.addRef(filepath.Join(root, "sibling"))
	tree.mu.Unlock()

	tree.handle(event.Event{Path: to, From: from, Op: event.Move, IsDir: true})

	gotFrom, gotTo, moved, calls := be.rename()
	if calls != 1 {
		t.Fatalf("backend saw %d renames, want 1", calls)
	}
	if gotFrom != from || gotTo != to {
		t.Fatalf("rename reported as %q -> %q, want %q -> %q", gotFrom, gotTo, from, to)
	}
	want := map[string]bool{
		from:                     true,
		filepath.Join(from, "a"): true,
		deep:                     true,
	}
	if len(moved) != len(want) {
		t.Fatalf("backend got %d paths %v, want %d", len(moved), moved, len(want))
	}
	for _, p := range moved {
		if !want[p] {
			t.Fatalf("backend got %q, which is not part of the renamed subtree: %v", p, moved)
		}
	}
	// Parents before children: the backend rewrites entries as it goes and a
	// child arriving first would still work, but the order is documented and
	// the log reads better for it.
	if moved[0] != from {
		t.Fatalf("subtree must start at the renamed directory, got %v", moved)
	}
}

// A rename of something the tree never recorded (an excluded subtree, or a
// directory whose watch the kernel refused) must not reach the backend at all:
// there is nothing to rewrite, and the call would allocate for no reason.
func TestRenameWithoutRecordedSubtreeSkipsBackend(t *testing.T) {
	root := t.TempDir()
	be := newRenameBackend(100)
	tree := New(be, Options{Workers: 2})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	from := filepath.Join(root, "never-recorded")
	tree.handle(event.Event{Path: filepath.Join(root, "new"), From: from, Op: event.Move, IsDir: true})
	if _, _, _, calls := be.rename(); calls != 0 {
		t.Fatalf("backend was told about a rename it has no paths for (%d calls)", calls)
	}
}
