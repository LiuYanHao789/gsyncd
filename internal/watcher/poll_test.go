//go:build !linux

package watcher

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gsyncd/internal/event"
)

func waitFor(t *testing.T, ch <-chan event.Event, want event.Op, timeout time.Duration) event.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-ch:
			if ev.Op&want != 0 {
				return ev
			}
		case <-deadline:
			t.Fatalf("no event with %v within %s", want, timeout)
		}
	}
}

// waitForPath is waitFor with a path filter: events about other paths (the
// staging files an atomic rename leaves behind, for instance) are dropped.
func waitForPath(t *testing.T, ch <-chan event.Event, want event.Op, path string, timeout time.Duration) event.Event {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-ch:
			if ev.Path == path && ev.Op&want != 0 {
				return ev
			}
		case <-deadline:
			t.Fatalf("no %v event for %s within %s", want, path, timeout)
		}
	}
}

func TestPollDetectsCreateWriteDelete(t *testing.T) {
	root := t.TempDir()
	be := NewPoll(50*time.Millisecond, t.Logf)
	defer be.Close()
	tree := New(be, Options{})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}

	// The first scan records the baseline; anything written before it is
	// part of that baseline by definition (the initial sync copies it).
	time.Sleep(250 * time.Millisecond)

	f := filepath.Join(root, "a.txt")
	if err := os.WriteFile(f, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	ev := waitFor(t, tree.Events(), event.Create, 5*time.Second)
	if ev.Path != f {
		t.Fatalf("create path: %q want %q", ev.Path, f)
	}

	time.Sleep(80 * time.Millisecond)
	if err := os.WriteFile(f, []byte("two-two"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, tree.Events(), event.Write, 5*time.Second)

	if err := os.Remove(f); err != nil {
		t.Fatal(err)
	}
	waitFor(t, tree.Events(), event.Delete, 5*time.Second)
}

func TestPollSeesSubdirectoryFiles(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "x/y")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	be := NewPoll(50*time.Millisecond, t.Logf)
	defer be.Close()
	tree := New(be, Options{})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	f := filepath.Join(sub, "deep.txt")
	if err := os.WriteFile(f, []byte("d"), 0o644); err != nil {
		t.Fatal(err)
	}
	ev := waitFor(t, tree.Events(), event.Create, 5*time.Second)
	if ev.Path != f {
		t.Fatalf("path %q want %q", ev.Path, f)
	}
}

func TestIsUnderSeparators(t *testing.T) {
	cases := []struct {
		root, path string
		want       bool
	}{
		{"/a", "/a/b", true},
		{"/a", "/a", true},
		{"/a", "/ab", false},
		{"/a/", "/a/b", true},
		{"C:\\d\\a", "C:\\d\\a\\b", true},
		{"/a", "/b/c", false},
	}
	for _, c := range cases {
		if got := IsUnder(c.root, c.path); got != c.want {
			t.Errorf("IsUnder(%q,%q)=%v want %v", c.root, c.path, got, c.want)
		}
	}
}

// TestPollEmitsEachChangeOnce guards the in-place snapshot sweep: an entry that
// is merely re-seen must not be reported again, and a deleted entry must leave
// the snapshot so a tree that is created and removed repeatedly does not grow
// it without bound.
func TestPollEmitsEachChangeOnce(t *testing.T) {
	root := t.TempDir()
	be := NewPoll(50*time.Millisecond, t.Logf).(*pollBackend)
	tree := New(be, Options{})
	defer tree.Close()
	if err := tree.AddRoot(root); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)

	// The file is staged under another name and renamed into place. A plain
	// WriteFile lets a scan land between the create and the write, which
	// legitimately produces Create followed by Write for the same path (the
	// content really did change between two scans). That is correct behaviour
	// and the aggregator folds it into one change, but it makes a test that
	// counts events non-deterministic.
	f := filepath.Join(root, "once.txt")
	staged := filepath.Join(root, "once.txt.part")
	if err := os.WriteFile(staged, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, f); err != nil {
		t.Fatal(err)
	}
	waitForPath(t, tree.Events(), event.Create, f, 5*time.Second)

	// Let several more scans pass: the file is unchanged, so nothing new.
	time.Sleep(300 * time.Millisecond)
	for {
		select {
		case ev := <-tree.Events():
			if ev.Path == f {
				t.Fatalf("unexpected repeat event %v for %s", ev.Op, ev.Path)
			}
			continue
		default:
		}
		break
	}

	if err := os.Remove(f); err != nil {
		t.Fatal(err)
	}
	waitForPath(t, tree.Events(), event.Delete, f, 5*time.Second)
	time.Sleep(150 * time.Millisecond)

	be.mu.Lock()
	n := len(be.snap)
	_, still := be.snap[f]
	be.mu.Unlock()
	if still || n != 1 {
		t.Fatalf("snapshot not swept: %d entries, deleted path present=%v", n, still)
	}
}
