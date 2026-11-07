package sync

import (
	"strings"
	"testing"

	"gsyncd/internal/aggregate"
	"gsyncd/internal/event"
)

// Excluded paths must reach rsync itself: the event filter keeps them out of
// incremental batches, but the initial whole-tree transfer would copy them
// once and never remove them.
func TestExcludesForwardedInReverseOrder(t *testing.T) {
	r := NewRSync("/src", "/dst", false, nil, nil)
	r.Excludes = []string{"*.tmp", "!keep.tmp", "cache/"}
	args := r.baseArgs()
	want := []string{"--exclude=cache/", "--include=keep.tmp", "--exclude=*.tmp"}
	var got []string
	for _, a := range args {
		if strings.HasPrefix(a, "--exclude=") || strings.HasPrefix(a, "--include=") {
			got = append(got, a)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("exclude args = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("exclude args = %v, want %v (rsync stops at the first match, got to reverse)", got, want)
		}
	}
}

func contains(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

// An incremental run carries only the changed paths. Adding --delete there
// makes rsync treat every other destination entry as extraneous, so deleting
// one file would wipe the mirror. Only Startup, which mirrors the whole tree,
// may pass --delete.
func TestIncrementalArgsNeverDelete(t *testing.T) {
	r := NewRSync("/src", "/dst", true, nil, nil)
	args := r.baseArgs()
	if contains(args, "--delete") {
		t.Fatalf("incremental args must not contain --delete: %v", args)
	}
	if !contains(args, "--files-from=-") || !contains(args, "--from0") {
		t.Fatalf("path list must be streamed over stdin: %v", args)
	}
	// rsync drops the implied -r as soon as --files-from is present, so an
	// incremental batch must ask for recursion explicitly or a directory that
	// appeared at runtime arrives without its contents.
	if !contains(args, "-r") {
		t.Fatalf("--files-from suppresses the implied -r, so -r must be explicit: %v", args)
	}
	if !contains(args, "--partial-dir=.rsync-partial") {
		t.Fatalf("a 100TB transfer must be restartable: %v", args)
	}
}

func TestWithoutDeleteFlags(t *testing.T) {
	got := withoutDeleteFlags([]string{"--delete", "--delete-after", "--del", "-av", "-d", "--exclude=x"})
	want := []string{"-av", "-d", "--exclude=x"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestRSHEndpoint(t *testing.T) {
	r := NewRSync("/src", "host", true, nil, nil)
	r.SSH = true
	r.SSHOpts = SSHOptions{Port: 2222, IdentityFile: "/k/id", Extra: []string{"-o", "Compression=no"}}
	got := r.rshArg()
	for _, want := range []string{"--rsh=ssh", "-p 2222", "-i /k/id", "BatchMode=yes", "Compression=no"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q missing %q", got, want)
		}
	}
	if r.dest() != "host:/src" {
		t.Fatalf("dest=%q", r.dest())
	}
}

func TestNulReader(t *testing.T) {
	n := newNulReader([]string{"a", "b/c"})
	var sb strings.Builder
	buf := make([]byte, 3)
	for {
		k, err := n.Read(buf)
		sb.Write(buf[:k])
		if err != nil {
			break
		}
	}
	if sb.String() != "a\x00b/c\x00" {
		t.Fatalf("got %q", sb.String())
	}
}

// A remote deletion of a directory must carry --force. Measured on rsync
// 3.1.2: "cannot delete non-empty directory: keep" is printed, the exit status
// stays 0, and the subtree survives - so a renamed directory would leave its
// whole old copy on the target and the mirror would silently drift.
func TestRemoteDeleteForcesNonEmptyDirectories(t *testing.T) {
	r := NewRSync("/src", "host", true, nil, nil)
	r.SSH = true
	args := r.deleteArgs()
	if !contains(args, "--force") {
		t.Fatalf("remote delete must pass --force or non-empty directories survive: %v", args)
	}
	if !contains(args, "--delete-missing-args") {
		t.Fatalf("remote delete must use --delete-missing-args: %v", args)
	}
	if contains(args, "--delete") {
		t.Fatalf("--delete with --files-from would wipe the unlisted rest of the mirror: %v", args)
	}
	if r.dest() != "host:/src" {
		t.Fatalf("dest=%q", r.dest())
	}
}

// A rename arrives as a single change carrying both names. The old name has to
// be deleted explicitly, otherwise every "mv a b" keeps a duplicate of a on the
// target forever.
func TestRenameSchedulesOldNameForDeletion(t *testing.T) {
	r := NewRSync("/src", "/dst", true, nil, nil)
	uploads, deletes := r.plan([]aggregate.Change{
		{Path: "/src/new.txt", From: "/src/old.txt", Op: event.Move, Kind: aggregate.KindFile},
		{Path: "/src/gone.txt", Op: event.Delete, Kind: aggregate.KindFile},
		{Path: "/src/fresh.txt", Op: event.Create | event.Write, Kind: aggregate.KindFile},
	})
	if len(uploads) != 2 || uploads[0] != "new.txt" || uploads[1] != "fresh.txt" {
		t.Fatalf("uploads = %v, want [new.txt fresh.txt]", uploads)
	}
	// Order follows the change list: the rename came first, then the deletion.
	if len(deletes) != 2 || deletes[0] != "/src/old.txt" || deletes[1] != "/src/gone.txt" {
		t.Fatalf("deletes = %v, want the deletion plus the old name of the rename", deletes)
	}
}
