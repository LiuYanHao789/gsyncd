package scanstate

import (
	"os"
	"path/filepath"
	"testing"
)

func rec(path string, mtime int64, count uint32, tag byte) Record {
	var h [16]byte
	for i := range h {
		h[i] = tag
	}
	return Record{Path: path, Mtime: mtime, Count: count, Hash: h}
}

// Walk order is by path component, not by byte: "a-b" sorts before "a" only in
// a byte comparison, but the walk descends into "a" and visits "a/b" before ever
// returning to the top level. Getting this wrong makes the cursor skip records
// or match the wrong one, which is why it has its own test.
func TestWalkLessComparesByComponent(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{".", "a", true},
		{"a", "a/b", true},
		{"a/b", "a", false},
		{"a", "a-b", true},
		{"a/b", "a-b", true}, // "a/b" is visited first even though it sorts later
		{"a-b", "a/b", false},
		{"a/b/c", "a/b/d", true},
		{"a/b", "a/c", true},
		{"a/b", "a/b", false},
	}
	for _, c := range cases {
		if got := walkLess(c.a, c.b); got != c.want {
			t.Errorf("walkLess(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scan")
	fp := Fingerprint("/src", "/dst", nil)

	s := Open(path, fp, nil)
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	want := []Record{
		rec(".", 100, 2, 1),
		rec("a", 200, 1, 2),
		rec("a/b", 300, 5, 3),
	}
	for _, r := range want {
		s.Add(r)
	}
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s.Finish(true)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("complete segment was not promoted: %v", err)
	}

	// A second store on the same path must see every record, in order, through
	// the forward cursor the walk uses.
	s2 := Open(path, fp, nil)
	if err := s2.Begin(); err != nil {
		t.Fatal(err)
	}
	for _, w := range want {
		got, ok := s2.Lookup(w.Path)
		if !ok {
			t.Fatalf("Lookup(%q) missed", w.Path)
		}
		if got != w {
			t.Fatalf("Lookup(%q) = %+v, want %+v", w.Path, got, w)
		}
	}
	if s2.Verified() != uint64(len(want)) {
		t.Fatalf("Verified() = %d, want %d", s2.Verified(), len(want))
	}
	s2.Finish(false)
}

// A checkpoint belonging to a different source, target or exclude set describes
// a different tree, so it must never be used.
func TestStoreRejectsForeignFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scan")
	s := Open(path, Fingerprint("/src", "/dst", nil), nil)
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	s.Add(rec("a", 1, 1, 9))
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s.Finish(true)

	other := Open(path, Fingerprint("/src", "/dst2", nil), nil)
	if err := other.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, ok := other.Lookup("a"); ok {
		t.Fatal("record from a different target was accepted")
	}
	if other.Verified() != 0 {
		t.Fatalf("Verified() = %d, want 0", other.Verified())
	}
	other.Finish(false)
}

func TestFingerprintTracksConfig(t *testing.T) {
	base := Fingerprint("/src", "/dst", []string{"*.tmp"})
	if base == Fingerprint("/src2", "/dst", []string{"*.tmp"}) {
		t.Error("source is not part of the fingerprint")
	}
	if base == Fingerprint("/src", "/dst2", []string{"*.tmp"}) {
		t.Error("target is not part of the fingerprint")
	}
	if base == Fingerprint("/src", "/dst", []string{"*.log"}) {
		t.Error("excludes are not part of the fingerprint")
	}
	if base != Fingerprint("/src", "/dst", []string{"*.tmp"}) {
		t.Error("fingerprint is not deterministic")
	}
}

// A run that is killed leaves a half written segment. Records are only appended
// after the directory they describe has been copied, so the usable prefix must
// survive and the torn tail must be discarded - never the other way round.
func TestStoreTornTailKeepsEarlierRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scan")
	fp := Fingerprint("/src", "/dst", nil)
	s := Open(path, fp, nil)
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	for _, r := range []Record{rec("a", 1, 1, 1), rec("b", 2, 1, 2), rec("c", 3, 1, 3)} {
		s.Add(r)
	}
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s.Finish(true)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Chop the last record in half: its crc no longer matches.
	if err := os.WriteFile(path, raw[:len(raw)-3], 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := Open(path, fp, nil)
	if err := s2.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Lookup("a"); !ok {
		t.Fatal("intact record before the tear was lost")
	}
	if _, ok := s2.Lookup("b"); !ok {
		t.Fatal("intact record before the tear was lost")
	}
	if _, ok := s2.Lookup("c"); ok {
		t.Fatal("torn record was accepted")
	}
	s2.Finish(false)
}

// An interrupted run keeps its segment and the next run continues from it,
// which is the whole point of the feature.
func TestStoreIncompleteRunIsStillUsable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scan")
	fp := Fingerprint("/src", "/dst", nil)
	s := Open(path, fp, nil)
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	s.Add(rec("a", 1, 1, 1))
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	s.Finish(false) // interrupted

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("interrupted run should not have promoted its segment: %v", err)
	}

	s2 := Open(path, fp, nil)
	if err := s2.Begin(); err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Lookup("a"); !ok {
		t.Fatal("record from the interrupted run was not picked up")
	}
	s2.Finish(false)
}

func TestStoreUnwritablePathDegrades(t *testing.T) {
	// A directory where the checkpoint file should be: every write fails, and
	// the only consequence must be that nothing is remembered.
	path := filepath.Join(t.TempDir(), "scan")
	// The store writes to path+".tmp", so that is where the obstruction goes.
	if err := os.MkdirAll(path+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}
	s := Open(path, Fingerprint("/src", "/dst", nil), nil)
	if err := s.Begin(); err == nil {
		t.Fatal("Begin() over a directory reported success")
	}
	s.Add(rec("a", 1, 1, 1))
	s.Finish(true)
	if _, ok := s.Lookup("a"); ok {
		t.Fatal("record was readable from an unwritable store")
	}
}
