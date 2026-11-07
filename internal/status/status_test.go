package status

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func sampleDoc() Document {
	now := time.Unix(1700000000, 0).UTC()
	return Document{
		Version:   1,
		PID:       4242,
		Started:   now,
		Updated:   now.Add(time.Minute),
		UptimeSec: 60,
		Watches:   1234,
		Phase:     "initial",
		Syncs: []SyncState{{
			Name:       "data",
			Source:     "/srv/data",
			Target:     "/backup/data",
			Backend:    "direct",
			Pending:    7,
			Batches:    3,
			Events:     99,
			Applied:    42,
			Files:      42,
			Deletes:    5,
			Errors:     1,
			Retries:    2,
			FullSyncs:  1,
			Overflows:  1,
			LastError:  "connection reset",
			LastFlush:  now,
			Peak:       500,
			Open:       3,
			CloseWrite: true,
		}},
	}
}

// A status file must survive a round trip unchanged; operators script against
// these field names.
func TestWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "status.json")

	w := NewWriter(path, time.Hour, sampleDoc)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.PID != 4242 || got.Watches != 1234 || got.Phase != "initial" || len(got.Syncs) != 1 {
		t.Fatalf("unexpected document: %+v", got)
	}
	s := got.Syncs[0]
	if s.Name != "data" || s.Pending != 7 || s.Peak != 500 || s.LastError != "connection reset" ||
		s.Files != 42 || s.Deletes != 5 || s.Open != 3 || !s.CloseWrite {
		t.Fatalf("unexpected sync state: %+v", s)
	}
}

// The writer must never leave a partially written file behind, so a reader
// that races with a flush still sees complete JSON.
func TestFlushIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "status.json")
	doc := sampleDoc()

	w := NewWriter(path, time.Millisecond, func() Document { return doc })
	w.Start()
	defer w.Stop()

	deadline := time.Now().Add(3 * time.Second)
	sawValid := false
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err != nil {
			// A missing file (before the first flush) and a Windows sharing
			// violation (a reader racing os.Rename) are both transient; only
			// a partial document would be a real failure.
			time.Sleep(time.Millisecond)
			continue
		}
		var probe Document
		if err := json.Unmarshal(b, &probe); err != nil {
			t.Fatalf("reader observed a partial file: %v\n%s", err, b)
		}
		sawValid = true
	}
	if !sawValid {
		t.Fatal("writer never produced a file")
	}
}

// No temporary file may be left next to the status file.
func TestNoTempLeftover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "status.json")
	w := NewWriter(path, time.Hour, sampleDoc)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "status.json" {
			t.Fatalf("leftover file: %s", e.Name())
		}
	}
}

func TestReadMissingFile(t *testing.T) {
	if _, err := Read(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}
