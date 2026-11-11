// Package status periodically persists daemon state to disk.
//
// The original lsyncd kept everything in memory: the delay queue, the wd to
// path map, the exclude tables. After a crash or a machine reboot there was no
// record of what had already been transferred, so 100 TB had to be rescanned
// from scratch. gsyncd writes a small JSON document atomically, so a restart is
// cheap and an operator can see what the daemon is doing without attaching a
// debugger.
package status

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SyncState is the persisted view of one sync.
type SyncState struct {
	Name     string `json:"name"`
	Source   string `json:"source"`
	Target   string `json:"target"`
	Backend  string `json:"backend"`
	Pending  int    `json:"pending"`
	Batches  uint64 `json:"batches"`
	Events   uint64 `json:"events"`
	Filtered uint64 `json:"filtered"`
	Applied  uint64 `json:"applied"`
	// Files counts files actually written to the target and Deletes counts
	// entries removed there. They are backend counters, so unlike Applied (a
	// count of collapsed changes) they measure work done, not work decided.
	Files     uint64 `json:"files"`
	Deletes   uint64 `json:"deletes"`
	Errors    uint64 `json:"errors"`
	Retries   uint64 `json:"retries"`
	FullSyncs uint64 `json:"full_syncs"`
	Overflows uint64 `json:"overflows"`
	// RootGone counts how often the watch on this sync's root had to be
	// rebuilt, and Lost says whether it is un-watched right now. While Lost
	// is true nothing below the root is observed, so the other counters
	// describe a blind period.
	RootGone uint64 `json:"root_gone"`
	// Starting is true while the initial transfer of this sync is still
	// running. On a large tree that is hours, and it is the only state where
	// a daemon doing real work reports zero everywhere else.
	Starting  bool      `json:"starting,omitempty"`
	Lost      bool      `json:"root_lost,omitempty"`
	LastError string    `json:"last_error,omitempty"`
	LastFlush time.Time `json:"last_flush,omitempty"`
	Peak      int       `json:"peak_pending"`
	// Open counts pending changes whose newest event was a write with no
	// matching close, so it is non-zero whenever writers are mid-write, with
	// or without close_write. With close_write it is the one reason a window
	// with pending work is held back on purpose, so an operator seeing pending
	// stay put has an explanation instead of a stuck-looking daemon.
	Open int `json:"open,omitempty"`
	// CloseWrite echoes the per-sync option, so a status file read on its own
	// says whether Open is merely informational or actually gating flushes.
	CloseWrite bool `json:"close_write,omitempty"`
	// FullResync is the effective periodic resync interval for this sync,
	// empty when disabled. It is per sync because the override is.
	FullResync string `json:"full_resync,omitempty"`
}

// Document is the whole status file.
type Document struct {
	Version   int       `json:"version"`
	PID       int       `json:"pid"`
	Started   time.Time `json:"started"`
	Updated   time.Time `json:"updated"`
	UptimeSec int64     `json:"uptime_seconds"`
	Watches   int       `json:"watched_dirs"`
	// Unwatched counts directories the kernel refused to register because its
	// watch table was full. A non-zero value means the mirror is only kept
	// consistent by the periodic full resync.
	Unwatched int `json:"unwatched_dirs"`
	// FullResync is the periodic resync interval, empty when disabled.
	FullResync string `json:"full_resync,omitempty"`
	// Phase says what the daemon is doing: "watching" while it installs the
	// kernel watches, "initial" during the first transfer, "running" once the
	// event loop owns the tree. The first two last for hours on a large tree
	// and every transfer counter reads zero throughout, so without this an
	// operator cannot tell progress from a hang.
	Phase string `json:"phase,omitempty"`
	// RSSKB and HeapKB are the resident size and the live Go heap. They exist
	// because "does this daemon stay small on a huge tree" is a question only
	// a running process can answer, and the answer has to be readable without
	// a profiler on the host.
	RSSKB  int64 `json:"rss_kb,omitempty"`
	HeapKB int64 `json:"heap_kb,omitempty"`
	// LostRoots lists the sync sources whose watch is currently gone. It is
	// the one state an operator has to act on: the daemon keeps running and
	// the counters stop moving, which looks like an idle system.
	LostRoots []string    `json:"lost_roots,omitempty"`
	Syncs     []SyncState `json:"syncs"`
}

// Writer periodically replaces the status file.
type Writer struct {
	path     string
	interval time.Duration
	started  time.Time
	pid      int
	build    func() Document

	mu      sync.Mutex
	running bool
	stop    chan struct{}
	done    chan struct{}
	last    error
}

// NewWriter creates a writer. build is called on every flush.
func NewWriter(path string, interval time.Duration, build func() Document) *Writer {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &Writer{
		path:     path,
		interval: interval,
		started:  time.Now(),
		pid:      os.Getpid(),
		build:    build,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins the periodic flush loop.
func (w *Writer) Start() {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()
	go func() {
		defer close(w.done)
		t := time.NewTicker(w.interval)
		defer t.Stop()
		w.Flush()
		for {
			select {
			case <-w.stop:
				w.Flush()
				return
			case <-t.C:
				w.Flush()
			}
		}
	}()
}

// Stop flushes one last time and stops the loop. It is safe to call on a
// writer that was never started.
func (w *Writer) Stop() {
	w.mu.Lock()
	running := w.running
	w.mu.Unlock()
	if !running {
		return
	}
	select {
	case <-w.stop:
	default:
		close(w.stop)
	}
	<-w.done
}

// Err returns the last write error, if any.
func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last
}

// Flush writes the document atomically and reports the write error.
func (w *Writer) Flush() error {
	doc := w.build()
	doc.Version = 1
	if doc.PID == 0 {
		doc.PID = w.pid
	}
	if doc.Started.IsZero() {
		doc.Started = w.started
	}
	doc.Updated = time.Now()
	doc.UptimeSec = int64(doc.Updated.Sub(doc.Started).Seconds())

	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		w.setErr(err)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil {
		w.setErr(err)
		return err
	}
	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		w.setErr(err)
		return err
	}
	// Rename is atomic on POSIX and on NTFS, so a reader never sees a partial
	// document even if the daemon is killed mid-write. Windows refuses the
	// replace while a reader holds the file open, so retry briefly before
	// giving up; the next tick will try again anyway.
	var renameErr error
	for attempt := 0; attempt < 5; attempt++ {
		if renameErr = os.Rename(tmp, w.path); renameErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if renameErr != nil {
		os.Remove(tmp)
		w.setErr(renameErr)
		return renameErr
	}
	w.setErr(nil)
	return nil
}

func (w *Writer) setErr(err error) {
	w.mu.Lock()
	w.last = err
	w.mu.Unlock()
}

// Read parses an existing status file, for the "-status" CLI command.
func Read(path string) (Document, error) {
	var d Document
	data, err := os.ReadFile(path)
	if err != nil {
		return d, err
	}
	err = json.Unmarshal(data, &d)
	return d, err
}
