// Package sync applies aggregated changes to a target.
package sync

import (
	"context"
	"path/filepath"
	"strings"

	"gsyncd/internal/aggregate"
)

// Backend applies a batch of changes to the target.
type Backend interface {
	// Name identifies the backend in logs and the status file.
	Name() string
	// Startup performs the initial full synchronisation.
	Startup(ctx context.Context) error
	// Apply pushes one batch. It may be called concurrently.
	Apply(ctx context.Context, batch Batch) error
	// Close releases resources.
	Close() error
}

// Batch is one unit of work handed to a backend.
type Batch struct {
	// Changes is the collapsed window.
	Changes []aggregate.Change
	// Full requests a wholesale resync of the tree, e.g. after an inotify
	// queue overflow. Unlike the original lsyncd this does not restart the
	// process, it only re-runs the transfer for the affected sync.
	Full bool
	// Reason is a short human readable explanation used in logs.
	Reason string
}

// relInSource returns p relative to root, and false when p is not inside it.
//
// Every backend turns an event path into a destination path with
// filepath.Join(target, rel). A path that is not under the sync root - a stray
// event, or a rename that crosses two syncs - produces "..", and Join is happy
// to walk back out of the target. Nothing may be written or removed outside the
// directory the operator named, so this is checked in one place.
func relInSource(root, p string) (string, bool) {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return "", false
	}
	if rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return rel, true
}

// Stats exposes cheap counters for the status file.
type Stats struct {
	Batches uint64
	Files   uint64
	Deletes uint64
	Errors  uint64
	Retries uint64
}
