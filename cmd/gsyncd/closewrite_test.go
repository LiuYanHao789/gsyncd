package main

import (
	"strings"
	"testing"

	"gsyncd/internal/config"
	"gsyncd/internal/event"
	"gsyncd/internal/watcher"
)

// plainBackend implements watcher.Backend and nothing else, the way a backend
// that predates CloseReporter would.
type plainBackend struct{}

// Compile-time proof that the test double really is a backend.
var _ watcher.Backend = plainBackend{}

func (plainBackend) Add(string) error           { return nil }
func (plainBackend) Remove(string) error        { return nil }
func (plainBackend) Events() <-chan event.Event { return nil }
func (plainBackend) Close() error               { return nil }

type closingBackend struct{ plainBackend }

func (closingBackend) ReportsClose() bool { return true }

type snapBackend struct{ plainBackend }

func (snapBackend) ReportsClose() bool { return false }

func TestCheckCloseWriteAcceptsReportingBackend(t *testing.T) {
	syncs := []config.Sync{{Name: "data", CloseWrite: true}}
	if err := checkCloseWrite(closingBackend{}, syncs); err != nil {
		t.Fatalf("a backend that reports closes must be accepted: %v", err)
	}
}

func TestCheckCloseWriteRejectsSnapshotBackend(t *testing.T) {
	syncs := []config.Sync{{Name: "data", CloseWrite: true}}
	err := checkCloseWrite(snapBackend{}, syncs)
	if err == nil {
		t.Fatal("close_write on a snapshot backend must be refused, not silently degraded")
	}
	if !strings.Contains(err.Error(), "close_write") {
		t.Fatalf("error should name the option: %v", err)
	}
}

func TestCheckCloseWriteAllowsSnapshotBackendWithoutOption(t *testing.T) {
	// The option is off: the backend's inability to report closes is irrelevant
	// and must not block a configuration that worked before close_write existed.
	syncs := []config.Sync{{Name: "data"}}
	if err := checkCloseWrite(snapBackend{}, syncs); err != nil {
		t.Fatalf("without close_write the backend capability must not matter: %v", err)
	}
}

func TestCheckCloseWriteAcceptsBackendWithoutInterface(t *testing.T) {
	// A backend that says nothing about closes is assumed capable: the check is
	// a guard for backends that explicitly declare they cannot, not a
	// requirement that every backend implement the interface.
	syncs := []config.Sync{{Name: "data", CloseWrite: true}}
	if err := checkCloseWrite(plainBackend{}, syncs); err != nil {
		t.Fatalf("an unaware backend must not be rejected: %v", err)
	}
}
