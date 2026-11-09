//go:build linux

package sync

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestIsNoSpace(t *testing.T) {
	if !isNoSpace(&os.PathError{Err: syscall.ENOSPC}) {
		t.Fatal("ENOSPC not recognised")
	}
	if isNoSpace(errors.New("other")) {
		t.Fatal("unrelated error treated as disk full")
	}
}
