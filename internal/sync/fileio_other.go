//go:build !unix

package sync

import "os"

// Non-unix platforms keep the os package implementations. The Windows
// CreateFile path and the runtime's poller behave differently enough that the
// syscall shortcut is only justified where it was measured.

func openRead(path string) (*os.File, error) { return os.Open(path) }

func createTemp(dir, pattern string) (*os.File, error) { return os.CreateTemp(dir, pattern) }

func readDirFast(path string) ([]os.DirEntry, error) { return os.ReadDir(path) }

// Windows reports a full disk as ERROR_DISK_FULL, which the os package does
// not wrap in a portable error; the pass keeps its previous behaviour there.
func isNoSpace(error) bool { return false }
