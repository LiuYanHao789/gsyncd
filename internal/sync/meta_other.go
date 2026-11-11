//go:build !unix

package sync

import "os"

// Ownership and inode identity are not exposed through the portable os API.
// The Windows port keeps the previous behaviour: content, mode and mtime.
func ownerOf(os.FileInfo) (uid, gid int, ok bool) { return 0, 0, false }

func canChown() bool { return false }

func chownPath(string, int, int) error { return nil }

func linkKeyOf(os.FileInfo) (dev, ino uint64, ok bool) { return 0, 0, false }
