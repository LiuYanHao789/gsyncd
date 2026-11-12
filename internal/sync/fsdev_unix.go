//go:build unix

package sync

import (
	"os"
	"syscall"
)

// differentFilesystem reports whether a and b live on different devices.
// Bind mounts of the root filesystem share a device and therefore still
// require the marker; a real backup disk or network mount does not.
func differentFilesystem(a, b os.FileInfo) bool {
	as, ok := a.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	bs, ok := b.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return uint64(as.Dev) != uint64(bs.Dev)
}
