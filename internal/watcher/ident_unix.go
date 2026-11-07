//go:build !windows

package watcher

import (
	"os"
	"syscall"
)

// fileIdentity names a directory independently of its path: the device it lives
// on and its inode number. Removing a directory and creating a new one under the
// same name produces a different identity, which is exactly what a mirror has to
// notice.
type fileIdentity struct {
	volume uint64
	index  uint64
}

// identityOf reads the identity of path.
func identityOf(path string) (fileIdentity, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fileIdentity{}, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		// No inode to compare: fall back to "cannot tell", which the caller
		// treats as "verify failed" rather than "unchanged".
		return fileIdentity{}, os.ErrInvalid
	}
	return fileIdentity{volume: uint64(st.Dev), index: uint64(st.Ino)}, nil
}
