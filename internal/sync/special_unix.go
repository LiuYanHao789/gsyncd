//go:build linux

package sync

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// makeSpecial recreates a FIFO or device node. Sockets are skipped: a socket
// is a live endpoint, not data, and recreating a filesystem entry for it
// would not restore anything useful.
func makeSpecial(path string, info os.FileInfo, opts copyOptions) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	mode := info.Mode()
	var err error
	switch {
	case mode&os.ModeNamedPipe != 0:
		err = syscall.Mkfifo(path, uint32(mode.Perm()))
	case mode&os.ModeDevice != 0:
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("device node without Stat_t")
		}
		err = syscall.Mknod(path, nodeMode(mode), int(st.Rdev))
	default:
		// A socket is a live endpoint, not data; recreating its directory
		// entry would not restore anything. Report it so the caller logs and
		// the stale target entry is pruned instead of silently kept.
		return errors.New("socket or unsupported special file cannot be recreated")
	}
	if err != nil {
		return err
	}
	if opts.Chown {
		if uid, gid, ok := ownerOf(info); ok {
			_ = chownPath(path, uid, gid)
		}
	}
	if chErr := os.Chmod(path, fullModeOf(mode, opts.FullMode)); chErr != nil && opts.Logf != nil {
		opts.Logf("warn", "cannot set permissions on special file %s: %v", path, chErr)
	}
	return nil
}

// nodeMode maps os.FileMode device bits to the S_IF* values mknod expects.
func nodeMode(m os.FileMode) uint32 {
	mode := uint32(m.Perm())
	switch {
	case m&os.ModeCharDevice != 0:
		mode |= syscall.S_IFCHR
	case m&os.ModeDevice != 0:
		mode |= syscall.S_IFBLK
	case m&os.ModeNamedPipe != 0:
		mode |= syscall.S_IFIFO
	case m&os.ModeSocket != 0:
		mode |= syscall.S_IFSOCK
	}
	return mode
}
