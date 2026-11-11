//go:build unix

package sync

import (
	"os"
	"syscall"
)

// ownerOf returns the uid/gid recorded in info, when the platform exposes
// them. It is used only to restore ownership on a mirror; a missing Stat_t is
// not an error because the callers treat it as "nothing to copy".
func ownerOf(info os.FileInfo) (uid, gid int, ok bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return int(st.Uid), int(st.Gid), true
}

// canChown reports whether this process can be expected to change ownership.
// Only root can, and probing per file would turn every copy into a logged
// EPERM on an ordinary deployment.
func canChown() bool { return os.Geteuid() == 0 }

func chownPath(path string, uid, gid int) error { return os.Chown(path, uid, gid) }

// linkKeyOf identifies a hardlink group. Nlink < 2 means the file is not
// hardlinked and tracking it would only waste memory.
func linkKeyOf(info os.FileInfo) (dev, ino uint64, ok bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Nlink < 2 {
		return 0, 0, false
	}
	return uint64(st.Dev), uint64(st.Ino), true
}
