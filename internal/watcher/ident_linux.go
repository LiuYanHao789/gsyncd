//go:build linux

package watcher

import (
	"os"
	"syscall"
	"unsafe"
)

// fileIdentity names a directory independently of its path: the device, the
// inode number and, when the filesystem provides it, the inode birth time.
//
// Device+inode alone is not enough: ext4 and other local filesystems routinely
// reuse the inode number when a directory is removed and recreated under the
// same name. Without a birth time the supervisor sees "same inode" and skips
// the reattach, even though every watch under the old inode is gone.
type fileIdentity struct {
	volume    uint64
	index     uint64
	btimeSec  int64
	btimeNsec int32
	hasBtime  bool
}

// statx timestamp and prefix. The kernel writes 256 bytes into the buffer; only
// the fields through stx_btime are read here.
type statxTimestamp struct {
	Sec  int64
	Nsec uint32
	_    int32
}

type statx struct {
	Mask       uint32
	Blksize    uint32
	Attributes uint64
	Nlink      uint32
	Uid        uint32
	Gid        uint32
	Mode       uint16
	_          uint16
	Ino        uint64
	Size       uint64
	Blocks     uint64
	AttrMask   uint64
	Atime      statxTimestamp
	Btime      statxTimestamp
}

const (
	// AT_FDCWD as a uintptr, without relying on a negative constant
	// conversion.
	statxAtFDCWD        = ^uintptr(99)
	statxAtSymlinkNofollow = 0x100
	statxBasicStats     = 0x7ff
	statxBtime          = 0x800
)

var _ = [1]struct{}{}[unsafe.Sizeof(statx{})-96]

// statxBtimeOf asks the kernel for the inode birth time. ok is false when the
// syscall is unavailable or the filesystem does not report btime.
func statxBtimeOf(path string) (sec int64, nsec int32, ok bool) {
	if !statxSupported() {
		return 0, 0, false
	}
	p, err := syscall.BytePtrFromString(path)
	if err != nil {
		return 0, 0, false
	}
	var buf [256]byte
	_, _, errno := syscall.Syscall6(
		statxSyscall,
		statxAtFDCWD,
		uintptr(unsafe.Pointer(p)),
		statxAtSymlinkNofollow,
		statxBasicStats|statxBtime,
		uintptr(unsafe.Pointer(&buf[0])),
		0,
	)
	if errno != 0 {
		return 0, 0, false
	}
	st := (*statx)(unsafe.Pointer(&buf[0]))
	if st.Mask&statxBtime == 0 {
		return 0, 0, false
	}
	return st.Btime.Sec, int32(st.Btime.Nsec), true
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
	id := fileIdentity{volume: uint64(st.Dev), index: uint64(st.Ino)}
	if sec, nsec, ok := statxBtimeOf(path); ok {
		id.btimeSec, id.btimeNsec, id.hasBtime = sec, nsec, true
	}
	return id, nil
}
