//go:build windows

package watcher

import (
	"os"
	"syscall"
)

// fileIdentity names a directory independently of its path: the volume serial
// number and the file index. Removing a directory and creating a new one under
// the same name produces a different index.
//
// os.SameFile is not usable here: on Windows it compares creation times, and two
// directories created within the same clock tick compare equal even when they
// are different objects (measured: a deleted-and-recreated directory with a
// different file index still reported SameFile == true).
type fileIdentity struct {
	volume uint64
	index  uint64
}

const (
	fileFlagBackupSemantics = 0x02000000
	fileShareAll            = syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE
)

// identityOf reads the identity of path.
func identityOf(path string) (fileIdentity, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return fileIdentity{}, err
	}
	h, err := syscall.CreateFile(p, 0, fileShareAll, nil, syscall.OPEN_EXISTING, fileFlagBackupSemantics, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return fileIdentity{}, os.ErrNotExist
		}
		return fileIdentity{}, err
	}
	defer syscall.CloseHandle(h)
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(h, &info); err != nil {
		return fileIdentity{}, err
	}
	return fileIdentity{
		volume: uint64(info.VolumeSerialNumber),
		index:  uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow),
	}, nil
}
