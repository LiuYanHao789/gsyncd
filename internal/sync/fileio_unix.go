//go:build unix

package sync

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// The os package opens every file with kindOpenFile, which registers it with
// the runtime poller: on Linux that costs an epoll_ctl (EPERM for regular
// files), a SetNonblock pair and several fcntl calls per file. For a tree of
// millions of small files that is pure overhead: regular files are never
// pollable, and this process never sets a read deadline on them.
//
// Opening through syscall.Open and wrapping with os.NewFile uses kindNewFile:
// the descriptor stays blocking, NewFile only does one F_GETFL, and no epoll
// registration is attempted. The measurement on a small sync drops from ~8
// fcntl + 2 epoll_ctl per file to ~1 fcntl and no epoll_ctl.

var tmpSeq uint64

// isNoSpace reports whether a copy failure means the target filesystem is
// full. Consecutive failures of this kind stop the pass: hammering a full disk
// with millions of doomed small files can run for hours on a large tree.
func isNoSpace(err error) bool { return errors.Is(err, syscall.ENOSPC) }

func openRead(path string) (*os.File, error) {
	var fd int
	var err error
	for {
		fd, err = syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.EBADF}
	}
	return f, nil
}

func createTemp(dir, pattern string) (*os.File, error) {
	prefix := strings.TrimSuffix(pattern, "*")
	base := fmt.Sprintf("%s%d-%d", prefix, os.Getpid(), time.Now().UnixNano())
	var last error
	for i := 0; i < 10000; i++ {
		name := filepath.Join(dir, fmt.Sprintf("%s-%d", base, atomic.AddUint64(&tmpSeq, 1)))
		fd, err := syscall.Open(name, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_CLOEXEC, 0o600)
		if err == nil {
			f := os.NewFile(uintptr(fd), name)
			if f == nil {
				_ = syscall.Close(fd)
				return nil, &os.PathError{Op: "createtemp", Path: name, Err: syscall.EBADF}
			}
			return f, nil
		}
		last = err
		if err != syscall.EEXIST {
			return nil, &os.PathError{Op: "createtemp", Path: name, Err: err}
		}
	}
	return nil, &os.PathError{Op: "createtemp", Path: filepath.Join(dir, prefix+"*"), Err: last}
}

func readDirFast(path string) ([]os.DirEntry, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.EBADF}
	}
	defer f.Close()
	return f.ReadDir(-1)
}
