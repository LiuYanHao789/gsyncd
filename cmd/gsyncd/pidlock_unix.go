//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"
)

// pidLock holds the flock for the process lifetime. The kernel releases it on
// exit or exec, so a SIGHUP re-exec re-acquires it without a stale state.
var pidLock *os.File

// acquirePidLock makes the pid-file claim atomic. The old read-check-write
// sequence let two processes starting together both decide they were alone.
func acquirePidLock(path string) error {
	lockPath := path + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if err == syscall.EWOULDBLOCK {
			return fmt.Errorf("another gsyncd is already running (pid lock %s held)", lockPath)
		}
		return fmt.Errorf("pid lock %s: %w", lockPath, err)
	}
	pidLock = f
	return nil
}

func releasePidLock() {
	if pidLock == nil {
		return
	}
	_ = syscall.Flock(int(pidLock.Fd()), syscall.LOCK_UN)
	_ = pidLock.Close()
	pidLock = nil
}
