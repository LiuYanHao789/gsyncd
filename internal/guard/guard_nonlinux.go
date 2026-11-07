//go:build unix && !linux

package guard

import "syscall"

// applyNice renices the process on systems where the call is process-wide, so
// there is no per-thread sweep to do.
func applyNice(nice int) error {
	return syscall.Setpriority(syscall.PRIO_PROCESS, 0, nice)
}
