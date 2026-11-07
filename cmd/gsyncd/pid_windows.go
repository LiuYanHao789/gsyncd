//go:build windows

package main

import "syscall"

// processQueryLimitedInformation is what Task Manager uses to ask a process
// whether it is still there; it needs no elevated rights.
const processQueryLimitedInformation = 0x1000

// stillActive is the exit code Windows reports for a process that has not
// exited yet.
const stillActive = 259

// pidAlive reports whether pid names a process that still exists.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		// Opened but cannot ask: assume it is alive, which is the safe answer
		// for "may I start a second writer".
		return true
	}
	return code == stillActive
}
