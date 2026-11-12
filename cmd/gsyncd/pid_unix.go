//go:build unix

package main

import "syscall"

// pidAlive reports whether pid names a process that still exists.
//
// Signal 0 delivers nothing; it only runs the existence and permission checks.
// EPERM means "the process is there but belongs to somebody else", which for
// our purpose - is a second writer already running - counts as alive.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return err == syscall.EPERM
}
