//go:build !unix && !windows

package main

// There is no portable signal-0 query here. Reporting "not alive" avoids
// blocking startup; the pid file is advisory on these platforms.
func pidAlive(pid int) bool { return false }
