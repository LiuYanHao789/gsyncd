//go:build windows

package main

// daemonSupported reports whether the platform can detach itself.
const daemonSupported = false

// daemonize is a no-op on Windows: there is no fork, no setsid and therefore no
// way to detach from the console reliably. Windows expects a service (which must
// be a foreground process controlled by the service manager) or a scheduled
// task, so gsyncd stays in the foreground and says so once.
func daemonize(string) (bool, error) { return false, nil }
