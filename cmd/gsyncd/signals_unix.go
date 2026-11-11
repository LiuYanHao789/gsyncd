//go:build unix

package main

import (
	"os"
	"os/signal"
	"syscall"
)

// ignoreReloadSignalUntilReady makes SIGHUP harmless during the window between
// process start (including a reload re-exec) and notifySignals. Without it the
// kernel default terminates the daemon, which is exactly when a second
// `systemctl reload` would arrive.
func ignoreReloadSignalUntilReady() {
	signal.Ignore(syscall.SIGHUP)
}

// notifySignals installs the handlers gsyncd understands. SIGHUP is included
// here because a signal.Notify that omits it leaves the kernel default in
// place, which is to terminate the daemon - the opposite of a reload.
func notifySignals(c chan<- os.Signal) {
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
}

// isReloadSignal reports whether s asks for a configuration reload.
func isReloadSignal(s os.Signal) bool { return s == syscall.SIGHUP }

// reExecSelf replaces the current process image with a fresh gsyncd, keeping
// the pid. syscall.Exec is the only Unix primitive that does that; fork+exec
// would either change the pid (breaking the pid file) or need a second
// daemonize path that races the status file.
func reExecSelf() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	env := append(os.Environ(), envReloaded+"=1")
	return syscall.Exec(exe, os.Args, env)
}
