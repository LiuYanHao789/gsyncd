//go:build !unix && !windows

package main

import (
	"errors"
	"os"
	"os/signal"
)

// notifySignals installs the only portable handler. Platforms in this group
// have neither SIGHUP nor syscall.Exec, so a configuration change needs a
// restart.
func ignoreReloadSignalUntilReady() {}

func notifySignals(c chan<- os.Signal) {
	signal.Notify(c, os.Interrupt)
}

func isReloadSignal(os.Signal) bool { return false }

func reExecSelf() error {
	return errors.New("reload is not supported on this platform")
}
