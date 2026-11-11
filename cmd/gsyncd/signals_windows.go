//go:build windows

package main

import (
	"errors"
	"os"
	"os/signal"
)

// notifySignals installs the handlers Windows supports. There is no SIGHUP,
// so a reload is not available; a configuration change needs a restart.
func ignoreReloadSignalUntilReady() {}

func notifySignals(c chan<- os.Signal) {
	signal.Notify(c, os.Interrupt)
}

func isReloadSignal(os.Signal) bool { return false }

func reExecSelf() error {
	return errors.New("reload is not supported on this platform")
}
