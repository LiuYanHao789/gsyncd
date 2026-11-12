//go:build unix

package main

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// startupGrace is how long the parent waits for the child to fail before it
// claims the daemon is up.
const startupGrace = 2 * time.Second

// envDaemonized marks the re-executed child, so it does not fork again.
const envDaemonized = "GSYNCD_DAEMONIZED"

// daemonSupported reports whether the platform can detach itself.
const daemonSupported = true

// daemonize detaches the process and returns true in the parent, which must
// then exit.
//
// lsyncd forked itself and called setsid. Go cannot fork without execing, so the
// binary re-executes itself with a marker in the environment and the parent
// exits. setsid on the child gives it its own session, which is what makes the
// shell not send it SIGHUP when the terminal closes.
//
// This is deliberately not done when the process is already a service manager's
// child with nodaemon set: systemd and friends want to supervise a foreground
// process, and double-forking is what makes "the unit is active but the daemon
// died" possible.
func daemonize(logFile string) (bool, error) {
	if os.Getenv(envDaemonized) != "" {
		return false, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return false, fmt.Errorf("locate own binary: %w", err)
	}
	// The log file is opened by the child from the configuration; the stdio of
	// the parent is the terminal, which will not exist once it exits.
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return false, err
	}
	defer devNull.Close()

	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Env = append(os.Environ(), envDaemonized+"=1")
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return false, fmt.Errorf("daemonize: %w", err)
	}
	// Wait briefly for the child: reporting "started, pid N" for a process that
	// died two milliseconds later on a bad configuration is worse than useless,
	// and the daemon has nothing to do before it reads its configuration and
	// installs watches.
	errc := make(chan error, 1)
	go func() { errc <- cmd.Wait() }()
	select {
	case err := <-errc:
		if err == nil {
			return false, fmt.Errorf("background process exited immediately with status 0")
		}
		return false, fmt.Errorf("background process exited immediately: %w", err)
	case <-time.After(startupGrace):
	}
	if logFile == "" {
		fmt.Fprintln(os.Stderr, "gsyncd: warning: no log_file configured, the background process has nowhere to log")
	}
	fmt.Printf("gsyncd: running in the background, pid %d\n", cmd.Process.Pid)
	return true, nil
}
