//go:build windows

package sync

import "os/exec"

// setProcGroup is a no-op on Windows: there is no process group to signal, and
// CreateProcess has no equivalent of setpgid.
func setProcGroup(cmd *exec.Cmd) {}

// killGroup terminates the direct child only.
func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
