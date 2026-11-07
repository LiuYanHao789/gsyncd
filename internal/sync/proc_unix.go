//go:build !windows

package sync

import (
	"os/exec"
	"syscall"
)

// setProcGroup puts the child into its own process group so the whole tree can
// be signalled at once. rsyncssh spawns ssh which spawns rsync on the far side;
// killing only the direct child orphans everything below it.
func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killGroup signals the child's process group.
//
// SIGTERM first would let rsync finish its current file, but a transfer that
// has already blown past its deadline is more likely stuck in a read than
// working, so the group gets SIGKILL. The remote side is left to ssh's own
// ServerAliveInterval, which the deploy examples set.
func killGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		// The group may already be gone; fall back to the child itself.
		_ = cmd.Process.Kill()
	}
}
