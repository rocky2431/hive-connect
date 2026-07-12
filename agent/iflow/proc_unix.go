//go:build unix

package iflow

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureIFlowCmdCancellation replaces CommandContext's direct-child kill
// with a process-group kill. pty.Start creates a new session whose leader is
// cmd.Process, so descendants spawned by the CLI share the -PID process group.
func configureIFlowCmdCancellation(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	cmd.Cancel = func() error {
		return forceKillIFlowCmd(cmd)
	}
}

func forceKillIFlowCmd(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil &&
		!errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
