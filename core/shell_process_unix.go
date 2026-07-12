//go:build unix

package core

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureShellProcessTree gives each shell command its own process group and
// makes CommandContext cancellation terminate the complete tree. Shell tools
// commonly fork workers that inherit stdout and workspace access; killing only
// the direct shell would let those workers outlive Engine.Stop.
func configureShellProcessTree(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	cmd.Cancel = func() error {
		return terminateShellProcessTree(cmd)
	}
}

func terminateShellProcessTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil &&
		!errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
