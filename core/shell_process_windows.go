//go:build windows

package core

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

func configureShellProcessTree(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP
	cmd.Cancel = func() error {
		return terminateShellProcessTree(cmd)
	}
}

func terminateShellProcessTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	killCmd := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid))
	output, err := killCmd.CombinedOutput()
	if err == nil {
		return nil
	}
	lowerOutput := bytes.ToLower(output)
	if bytes.Contains(lowerOutput, []byte("there is no running instance")) ||
		bytes.Contains(lowerOutput, []byte("not found")) {
		return nil
	}
	if killErr := cmd.Process.Kill(); killErr == nil || errors.Is(killErr, os.ErrProcessDone) {
		return nil
	} else {
		trimmed := strings.TrimSpace(string(output))
		if trimmed == "" {
			trimmed = "(empty output)"
		}
		return fmt.Errorf("taskkill failed: %w: %s; process kill fallback failed: %w", err, trimmed, killErr)
	}
}
