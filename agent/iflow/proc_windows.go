//go:build windows

package iflow

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

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
		return fmt.Errorf(
			"taskkill failed: %w: %s; process kill fallback failed: %w",
			err,
			iflowProcessKillOutput(output),
			killErr,
		)
	}
}

func iflowProcessKillOutput(output []byte) string {
	trimmed := strings.TrimSpace(string(output))
	if trimmed == "" {
		return "(empty output)"
	}
	return trimmed
}
