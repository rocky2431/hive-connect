//go:build unix

package iflow

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
)

func TestConfigureIFlowCmdCancellation_KillsPTYGrandchild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pidPath := filepath.Join(t.TempDir(), "grandchild.pid")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIFlowHelperProcess$", "--")
	cmd.Env = append(os.Environ(),
		"GO_WANT_IFLOW_HELPER_PROCESS=1",
		"IFLOW_HELPER_MODE=spawn-grandchild",
		"IFLOW_HELPER_GRANDCHILD_PID_FILE="+pidPath,
	)
	configureIFlowCmdCancellation(cmd)

	ptmx, err := pty.Start(cmd)
	if err != nil {
		t.Fatalf("start PTY helper: %v", err)
	}
	defer ptmx.Close()

	grandchildPID := waitForIFlowGrandchildPID(t, pidPath)
	cancel()

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("PTY command did not exit after context cancellation")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(grandchildPID, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grandchild process %d survived PTY command cancellation", grandchildPID)
}

func waitForIFlowGrandchildPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if convErr != nil {
				t.Fatalf("parse grandchild pid: %v", convErr)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read grandchild pid: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout waiting for grandchild pid")
	return 0
}
