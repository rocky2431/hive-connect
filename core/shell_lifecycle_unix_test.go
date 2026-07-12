//go:build unix

package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCoreShellLifecycleHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_CORE_SHELL_HELPER") != "1" {
		return
	}

	pidPath := os.Getenv("CORE_SHELL_HELPER_PID_FILE")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	switch os.Getenv("CORE_SHELL_HELPER_MODE") {
	case "late-write":
		timer := time.NewTimer(250 * time.Millisecond)
		defer timer.Stop()
		<-timer.C
		if err := os.WriteFile(os.Getenv("CORE_SHELL_HELPER_LATE_FILE"), []byte("late"), 0o644); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	case "wait-signal":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		<-ctx.Done()
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func TestEngineStopKillsShellProcessTreeBeforeLateWorkspaceWrite(t *testing.T) {
	workspace := t.TempDir()
	pidPath := filepath.Join(workspace, "grandchild.pid")
	latePath := filepath.Join(workspace, "late.txt")
	command := coreShellHelperCommand("late-write", pidPath, latePath)

	platform := &stubPlatformEngine{n: "test"}
	engine := NewEngine("shell-lifecycle", &stubAgent{}, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	msg := &Message{
		SessionKey: "test:chat:user",
		Platform:   "test",
		UserID:     "admin",
		Content:    "/shell " + command,
		ReplyCtx:   "reply",
	}
	engine.cmdShell(platform, msg, msg.Content)

	pid := waitCoreShellHelperPID(t, pidPath)
	t.Cleanup(func() {
		if process, err := os.FindProcess(pid); err == nil {
			_ = process.Kill()
		}
	})

	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	waitCoreShellProcessGone(t, pid)

	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(latePath); err == nil {
			t.Fatal("shell grandchild wrote to workspace after Engine.Stop")
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat late workspace write: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func TestCronSchedulerStopWaitsForManualShellRun(t *testing.T) {
	workspace := t.TempDir()
	pidPath := filepath.Join(workspace, "cron.pid")
	command := coreShellHelperCommand("wait-signal", pidPath, "")
	engine, platform := newShellSchedulerEngine(t, "cron-shell")
	defer engine.Stop()

	store, err := NewCronStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCronStore: %v", err)
	}
	scheduler := NewCronScheduler(store)
	scheduler.RegisterEngine("cron-shell", engine)
	job := &CronJob{
		ID:         "manual-shell",
		Project:    "cron-shell",
		SessionKey: platform.Name() + ":chat:user",
		CronExpr:   "0 6 * * *",
		Exec:       command,
		WorkDir:    workspace,
	}
	if err := scheduler.AddJob(job); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	if err := scheduler.RunJobNow(job.ID); err != nil {
		t.Fatalf("RunJobNow: %v", err)
	}

	pid := waitCoreShellHelperPID(t, pidPath)
	scheduler.Stop()
	waitCoreShellProcessGone(t, pid)
}

func TestTimerSchedulerStopWaitsForTriggeredShellRun(t *testing.T) {
	workspace := t.TempDir()
	pidPath := filepath.Join(workspace, "timer.pid")
	command := coreShellHelperCommand("wait-signal", pidPath, "")
	engine, platform := newShellSchedulerEngine(t, "timer-shell")
	defer engine.Stop()

	store, err := NewTimerStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTimerStore: %v", err)
	}
	scheduler := NewTimerScheduler(store)
	scheduler.RegisterEngine("timer-shell", engine)
	job := &TimerJob{
		ID:          "triggered-shell",
		Project:     "timer-shell",
		SessionKey:  platform.Name() + ":chat:user",
		ScheduledAt: time.Now().Add(10 * time.Millisecond),
		Exec:        command,
		WorkDir:     workspace,
	}
	if err := scheduler.AddJob(job); err != nil {
		t.Fatalf("AddJob: %v", err)
	}

	pid := waitCoreShellHelperPID(t, pidPath)
	scheduler.Stop()
	waitCoreShellProcessGone(t, pid)
	if persisted := store.Get(job.ID); persisted == nil || !persisted.Fired || !strings.Contains(persisted.LastError, "interrupted_outcome_unknown") {
		t.Fatalf("timer shutdown did not fail closed: %#v", persisted)
	}
}

func coreShellHelperCommand(mode, pidPath, latePath string) string {
	env := []string{
		"GO_WANT_CORE_SHELL_HELPER=1",
		"CORE_SHELL_HELPER_MODE=" + mode,
		"CORE_SHELL_HELPER_PID_FILE=" + pidPath,
	}
	if latePath != "" {
		env = append(env, "CORE_SHELL_HELPER_LATE_FILE="+latePath)
	}
	parts := []string{"env"}
	for _, item := range env {
		parts = append(parts, shellQuote(item))
	}
	parts = append(parts, shellQuote(os.Args[0]), "-test.run=^TestCoreShellLifecycleHelperProcess$", "--")
	return strings.Join(parts, " ")
}

func newShellSchedulerEngine(t *testing.T, name string) (*Engine, *stubCronReplyTargetPlatform) {
	t.Helper()
	platform := &stubCronReplyTargetPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	engine := NewEngine(name, &stubAgent{}, []Platform{platform}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	return engine, platform
}

func waitCoreShellHelperPID(t *testing.T, path string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, convErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if convErr != nil {
				t.Fatalf("parse helper pid: %v", convErr)
			}
			return pid
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read helper pid: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for helper pid: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitCoreShellProcessGone(t *testing.T, pid int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("shell grandchild %d survived cancellation", pid)
		case <-ticker.C:
		}
	}
}
