package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoppedEngineRejectsSynchronousSchedulerEntries(t *testing.T) {
	engine := NewEngine("stopped-scheduler", &stubAgent{}, nil, t.TempDir()+"/sessions.json", LangEnglish)
	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if err := engine.ExecuteCronJob(&CronJob{}); !errors.Is(err, ErrEngineStopping) {
		t.Fatalf("ExecuteCronJob() error = %v, want ErrEngineStopping", err)
	}
	if err := engine.ExecuteTimerJob(&TimerJob{}); !errors.Is(err, ErrEngineStopping) {
		t.Fatalf("ExecuteTimerJob() error = %v, want ErrEngineStopping", err)
	}
}

func TestStoppedSchedulersRejectNewExecutionMutations(t *testing.T) {
	t.Run("cron", func(t *testing.T) {
		store, err := NewCronStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewCronStore: %v", err)
		}
		scheduler := NewCronScheduler(store)
		existing := &CronJob{
			ID:         "existing",
			Project:    "project",
			SessionKey: "test:chat:user",
			CronExpr:   "0 6 * * *",
			Prompt:     "hello",
		}
		if err := scheduler.AddJob(existing); err != nil {
			t.Fatalf("AddJob existing: %v", err)
		}
		scheduler.Stop()

		late := &CronJob{
			ID:         "late",
			Project:    "project",
			SessionKey: "test:chat:user",
			CronExpr:   "0 7 * * *",
			Prompt:     "late",
		}
		if err := scheduler.AddJob(late); !errors.Is(err, ErrCronSchedulerStopped) {
			t.Fatalf("AddJob after Stop error = %v, want ErrCronSchedulerStopped", err)
		}
		if store.Get(late.ID) != nil {
			t.Fatal("stopped CronScheduler persisted an unschedulable job")
		}
		if err := scheduler.RunJobNow(existing.ID); !errors.Is(err, ErrCronSchedulerStopped) {
			t.Fatalf("RunJobNow after Stop error = %v, want ErrCronSchedulerStopped", err)
		}
	})

	t.Run("timer", func(t *testing.T) {
		store, err := NewTimerStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewTimerStore: %v", err)
		}
		scheduler := NewTimerScheduler(store)
		future := &TimerJob{
			ID:          "future",
			Project:     "project",
			SessionKey:  "test:chat:user",
			ScheduledAt: time.Now().Add(time.Hour),
			Prompt:      "future",
		}
		if err := scheduler.AddJob(future); err != nil {
			t.Fatalf("AddJob future: %v", err)
		}
		scheduler.Stop()
		if persisted := store.Get(future.ID); persisted == nil || persisted.Fired {
			t.Fatalf("untriggered timer was consumed during Stop: %#v", persisted)
		}
		late := &TimerJob{
			ID:          "late",
			Project:     "project",
			SessionKey:  "test:chat:user",
			ScheduledAt: time.Now().Add(time.Hour),
			Prompt:      "late",
		}
		if err := scheduler.AddJob(late); !errors.Is(err, ErrTimerSchedulerStopped) {
			t.Fatalf("AddJob after Stop error = %v, want ErrTimerSchedulerStopped", err)
		}
		if store.Get(late.ID) != nil {
			t.Fatal("stopped TimerScheduler persisted an unschedulable job")
		}
		scheduler.mu.RLock()
		_, scheduled := scheduler.timers[late.ID]
		scheduler.mu.RUnlock()
		if scheduled {
			t.Fatal("stopped TimerScheduler retained a late timer")
		}
	})
}

func TestTimerSchedulerStopFailsClosedForActivePromptRun(t *testing.T) {
	session := newControllableSession("timer-active")
	started := make(chan struct{})
	agent := &controllableAgent{startSessionFn: func(context.Context, string) (AgentSession, error) {
		close(started)
		return session, nil
	}}
	platform := &stubCronReplyTargetPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	engine := NewEngine("timer-prompt", agent, []Platform{platform}, t.TempDir()+"/sessions.json", LangEnglish)
	defer engine.Stop()

	store, err := NewTimerStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewTimerStore: %v", err)
	}
	scheduler := NewTimerScheduler(store)
	scheduler.RegisterEngine("timer-prompt", engine)
	job := &TimerJob{
		ID:          "active-prompt",
		Project:     "timer-prompt",
		SessionKey:  "test:chat:user",
		ScheduledAt: time.Now().Add(10 * time.Millisecond),
		Prompt:      "work",
	}
	if err := scheduler.AddJob(job); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	waitLifecycleSignal(t, started, "timer prompt start")

	scheduler.Stop()
	waitLifecycleSignal(t, session.closed, "timer prompt cancellation")
	persisted := store.Get(job.ID)
	if persisted == nil || !persisted.Fired || !strings.Contains(persisted.LastError, "interrupted_outcome_unknown") {
		t.Fatalf("active prompt timer did not fail closed: %#v", persisted)
	}
}

// Test Double rationale: Engine shutdown needs observable Agent and Platform
// boundaries without starting a real provider process or messaging transport.
type lifecycleCountingAgent struct {
	stubAgent
	stopCalls atomic.Int32
}

func (a *lifecycleCountingAgent) Stop() error {
	a.stopCalls.Add(1)
	return nil
}

type lifecycleCountingPlatform struct {
	stubPlatformEngine
	stopCalls   atomic.Int32
	stopStarted chan struct{}
	stopOnce    sync.Once
}

func (p *lifecycleCountingPlatform) Stop() error {
	p.stopCalls.Add(1)
	p.stopOnce.Do(func() { close(p.stopStarted) })
	return nil
}

func TestEngineStopWaitsForLifecycleTasksAndIsIdempotent(t *testing.T) {
	agent := &lifecycleCountingAgent{}
	platform := &lifecycleCountingPlatform{stopStarted: make(chan struct{})}
	engine := NewEngine("lifecycle", agent, []Platform{platform}, t.TempDir()+"/sessions.json", LangEnglish)

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	if !engine.startLifecycleTask(func() {
		close(started)
		<-release
		close(finished)
	}) {
		t.Fatal("initial lifecycle task was rejected")
	}
	waitLifecycleSignal(t, started, "lifecycle task start")

	stopDone := make(chan error, 1)
	go func() { stopDone <- engine.Stop() }()
	waitLifecycleSignal(t, platform.stopStarted, "platform stop boundary")
	engine.platformLifecycleMu.Lock()
	stopping := engine.stopping
	engine.platformLifecycleMu.Unlock()
	if !stopping {
		t.Fatal("platform Stop ran before engine entered stopping state")
	}
	if engine.startLifecycleTask(func() {}) {
		t.Fatal("late lifecycle task started after stopping boundary")
	}

	blockedCtx, cancelBlocked := contextWithTestTimeout(t, 50*time.Millisecond)
	select {
	case err := <-stopDone:
		cancelBlocked()
		t.Fatalf("Stop returned before lifecycle task completed: %v", err)
	case <-blockedCtx.Done():
		cancelBlocked()
	}

	close(release)
	waitLifecycleSignal(t, finished, "lifecycle task completion")
	if err := waitLifecycleResult(t, stopDone, "initial Stop"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- engine.Stop()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("idempotent Stop() error = %v", err)
		}
	}

	if got := agent.stopCalls.Load(); got != 1 {
		t.Fatalf("Agent.Stop calls = %d, want 1", got)
	}
	if got := platform.stopCalls.Load(); got != 1 {
		t.Fatalf("Platform.Stop calls = %d, want 1", got)
	}

	lateRan := make(chan struct{})
	if engine.startLifecycleTask(func() { close(lateRan) }) {
		t.Fatal("lifecycle task started after Stop")
	}
	select {
	case <-lateRan:
		t.Fatal("rejected lifecycle task still ran")
	default:
	}
}

func TestEngineStopWaitsBeforeStoppingDistinctWorkspaceAgents(t *testing.T) {
	globalAgent := &lifecycleCountingAgent{}
	workspaceAgent := &lifecycleCountingAgent{}
	platform := &lifecycleCountingPlatform{stopStarted: make(chan struct{})}
	engine := NewEngine("workspace-lifecycle", globalAgent, []Platform{platform}, t.TempDir()+"/sessions.json", LangEnglish)
	engine.workspacePool = newWorkspacePool(0)

	workspaceOne := engine.workspacePool.GetOrCreate(t.TempDir())
	workspaceOne.mu.Lock()
	workspaceOne.agent = workspaceAgent
	workspaceOne.mu.Unlock()
	workspaceTwo := engine.workspacePool.GetOrCreate(t.TempDir())
	workspaceTwo.mu.Lock()
	workspaceTwo.agent = globalAgent
	workspaceTwo.mu.Unlock()

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var stoppedBeforeTaskFinished atomic.Bool
	if !engine.startLifecycleTask(func() {
		close(started)
		<-release
		if globalAgent.stopCalls.Load() != 0 || workspaceAgent.stopCalls.Load() != 0 {
			stoppedBeforeTaskFinished.Store(true)
		}
		close(finished)
	}) {
		t.Fatal("workspace lifecycle task was rejected")
	}
	waitLifecycleSignal(t, started, "workspace lifecycle task start")

	stopDone := make(chan error, 1)
	go func() { stopDone <- engine.Stop() }()
	waitLifecycleSignal(t, platform.stopStarted, "workspace platform stop boundary")
	close(release)
	waitLifecycleSignal(t, finished, "workspace lifecycle task completion")
	if err := waitLifecycleResult(t, stopDone, "workspace Stop"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if stoppedBeforeTaskFinished.Load() {
		t.Fatal("Agent.Stop ran before lifecycle task completed")
	}
	if got := globalAgent.stopCalls.Load(); got != 1 {
		t.Fatalf("global Agent.Stop calls = %d, want 1", got)
	}
	if got := workspaceAgent.stopCalls.Load(); got != 1 {
		t.Fatalf("workspace Agent.Stop calls = %d, want 1", got)
	}
}

func TestLifecycleGateRejectionClearsTransientCardStates(t *testing.T) {
	t.Run("model switch", func(t *testing.T) {
		agent := &stubModelModeAgent{}
		engine := NewEngine("late-model", agent, nil, t.TempDir()+"/sessions.json", LangEnglish)
		if err := engine.Stop(); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}

		sessionKey := "media:late-model:user"
		engine.executeCardAction("/model", "switch 1", sessionKey)
		interactiveKey := engine.interactiveKeyForSessionKey(sessionKey)
		engine.interactiveMu.Lock()
		state := engine.interactiveStates[interactiveKey]
		engine.interactiveMu.Unlock()
		if state == nil {
			t.Fatal("late model callback did not retain a readable state")
		}
		state.mu.Lock()
		modelSwitch := state.modelSwitch
		state.mu.Unlock()
		if modelSwitch != nil {
			t.Fatalf("late model state = %#v, want no switching residue", modelSwitch)
		}
	})

	t.Run("delete", func(t *testing.T) {
		engine := NewEngine("late-delete", &stubAgent{}, nil, t.TempDir()+"/sessions.json", LangEnglish)
		if err := engine.Stop(); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}

		sessionKey := "media:late-delete:user"
		interactiveKey := engine.interactiveKeyForSessionKey(sessionKey)
		state := &interactiveState{deleteMode: &deleteModeState{
			selectedIDs: map[string]struct{}{"session-1": {}},
			phase:       "confirm",
		}}
		engine.interactiveMu.Lock()
		engine.interactiveStates[interactiveKey] = state
		engine.interactiveMu.Unlock()

		engine.executeDeleteModeAction(sessionKey, "submit")
		state.mu.Lock()
		phase := state.deleteMode.phase
		result := state.deleteMode.result
		state.mu.Unlock()
		if phase != "result" || result == "" {
			t.Fatalf("late delete state = phase %q, result %q; want terminal failure", phase, result)
		}
	})
}

func TestReapIdleWorkspacesStopsRemovedAgentExactlyOnce(t *testing.T) {
	globalAgent := &lifecycleCountingAgent{}
	workspaceAgent := &lifecycleCountingAgent{}
	engine := NewEngine("idle-workspace", globalAgent, nil, t.TempDir()+"/sessions.json", LangEnglish)
	engine.workspacePool = newWorkspacePool(time.Nanosecond)

	for range 2 {
		workspace := engine.workspacePool.GetOrCreate(t.TempDir())
		workspace.mu.Lock()
		workspace.agent = workspaceAgent
		workspace.lastActivity = time.Now().Add(-time.Hour)
		workspace.mu.Unlock()
	}

	engine.reapIdleWorkspaces()
	if got := workspaceAgent.stopCalls.Load(); got != 1 {
		t.Fatalf("reaped workspace Agent.Stop calls = %d, want 1", got)
	}
	if got := globalAgent.stopCalls.Load(); got != 0 {
		t.Fatalf("global Agent.Stop calls during idle reap = %d, want 0", got)
	}
	if states := engine.workspacePool.All(); len(states) != 0 {
		t.Fatalf("workspace pool retained %d reaped states", len(states))
	}

	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got := workspaceAgent.stopCalls.Load(); got != 1 {
		t.Fatalf("reaped workspace Agent.Stop calls after Engine.Stop = %d, want 1", got)
	}
	if got := globalAgent.stopCalls.Load(); got != 1 {
		t.Fatalf("global Agent.Stop calls after Engine.Stop = %d, want 1", got)
	}
}

func contextWithTestTimeout(t *testing.T, timeout time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), timeout)
}

func waitLifecycleSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	ctx, cancel := contextWithTestTimeout(t, 2*time.Second)
	defer cancel()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", label, ctx.Err())
	}
}

func waitLifecycleResult(t *testing.T, result <-chan error, label string) error {
	t.Helper()
	ctx, cancel := contextWithTestTimeout(t, 2*time.Second)
	defer cancel()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", label, ctx.Err())
		return nil
	}
}
