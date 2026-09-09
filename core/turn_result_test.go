package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

type terminalPlatform struct {
	stubPlatformEngine
	results []Event
}

type terminalAgentSession struct{ *controllableAgentSession }

func (s *terminalAgentSession) Close() error { return nil }

func (p *terminalPlatform) SendTurnResult(_ context.Context, _ any, result Event) error {
	p.results = append(p.results, result)
	return nil
}

func TestTurnResultSeparatesProgressFromCompletion(t *testing.T) {
	for _, mode := range []string{"completed", "failed", "interrupted"} {
		t.Run(mode, func(t *testing.T) {
			p := &terminalPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
			e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
			session := e.sessions.GetOrCreateActive("test:user")
			agent := &terminalAgentSession{newControllableSession("session")}
			state := &interactiveState{agentSession: agent, platform: p, replyCtx: "request"}
			e.interactiveStates["test:user"] = state
			agent.events <- Event{Type: EventThinking, Content: "working"}
			agent.events <- Event{Type: EventResult, Done: false}
			switch mode {
			case "completed":
				agent.events <- Event{Type: EventResult, Done: true, Content: "actual final result"}
			case "failed":
				agent.events <- Event{Type: EventError, Error: errors.New("provider failed")}
			case "interrupted":
				close(agent.events)
			}
			e.processInteractiveEvents(state, session, e.sessions, "test:user", "message", time.Now(), nil, nil, "request")
			if len(p.results) != 1 {
				t.Fatalf("terminal results = %#v, want exactly one", p.results)
			}
			if mode == "completed" && (!p.results[0].Done || p.results[0].Content != "actual final result") {
				t.Fatalf("wrong final result: %#v", p.results[0])
			}
			if mode != "completed" && p.results[0].Done {
				t.Fatal("failure was reported as completed")
			}
		})
	}
}
