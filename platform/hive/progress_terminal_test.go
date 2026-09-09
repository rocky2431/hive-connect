package hive

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestProgressDoesNotCommitTerminalReceipt(t *testing.T) {
	store, err := newExecutionReceiptStore(filepath.Join(t.TempDir(), "receipts.json"), 10)
	if err != nil {
		t.Fatal(err)
	}
	rc := replyContext{SessionID: "session", MessageID: "message", ReplayKey: "replay", RequestHash: "hash"}
	if _, err := store.claim(rc.ReplayKey, rc.MessageID, rc.RequestHash, rc.SessionID); err != nil {
		t.Fatal(err)
	}
	p := &Platform{receipts: store}
	// A disconnected transport must not turn progress into a durable final answer.
	_ = p.Send(context.Background(), rc, "working")
	if got := store.records[rc.ReplayKey]; got.State != executionReceiptStateClaimed || got.Result != nil {
		t.Fatalf("progress committed a terminal result: %#v", got)
	}
	_ = p.Reply(context.Background(), rc, "actual final result")
	got := store.records[rc.ReplayKey]
	if got.State != executionReceiptStateTerminal || got.Result.Output != "actual final result" {
		t.Fatalf("final reply was not preserved: %#v", got)
	}
}

func TestTurnFailureNeverCommitsSuccess(t *testing.T) {
	for _, event := range []core.Event{
		{Type: core.EventError, Error: errors.New("provider failed")},
		{Type: core.EventError},
	} {
		store, err := newExecutionReceiptStore(filepath.Join(t.TempDir(), "receipts.json"), 10)
		if err != nil {
			t.Fatal(err)
		}
		rc := replyContext{SessionID: "session", MessageID: "message", ReplayKey: "replay", RequestHash: "hash"}
		if _, err := store.claim(rc.ReplayKey, rc.MessageID, rc.RequestHash, rc.SessionID); err != nil {
			t.Fatal(err)
		}
		p := &Platform{receipts: store}
		_ = p.SendTurnResult(context.Background(), rc, event)
		got := store.records[rc.ReplayKey]
		if got.Result == nil || got.Result.Status != "failed" || got.Result.ErrorCode == "" {
			t.Fatalf("failure was not preserved: %#v", got)
		}
	}
}
