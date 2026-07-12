package hive

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestAttachmentUploadPersistsArtifactsForTerminalAndRestartReplay(t *testing.T) {
	tests := []struct {
		name      string
		send      func(*Platform, replyContext) error
		complete  bool
		wantState receiptClaimKind
	}{
		{
			name: "file terminal replay",
			send: func(p *Platform, rc replyContext) error {
				return p.SendFile(context.Background(), rc, core.FileAttachment{
					FileName: "report.xlsx",
					MimeType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
					Data:     []byte("xlsx-content"),
				})
			},
			complete:  true,
			wantState: receiptClaimReplay,
		},
		{
			name: "image recovered unknown replay",
			send: func(p *Platform, rc replyContext) error {
				return p.SendImage(context.Background(), rc, core.ImageAttachment{
					FileName: "chart.png",
					MimeType: "image/png",
					Data:     []byte("png-content"),
				})
			},
			wantState: receiptClaimRecoveredUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Double rationale: the upload API is outside this repository;
			// this server preserves its committed artifact response contract while
			// the real receipt store and filesystem persistence remain in-process.
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/local-bridge/upload" {
					http.NotFound(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"workspace_path": "workspace/uploads/report.xlsx",
					"artifacts": []map[string]any{{
						"id":       "artifact-1",
						"path":     "workspace/uploads/report.xlsx",
						"filename": "report.xlsx",
					}},
				})
			}))
			defer server.Close()

			receiptPath := filepath.Join(t.TempDir(), "execution-receipts.json")
			platformAny, err := New(map[string]any{
				"backend_url":        server.URL,
				"token":              "hb_test",
				"receipt_store_path": receiptPath,
			})
			if err != nil {
				t.Fatalf("New returned error: %v", err)
			}
			platform := platformAny.(*Platform)
			platform.httpClient = server.Client()

			rc := replyContext{
				SessionID:   "session-artifact",
				MessageID:   "message-artifact",
				ReplayKey:   "local:artifact",
				RequestHash: "request-hash-artifact",
			}
			if _, err := platform.receipts.claim(rc.ReplayKey, rc.MessageID, rc.RequestHash, rc.SessionID); err != nil {
				t.Fatalf("claim returned error: %v", err)
			}

			if err := tt.send(platform, rc); err == nil || !strings.Contains(err.Error(), "websocket is not connected") {
				t.Fatalf("send error = %v, want disconnected websocket after committed upload", err)
			}
			if tt.complete {
				if err := platform.Reply(context.Background(), rc, "done"); err == nil || !strings.Contains(err.Error(), "websocket is not connected") {
					t.Fatalf("Reply error = %v, want disconnected websocket after durable terminal result", err)
				}
			}

			restarted, err := newExecutionReceiptStore(receiptPath, defaultReceiptMaxRecords)
			if err != nil {
				t.Fatalf("restart receipt store: %v", err)
			}
			claim, err := restarted.claim(rc.ReplayKey, rc.MessageID, rc.RequestHash, rc.SessionID)
			if err != nil {
				t.Fatalf("restart claim returned error: %v", err)
			}
			if claim.Kind != tt.wantState || claim.Result == nil {
				t.Fatalf("restart claim = %#v, want kind %v with result", claim, tt.wantState)
			}
			encoded, err := json.Marshal(claim.Result)
			if err != nil {
				t.Fatalf("marshal replayed result: %v", err)
			}
			var frame map[string]any
			if err := json.Unmarshal(encoded, &frame); err != nil {
				t.Fatalf("decode replayed result: %v", err)
			}
			artifacts, ok := frame["artifacts"].([]any)
			if !ok || len(artifacts) != 1 {
				t.Fatalf("replayed result artifacts = %#v, want one backend-consumable artifact", frame["artifacts"])
			}
			artifact, ok := artifacts[0].(map[string]any)
			if !ok || artifact["id"] != "artifact-1" || artifact["path"] != "workspace/uploads/report.xlsx" {
				t.Fatalf("replayed artifact = %#v", artifacts[0])
			}
		})
	}
}

func TestAttachmentReceiptBindingRejectsTamperBeforeUpload(t *testing.T) {
	var uploads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uploads.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"artifacts": []map[string]any{{"id": "unexpected"}}})
	}))
	defer server.Close()

	platformAny, err := New(map[string]any{
		"backend_url":        server.URL,
		"token":              "hb_test",
		"receipt_store_path": filepath.Join(t.TempDir(), "execution-receipts.json"),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	platform := platformAny.(*Platform)
	platform.httpClient = server.Client()
	if _, err := platform.receipts.claim("local:bound", "message-bound", "request-hash-bound", "session-bound"); err != nil {
		t.Fatalf("claim returned error: %v", err)
	}

	err = platform.SendFile(context.Background(), replyContext{
		SessionID:   "session-bound",
		MessageID:   "message-bound",
		ReplayKey:   "local:bound",
		RequestHash: "tampered-request-hash",
	}, core.FileAttachment{FileName: "secret.txt", MimeType: "text/plain", Data: []byte("secret")})
	if err == nil || !strings.Contains(err.Error(), "replay key is bound to another message") {
		t.Fatalf("SendFile error = %v, want fail-closed binding conflict", err)
	}
	if got := uploads.Load(); got != 0 {
		t.Fatalf("tampered context performed %d uploads, want 0", got)
	}
}

func TestArtifactReceiptCorruptionFailsClosed(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "execution-receipts.json")
	store, err := newExecutionReceiptStore(receiptPath, defaultReceiptMaxRecords)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, err := store.claim("local:corrupt-artifact", "message-corrupt", "request-hash-corrupt", "session-corrupt"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.appendArtifacts(
		"local:corrupt-artifact",
		"message-corrupt",
		"request-hash-corrupt",
		[]map[string]any{{"id": "artifact-original", "path": "workspace/original.txt"}},
	); err != nil {
		t.Fatalf("append artifacts: %v", err)
	}

	payload, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatalf("read receipt: %v", err)
	}
	tampered := strings.Replace(string(payload), "artifact-original", "artifact-tampered", 1)
	if tampered == string(payload) {
		t.Fatal("receipt fixture did not contain artifact metadata")
	}
	if err := os.WriteFile(receiptPath, []byte(tampered), 0o600); err != nil {
		t.Fatalf("tamper receipt: %v", err)
	}
	if _, err := newExecutionReceiptStore(receiptPath, defaultReceiptMaxRecords); err == nil {
		t.Fatal("receipt store accepted artifact metadata whose integrity hash no longer matched")
	}
}

func TestArtifactReceiptDeduplicatesAndRejectsLateTerminalAppend(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "execution-receipts.json")
	store, err := newExecutionReceiptStore(receiptPath, defaultReceiptMaxRecords)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	const (
		replayKey   = "local:dedupe-artifact"
		messageID   = "message-dedupe"
		requestHash = "request-hash-dedupe"
	)
	if _, err := store.claim(replayKey, messageID, requestHash, "session-dedupe"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	artifact := []map[string]any{{"id": "artifact-dedupe", "path": "workspace/dedupe.txt"}}
	if err := store.appendArtifacts(replayKey, messageID, requestHash, artifact); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := store.appendArtifacts(replayKey, messageID, requestHash, artifact); err != nil {
		t.Fatalf("duplicate append: %v", err)
	}

	restarted, err := newExecutionReceiptStore(receiptPath, defaultReceiptMaxRecords)
	if err != nil {
		t.Fatalf("restart store: %v", err)
	}
	if got := len(restarted.records[replayKey].Artifacts); got != 1 {
		t.Fatalf("durable artifact count = %d, want 1", got)
	}
	if _, err := restarted.claim(replayKey, messageID, requestHash, "session-dedupe"); err != nil {
		t.Fatalf("restart claim: %v", err)
	}
	if err := restarted.appendArtifacts(replayKey, messageID, requestHash, []map[string]any{{
		"id": "artifact-late", "path": "workspace/late.txt",
	}}); err == nil || !strings.Contains(err.Error(), "terminal") {
		t.Fatalf("late append error = %v, want terminal receipt rejection", err)
	}
}

func TestArtifactMetadataIsBoundedAndRequiresStableReference(t *testing.T) {
	store, err := newExecutionReceiptStore(filepath.Join(t.TempDir(), "execution-receipts.json"), defaultReceiptMaxRecords)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if _, err := store.claim("local:bounded", "message-bounded", "request-hash-bounded", "session-bounded"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	tests := []struct {
		name     string
		artifact map[string]any
	}{
		{name: "missing stable reference", artifact: map[string]any{"filename": "orphan.txt"}},
		{name: "nested untrusted payload", artifact: map[string]any{"id": "artifact-nested", "metadata": map[string]any{"secret": "value"}}},
		{name: "oversized value", artifact: map[string]any{"id": "artifact-large", "path": strings.Repeat("x", 20*1024)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := store.appendArtifacts(
				"local:bounded", "message-bounded", "request-hash-bounded", []map[string]any{tt.artifact},
			); err == nil {
				t.Fatal("appendArtifacts accepted unsafe artifact metadata")
			}
		})
	}
}

func TestActionBoundAttachmentUploadFailureIsVisibleAndNotSentInline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	platformAny, err := New(map[string]any{
		"backend_url":        server.URL,
		"token":              "hb_test",
		"receipt_store_path": filepath.Join(t.TempDir(), "execution-receipts.json"),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	platform := platformAny.(*Platform)
	platform.httpClient = server.Client()
	rc := replyContext{
		SessionID: "session-upload-failure", MessageID: "message-upload-failure",
		ReplayKey: "local:upload-failure", RequestHash: "request-hash-upload-failure",
	}
	if _, err := platform.receipts.claim(rc.ReplayKey, rc.MessageID, rc.RequestHash, rc.SessionID); err != nil {
		t.Fatalf("claim: %v", err)
	}

	err = platform.SendFile(context.Background(), rc, core.FileAttachment{
		FileName: "report.xlsx", MimeType: "application/octet-stream", Data: []byte("report"),
	})
	if err == nil || !strings.Contains(err.Error(), "durable file upload failed") {
		t.Fatalf("SendFile error = %v, want visible durable upload failure", err)
	}
	record := platform.receipts.records[rc.ReplayKey]
	if len(record.Artifacts) != 0 || record.State != executionReceiptStateClaimed {
		t.Fatalf("receipt after failed upload = %#v", record)
	}
}

func TestUploadArtifactPayloadUsesCanonicalFrontendConsumableList(t *testing.T) {
	payload := map[string]any{
		"filename": "report.xlsx",
		"data":     "base64-file-content-must-not-be-duplicated-after-upload",
	}
	upload := map[string]any{
		"workspace_path": "workspace/uploads/report.xlsx",
		"artifacts": []any{map[string]any{
			"type":                       "artifact",
			"artifact_id":                "artifact-real",
			"path":                       "workspace/uploads/report.xlsx",
			"name":                       "report.xlsx",
			"mime_type":                  "application/octet-stream",
			"size":                       float64(42),
			"source":                     "local_bridge_upload",
			"preview_snapshot_content":   "sensitive preview must not enter receipt",
			"preview_snapshot_truncated": false,
			"snapshot_storage_path":      "internal/snapshot/path",
			"tool_call_id":               "tool-call-evolution-field",
		}},
	}
	artifacts, err := bindUploadArtifacts(payload, upload)
	if err != nil {
		t.Fatalf("bindUploadArtifacts returned error: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0]["artifact_id"] != "artifact-real" {
		t.Fatalf("canonical artifacts = %#v", artifacts)
	}
	if _, leaked := artifacts[0]["preview_snapshot_content"]; leaked {
		t.Fatalf("canonical artifact retained preview content: %#v", artifacts[0])
	}
	if _, leaked := artifacts[0]["snapshot_storage_path"]; leaked {
		t.Fatalf("canonical artifact retained internal storage path: %#v", artifacts[0])
	}
	if got, ok := payload["artifacts"].([]map[string]any); !ok || len(got) != 1 || got[0]["path"] != "workspace/uploads/report.xlsx" {
		t.Fatalf("event payload artifacts = %#v", payload["artifacts"])
	}
	if _, duplicated := payload["data"]; duplicated {
		t.Fatalf("successful artifact event duplicated inline file bytes: %#v", payload)
	}
	legacy, ok := payload["artifact"].(map[string]any)
	if !ok || legacy["artifact_id"] != "artifact-real" {
		t.Fatalf("legacy artifact field = %#v", payload["artifact"])
	}
	if _, leaked := legacy["preview_snapshot_content"]; leaked {
		t.Fatalf("legacy artifact field retained preview content: %#v", legacy)
	}
	if _, leaked := legacy["snapshot_storage_path"]; leaked {
		t.Fatalf("legacy artifact field retained internal storage path: %#v", legacy)
	}
}

func TestFailedArtifactBindingPreservesSessionOnlyInlineFallbackData(t *testing.T) {
	payload := map[string]any{
		"filename": "fallback.txt",
		"data":     "base64-inline-fallback",
	}
	if _, err := bindUploadArtifacts(payload, map[string]any{"status": "upload-response-without-artifacts"}); err == nil {
		t.Fatal("bindUploadArtifacts accepted response without artifacts")
	}
	if payload["data"] != "base64-inline-fallback" {
		t.Fatalf("failed artifact binding removed inline fallback data: %#v", payload)
	}
}
