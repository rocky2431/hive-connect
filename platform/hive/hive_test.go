package hive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

func TestNewRequiresBaseURLAndToken(t *testing.T) {
	t.Setenv("HIVE_BACKEND_URL", "")
	t.Setenv("HIVE_BRIDGE_TOKEN", "")
	t.Setenv("HIVE_CONNECT_TOKEN", "")

	if _, err := New(map[string]any{"backend_url": "https://hive.example"}); err == nil {
		t.Fatal("New without token returned nil error")
	}
	if _, err := New(map[string]any{"token": "hb_test"}); err == nil {
		t.Fatal("New without backend_url returned nil error")
	}
}

func TestNewReadsOptionsAndEnvironment(t *testing.T) {
	t.Setenv("HIVE_BACKEND_URL", "https://hive.example/")
	t.Setenv("HIVE_CONNECT_TOKEN", "hb_env_token")

	plat, err := New(map[string]any{
		"api_prefix":         "/api/v1",
		"device_name":        "Rocky Mac",
		"runtime_kind":       "codex",
		"allow_from":         "owner-1",
		"receipt_store_path": filepath.Join(t.TempDir(), "execution-receipts.json"),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	p := plat.(*Platform)
	if p.baseURL != "https://hive.example" {
		t.Fatalf("baseURL = %q, want https://hive.example", p.baseURL)
	}
	if p.token != "hb_env_token" {
		t.Fatalf("token = %q, want hb_env_token", p.token)
	}
	if p.apiPrefix != "/api/v1" {
		t.Fatalf("apiPrefix = %q, want /api/v1", p.apiPrefix)
	}
	if p.deviceName != "Rocky Mac" {
		t.Fatalf("deviceName = %q, want Rocky Mac", p.deviceName)
	}
	if p.runtimeKind != "codex" {
		t.Fatalf("runtimeKind = %q, want codex", p.runtimeKind)
	}
	if p.allowFrom != "owner-1" {
		t.Fatalf("allowFrom = %q, want owner-1", p.allowFrom)
	}
}

func TestChannelURLsUseAPIPrefixAndTicketEscaping(t *testing.T) {
	p := &Platform{baseURL: "https://hive.example", apiPrefix: "/api"}

	ticketURL, err := p.apiURL("/local-bridge/channel/ws-ticket")
	if err != nil {
		t.Fatalf("apiURL returned error: %v", err)
	}
	if ticketURL != "https://hive.example/api/local-bridge/channel/ws-ticket" {
		t.Fatalf("ticketURL = %q", ticketURL)
	}

	wsURL, err := p.wsURL("HIVE_WS_a/b+")
	if err != nil {
		t.Fatalf("wsURL returned error: %v", err)
	}
	if wsURL != "wss://hive.example/api/local-bridge/channel/ws?ticket=HIVE_WS_a%2Fb%2B" {
		t.Fatalf("wsURL = %q", wsURL)
	}
}

func TestCoreMessageFromHivePayload(t *testing.T) {
	fileBody := []byte("# hello")
	msg, err := coreMessageFromHive(hiveMessagePayload{
		ID:          "msg-1",
		ReplayKey:   "local:msg-1",
		RequestHash: "request-hash-msg-1",
		SessionID:   "sess-1",
		OwnerUserID: "owner-1",
		Content:     "please inspect this",
		Attachments: []hiveAttachmentPayload{{
			Type:     "file",
			FileName: "note.md",
			MimeType: "text/markdown",
			Data:     base64.StdEncoding.EncodeToString(fileBody),
		}},
		Metadata: map[string]any{"sender_name": "Rocky"},
	})
	if err != nil {
		t.Fatalf("coreMessageFromHive returned error: %v", err)
	}

	if msg.SessionKey != "hive:sess-1" {
		t.Fatalf("SessionKey = %q", msg.SessionKey)
	}
	if msg.Platform != "hive" {
		t.Fatalf("Platform = %q", msg.Platform)
	}
	if msg.MessageID != "msg-1" {
		t.Fatalf("MessageID = %q", msg.MessageID)
	}
	if msg.UserID != "owner-1" {
		t.Fatalf("UserID = %q", msg.UserID)
	}
	if msg.UserName != "Rocky" {
		t.Fatalf("UserName = %q", msg.UserName)
	}
	if msg.Content != "please inspect this" {
		t.Fatalf("Content = %q", msg.Content)
	}
	if len(msg.Files) != 1 || string(msg.Files[0].Data) != string(fileBody) || msg.Files[0].FileName != "note.md" {
		t.Fatalf("Files = %#v", msg.Files)
	}
	rctx, ok := msg.ReplyCtx.(replyContext)
	if !ok {
		t.Fatalf("ReplyCtx type = %T, want replyContext", msg.ReplyCtx)
	}
	if rctx.SessionID != "sess-1" || rctx.MessageID != "msg-1" {
		t.Fatalf("ReplyCtx = %#v", rctx)
	}
	if rctx.ReplayKey != "local:msg-1" {
		t.Fatalf("ReplyCtx replay key = %q", rctx.ReplayKey)
	}
}

func TestPlatformWebSocketRoundTrip(t *testing.T) {
	var readyFrame map[string]any
	var ackFrame map[string]any
	var resultFrame map[string]any
	serverDone := make(chan struct{})

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/local-bridge/channel/ws-ticket":
			if r.Method != http.MethodPost {
				t.Fatalf("ticket method = %s", r.Method)
			}
			if got := r.Header.Get("Authorization"); got != "Bearer hb_test" {
				t.Fatalf("Authorization = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ticket":     "HIVE_WS_test",
				"expires_in": 60,
				"single_use": true,
			})
		case "/api/local-bridge/channel/ws":
			if got := r.URL.Query().Get("ticket"); got != "HIVE_WS_test" {
				t.Fatalf("ticket = %q", got)
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Fatalf("upgrade: %v", err)
			}
			defer conn.Close()
			if err := conn.WriteJSON(map[string]any{"type": "hello", "connection_id": "conn-1", "owner_user_id": "owner-1"}); err != nil {
				t.Fatalf("write hello: %v", err)
			}
			if err := conn.ReadJSON(&readyFrame); err != nil {
				t.Fatalf("read ready: %v", err)
			}
			if err := conn.WriteJSON(map[string]any{
				"type": "message",
				"message": map[string]any{
					"id":            "msg-1",
					"replay_key":    "local:msg-1",
					"request_hash":  "request-hash-msg-1",
					"session_id":    "sess-1",
					"owner_user_id": "owner-1",
					"content":       "hello from Hive",
				},
			}); err != nil {
				t.Fatalf("write message: %v", err)
			}
			if err := conn.ReadJSON(&ackFrame); err != nil {
				t.Fatalf("read ack: %v", err)
			}
			if err := conn.ReadJSON(&resultFrame); err != nil {
				t.Fatalf("read result: %v", err)
			}
			close(serverDone)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	plat, err := New(map[string]any{
		"backend_url":        server.URL,
		"token":              "hb_test",
		"runtime_kind":       "codex",
		"receipt_store_path": filepath.Join(t.TempDir(), "execution-receipts.json"),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	received := make(chan *core.Message, 1)
	if err := plat.Start(func(p core.Platform, msg *core.Message) {
		received <- msg
		if err := p.Send(context.Background(), msg.ReplyCtx, "hello from local"); err != nil {
			t.Errorf("Send returned error: %v", err)
		}
	}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer plat.Stop()

	select {
	case msg := <-received:
		if msg.Content != "hello from Hive" {
			t.Fatalf("message content = %q", msg.Content)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inbound message")
	}
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for websocket exchange")
	}

	if readyFrame["type"] != "ready" {
		t.Fatalf("readyFrame = %#v", readyFrame)
	}
	if readyFrame["runtime_kind"] != "codex" {
		t.Fatalf("readyFrame runtime_kind = %#v", readyFrame["runtime_kind"])
	}
	if ackFrame["type"] != "ack" || ackFrame["message_id"] != "msg-1" {
		t.Fatalf("ackFrame = %#v", ackFrame)
	}
	if resultFrame["type"] != "result" {
		t.Fatalf("resultFrame = %#v", resultFrame)
	}
	if resultFrame["session_id"] != "sess-1" || resultFrame["message_id"] != "msg-1" {
		t.Fatalf("resultFrame identifiers = %#v", resultFrame)
	}
	if resultFrame["status"] != "completed" {
		t.Fatalf("resultFrame status = %#v", resultFrame["status"])
	}
	if text := strings.TrimSpace(resultFrame["output"].(string)); text != "hello from local" {
		t.Fatalf("resultFrame output = %q", text)
	}
}

func TestPlatformReconnectsAfterServerClose(t *testing.T) {
	var ticketCount atomic.Int32
	var wsCount atomic.Int32
	reconnected := make(chan struct{})

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/local-bridge/channel/ws-ticket":
			ticket := fmt.Sprintf("HIVE_WS_%d", ticketCount.Add(1))
			_ = json.NewEncoder(w).Encode(map[string]any{
				"ticket":     ticket,
				"expires_in": 60,
				"single_use": true,
			})
		case "/api/local-bridge/channel/ws":
			n := wsCount.Add(1)
			if got, want := r.URL.Query().Get("ticket"), fmt.Sprintf("HIVE_WS_%d", n); got != want {
				t.Fatalf("ticket = %q, want %q", got, want)
			}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Fatalf("upgrade: %v", err)
			}
			defer conn.Close()
			if err := conn.WriteJSON(map[string]any{"type": "hello", "connection_id": "conn-1", "owner_user_id": "owner-1"}); err != nil {
				t.Fatalf("write hello: %v", err)
			}
			var readyFrame map[string]any
			if err := conn.ReadJSON(&readyFrame); err != nil {
				t.Fatalf("read ready: %v", err)
			}
			if n == 1 {
				_ = conn.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseServiceRestart, "restart"),
					time.Now().Add(time.Second),
				)
				return
			}
			if err := conn.WriteJSON(map[string]any{
				"type": "message",
				"message": map[string]any{
					"id":            "msg-reconnected",
					"replay_key":    "local:msg-reconnected",
					"request_hash":  "request-hash-msg-reconnected",
					"session_id":    "sess-1",
					"owner_user_id": "owner-1",
					"content":       "after reconnect",
				},
			}); err != nil {
				t.Fatalf("write message after reconnect: %v", err)
			}
			var ackFrame map[string]any
			if err := conn.ReadJSON(&ackFrame); err != nil {
				t.Fatalf("read ack after reconnect: %v", err)
			}
			close(reconnected)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	plat, err := New(map[string]any{
		"backend_url":        server.URL,
		"token":              "hb_test",
		"runtime_kind":       "codex",
		"receipt_store_path": filepath.Join(t.TempDir(), "execution-receipts.json"),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	received := make(chan *core.Message, 1)
	if err := plat.Start(func(p core.Platform, msg *core.Message) {
		received <- msg
	}); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer plat.Stop()

	select {
	case msg := <-received:
		if msg.Content != "after reconnect" {
			t.Fatalf("message content = %q", msg.Content)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for reconnected message; tickets=%d websockets=%d", ticketCount.Load(), wsCount.Load())
	}
	select {
	case <-reconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reconnect server exchange")
	}
}

func TestReplayKeySuppressesAckLostDuplicateAndReplaysDurableResultAfterRestart(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "execution-receipts.json")
	payload := hiveMessagePayload{
		ID:          "msg-replay-1",
		ReplayKey:   "local:replay-1",
		RequestHash: "request-hash-replay-1",
		SessionID:   "session-replay-1",
		OwnerUserID: "owner-1",
		Content:     "perform one local side effect",
	}

	first, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": receiptPath,
	})
	if err != nil {
		t.Fatalf("New first platform: %v", err)
	}
	firstPlatform := first.(*Platform)
	var executions atomic.Int32
	var replyCtx any
	firstPlatform.handler = func(_ core.Platform, msg *core.Message) {
		executions.Add(1)
		replyCtx = msg.ReplyCtx
	}

	// No websocket is attached, so both ACK writes are lost. The second cloud
	// delivery must still be suppressed before it reaches the local agent.
	firstPlatform.handleMessage(payload)
	firstPlatform.handleMessage(payload)
	if got := executions.Load(); got != 1 {
		t.Fatalf("local executions after duplicate before ACK = %d, want 1", got)
	}
	if err := firstPlatform.Reply(context.Background(), replyCtx, "first durable result"); err == nil {
		t.Fatal("Reply without websocket returned nil error")
	}
	// A second terminal write must never replace the first durable result.
	if err := firstPlatform.Reply(context.Background(), replyCtx, "must not replace first result"); err == nil {
		t.Fatal("second Reply without websocket returned nil error")
	}

	second, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": receiptPath,
	})
	if err != nil {
		t.Fatalf("New restarted platform: %v", err)
	}
	secondPlatform := second.(*Platform)
	secondPlatform.handler = func(_ core.Platform, _ *core.Message) {
		executions.Add(1)
	}
	frames := attachFrameCollector(t, secondPlatform)

	secondPlatform.handleMessage(payload)
	ack := waitForFrame(t, frames)
	result := waitForFrame(t, frames)
	if ack["type"] != "ack" || ack["message_id"] != payload.ID {
		t.Fatalf("replay ACK = %#v", ack)
	}
	if result["type"] != "result" || result["status"] != "completed" {
		t.Fatalf("replayed terminal frame = %#v", result)
	}
	if result["output"] != "first durable result" {
		t.Fatalf("replayed output = %#v, want first durable result", result["output"])
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("local executions after restart/redelivery = %d, want 1", got)
	}
}

func TestRestartWithUnfinishedReplayKeyFailsClosedWithoutReexecution(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "execution-receipts.json")
	payload := hiveMessagePayload{
		ID:          "msg-unknown-outcome",
		ReplayKey:   "local:unknown-outcome",
		RequestHash: "request-hash-unknown-outcome",
		SessionID:   "session-unknown-outcome",
		OwnerUserID: "owner-1",
		Content:     "do not repeat after process death",
	}

	first, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": receiptPath,
	})
	if err != nil {
		t.Fatalf("New first platform: %v", err)
	}
	var executions atomic.Int32
	first.(*Platform).handler = func(_ core.Platform, _ *core.Message) { executions.Add(1) }
	first.(*Platform).handleMessage(payload)
	if got := executions.Load(); got != 1 {
		t.Fatalf("initial execution count = %d, want 1", got)
	}

	restarted, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": receiptPath,
	})
	if err != nil {
		t.Fatalf("New restarted platform: %v", err)
	}
	restartedPlatform := restarted.(*Platform)
	restartedPlatform.handler = func(_ core.Platform, _ *core.Message) { executions.Add(1) }
	frames := attachFrameCollector(t, restartedPlatform)

	restartedPlatform.handleMessage(payload)
	_ = waitForFrame(t, frames) // ACK closes the cloud delivery lease.
	result := waitForFrame(t, frames)
	if result["status"] != "failed" || result["error_code"] != "local_execution_outcome_unknown" {
		t.Fatalf("restart reconciliation frame = %#v", result)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("local executions after unfinished restart = %d, want 1", got)
	}
}

func TestMissingReplayKeyFailsClosed(t *testing.T) {
	p, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": filepath.Join(t.TempDir(), "execution-receipts.json"),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var executions atomic.Int32
	p.(*Platform).handler = func(_ core.Platform, _ *core.Message) { executions.Add(1) }

	p.(*Platform).handleMessage(hiveMessagePayload{
		ID:          "msg-without-replay-key",
		RequestHash: "request-hash-without-replay-key",
		SessionID:   "session-1",
		OwnerUserID: "owner-1",
		Content:     "must not execute",
	})
	if got := executions.Load(); got != 0 {
		t.Fatalf("local executions without replay key = %d, want 0", got)
	}

	p.(*Platform).handleMessage(hiveMessagePayload{
		ID:          "msg-without-request-hash",
		ReplayKey:   "local:without-request-hash",
		SessionID:   "session-1",
		OwnerUserID: "owner-1",
		Content:     "must also not execute",
	})
	if got := executions.Load(); got != 0 {
		t.Fatalf("local executions without request hash = %d, want 0", got)
	}
}

func TestReplayKeyRequestHashTamperFailsClosed(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "execution-receipts.json")
	original := hiveMessagePayload{
		ID:          "msg-tamper",
		ReplayKey:   "local:tamper",
		RequestHash: "request-hash-original",
		SessionID:   "session-tamper",
		OwnerUserID: "owner-1",
		Content:     "approved command",
	}
	first, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": receiptPath,
	})
	if err != nil {
		t.Fatalf("New first platform: %v", err)
	}
	var executions atomic.Int32
	var replyCtx any
	first.(*Platform).handler = func(_ core.Platform, msg *core.Message) {
		executions.Add(1)
		replyCtx = msg.ReplyCtx
	}
	first.(*Platform).handleMessage(original)
	_ = first.(*Platform).Reply(context.Background(), replyCtx, "approved result")

	restarted, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": receiptPath,
	})
	if err != nil {
		t.Fatalf("New restarted platform: %v", err)
	}
	restartedPlatform := restarted.(*Platform)
	restartedPlatform.handler = func(_ core.Platform, _ *core.Message) { executions.Add(1) }
	frames := attachFrameCollector(t, restartedPlatform)
	tampered := original
	tampered.RequestHash = "request-hash-tampered"
	tampered.Content = "different command under the same ids"

	restartedPlatform.handleMessage(tampered)
	_ = waitForFrame(t, frames)
	result := waitForFrame(t, frames)
	if result["status"] != "failed" || result["error_code"] != "replay_key_binding_conflict" {
		t.Fatalf("tampered replay result = %#v", result)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("local executions after tampered replay = %d, want 1", got)
	}
}

func TestResultAckPrunesOnlyAcknowledgedTerminalReceipts(t *testing.T) {
	receiptPath := filepath.Join(t.TempDir(), "execution-receipts.json")
	p, err := New(map[string]any{
		"backend_url":         "https://hive.example",
		"token":               "hb_test",
		"allow_from":          "owner-1",
		"receipt_store_path":  receiptPath,
		"receipt_max_records": 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	platform := p.(*Platform)
	contexts := make(map[string]any)
	platform.handler = func(_ core.Platform, msg *core.Message) { contexts[msg.MessageID] = msg.ReplyCtx }
	for index := 1; index <= 2; index++ {
		messageID := fmt.Sprintf("msg-ack-%d", index)
		platform.handleMessage(hiveMessagePayload{
			ID:          messageID,
			ReplayKey:   fmt.Sprintf("local:ack-%d", index),
			RequestHash: fmt.Sprintf("request-hash-ack-%d", index),
			SessionID:   "session-ack",
			OwnerUserID: "owner-1",
			Content:     "one governed action",
		})
		_ = platform.Reply(context.Background(), contexts[messageID], fmt.Sprintf("result-%d", index))
	}
	if got := len(platform.receipts.records); got != 2 {
		t.Fatalf("unacknowledged terminal receipt count = %d, want 2", got)
	}

	platform.handleFrame([]byte(`{
		"type":"result_ack",
		"message_id":"msg-ack-1",
		"status":"completed",
		"receipt":{"replay_key":"local:ack-1","request_hash":"tampered-request-hash"}
	}`))
	if got := len(platform.receipts.records); got != 2 {
		t.Fatalf("mismatched result_ack pruned a receipt; count = %d, want 2", got)
	}

	platform.handleFrame([]byte(`{
		"type":"result_ack",
		"message_id":"msg-ack-1",
		"status":"completed",
		"receipt":{"replay_key":"local:ack-1","request_hash":"request-hash-ack-1"}
	}`))
	if got := len(platform.receipts.records); got != 1 {
		t.Fatalf("receipt count after durable result_ack = %d, want 1", got)
	}
	if _, exists := platform.receipts.records["local:ack-2"]; !exists {
		t.Fatal("unacknowledged terminal receipt was pruned")
	}
}

func TestReceiptStoreFailsClosedOnCorruptionAndUsesPrivatePermissions(t *testing.T) {
	root := t.TempDir()
	corruptPath := filepath.Join(root, "corrupt-receipts.json")
	if err := os.WriteFile(corruptPath, []byte("{not-json}"), 0o600); err != nil {
		t.Fatalf("write corrupt receipt store: %v", err)
	}
	if _, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": corruptPath,
	}); err == nil {
		t.Fatal("New accepted a corrupt execution receipt store")
	}

	receiptPath := filepath.Join(root, "private", "execution-receipts.json")
	p, err := New(map[string]any{
		"backend_url":        "https://hive.example",
		"token":              "hb_test",
		"allow_from":         "owner-1",
		"receipt_store_path": receiptPath,
	})
	if err != nil {
		t.Fatalf("New private store: %v", err)
	}
	p.(*Platform).handler = func(_ core.Platform, _ *core.Message) {}
	p.(*Platform).handleMessage(hiveMessagePayload{
		ID:          "msg-private-receipt",
		ReplayKey:   "local:private-receipt",
		RequestHash: "request-hash-private-receipt",
		SessionID:   "session-private-receipt",
		OwnerUserID: "owner-1",
		Content:     "persist privately",
	})
	info, err := os.Stat(receiptPath)
	if err != nil {
		t.Fatalf("stat receipt store: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("receipt store permissions = %o, want 600", got)
	}
}

func attachFrameCollector(t *testing.T, p *Platform) <-chan map[string]any {
	t.Helper()
	frames := make(chan map[string]any, 8)
	connected := make(chan *websocket.Conn, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade frame collector: %v", err)
			return
		}
		connected <- conn
		for {
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err != nil {
				return
			}
			frames <- frame
		}
	}))
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial frame collector: %v", err)
	}
	serverConn := <-connected
	p.connMu.Lock()
	p.conn = client
	p.connMu.Unlock()
	t.Cleanup(func() {
		_ = p.closeConn()
		_ = serverConn.Close()
		server.Close()
	})
	return frames
}

func waitForFrame(t *testing.T, frames <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for websocket frame")
		return nil
	}
}
