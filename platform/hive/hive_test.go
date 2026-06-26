package hive

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
		"api_prefix":   "/api/v1",
		"device_name":  "Rocky Mac",
		"runtime_kind": "codex",
		"allow_from":   "owner-1",
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
}

func TestPlatformWebSocketRoundTrip(t *testing.T) {
	var readyFrame map[string]any
	var ackFrame map[string]any
	var eventFrame map[string]any
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
			if err := conn.ReadJSON(&eventFrame); err != nil {
				t.Fatalf("read event: %v", err)
			}
			close(serverDone)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	plat, err := New(map[string]any{
		"backend_url":  server.URL,
		"token":        "hb_test",
		"runtime_kind": "codex",
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
	if eventFrame["type"] != "event" || eventFrame["event_type"] != "text" {
		t.Fatalf("eventFrame = %#v", eventFrame)
	}
	payload, ok := eventFrame["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T", eventFrame["payload"])
	}
	if text := strings.TrimSpace(payload["text"].(string)); text != "hello from local" {
		t.Fatalf("payload text = %q", text)
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
		"backend_url":  server.URL,
		"token":        "hb_test",
		"runtime_kind": "codex",
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
