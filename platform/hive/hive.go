package hive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
	"github.com/gorilla/websocket"
)

const (
	defaultAPIPrefix    = "/api"
	defaultRuntimeKind  = "hive-connect"
	hiveUserAgent       = "hive-connect"
	wsPingInterval      = 25 * time.Second
	reconnectInitial    = time.Second
	reconnectMax        = 30 * time.Second
	httpRequestTimeout  = 30 * time.Second
	maxInlineAttachment = 8 * 1024 * 1024
)

func init() {
	core.RegisterPlatform("hive", New)
}

type Platform struct {
	baseURL      string
	apiPrefix    string
	token        string
	deviceName   string
	runtimeKind  string
	allowFrom    string
	capabilities map[string]any

	httpClient *http.Client
	dialer     *websocket.Dialer

	handler core.MessageHandler
	ctx     context.Context
	cancel  context.CancelFunc
	life    core.PlatformLifecycleHandler

	connMu sync.Mutex
	conn   *websocket.Conn

	receipts *executionReceiptStore
}

type wsTicketResponse struct {
	Ticket string `json:"ticket"`
}

type wsEnvelope struct {
	Type      string              `json:"type"`
	Message   *hiveMessagePayload `json:"message,omitempty"`
	MessageID string              `json:"message_id,omitempty"`
	Receipt   *resultAckReceipt   `json:"receipt,omitempty"`
	Error     string              `json:"error,omitempty"`
}

type resultAckReceipt struct {
	ReplayKey   string `json:"replay_key"`
	RequestHash string `json:"request_hash"`
}

type hiveMessagePayload struct {
	ID            string                  `json:"id"`
	SessionID     string                  `json:"session_id"`
	OwnerUserID   string                  `json:"owner_user_id"`
	SenderUserID  string                  `json:"sender_user_id,omitempty"`
	SourceAgentID string                  `json:"source_agent_id,omitempty"`
	TenantID      string                  `json:"tenant_id,omitempty"`
	Content       string                  `json:"content"`
	Attachments   []hiveAttachmentPayload `json:"attachments,omitempty"`
	Metadata      map[string]any          `json:"metadata,omitempty"`
	ReplayKey     string                  `json:"replay_key"`
	RequestHash   string                  `json:"request_hash,omitempty"`
	CreatedAt     string                  `json:"created_at,omitempty"`
}

type hiveAttachmentPayload struct {
	Type        string `json:"type,omitempty"`
	FileName    string `json:"filename,omitempty"`
	Name        string `json:"name,omitempty"`
	MimeType    string `json:"mime_type,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Data        string `json:"data,omitempty"`
	Content     string `json:"content,omitempty"`
	Path        string `json:"path,omitempty"`
	URL         string `json:"url,omitempty"`
}

type replyContext struct {
	SessionID   string
	MessageID   string
	ReplayKey   string
	RequestHash string
}

type resultFrame struct {
	Type      string           `json:"type"`
	SessionID string           `json:"session_id"`
	MessageID string           `json:"message_id"`
	Status    string           `json:"status"`
	Output    string           `json:"output,omitempty"`
	ErrorCode string           `json:"error_code,omitempty"`
	Artifacts []map[string]any `json:"artifacts,omitempty"`
}

func New(opts map[string]any) (core.Platform, error) {
	baseURL := firstNonEmptyString(
		stringOpt(opts, "backend_url"),
		stringOpt(opts, "base_url"),
		stringOpt(opts, "hive_url"),
		os.Getenv("HIVE_BACKEND_URL"),
	)
	if baseURL == "" {
		return nil, fmt.Errorf("hive: backend_url is required (or set HIVE_BACKEND_URL)")
	}
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("hive: invalid backend_url %q: %w", baseURL, err)
	}

	token := firstNonEmptyString(
		stringOpt(opts, "token"),
		stringOpt(opts, "bridge_token"),
		os.Getenv("HIVE_CONNECT_TOKEN"),
		os.Getenv("HIVE_BRIDGE_TOKEN"),
	)
	if token == "" {
		return nil, fmt.Errorf("hive: token is required (or set HIVE_CONNECT_TOKEN)")
	}

	apiPrefix := firstNonEmptyString(stringOpt(opts, "api_prefix"), os.Getenv("HIVE_API_PREFIX"), defaultAPIPrefix)
	apiPrefix = normalizeAPIPrefix(apiPrefix)

	deviceName := firstNonEmptyString(stringOpt(opts, "device_name"), os.Getenv("HIVE_DEVICE_NAME"), defaultDeviceName())
	runtimeKind := firstNonEmptyString(stringOpt(opts, "runtime_kind"), os.Getenv("HIVE_RUNTIME_KIND"), defaultRuntimeKind)
	allowFrom := stringOpt(opts, "allow_from")
	core.CheckAllowFrom("hive", allowFrom)

	capabilities := defaultCapabilities()
	for k, v := range mapOpt(opts, "capabilities") {
		capabilities[k] = v
	}
	receiptPath := resolveReceiptStorePath(opts, baseURL, deviceName)
	receipts, err := newExecutionReceiptStore(receiptPath, positiveIntOpt(opts["receipt_max_records"], defaultReceiptMaxRecords))
	if err != nil {
		return nil, fmt.Errorf("hive: initialize execution receipt store: %w", err)
	}

	return &Platform{
		baseURL:      baseURL,
		apiPrefix:    apiPrefix,
		token:        token,
		deviceName:   deviceName,
		runtimeKind:  runtimeKind,
		allowFrom:    allowFrom,
		capabilities: capabilities,
		httpClient:   &http.Client{Timeout: httpRequestTimeout},
		dialer:       websocket.DefaultDialer,
		receipts:     receipts,
	}, nil
}

func (p *Platform) Name() string { return "hive" }

func (p *Platform) SetLifecycleHandler(h core.PlatformLifecycleHandler) {
	p.life = h
}

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler
	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel

	go p.reconnectLoop(ctx)
	return nil
}

func (p *Platform) Stop() error {
	if p.cancel != nil {
		p.cancel()
	}
	return p.closeConn()
}

func (p *Platform) closeConn() error {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.conn == nil {
		return nil
	}
	err := p.conn.Close()
	p.conn = nil
	return err
}

func (p *Platform) Reply(ctx context.Context, rctx any, content string) error {
	rc, err := replyContextFromAny(rctx)
	if err != nil {
		return err
	}
	if rc.MessageID == "" {
		return p.sendEvent(ctx, rc, "text", map[string]any{
			"text":    content,
			"content": content,
		})
	}
	if p.receipts == nil {
		return errors.New("hive: execution receipt store is unavailable")
	}
	result, err := p.receipts.complete(rc.ReplayKey, rc.MessageID, rc.RequestHash, resultFrame{
		Type:      "result",
		SessionID: rc.SessionID,
		MessageID: rc.MessageID,
		Status:    "completed",
		Output:    content,
	})
	if err != nil {
		return fmt.Errorf("hive: persist execution result: %w", err)
	}
	return p.writeJSONWithContext(ctx, result)
}

func (p *Platform) Send(ctx context.Context, rctx any, content string) error {
	return p.Reply(ctx, rctx, content)
}

func (p *Platform) ReconstructReplyCtx(sessionKey string) (any, error) {
	sessionID, ok := strings.CutPrefix(sessionKey, "hive:")
	if !ok || strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("hive: invalid session key %q", sessionKey)
	}
	return replyContext{SessionID: sessionID}, nil
}

func (p *Platform) SendImage(ctx context.Context, rctx any, img core.ImageAttachment) error {
	rc, err := replyContextFromAny(rctx)
	if err != nil {
		return err
	}
	if err := p.validateArtifactReplyContext(rc); err != nil {
		return err
	}
	payload := map[string]any{
		"filename":  fallbackFileName(img.FileName, "image"),
		"mime_type": firstNonEmptyString(img.MimeType, "application/octet-stream"),
		"size":      len(img.Data),
	}
	if len(img.Data) <= maxInlineAttachment {
		payload["data"] = base64.StdEncoding.EncodeToString(img.Data)
	}
	if upload, err := p.uploadAttachment(ctx, payload["filename"].(string), payload["mime_type"].(string), img.Data); err == nil {
		artifacts, normalizeErr := bindUploadArtifacts(payload, upload)
		if normalizeErr != nil {
			return normalizeErr
		}
		if err := p.persistReplyArtifacts(rc, artifacts); err != nil {
			return err
		}
	} else {
		if rc.MessageID != "" {
			return fmt.Errorf("hive: durable image upload failed: %w", err)
		}
		slog.Warn("hive: image upload failed, falling back to event payload", "error", err)
	}
	return p.sendEvent(ctx, rc, "image", payload)
}

func (p *Platform) SendFile(ctx context.Context, rctx any, file core.FileAttachment) error {
	rc, err := replyContextFromAny(rctx)
	if err != nil {
		return err
	}
	if err := p.validateArtifactReplyContext(rc); err != nil {
		return err
	}
	payload := map[string]any{
		"filename":  fallbackFileName(file.FileName, "file"),
		"mime_type": firstNonEmptyString(file.MimeType, "application/octet-stream"),
		"size":      len(file.Data),
	}
	if len(file.Data) <= maxInlineAttachment {
		payload["data"] = base64.StdEncoding.EncodeToString(file.Data)
	}
	if upload, err := p.uploadAttachment(ctx, payload["filename"].(string), payload["mime_type"].(string), file.Data); err == nil {
		artifacts, normalizeErr := bindUploadArtifacts(payload, upload)
		if normalizeErr != nil {
			return normalizeErr
		}
		if err := p.persistReplyArtifacts(rc, artifacts); err != nil {
			return err
		}
	} else {
		if rc.MessageID != "" {
			return fmt.Errorf("hive: durable file upload failed: %w", err)
		}
		slog.Warn("hive: file upload failed, falling back to event payload", "error", err)
	}
	return p.sendEvent(ctx, rc, "file", payload)
}

func (p *Platform) connect(ctx context.Context) error {
	ticket, err := p.createWSTicket(ctx)
	if err != nil {
		return err
	}
	wsURL, err := p.wsURL(ticket)
	if err != nil {
		return err
	}
	header := http.Header{}
	header.Set("User-Agent", hiveUserAgent)
	conn, _, err := p.dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return fmt.Errorf("hive: websocket connect failed (%s): %s", wsURL, core.RedactToken(err.Error(), p.token))
	}
	p.connMu.Lock()
	p.conn = conn
	p.connMu.Unlock()
	slog.Info("hive: websocket connected", "url", wsURL)
	return p.writeJSON(map[string]any{
		"type":         "ready",
		"runtime_kind": p.runtimeKind,
		"device_name":  p.deviceName,
		"capabilities": p.capabilities,
	})
}

func (p *Platform) reconnectLoop(ctx context.Context) {
	delay := reconnectInitial
	for {
		if ctx.Err() != nil {
			return
		}
		if err := p.connect(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			p.notifyUnavailable(err)
			slog.Warn("hive: websocket connect failed, retrying", "error", err, "backoff", delay)
			if !sleepContext(ctx, delay) {
				return
			}
			delay = nextReconnectDelay(delay)
			continue
		}

		delay = reconnectInitial
		p.notifyReady()
		err := p.serveConnected(ctx)
		_ = p.closeConn()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = fmt.Errorf("hive: websocket disconnected")
		}
		p.notifyUnavailable(err)
		slog.Warn("hive: websocket disconnected, reconnecting", "error", err, "backoff", delay)
		if !sleepContext(ctx, delay) {
			return
		}
		delay = nextReconnectDelay(delay)
	}
}

func (p *Platform) serveConnected(ctx context.Context) error {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	go func() { errCh <- p.readLoop(connCtx) }()
	go func() { errCh <- p.pingLoop(connCtx) }()
	err := <-errCh
	cancel()
	_ = p.closeConn()
	return err
}

func (p *Platform) notifyReady() {
	if p.life != nil {
		p.life.OnPlatformReady(p)
	}
}

func (p *Platform) notifyUnavailable(err error) {
	if p.life != nil {
		p.life.OnPlatformUnavailable(p, err)
	}
}

func nextReconnectDelay(current time.Duration) time.Duration {
	if current <= 0 {
		return reconnectInitial
	}
	next := current * 2
	if next > reconnectMax {
		return reconnectMax
	}
	return next
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (p *Platform) createWSTicket(ctx context.Context) (string, error) {
	ticketURL, err := p.apiURL("/local-bridge/channel/ws-ticket")
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ticketURL, nil)
	if err != nil {
		return "", fmt.Errorf("hive: create ws ticket request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("User-Agent", hiveUserAgent)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("hive: create ws ticket: %s", core.RedactToken(err.Error(), p.token))
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("hive: create ws ticket failed: status=%d body=%s", resp.StatusCode, core.RedactToken(string(body), p.token))
	}

	var out wsTicketResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("hive: decode ws ticket response: %w", err)
	}
	if out.Ticket == "" {
		return "", fmt.Errorf("hive: ws ticket response missing ticket")
	}
	return out.Ticket, nil
}

func (p *Platform) readLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		p.connMu.Lock()
		conn := p.conn
		p.connMu.Unlock()
		if conn == nil {
			return fmt.Errorf("hive: websocket is not connected")
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("hive: websocket read stopped", "error", err)
			}
			return err
		}
		p.handleFrame(raw)
	}
}

func (p *Platform) pingLoop(ctx context.Context) error {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.writeJSON(map[string]any{"type": "ping"}); err != nil {
				slog.Debug("hive: ping failed", "error", err)
				return err
			}
		}
	}
}

func (p *Platform) handleFrame(raw []byte) {
	var frame wsEnvelope
	if err := json.Unmarshal(raw, &frame); err != nil {
		slog.Debug("hive: ignoring non-json frame", "error", err)
		return
	}
	switch frame.Type {
	case "hello", "ready_ack", "ack_ack", "event_ack", "pong":
		return
	case "result_ack":
		p.handleResultAck(frame)
		return
	case "error":
		slog.Warn("hive: websocket error frame", "error", frame.Error)
	case "message":
		if frame.Message == nil {
			return
		}
		p.handleMessage(*frame.Message)
	default:
		slog.Debug("hive: unhandled websocket frame", "type", frame.Type)
	}
}

func (p *Platform) handleMessage(payload hiveMessagePayload) {
	msg, err := coreMessageFromHive(payload)
	if err != nil {
		slog.Warn("hive: invalid channel message", "error", err)
		return
	}
	if !core.AllowList(p.allowFrom, msg.UserID) {
		slog.Debug("hive: message from unauthorized owner", "user", msg.UserID)
		return
	}
	if strings.TrimSpace(payload.ReplayKey) == "" {
		slog.Error("hive: refusing local execution without replay key", "message_id", payload.ID)
		p.ackAndReportFailure(payload, "missing_replay_key", "Hive did not provide the required replay key; the local action was not executed.")
		return
	}
	if strings.TrimSpace(payload.RequestHash) == "" {
		slog.Error("hive: refusing local execution without request hash", "message_id", payload.ID, "replay_key", payload.ReplayKey)
		p.ackAndReportFailure(payload, "missing_request_hash", "Hive did not provide the required request hash; the local action was not executed.")
		return
	}
	if p.receipts == nil {
		slog.Error("hive: refusing local execution without receipt store", "message_id", payload.ID, "replay_key", payload.ReplayKey)
		return
	}
	claim, err := p.receipts.claim(payload.ReplayKey, payload.ID, payload.RequestHash, payload.SessionID)
	if err != nil {
		if errors.Is(err, errReplayBindingConflict) {
			slog.Error("hive: refusing replay key binding conflict", "message_id", payload.ID, "replay_key", payload.ReplayKey, "error", err)
			p.ackAndReportFailure(payload, "replay_key_binding_conflict", "The replay key is already bound to a different Hive message; the local action was not executed.")
			return
		}
		// No ACK is sent when the durable claim cannot be committed. The cloud
		// lease may retry after local storage is repaired, but no side effect ran.
		slog.Error("hive: failed to persist local execution claim", "message_id", payload.ID, "replay_key", payload.ReplayKey, "error", err)
		return
	}
	p.ackMessage(payload.ID)
	switch claim.Kind {
	case receiptClaimActive:
		slog.Info("hive: duplicate delivery joined active local execution", "message_id", payload.ID, "replay_key", payload.ReplayKey)
		return
	case receiptClaimReplay, receiptClaimRecoveredUnknown:
		if claim.Result == nil {
			slog.Error("hive: durable receipt is missing result", "message_id", payload.ID, "replay_key", payload.ReplayKey)
			return
		}
		if err := p.writeJSON(*claim.Result); err != nil {
			slog.Warn("hive: durable result replay failed", "message_id", payload.ID, "replay_key", payload.ReplayKey, "error", err)
		}
		return
	case receiptClaimNew:
		if p.handler != nil {
			p.handler(p, msg)
			return
		}
		result, completeErr := p.receipts.complete(payload.ReplayKey, payload.ID, payload.RequestHash, resultFrame{
			Type:      "result",
			SessionID: payload.SessionID,
			MessageID: payload.ID,
			Status:    "failed",
			Output:    "Hive Connect has no local message handler; the action was not executed.",
			ErrorCode: "local_handler_unavailable",
		})
		if completeErr != nil {
			slog.Error("hive: failed to persist missing-handler result", "message_id", payload.ID, "replay_key", payload.ReplayKey, "error", completeErr)
			return
		}
		if err := p.writeJSON(result); err != nil {
			slog.Warn("hive: missing-handler result delivery failed", "message_id", payload.ID, "replay_key", payload.ReplayKey, "error", err)
		}
	}
}

func (p *Platform) handleResultAck(frame wsEnvelope) {
	if p.receipts == nil {
		slog.Error("hive: result acknowledgement arrived without receipt store", "message_id", frame.MessageID)
		return
	}
	if frame.Receipt == nil {
		// A legacy/partial acknowledgement is not sufficient evidence to delete
		// the terminal result. Retaining it is safer than enabling re-execution.
		slog.Warn("hive: result acknowledgement omitted durable receipt", "message_id", frame.MessageID)
		return
	}
	if err := p.receipts.acknowledge(frame.Receipt.ReplayKey, frame.MessageID, frame.Receipt.RequestHash); err != nil {
		slog.Warn("hive: result acknowledgement did not match durable receipt", "message_id", frame.MessageID, "replay_key", frame.Receipt.ReplayKey, "error", err)
	}
}

func (p *Platform) ackMessage(messageID string) {
	if strings.TrimSpace(messageID) == "" {
		return
	}
	if err := p.writeJSON(map[string]any{"type": "ack", "message_id": messageID}); err != nil {
		slog.Warn("hive: ack failed", "message_id", messageID, "error", err)
	}
}

func (p *Platform) ackAndReportFailure(payload hiveMessagePayload, code, output string) {
	p.ackMessage(payload.ID)
	if err := p.writeJSON(resultFrame{
		Type:      "result",
		SessionID: payload.SessionID,
		MessageID: payload.ID,
		Status:    "failed",
		Output:    output,
		ErrorCode: code,
	}); err != nil {
		slog.Warn("hive: failure result delivery failed", "message_id", payload.ID, "error", err)
	}
}

func (p *Platform) sendEvent(ctx context.Context, rctx any, eventType string, payload map[string]any) error {
	rc, err := replyContextFromAny(rctx)
	if err != nil {
		return err
	}
	frame := map[string]any{
		"type":       "event",
		"session_id": rc.SessionID,
		"event_type": eventType,
		"payload":    payload,
	}
	if rc.MessageID != "" {
		frame["message_id"] = rc.MessageID
	}
	return p.writeJSONWithContext(ctx, frame)
}

func (p *Platform) writeJSON(v any) error {
	return p.writeJSONWithContext(context.Background(), v)
}

func (p *Platform) writeJSONWithContext(ctx context.Context, v any) error {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.conn == nil {
		return fmt.Errorf("hive: websocket is not connected")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(httpRequestTimeout)
	}
	_ = p.conn.SetWriteDeadline(deadline)
	return p.conn.WriteJSON(v)
}

func (p *Platform) apiURL(urlPath string) (string, error) {
	u, err := url.Parse(p.baseURL)
	if err != nil {
		return "", fmt.Errorf("hive: parse backend_url: %w", err)
	}
	u.Path = joinURLPath(u.Path, p.apiPrefix, urlPath)
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func (p *Platform) wsURL(ticket string) (string, error) {
	raw, err := p.apiURL("/local-bridge/channel/ws")
	if err != nil {
		return "", err
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("hive: parse ws url: %w", err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	default:
		return "", fmt.Errorf("hive: backend_url must use http or https, got %q", u.Scheme)
	}
	q := u.Query()
	q.Set("ticket", ticket)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func coreMessageFromHive(payload hiveMessagePayload) (*core.Message, error) {
	if strings.TrimSpace(payload.ID) == "" {
		return nil, fmt.Errorf("missing message id")
	}
	if strings.TrimSpace(payload.SessionID) == "" {
		return nil, fmt.Errorf("missing session id")
	}
	userID := firstNonEmptyString(payload.SenderUserID, payload.OwnerUserID)
	if strings.TrimSpace(userID) == "" {
		return nil, fmt.Errorf("missing owner user id")
	}

	images, files, extra := attachmentsFromHive(payload.Attachments)
	userName := stringFromMap(payload.Metadata, "sender_name")
	if userName == "" {
		userName = stringFromMap(payload.Metadata, "user_name")
	}
	if userName == "" {
		userName = userID
	}

	return &core.Message{
		SessionKey:   "hive:" + payload.SessionID,
		Platform:     "hive",
		MessageID:    payload.ID,
		ChannelID:    payload.SessionID,
		UserID:       userID,
		UserName:     userName,
		Content:      payload.Content,
		Images:       images,
		Files:        files,
		ExtraContent: extra,
		ChannelKey:   payload.SessionID,
		ReplyCtx: replyContext{
			SessionID:   payload.SessionID,
			MessageID:   payload.ID,
			ReplayKey:   payload.ReplayKey,
			RequestHash: payload.RequestHash,
		},
	}, nil
}

func attachmentsFromHive(items []hiveAttachmentPayload) ([]core.ImageAttachment, []core.FileAttachment, string) {
	var images []core.ImageAttachment
	var files []core.FileAttachment
	var remoteRefs []string
	for _, item := range items {
		name := firstNonEmptyString(item.FileName, item.Name, "attachment")
		mimeType := firstNonEmptyString(item.MimeType, item.ContentType, "application/octet-stream")
		data := decodeAttachmentData(item)
		if len(data) == 0 {
			ref := firstNonEmptyString(item.Path, item.URL)
			if ref != "" {
				remoteRefs = append(remoteRefs, fmt.Sprintf("%s (%s)", name, ref))
			}
			continue
		}
		if strings.EqualFold(item.Type, "image") || strings.HasPrefix(strings.ToLower(mimeType), "image/") {
			images = append(images, core.ImageAttachment{MimeType: mimeType, Data: data, FileName: name})
			continue
		}
		files = append(files, core.FileAttachment{MimeType: mimeType, Data: data, FileName: name})
	}
	if len(remoteRefs) == 0 {
		return images, files, ""
	}
	return images, files, "Hive attachments available for download: " + strings.Join(remoteRefs, "; ")
}

func decodeAttachmentData(item hiveAttachmentPayload) []byte {
	raw := firstNonEmptyString(item.Data, item.Content)
	if raw == "" {
		return nil
	}
	if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil {
		return decoded
	}
	return []byte(raw)
}

func replyContextFromAny(rctx any) (replyContext, error) {
	switch v := rctx.(type) {
	case replyContext:
		if v.SessionID == "" {
			return replyContext{}, fmt.Errorf("hive: reply context missing session id")
		}
		return v, nil
	case *replyContext:
		if v == nil || v.SessionID == "" {
			return replyContext{}, fmt.Errorf("hive: reply context missing session id")
		}
		return *v, nil
	default:
		return replyContext{}, fmt.Errorf("hive: invalid reply context type %T", rctx)
	}
}

func (p *Platform) validateArtifactReplyContext(rc replyContext) error {
	if rc.MessageID == "" {
		return nil
	}
	if p.receipts == nil {
		return errors.New("hive: execution receipt store is unavailable")
	}
	if err := p.receipts.validateActiveBinding(rc.ReplayKey, rc.MessageID, rc.RequestHash); err != nil {
		return fmt.Errorf("hive: validate artifact receipt binding: %w", err)
	}
	return nil
}

func (p *Platform) persistReplyArtifacts(rc replyContext, artifacts []map[string]any) error {
	if rc.MessageID == "" {
		return nil
	}
	if err := p.receipts.appendArtifacts(rc.ReplayKey, rc.MessageID, rc.RequestHash, artifacts); err != nil {
		return fmt.Errorf("hive: persist artifact receipt binding: %w", err)
	}
	return nil
}

func artifactsFromUploadResponse(upload map[string]any) ([]map[string]any, error) {
	rawItems, ok := upload["artifacts"]
	if !ok {
		return nil, errors.New("hive: upload response omitted artifacts")
	}
	var items []any
	switch typed := rawItems.(type) {
	case []any:
		items = typed
	case []map[string]any:
		items = make([]any, len(typed))
		for index := range typed {
			items[index] = typed[index]
		}
	default:
		return nil, errors.New("hive: upload response artifacts must be a list")
	}
	if len(items) == 0 || len(items) > maxReceiptArtifacts {
		return nil, fmt.Errorf("hive: upload response artifact count must be between 1 and %d", maxReceiptArtifacts)
	}
	artifacts := make([]map[string]any, 0, len(items))
	for _, item := range items {
		metadata, ok := item.(map[string]any)
		if !ok {
			return nil, errors.New("hive: upload response artifact must be an object")
		}
		bounded := make(map[string]any)
		for key, value := range metadata {
			if _, allowed := artifactMetadataKeys[key]; allowed {
				bounded[key] = value
			}
		}
		if _, err := encodeDurableArtifact(bounded); err != nil {
			return nil, fmt.Errorf("hive: unsafe upload artifact metadata: %w", err)
		}
		artifacts = append(artifacts, bounded)
	}
	return artifacts, nil
}

func bindUploadArtifacts(payload, upload map[string]any) ([]map[string]any, error) {
	artifacts, err := artifactsFromUploadResponse(upload)
	if err != nil {
		return nil, err
	}
	payload["artifact"] = artifacts[0]
	payload["artifacts"] = artifacts
	return artifacts, nil
}

func (p *Platform) uploadAttachment(ctx context.Context, filename, mimeType string, data []byte) (map[string]any, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty attachment")
	}
	uploadURL, err := p.apiURL("/local-bridge/upload")
	if err != nil {
		return nil, err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return nil, fmt.Errorf("hive: create upload form file: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return nil, fmt.Errorf("hive: write upload form file: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("hive: close upload form: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, &body)
	if err != nil {
		return nil, fmt.Errorf("hive: create upload request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("User-Agent", hiveUserAgent)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if mimeType != "" {
		req.Header.Set("X-Hive-File-Mime-Type", mimeType)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hive: upload attachment: %s", core.RedactToken(err.Error(), p.token))
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("hive: upload attachment failed: status=%d body=%s", resp.StatusCode, core.RedactToken(string(respBody), p.token))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("hive: decode upload response: %w", err)
	}
	return out, nil
}

func stringOpt(opts map[string]any, key string) string {
	if opts == nil {
		return ""
	}
	v, _ := opts[key].(string)
	return strings.TrimSpace(v)
}

func mapOpt(opts map[string]any, key string) map[string]any {
	if opts == nil {
		return nil
	}
	switch v := opts[key].(type) {
	case map[string]any:
		return v
	case map[string]string:
		out := make(map[string]any, len(v))
		for k, val := range v {
			out[k] = val
		}
		return out
	default:
		return nil
	}
}

func resolveReceiptStorePath(opts map[string]any, baseURL, deviceName string) string {
	if override := firstNonEmptyString(
		stringOpt(opts, "receipt_store_path"),
		os.Getenv("HIVE_CONNECT_RECEIPT_STORE_PATH"),
	); override != "" {
		return filepath.Clean(override)
	}
	dataDir := stringOpt(opts, "cc_data_dir")
	if dataDir == "" {
		if home, err := os.UserHomeDir(); err == nil {
			dataDir = filepath.Join(home, ".hive-connect", "data")
		} else {
			dataDir = filepath.Join(".hive-connect", "data")
		}
	}
	identity := strings.Join([]string{
		strings.TrimSpace(baseURL),
		stringOpt(opts, "cc_project"),
		strings.TrimSpace(deviceName),
	}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	return filepath.Join(dataDir, "hive", "execution-receipts", fmt.Sprintf("%x.json", digest[:8]))
}

func positiveIntOpt(value any, fallback int) int {
	var parsed int
	switch v := value.(type) {
	case int:
		parsed = v
	case int8:
		parsed = int(v)
	case int16:
		parsed = int(v)
	case int32:
		parsed = int(v)
	case int64:
		parsed = int(v)
	case uint:
		parsed = int(v)
	case uint8:
		parsed = int(v)
	case uint16:
		parsed = int(v)
	case uint32:
		parsed = int(v)
	case uint64:
		if v <= uint64(^uint(0)>>1) {
			parsed = int(v)
		}
	case float64:
		parsed = int(v)
	}
	if parsed <= 0 {
		return fallback
	}
	return parsed
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizeAPIPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || prefix == "/" {
		return ""
	}
	prefix = "/" + strings.Trim(prefix, "/")
	return prefix
}

func joinURLPath(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			cleaned = append(cleaned, part)
		}
	}
	if len(cleaned) == 0 {
		return "/"
	}
	return "/" + path.Join(cleaned...)
}

func defaultDeviceName() string {
	if host, err := os.Hostname(); err == nil && strings.TrimSpace(host) != "" {
		return host
	}
	return "local-agent"
}

func defaultCapabilities() map[string]any {
	return map[string]any{
		"im":          true,
		"streaming":   true,
		"attachments": true,
		"workspace":   true,
		"runner":      defaultRuntimeKind,
	}
}

func stringFromMap(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	v, _ := values[key].(string)
	return strings.TrimSpace(v)
}

func fallbackFileName(name, fallback string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		return name
	}
	return fallback
}
