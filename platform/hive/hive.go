package hive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
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
}

type wsTicketResponse struct {
	Ticket string `json:"ticket"`
}

type wsEnvelope struct {
	Type    string              `json:"type"`
	Message *hiveMessagePayload `json:"message,omitempty"`
	Error   string              `json:"error,omitempty"`
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
	SessionID string
	MessageID string
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
	return p.sendEvent(ctx, rctx, "text", map[string]any{
		"text":    content,
		"content": content,
	})
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
	payload := map[string]any{
		"filename":  fallbackFileName(img.FileName, "image"),
		"mime_type": firstNonEmptyString(img.MimeType, "application/octet-stream"),
		"size":      len(img.Data),
	}
	if len(img.Data) <= maxInlineAttachment {
		payload["data"] = base64.StdEncoding.EncodeToString(img.Data)
	}
	if artifact, err := p.uploadAttachment(ctx, payload["filename"].(string), payload["mime_type"].(string), img.Data); err == nil {
		payload["artifact"] = artifact
	} else {
		slog.Warn("hive: image upload failed, falling back to event payload", "error", err)
	}
	return p.sendEvent(ctx, rctx, "image", payload)
}

func (p *Platform) SendFile(ctx context.Context, rctx any, file core.FileAttachment) error {
	payload := map[string]any{
		"filename":  fallbackFileName(file.FileName, "file"),
		"mime_type": firstNonEmptyString(file.MimeType, "application/octet-stream"),
		"size":      len(file.Data),
	}
	if len(file.Data) <= maxInlineAttachment {
		payload["data"] = base64.StdEncoding.EncodeToString(file.Data)
	}
	if artifact, err := p.uploadAttachment(ctx, payload["filename"].(string), payload["mime_type"].(string), file.Data); err == nil {
		payload["artifact"] = artifact
	} else {
		slog.Warn("hive: file upload failed, falling back to event payload", "error", err)
	}
	return p.sendEvent(ctx, rctx, "file", payload)
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
	case "hello", "ready_ack", "ack_ack", "event_ack", "result_ack", "pong":
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
	if payload.ID != "" {
		if err := p.writeJSON(map[string]any{"type": "ack", "message_id": payload.ID}); err != nil {
			slog.Warn("hive: ack failed", "error", err)
		}
	}
	if p.handler != nil {
		p.handler(p, msg)
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
			SessionID: payload.SessionID,
			MessageID: payload.ID,
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
