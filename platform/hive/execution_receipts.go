package hive

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const (
	executionReceiptSchema        = "hive.connect.execution_receipts.v1"
	executionReceiptStateClaimed  = "claimed"
	executionReceiptStateTerminal = "terminal"
	defaultReceiptMaxRecords      = 10000
	maxReceiptArtifacts           = 64
	maxArtifactMetadataBytes      = 16 * 1024
	maxArtifactStringBytes        = 8 * 1024
)

var errReplayBindingConflict = errors.New("replay key is bound to another message")

type executionReceipt struct {
	ReplayKey      string            `json:"replay_key"`
	MessageID      string            `json:"message_id"`
	RequestHash    string            `json:"request_hash"`
	State          string            `json:"state"`
	ClaimedAt      time.Time         `json:"claimed_at"`
	CompletedAt    *time.Time        `json:"completed_at,omitempty"`
	AcknowledgedAt *time.Time        `json:"acknowledged_at,omitempty"`
	Artifacts      []durableArtifact `json:"artifacts,omitempty"`
	Result         *resultFrame      `json:"result,omitempty"`
}

type durableArtifact struct {
	Metadata json.RawMessage `json:"metadata"`
	SHA256   string          `json:"sha256"`
}

var artifactMetadataKeys = map[string]struct{}{
	"type": {}, "artifact_id": {}, "id": {}, "source_ref": {}, "uri": {}, "path": {},
	"name": {}, "filename": {}, "mime_type": {}, "size": {}, "modified_at": {},
	"preview_kind": {}, "source": {}, "runtime_task_id": {}, "owner_agent_id": {},
	"source_agent_id": {}, "download_agent_id": {}, "delivery_agent_id": {}, "created_at": {},
	"revision_id": {}, "content_hash": {}, "action": {}, "scope": {},
}

type executionReceiptSnapshot struct {
	Schema  string                      `json:"schema"`
	Records map[string]executionReceipt `json:"records"`
}

type receiptClaimKind int

const (
	receiptClaimNew receiptClaimKind = iota
	receiptClaimActive
	receiptClaimReplay
	receiptClaimRecoveredUnknown
)

type receiptClaim struct {
	Kind   receiptClaimKind
	Result *resultFrame
}

// executionReceiptStore is the single-writer durable idempotency boundary for
// Hive work delivered to this local daemon. The process-level instance lock
// owns the file; the mutex serializes websocket delivery and agent replies.
type executionReceiptStore struct {
	mu         sync.Mutex
	path       string
	maxRecords int
	records    map[string]executionReceipt
	active     map[string]struct{}
}

func newExecutionReceiptStore(path string, maxRecords int) (*executionReceiptStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("receipt store path is required")
	}
	if maxRecords <= 0 {
		maxRecords = defaultReceiptMaxRecords
	}
	store := &executionReceiptStore{
		path:       path,
		maxRecords: maxRecords,
		records:    make(map[string]executionReceipt),
		active:     make(map[string]struct{}),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *executionReceiptStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read receipt store: %w", err)
	}
	var snapshot executionReceiptSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode receipt store: %w", err)
	}
	if snapshot.Schema != executionReceiptSchema || snapshot.Records == nil {
		return fmt.Errorf("unsupported receipt store schema %q", snapshot.Schema)
	}
	for key, record := range snapshot.Records {
		if strings.TrimSpace(key) == "" || record.ReplayKey != key || strings.TrimSpace(record.MessageID) == "" || strings.TrimSpace(record.RequestHash) == "" {
			return fmt.Errorf("invalid receipt binding for replay key %q", key)
		}
		switch record.State {
		case executionReceiptStateClaimed:
			if record.Result != nil || record.CompletedAt != nil || record.AcknowledgedAt != nil {
				return fmt.Errorf("claimed receipt %q contains terminal data", key)
			}
		case executionReceiptStateTerminal:
			if record.Result == nil || record.CompletedAt == nil {
				return fmt.Errorf("terminal receipt %q is incomplete", key)
			}
		default:
			return fmt.Errorf("receipt %q has invalid state %q", key, record.State)
		}
		artifacts, err := decodeDurableArtifacts(record.Artifacts)
		if err != nil {
			return fmt.Errorf("receipt %q has invalid artifact metadata: %w", key, err)
		}
		if record.State == executionReceiptStateTerminal && !artifactMetadataEqual(record.Result.Artifacts, artifacts) {
			return fmt.Errorf("receipt %q terminal artifacts do not match durable artifact binding", key)
		}
	}
	s.records = snapshot.Records
	return nil
}

func (s *executionReceiptStore) claim(replayKey, messageID, requestHash, sessionID string) (receiptClaim, error) {
	replayKey = strings.TrimSpace(replayKey)
	messageID = strings.TrimSpace(messageID)
	requestHash = strings.TrimSpace(requestHash)
	if replayKey == "" || messageID == "" || requestHash == "" {
		return receiptClaim{}, errors.New("replay key, message id, and request hash are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[replayKey]
	if !exists {
		record = executionReceipt{
			ReplayKey:   replayKey,
			MessageID:   messageID,
			RequestHash: requestHash,
			State:       executionReceiptStateClaimed,
			ClaimedAt:   time.Now().UTC(),
		}
		s.records[replayKey] = record
		if err := s.persistLocked(); err != nil {
			delete(s.records, replayKey)
			return receiptClaim{}, err
		}
		s.active[replayKey] = struct{}{}
		return receiptClaim{Kind: receiptClaimNew}, nil
	}
	if record.MessageID != messageID || record.RequestHash != requestHash {
		return receiptClaim{}, fmt.Errorf("%w: replay_key=%s stored_message_id=%s received_message_id=%s stored_request_hash=%s received_request_hash=%s", errReplayBindingConflict, replayKey, record.MessageID, messageID, record.RequestHash, requestHash)
	}
	if record.State == executionReceiptStateTerminal {
		result := *record.Result
		return receiptClaim{Kind: receiptClaimReplay, Result: &result}, nil
	}
	if _, active := s.active[replayKey]; active {
		return receiptClaim{Kind: receiptClaimActive}, nil
	}

	// A claimed record without an in-memory owner survived a previous process.
	// Re-running it could duplicate an external side effect, so close it as an
	// explicit reconciliation failure. A new owner-approved action gets a new
	// replay key and can be executed safely.
	now := time.Now().UTC()
	artifacts, err := decodeDurableArtifacts(record.Artifacts)
	if err != nil {
		return receiptClaim{}, err
	}
	result := resultFrame{
		Type:      "result",
		SessionID: sessionID,
		MessageID: messageID,
		Status:    "failed",
		Output:    "Hive Connect restarted before the prior local execution outcome was recorded. The action was not repeated; retry it as a new approved action.",
		ErrorCode: "local_execution_outcome_unknown",
		Artifacts: artifacts,
	}
	previous := record
	record.State = executionReceiptStateTerminal
	record.CompletedAt = &now
	record.AcknowledgedAt = nil
	record.Result = &result
	s.records[replayKey] = record
	if err := s.persistLocked(); err != nil {
		s.records[replayKey] = previous
		return receiptClaim{}, err
	}
	return receiptClaim{Kind: receiptClaimRecoveredUnknown, Result: &result}, nil
}

func (s *executionReceiptStore) complete(replayKey, messageID, requestHash string, result resultFrame) (resultFrame, error) {
	replayKey = strings.TrimSpace(replayKey)
	messageID = strings.TrimSpace(messageID)
	requestHash = strings.TrimSpace(requestHash)
	if replayKey == "" || messageID == "" || requestHash == "" {
		return resultFrame{}, errors.New("replay key, message id, and request hash are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[replayKey]
	if !exists {
		return resultFrame{}, fmt.Errorf("no durable execution claim for replay key %q", replayKey)
	}
	if record.MessageID != messageID || record.RequestHash != requestHash {
		return resultFrame{}, fmt.Errorf("%w: replay_key=%s", errReplayBindingConflict, replayKey)
	}
	if record.State == executionReceiptStateTerminal {
		return *record.Result, nil
	}

	now := time.Now().UTC()
	artifacts, err := decodeDurableArtifacts(record.Artifacts)
	if err != nil {
		return resultFrame{}, err
	}
	result.Artifacts = artifacts
	previousRecords := cloneExecutionReceipts(s.records)
	record.State = executionReceiptStateTerminal
	record.CompletedAt = &now
	record.AcknowledgedAt = nil
	record.Result = &result
	s.records[replayKey] = record
	delete(s.active, replayKey)
	s.pruneLocked()
	if err := s.persistLocked(); err != nil {
		s.records = previousRecords
		s.active[replayKey] = struct{}{}
		return resultFrame{}, err
	}
	return result, nil
}

func (s *executionReceiptStore) validateActiveBinding(replayKey, messageID, requestHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[strings.TrimSpace(replayKey)]
	if !exists || record.MessageID != strings.TrimSpace(messageID) || record.RequestHash != strings.TrimSpace(requestHash) {
		return fmt.Errorf("%w: replay_key=%s", errReplayBindingConflict, replayKey)
	}
	if record.State != executionReceiptStateClaimed {
		return errors.New("terminal receipt cannot accept artifacts")
	}
	if _, active := s.active[record.ReplayKey]; !active {
		return errors.New("artifact receipt has no active execution owner")
	}
	return nil
}

func (s *executionReceiptStore) appendArtifacts(replayKey, messageID, requestHash string, artifacts []map[string]any) error {
	if len(artifacts) == 0 {
		return errors.New("at least one artifact is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[strings.TrimSpace(replayKey)]
	if !exists || record.MessageID != strings.TrimSpace(messageID) || record.RequestHash != strings.TrimSpace(requestHash) {
		return fmt.Errorf("%w: replay_key=%s", errReplayBindingConflict, replayKey)
	}
	if record.State != executionReceiptStateClaimed {
		return errors.New("terminal receipt cannot accept artifacts")
	}
	if _, active := s.active[record.ReplayKey]; !active {
		return errors.New("artifact receipt has no active execution owner")
	}
	existing := make(map[string]struct{}, len(record.Artifacts))
	for _, artifact := range record.Artifacts {
		existing[artifact.SHA256] = struct{}{}
	}
	next := append([]durableArtifact(nil), record.Artifacts...)
	for _, metadata := range artifacts {
		artifact, err := encodeDurableArtifact(metadata)
		if err != nil {
			return err
		}
		if _, duplicate := existing[artifact.SHA256]; duplicate {
			continue
		}
		existing[artifact.SHA256] = struct{}{}
		next = append(next, artifact)
	}
	if len(next) > maxReceiptArtifacts {
		return fmt.Errorf("artifact receipt exceeds %d items", maxReceiptArtifacts)
	}
	if len(next) == len(record.Artifacts) {
		return nil
	}
	previous := record
	record.Artifacts = next
	s.records[record.ReplayKey] = record
	if err := s.persistLocked(); err != nil {
		s.records[record.ReplayKey] = previous
		return err
	}
	return nil
}

func encodeDurableArtifact(metadata map[string]any) (durableArtifact, error) {
	if len(metadata) == 0 {
		return durableArtifact{}, errors.New("artifact metadata is empty")
	}
	for key, value := range metadata {
		if _, allowed := artifactMetadataKeys[key]; !allowed {
			return durableArtifact{}, fmt.Errorf("artifact metadata key %q is not allowed", key)
		}
		switch typed := value.(type) {
		case nil, bool, json.Number, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		case string:
			if len(typed) > maxArtifactStringBytes {
				return durableArtifact{}, fmt.Errorf("artifact metadata value %q is too large", key)
			}
		default:
			return durableArtifact{}, fmt.Errorf("artifact metadata value %q must be scalar", key)
		}
	}
	if !hasArtifactReference(metadata) {
		return durableArtifact{}, errors.New("artifact metadata requires a stable id, path, uri, or source_ref")
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return durableArtifact{}, fmt.Errorf("encode artifact metadata: %w", err)
	}
	if len(raw) > maxArtifactMetadataBytes {
		return durableArtifact{}, fmt.Errorf("artifact metadata exceeds %d bytes", maxArtifactMetadataBytes)
	}
	digest := sha256.Sum256(raw)
	return durableArtifact{Metadata: raw, SHA256: fmt.Sprintf("%x", digest[:])}, nil
}

func decodeDurableArtifacts(items []durableArtifact) ([]map[string]any, error) {
	if len(items) > maxReceiptArtifacts {
		return nil, fmt.Errorf("artifact receipt exceeds %d items", maxReceiptArtifacts)
	}
	artifacts := make([]map[string]any, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, duplicate := seen[item.SHA256]; duplicate {
			return nil, errors.New("artifact receipt contains duplicate metadata")
		}
		seen[item.SHA256] = struct{}{}
		decoder := json.NewDecoder(bytes.NewReader(item.Metadata))
		decoder.UseNumber()
		var metadata map[string]any
		if err := decoder.Decode(&metadata); err != nil {
			return nil, err
		}
		canonical, err := encodeDurableArtifact(metadata)
		if err != nil {
			return nil, err
		}
		if canonical.SHA256 != item.SHA256 {
			return nil, errors.New("artifact metadata integrity hash mismatch")
		}
		artifacts = append(artifacts, metadata)
	}
	return artifacts, nil
}

func hasArtifactReference(metadata map[string]any) bool {
	for _, key := range []string{"artifact_id", "id", "source_ref", "uri", "path"} {
		if value, ok := metadata[key].(string); ok && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func artifactMetadataEqual(left, right []map[string]any) bool {
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func (s *executionReceiptStore) acknowledge(replayKey, messageID, requestHash string) error {
	replayKey = strings.TrimSpace(replayKey)
	messageID = strings.TrimSpace(messageID)
	requestHash = strings.TrimSpace(requestHash)
	if replayKey == "" || messageID == "" || requestHash == "" {
		return errors.New("result acknowledgement requires replay key, message id, and request hash")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[replayKey]
	if !exists {
		return fmt.Errorf("result acknowledgement has no receipt for replay key %q", replayKey)
	}
	if record.MessageID != messageID || record.RequestHash != requestHash {
		return fmt.Errorf("%w: replay_key=%s stored_message_id=%s received_message_id=%s stored_request_hash=%s received_request_hash=%s", errReplayBindingConflict, replayKey, record.MessageID, messageID, record.RequestHash, requestHash)
	}
	if record.State != executionReceiptStateTerminal || record.Result == nil {
		return fmt.Errorf("result acknowledgement arrived before terminal receipt for replay key %q", replayKey)
	}
	if record.AcknowledgedAt != nil {
		return nil
	}

	previousRecords := cloneExecutionReceipts(s.records)
	now := time.Now().UTC()
	record.AcknowledgedAt = &now
	s.records[replayKey] = record
	s.pruneLocked()
	if err := s.persistLocked(); err != nil {
		s.records = previousRecords
		return err
	}
	return nil
}

func (s *executionReceiptStore) pruneLocked() {
	if len(s.records) <= s.maxRecords {
		return
	}
	type terminalRecord struct {
		key         string
		completedAt time.Time
	}
	terminal := make([]terminalRecord, 0, len(s.records))
	for key, record := range s.records {
		if record.State == executionReceiptStateTerminal && record.CompletedAt != nil && record.AcknowledgedAt != nil {
			terminal = append(terminal, terminalRecord{key: key, completedAt: *record.CompletedAt})
		}
	}
	sort.Slice(terminal, func(i, j int) bool {
		return terminal[i].completedAt.Before(terminal[j].completedAt)
	})
	for _, candidate := range terminal {
		if len(s.records) <= s.maxRecords {
			break
		}
		delete(s.records, candidate.key)
	}
}

func cloneExecutionReceipts(records map[string]executionReceipt) map[string]executionReceipt {
	cloned := make(map[string]executionReceipt, len(records))
	for key, record := range records {
		cloned[key] = record
	}
	return cloned
}

func (s *executionReceiptStore) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create receipt store directory: %w", err)
	}
	payload, err := json.MarshalIndent(executionReceiptSnapshot{
		Schema:  executionReceiptSchema,
		Records: s.records,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode receipt store: %w", err)
	}
	payload = append(payload, '\n')
	if err := core.AtomicWriteFile(s.path, payload, 0o600); err != nil {
		return fmt.Errorf("persist receipt store: %w", err)
	}
	return nil
}
