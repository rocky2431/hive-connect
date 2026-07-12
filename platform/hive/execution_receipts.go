package hive

import (
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
)

var errReplayBindingConflict = errors.New("replay key is bound to another message")

type executionReceipt struct {
	ReplayKey      string       `json:"replay_key"`
	MessageID      string       `json:"message_id"`
	RequestHash    string       `json:"request_hash"`
	State          string       `json:"state"`
	ClaimedAt      time.Time    `json:"claimed_at"`
	CompletedAt    *time.Time   `json:"completed_at,omitempty"`
	AcknowledgedAt *time.Time   `json:"acknowledged_at,omitempty"`
	Result         *resultFrame `json:"result,omitempty"`
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
	result := resultFrame{
		Type:      "result",
		SessionID: sessionID,
		MessageID: messageID,
		Status:    "failed",
		Output:    "Hive Connect restarted before the prior local execution outcome was recorded. The action was not repeated; retry it as a new approved action.",
		ErrorCode: "local_execution_outcome_unknown",
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

func (s *executionReceiptStore) complete(replayKey, messageID string, result resultFrame) (resultFrame, error) {
	replayKey = strings.TrimSpace(replayKey)
	messageID = strings.TrimSpace(messageID)
	if replayKey == "" || messageID == "" {
		return resultFrame{}, errors.New("replay key and message id are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[replayKey]
	if !exists {
		return resultFrame{}, fmt.Errorf("no durable execution claim for replay key %q", replayKey)
	}
	if record.MessageID != messageID {
		return resultFrame{}, fmt.Errorf("%w: replay_key=%s stored_message_id=%s received_message_id=%s", errReplayBindingConflict, replayKey, record.MessageID, messageID)
	}
	if record.State == executionReceiptStateTerminal {
		return *record.Result, nil
	}

	now := time.Now().UTC()
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
