package workspace

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
)

// ResumePointers is opaque official task metadata, decoded by the API owner.
// Durable delivery paths are intentionally not resume pointers.
type ResumePointers struct {
	// MissingSessionID is controller-local evidence, never a backend API field.
	MissingSessionID      string `json:"missingSessionID,omitempty"`
	SessionID             string `json:"session_id"`
	WorkDir               string `json:"work_dir"`
	RetiredSessionID      string `json:"retired_session_id"`
	SessionRolloutMissing bool   `json:"session_rollout_missing"`
	// ResumeRejectedTransient records the signed Pi lock rejection, which
	// occurs before provider execution and does not retire the selected session.
	ResumeRejectedTransient bool `json:"resumeRejectedTransient,omitempty"`
}

type Terminal struct {
	ResultDigest    string           `json:"resultDigest,omitempty"`
	Source          string           `json:"source"`
	ReceiptID       string           `json:"receiptID"`
	TaskID          string           `json:"taskID"`
	AttemptID       string           `json:"attemptID"`
	PodUID          string           `json:"podUID"`
	PVCUID          string           `json:"pvcUID"`
	Kind            string           `json:"kind"`
	Body            []byte           `json:"body"`
	RequestDigest   string           `json:"requestDigest"`
	Nonce           string           `json:"nonce"`
	State           string           `json:"state"`
	Seal            *SignedReceipt   `json:"seal,omitempty"`
	ResultReceipt   *ResultReceipt   `json:"resultReceipt,omitempty"`
	ReceivedAt      time.Time        `json:"receivedAt"`
	RecoveryFailure *RecoveryFailure `json:"recoveryFailure,omitempty"`
}

// RecoveryFailure ends an unsealed execution without replacing its observed
// native result or claiming that its task files were flushed successfully.
type RecoveryFailure struct {
	Body      []byte    `json:"body"`
	State     string    `json:"state"`
	DecidedAt time.Time `json:"decidedAt"`
}

type SignedReceipt struct {
	TaskID         string `json:"taskID"`
	AttemptID      string `json:"attemptID"`
	RequestDigest  string `json:"requestDigest"`
	PodUID         string `json:"podUID"`
	PVCUID         string `json:"pvcUID"`
	Nonce          string `json:"nonce"`
	WritersStopped bool   `json:"writersStopped"`
	FlushOK        bool   `json:"flushOK"`
	Signature      []byte `json:"signature"`
}

// ReceiptMessage uses a fixed JSON tuple, excluding the signature itself.
func ReceiptMessage(r SignedReceipt) []byte {
	raw, _ := json.Marshal([]any{r.TaskID, r.AttemptID, r.RequestDigest, r.PodUID, r.PVCUID, r.Nonce, r.WritersStopped, r.FlushOK})
	return raw
}
func TerminalDigest(kind string, body []byte) string {
	return digest(append(append([]byte(kind), 0), body...))
}

func (s *Store) checkPendingBudget(st *registry, incoming int64) error {
	count, total := 0, int64(0)
	for id, pending := range st.Terminals {
		if !deliverySettled(st, id) {
			count++
			total += int64(len(pending.Body))
			if pending.RecoveryFailure != nil {
				total += int64(len(pending.RecoveryFailure.Body))
			}
		}
	}
	if count >= s.options.MaxPendingResults || total >= s.options.MaxPendingResultBytes || incoming > s.options.MaxPendingResultBytes-total {
		return ErrAdmissionBudget
	}
	return nil
}

// ReceiveTerminal owns the exact redacted bytes before the worker receives a local ACK.
func (s *Store) ReceiveTerminal(id, kind string, body []byte, resume ResumePointers, resultDigest string) (Terminal, error) {
	if !fingerprint(resultDigest) || (kind != "complete" && kind != "fail" && kind != "cancel-ack") || !json.Valid(body) || int64(len(body)) > s.options.MaxRecordBytes {
		return Terminal{}, invalid("terminal payload")
	}
	var terminal Terminal
	err := s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		requestDigest := TerminalDigest(kind, body)
		if old, ok := st.Terminals[id]; ok {
			if old.Source != "worker" || old.ResultDigest != resultDigest || old.Kind != kind || old.RequestDigest != requestDigest || !slices.Equal(old.Body, body) {
				return ErrConflict
			}
			terminal = old
			return nil
		}
		if err := s.checkPendingBudget(st, int64(len(body))); err != nil {
			return err
		}
		if (g.State != "started" && g.State != "starting" && g.State != "offered") || len(g.SupervisorKey) == 0 || g.PodUID == "" || g.PVCUID == "" {
			return ErrConflict
		}
		if g.WorkerSessionID != "" && !currentSessionTurn(st, *g) {
			return ErrConflict
		}
		if !validTransientResumeResult(*g, kind, resume) {
			return invalid("transient resume rejection")
		}
		if err := terminalResume(st, g, resume); err != nil {
			return err
		}
		terminal = Terminal{Source: "worker", ResultDigest: resultDigest, ReceiptID: uuid.NewString(), TaskID: g.TaskID, AttemptID: id, PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: kind, Body: slices.Clone(body), RequestDigest: requestDigest, Nonce: uuid.NewString(), State: "received", ReceivedAt: time.Now().UTC()}
		st.Terminals[id] = terminal
		g.State = "terminal_received"
		if g.WorkerSessionID != "" {
			session := st.Sessions[g.WorkerSessionID]
			if session.Stop == nil {
				session.State = SessionFinishing
			}
			st.Sessions[session.ID] = session
		}
		return nil
	})
	if err != nil {
		return Terminal{}, err
	}
	return terminal, nil
}
func (s *Store) Terminal(id string) (Terminal, error) {
	st, err := s.snapshot()
	if err != nil {
		return Terminal{}, err
	}
	t, ok := st.Terminals[id]
	if !ok {
		return Terminal{}, ErrUnauthorized
	}
	return t, nil
}

func (s *Store) Seal(id string, receipt SignedReceipt) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.WorkerSessionID != "" {
			return ErrConflict
		}
		t, ok := st.Terminals[id]
		if !ok || t.Source != "worker" || g.State != "terminal_received" || t.RecoveryFailure != nil {
			return ErrConflict
		}
		if receipt.TaskID != g.TaskID || receipt.AttemptID != id || receipt.PodUID != g.PodUID || receipt.PVCUID != g.PVCUID || receipt.RequestDigest != t.RequestDigest || receipt.Nonce != t.Nonce || !receipt.WritersStopped || !receipt.FlushOK || len(g.SupervisorKey) != ed25519.PublicKeySize || !ed25519.Verify(g.SupervisorKey, ReceiptMessage(receipt), receipt.Signature) {
			return errors.New("supervisor seal identity or flush proof failed")
		}
		if t.Seal != nil {
			if !sameJSON(*t.Seal, receipt) {
				return ErrConflict
			}
			return nil
		}
		if t.State != "received" && t.ResultReceipt == nil || !g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil {
			return ErrConflict
		}
		receipt.Signature = slices.Clone(receipt.Signature)
		t.Seal = &receipt
		if t.State == "received" {
			t.State = "sealed"
		}
		st.Terminals[id] = t
		return nil
	})
}
func (s *Store) BeginForward(id string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.Source != "worker" || (t.State != "sealed" && t.State != "received") || t.Seal == nil && t.ResultReceipt == nil {
			return ErrConflict
		}
		t.State = "forwarding"
		return nil
	})
}

// RejectTerminal settles an authenticated result forbidden by current backend
// assignment evidence. It records non-delivery, never upstream acceptance.
func (s *Store) RejectTerminal(id string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.Source != "worker" || (t.State != "received" && t.State != "sealed" && t.State != "uncertain" && t.State != "forwarding") || t.Seal == nil && t.ResultReceipt == nil {
			return ErrConflict
		}
		t.State = "rejected"
		return nil
	})
}

// ResumeForward follows a fresh assignment observation by the controller.
// A confirmed start is retained across terminal publication and restarts;
// pre-start controller failures cannot use this replay authority.
func (s *Store) ResumeForward(id string) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		t, ok := st.Terminals[id]
		if !ok || !g.StartConfirmed && (t.RecoveryFailure != nil || t.Source != "worker" || t.Kind != "cancel-ack") {
			return ErrConflict
		}
		if t.RecoveryFailure != nil {
			if t.RecoveryFailure.State != "uncertain" && t.RecoveryFailure.State != "forwarding" {
				return ErrConflict
			}
			t.RecoveryFailure.State = "received"
		} else {
			if t.State != "uncertain" && t.State != "forwarding" {
				return ErrConflict
			}
			t.State = "received"
			if t.Source == "worker" {
				if t.Seal == nil && t.ResultReceipt == nil {
					return ErrConflict
				}
				if t.Seal != nil {
					t.State = "sealed"
				}
			}
		}
		st.Terminals[id] = t
		return nil
	})
}

// RecordRecoveryFailure, ResultReceipt and Seal compete in one transaction.
// Only positively terminated workers can take this failure-only alternative.
func (s *Store) RecordRecoveryFailure(id string, body []byte) error {
	if !json.Valid(body) || int64(len(body)) > s.options.MaxRecordBytes {
		return invalid("recovery failure")
	}
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		t, ok := st.Terminals[id]
		if !ok || t.Source != "worker" || t.State != "received" || t.Seal != nil || t.ResultReceipt != nil || g.Stop == nil ||
			g.Stop.Evidence == nil || g.Stop.Evidence.Kind != "terminated" || !validStopEvidence(*g, *g.Stop.Evidence) ||
			!g.CheckoutClosed || g.CheckoutProcess != nil || g.PreparationStarted && !g.PreparationStopped {
			return ErrConflict
		}
		if t.RecoveryFailure != nil {
			if !slices.Equal(t.RecoveryFailure.Body, body) {
				return ErrConflict
			}
			return nil
		}
		// This adds bytes to an existing result, not another pending result.
		total := int64(len(body))
		for pendingID, pending := range st.Terminals {
			if !deliverySettled(st, pendingID) {
				total += int64(len(pending.Body))
				if pending.RecoveryFailure != nil {
					total += int64(len(pending.RecoveryFailure.Body))
				}
			}
		}
		if total > s.options.MaxPendingResultBytes {
			return ErrAdmissionBudget
		}
		t.RecoveryFailure = &RecoveryFailure{Body: slices.Clone(body), State: "received", DecidedAt: time.Now().UTC()}
		storage := st.Storages[g.StorageID]
		storage.Dirty = true
		st.Storages[g.StorageID] = storage
		st.Terminals[id] = t
		return nil
	})
}

func (s *Store) BeginRecoveryForward(id string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.RecoveryFailure == nil || t.RecoveryFailure.State != "received" {
			return ErrConflict
		}
		t.RecoveryFailure.State = "forwarding"
		return nil
	})
}

func (s *Store) FinishRecoveryForward(id, outcome string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.RecoveryFailure == nil || t.RecoveryFailure.State != "forwarding" {
			return ErrConflict
		}
		switch outcome {
		case "delivered", "rejected", "uncertain":
			t.RecoveryFailure.State = outcome
		case "not-sent":
			t.RecoveryFailure.State = "received"
		default:
			return invalid("recovery delivery outcome")
		}
		return nil
	})
}

func (s *Store) RejectRecoveryFailure(id string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.RecoveryFailure == nil || t.RecoveryFailure.State == "delivered" {
			return ErrConflict
		}
		t.RecoveryFailure.State = "rejected"
		return nil
	})
}

// FinishForward distinguishes definite acceptance/rejection from an ambiguous
// transport. Only a definitely unsent request may return to its pre-send state.
func (s *Store) FinishForward(id, outcome string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.State != "forwarding" {
			return ErrConflict
		}
		switch outcome {
		case "delivered", "rejected", "uncertain":
			t.State = outcome
		case "not-sent":
			t.State = "received"
			if t.Seal != nil {
				t.State = "sealed"
			}
		default:
			return invalid("delivery outcome")
		}
		return nil
	})
}

func (s *Store) updateTerminal(id string, fn func(*Terminal) error) error {
	return s.transaction(func(st *registry) error {
		t, ok := st.Terminals[id]
		if !ok {
			return ErrUnauthorized
		}
		if err := fn(&t); err != nil {
			return err
		}
		st.Terminals[id] = t
		return nil
	})
}

func terminalResume(st *registry, g *TaskGrant, resume ResumePointers) error {
	if g.WorkerSessionID != "" {
		if resume.WorkDir != "" && resume.WorkDir != g.TaskRoot+"/workdir" || resume.RetiredSessionID != "" && !validOpaque(resume.RetiredSessionID) ||
			resume.MissingSessionID != "" && (!resume.SessionRolloutMissing || !validOpaque(resume.MissingSessionID)) {
			return invalid("turn resume result")
		}
		if session := effectiveSession(resume); session != "" {
			if g.Prepared == nil || ValidateSession(*g.Prepared, session) != nil {
				return invalid("turn session")
			}
		}
		g.PendingResume = clonePointer(&resume)
		storage := st.Storages[g.StorageID]
		for _, excluded := range []string{resume.RetiredSessionID, resume.MissingSessionID} {
			if excluded != "" {
				storage.RetiredSessions = append(storage.RetiredSessions, excluded)
			}
		}
		slices.Sort(storage.RetiredSessions)
		storage.RetiredSessions = slices.Compact(storage.RetiredSessions)
		if storage.Checkpoint != nil && slices.Contains(storage.RetiredSessions, storage.Checkpoint.SessionID) {
			storage.Checkpoint.SessionID, storage.Checkpoint.SessionSource, storage.SessionID = "", CheckpointSource{}, ""
		}
		st.Storages[storage.ID] = storage
		return nil
	}
	invalidate := []string{resume.RetiredSessionID}
	if resume.SessionRolloutMissing {
		invalidate = append(invalidate, g.ResumeSession, resume.SessionID)
	}
	for _, session := range invalidate {
		if session == "" {
			continue
		}
		if !validOpaque(session) {
			return invalid("retired session")
		}
		storage := st.Storages[g.StorageID]
		if storage.SessionID == session {
			storage.SessionID = ""
			st.Storages[g.StorageID] = storage
		}
		if g.ResumeSession == session {
			g.ResumeSession = ""
		}
	}
	session := resume.SessionID
	if resume.SessionRolloutMissing || session == resume.RetiredSessionID {
		session = ""
	}
	return publishResume(st, g, session, resume.WorkDir)
}

func validTransientResumeResult(g TaskGrant, kind string, resume ResumePointers) bool {
	if !resume.ResumeRejectedTransient {
		return true
	}
	return g.WorkerSessionID != "" && kind == "fail" && g.Prepared != nil && g.Prepared.Provider == "pi" &&
		g.Compatibility != nil && g.Compatibility.Provider == "pi" && g.Selection != nil && g.Selection.Mode == SelectionResume &&
		g.ResumeSession != "" && g.Selection.SessionID == g.ResumeSession && resume.WorkDir == g.TaskRoot+"/workdir" &&
		resume.SessionID == "" && resume.RetiredSessionID == "" && resume.MissingSessionID == "" && !resume.SessionRolloutMissing
}

// ReceiveFailure durably records a controller-observed failure before delivery.
// It never replaces an admitted native terminal and never releases uncertain
// storage. No successful result can use this unsealed failure-only path.
func (s *Store) ReceiveFailure(id string, body []byte) (Terminal, error) {
	if !json.Valid(body) || int64(len(body)) > s.options.MaxRecordBytes {
		return Terminal{}, invalid("controller failure")
	}
	var terminal Terminal
	err := s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		requestDigest := TerminalDigest("fail", body)
		if old, ok := st.Terminals[id]; ok {
			if old.Source != "controller" || old.Kind != "fail" || old.RequestDigest != requestDigest || !slices.Equal(old.Body, body) {
				return ErrConflict
			}
			terminal = old
			return nil
		}
		if g.State == "closed" {
			return ErrConflict
		}
		if err := s.checkPendingBudget(st, int64(len(body))); err != nil {
			return err
		}
		terminal = Terminal{Source: "controller", ReceiptID: uuid.NewString(), TaskID: g.TaskID, AttemptID: id, PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "fail", Body: slices.Clone(body), RequestDigest: requestDigest, Nonce: uuid.NewString(), State: "received", ReceivedAt: time.Now().UTC()}
		if g.WorkerSessionID != "" {
			session := st.Sessions[g.WorkerSessionID]
			requestSessionStop(st, &session, "controller_failure", s.now())
			storage := st.Storages[g.StorageID]
			storage.Quarantined = true
			g.State, g.ExecutionRevoked, g.Stop = "quarantined", true, st.Grants[id].Stop
			st.Storages[storage.ID], st.Sessions[session.ID] = storage, session
		} else if g.State == "waiting_storage" || g.State == "intent" && preparationStoppedWithoutWorker(*g) {
			// No provider authority or Kubernetes Pod creation was issued.
			g.State = "unexecuted"
		} else {
			storage := st.Storages[g.StorageID]
			storage.Quarantined = true
			st.Storages[g.StorageID] = storage
			g.State = "quarantined"
		}
		st.Terminals[id] = terminal
		return nil
	})
	if err != nil {
		return Terminal{}, err
	}
	return terminal, nil
}
func (s *Store) BeginFailureForward(id string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.Source != "controller" || t.Kind != "fail" || t.State != "received" || t.Seal != nil {
			return ErrConflict
		}
		t.State = "forwarding"
		return nil
	})
}

// RejectFailure settles a failure that current assignment evidence forbids
// sending. It does not claim that the backend accepted the callback.
func (s *Store) RejectFailure(id string) error {
	return s.updateTerminal(id, func(t *Terminal) error {
		if t.Source != "controller" || (t.State != "received" && t.State != "uncertain" && t.State != "forwarding") || t.Kind != "fail" || t.Seal != nil {
			return ErrConflict
		}
		t.State = "rejected"
		return nil
	})
}

func preparationStoppedWithoutWorker(g TaskGrant) bool {
	if g.PreparationStarted && !g.PreparationStopped || g.PodUID != "" || len(g.SupervisorKey) != 0 {
		return false
	}
	var resources struct {
		PodCreateRequested bool `json:"podCreateRequested"`
	}
	if len(g.Resources) > 0 && json.Unmarshal(g.Resources, &resources) != nil {
		return false
	}
	return !resources.PodCreateRequested
}
