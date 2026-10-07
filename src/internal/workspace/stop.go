package workspace

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"slices"
	"time"

	"github.com/google/uuid"
)

// StopRecord owns process termination independently of provider result delivery.
// Its challenge is immutable: retries cannot restore execution authority.
type StopRecord struct {
	Reason      string        `json:"reason"`
	Revision    uint64        `json:"revision"`
	Nonce       string        `json:"nonce"`
	RequestedAt time.Time     `json:"requestedAt"`
	Receipt     *StopReceipt  `json:"receipt,omitempty"`
	Evidence    *StopEvidence `json:"evidence,omitempty"`
}

type StopReceipt struct {
	WorkerSessionID string `json:"workerSessionID,omitempty"`
	TurnSequence    uint64 `json:"turnSequence,omitempty"`
	TaskID          string `json:"taskID"`
	AttemptID       string `json:"attemptID"`
	Generation      uint64 `json:"generation"`
	PodUID          string `json:"podUID"`
	PVCUID          string `json:"pvcUID"`
	Revision        uint64 `json:"revision"`
	Nonce           string `json:"nonce"`
	WritersStopped  bool   `json:"writersStopped"`
	FlushOK         bool   `json:"flushOK"`
	Signature       []byte `json:"signature"`
}

// StopEvidence is controller-owned proof from the exact recorded Kubernetes
// objects. A missing Pod, expired lease, or deletion request is not proof.
type StopEvidence struct {
	PodUID          string    `json:"podUID"`
	PVCUID          string    `json:"pvcUID"`
	Kind            string    `json:"kind"`
	ObservedAt      time.Time `json:"observedAt"`
	PreparedFlushOK bool      `json:"preparedFlushOK"`
}

func StopReceiptMessage(r StopReceipt) []byte {
	if r.WorkerSessionID != "" || r.TurnSequence != 0 {
		raw, _ := json.Marshal([]any{"multica-stop-v2", r.WorkerSessionID, r.TurnSequence, r.TaskID, r.AttemptID, r.Generation, r.PodUID, r.PVCUID, r.Revision, r.Nonce, r.WritersStopped, r.FlushOK})
		return raw
	}
	raw, _ := json.Marshal([]any{"multica-stop-v1", r.TaskID, r.AttemptID, r.Generation, r.PodUID, r.PVCUID, r.Revision, r.Nonce, r.WritersStopped, r.FlushOK})
	return raw
}

func (s *Store) RequestStop(id, reason string) (TaskGrant, error) {
	if !validOpaque(reason) || len(reason) > 256 {
		return TaskGrant{}, invalid("stop reason")
	}
	var stopped TaskGrant
	err := s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.Stop != nil {
			stopped = *g
			return nil
		}
		if g.State == "closed" {
			return ErrConflict
		}
		g.Stop = &StopRecord{Reason: reason, Revision: 1, Nonce: uuid.NewString(), RequestedAt: time.Now().UTC()}
		g.ExecutionRevoked = true
		if g.WorkerSessionID != "" {
			session := st.Sessions[g.WorkerSessionID]
			requestSessionStop(st, &session, reason, s.now())
			st.Sessions[session.ID] = session
		}
		stopped = *g
		return nil
	})
	return stopped, err
}

// PinStopSupervisor registers the authenticated bootstrap's supervisor before
// input admission. The caller verifies the immutable Pod spec and image first.
func (s *Store) PinStopSupervisor(id, podUID, pvcUID, node string, key []byte) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.WorkerSessionID != "" {
			session := st.Sessions[g.WorkerSessionID]
			if !currentSessionTurn(st, *g) || !slices.Equal(session.SupervisorKey, key) || len(key) != ed25519.PublicKeySize ||
				podUID != g.PodUID || pvcUID != g.PVCUID || node != g.NodeID || !validOpaque(node) {
				return ErrConflict
			}
			g.StopKey = slices.Clone(key)
			return nil
		}
		if g.PodUID == "" || podUID != g.PodUID || pvcUID != g.PVCUID || !validOpaque(node) ||
			(g.NodeID != "" && node != g.NodeID) || len(key) != ed25519.PublicKeySize ||
			!stopGenerationCurrent(st, *g) || (g.State == "closed" && len(g.StopKey) == 0) {
			return ErrConflict
		}
		if len(g.StopKey) != 0 && !slices.Equal(g.StopKey, key) || len(g.SupervisorKey) != 0 && !slices.Equal(g.SupervisorKey, key) {
			return ErrConflict
		}
		g.NodeID = node
		g.StopKey = slices.Clone(key)
		return nil
	})
}

func (s *Store) ReceiveStopReceipt(id string, receipt StopReceipt) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if !stopGenerationCurrent(st, *g) || !validStopReceipt(*g, receipt) {
			return ErrConflict
		}
		if g.Stop.Receipt != nil {
			if !sameJSON(*g.Stop.Receipt, receipt) {
				return ErrConflict
			}
			return nil
		}
		// A late first receipt must not silently upgrade already released dirty
		// storage. Closed records only acknowledge previously committed evidence.
		if g.State == "closed" || receipt.WritersStopped && receipt.FlushOK && (!g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil) {
			return ErrConflict
		}
		receipt.Signature = slices.Clone(receipt.Signature)
		g.Stop.Receipt = &receipt
		return nil
	})
}

func validStopReceipt(g TaskGrant, r StopReceipt) bool {
	return g.Stop != nil && g.PodUID != "" && g.PVCUID != "" && r.TaskID == g.TaskID && r.AttemptID == g.AttemptID &&
		r.WorkerSessionID == g.WorkerSessionID && r.TurnSequence == g.TurnSequence &&
		r.Generation == g.Generation && r.PodUID == g.PodUID && r.PVCUID == g.PVCUID && r.Revision == g.Stop.Revision &&
		r.Nonce == g.Stop.Nonce && len(g.StopKey) == ed25519.PublicKeySize &&
		ed25519.Verify(g.StopKey, StopReceiptMessage(r), r.Signature)
}

func (s *Store) ObserveStop(id string, evidence StopEvidence) error {
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if g.Stop == nil || !validStopEvidence(*g, evidence) {
			return ErrConflict
		}
		if old := g.Stop.Evidence; old != nil {
			if old.Kind != evidence.Kind || old.PodUID != evidence.PodUID || old.PVCUID != evidence.PVCUID || old.PreparedFlushOK != evidence.PreparedFlushOK {
				return ErrConflict
			}
			return nil
		}
		if g.State == "closed" {
			return ErrConflict
		}
		g.Stop.Evidence = &evidence
		return nil
	})
}

func validStopEvidence(g TaskGrant, e StopEvidence) bool {
	if g.CheckoutProcess != nil || e.ObservedAt.IsZero() || e.PodUID != g.PodUID || e.PVCUID != g.PVCUID || g.PreparationStarted && !g.PreparationStopped {
		return false
	}
	if e.PreparedFlushOK && (g.Prepared == nil || !g.PreparationStopped || e.Kind != "no-worker" && e.Kind != "never-started") {
		return false
	}
	switch e.Kind {
	case "terminated":
		return g.PodUID != "" && g.PVCUID != ""
	case "never-started":
		// Admission means this controller already observed a running worker.
		return g.PodUID != "" && g.PVCUID != "" && len(g.SupervisorKey) == 0 && len(g.StopKey) == 0
	case "no-worker":
		return noWorkerCreated(g)
	default:
		return false
	}
}

type resourceCreation struct {
	PodCreateRequested    *bool           `json:"podCreateRequested"`
	SecretCreateRequested *bool           `json:"secretCreateRequested"`
	PodRequest            json.RawMessage `json:"podRequest"`
	Reference             struct {
		PodUID    string `json:"podUID"`
		SecretUID string `json:"secretUID"`
		PodDigest string `json:"podDigest"`
	} `json:"reference"`
}

func noWorkerCreated(g TaskGrant) bool {
	if g.PodUID != "" || len(g.SupervisorKey) != 0 || len(g.StopKey) != 0 {
		return false
	}
	if len(g.Resources) == 0 {
		return true
	}
	var resources resourceCreation
	return json.Unmarshal(g.Resources, &resources) == nil && resources.PodCreateRequested != nil && !*resources.PodCreateRequested && resources.Reference.PodUID == ""
}

func preservesResourceCreation(old, next json.RawMessage, stopping bool) bool {
	var before, after resourceCreation
	if len(old) != 0 && json.Unmarshal(old, &before) != nil || json.Unmarshal(next, &after) != nil {
		return false
	}
	// A stop can race the last live grant read. Only acknowledge requests
	// issued before the stop; new creation would invalidate no-worker evidence.
	if stopping && (after.SecretCreateRequested != nil && *after.SecretCreateRequested && (before.SecretCreateRequested == nil || !*before.SecretCreateRequested) ||
		after.PodCreateRequested != nil && *after.PodCreateRequested && (before.PodCreateRequested == nil || !*before.PodCreateRequested)) {
		return false
	}
	if before.PodCreateRequested != nil && *before.PodCreateRequested && (after.PodCreateRequested == nil || !*after.PodCreateRequested) ||
		before.SecretCreateRequested != nil && *before.SecretCreateRequested && (after.SecretCreateRequested == nil || !*after.SecretCreateRequested) {
		return false
	}
	if before.PodCreateRequested != nil && *before.PodCreateRequested {
		if before.Reference.PodDigest != "" && before.Reference.PodDigest != after.Reference.PodDigest ||
			len(before.PodRequest) != 0 && !bytes.Equal(compactJSON(before.PodRequest), compactJSON(after.PodRequest)) {
			return false
		}
	}
	return (before.Reference.PodUID == "" || before.Reference.PodUID == after.Reference.PodUID) &&
		(before.Reference.SecretUID == "" || before.Reference.SecretUID == after.Reference.SecretUID)
}

func stoppedClean(g TaskGrant, terminal Terminal) bool {
	if g.Stop == nil || g.Stop.Evidence == nil {
		return false
	}
	if r := g.Stop.Receipt; r != nil {
		return r.WritersStopped && r.FlushOK
	}
	// Existing worker seals prove the same writer stop and flush, independently
	// of whether the backend response was delivered or became uncertain.
	if terminal.Source == "worker" && terminal.Seal != nil {
		return true
	}
	if g.Stop.Evidence.Kind == "no-worker" || g.Stop.Evidence.Kind == "never-started" {
		return !g.PreparationStarted || g.Prepared != nil && g.PreparationStopped && g.Stop.Evidence.PreparedFlushOK
	}
	return false
}

// CloseStopped releases execution capacity after durable positive termination
// proof. Dirty data and unresolved result delivery still block the next attempt.
func (s *Store) CloseStopped(id string) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.WorkerSessionID != "" {
			return ErrConflict
		}
		if g.Stop == nil || g.Stop.Evidence == nil || !validStopEvidence(*g, *g.Stop.Evidence) {
			return ErrConflict
		}
		if g.State == "closed" {
			return nil
		}
		if terminal := st.Terminals[id]; terminal.Source == "worker" && terminal.State == "received" && terminal.RecoveryFailure == nil && terminal.ResultReceipt == nil {
			return ErrConflict
		}
		if g.StorageID != "" {
			storage := st.Storages[g.StorageID]
			if storage.ActiveAttempt != id {
				return ErrConflict
			}
			storage.ActiveAttempt = ""
			storage.Quarantined = false
			storage.Dirty = storage.Dirty || !stoppedClean(*g, st.Terminals[id])
			st.Storages[g.StorageID] = storage
		}
		g.State = "closed"
		return nil
	})
}

func stopGenerationCurrent(st *registry, g TaskGrant) bool {
	if g.StorageID == "" {
		return false
	}
	storage := st.Storages[g.StorageID]
	if storage.ActiveAttempt != g.AttemptID && !(g.State == "closed" && storage.ActiveAttempt == "") {
		return false
	}
	for _, other := range st.Grants {
		if other.TaskID == g.TaskID && other.Generation > g.Generation {
			return false
		}
	}
	return true
}

func deliverySettled(st *registry, id string) bool {
	t, exists := st.Terminals[id]
	if t.RecoveryFailure != nil {
		return t.RecoveryFailure.State == "delivered" || t.RecoveryFailure.State == "rejected"
	}
	return !exists || t.State == "delivered" || t.State == "rejected"
}
