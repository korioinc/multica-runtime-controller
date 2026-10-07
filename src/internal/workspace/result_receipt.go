package workspace

import (
	"crypto/ed25519"
	"encoding/json"
	"slices"
)

// ResultReceipt authenticates the provider outcome, not writer termination or
// filesystem durability. Only Seal and StopReceipt can prove clean storage.
type ResultReceipt struct {
	WorkerSessionID string `json:"workerSessionID,omitempty"`
	TurnSequence    uint64 `json:"turnSequence,omitempty"`
	TaskID          string `json:"taskID"`
	AttemptID       string `json:"attemptID"`
	PodUID          string `json:"podUID"`
	PVCUID          string `json:"pvcUID"`
	RequestDigest   string `json:"requestDigest"`
	Nonce           string `json:"nonce"`
	Signature       []byte `json:"signature"`
}

func ResultReceiptMessage(r ResultReceipt) []byte {
	if r.WorkerSessionID != "" || r.TurnSequence != 0 {
		raw, _ := json.Marshal([]any{"multica-result-v2", r.WorkerSessionID, r.TurnSequence, r.TaskID, r.AttemptID, r.PodUID, r.PVCUID, r.RequestDigest, r.Nonce})
		return raw
	}
	raw, _ := json.Marshal([]any{"multica-result-v1", r.TaskID, r.AttemptID, r.PodUID, r.PVCUID, r.RequestDigest, r.Nonce})
	return raw
}

func validResultReceipt(g TaskGrant, t Terminal, r ResultReceipt) bool {
	return t.Source == "worker" && (g.StartConfirmed || t.Kind == "cancel-ack" && g.ExecutionRevoked) &&
		r.WorkerSessionID == g.WorkerSessionID && r.TurnSequence == g.TurnSequence &&
		r.TaskID == g.TaskID && r.AttemptID == g.AttemptID && r.PodUID == g.PodUID && r.PVCUID == g.PVCUID &&
		r.RequestDigest == t.RequestDigest && r.Nonce == t.Nonce && len(g.SupervisorKey) == ed25519.PublicKeySize &&
		ed25519.Verify(g.SupervisorKey, ResultReceiptMessage(r), r.Signature)
}

// RecordResultReceipt competes with recovery failure selection in the same
// transaction. A cleanup failure cannot replace an authenticated provider result.
func (s *Store) RecordResultReceipt(id string, receipt ResultReceipt) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		t, ok := st.Terminals[id]
		if !ok || t.RecoveryFailure != nil || !validResultReceipt(*g, t, receipt) {
			return ErrConflict
		}
		if t.ResultReceipt != nil {
			if !sameJSON(*t.ResultReceipt, receipt) {
				return ErrConflict
			}
			return nil
		}
		if g.State != "terminal_received" {
			return ErrConflict
		}
		receipt.Signature = slices.Clone(receipt.Signature)
		t.ResultReceipt = &receipt
		st.Terminals[id] = t
		return nil
	})
}
