package workspace

import (
	"encoding/json"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

// Workspace owns the persisted protocol and its assignment budget. Wire aliases
// these constants because its contracts already depend on workspace identities.
const (
	LegacySessionProtocolVersion = 1
	SessionProtocolVersion       = 2
	MaxAssignmentBytes           = 8 << 20
)

type ConversationKind string

const (
	ConversationIssue   ConversationKind = "issue"
	ConversationAgentDM ConversationKind = "agent_dm"
	ConversationTask    ConversationKind = "task"
)

// ConversationKey contains full backend identities, never display names or paths.
type ConversationKey struct {
	OwnerID     string           `json:"ownerID"`
	WorkspaceID string           `json:"workspaceID"`
	Kind        ConversationKind `json:"kind"`
	SubjectID   string           `json:"subjectID"`
	AgentID     string           `json:"agentID"`
}

func (k ConversationKey) Valid() bool {
	return canonicalUUID(k.OwnerID) && canonicalUUID(k.WorkspaceID) && canonicalUUID(k.SubjectID) && canonicalUUID(k.AgentID) &&
		(k.Kind == ConversationIssue || k.Kind == ConversationAgentDM || k.Kind == ConversationTask)
}

func (k ConversationKey) Digest() string {
	raw, _ := json.Marshal([]any{"multica-conversation-v1", k.OwnerID, k.WorkspaceID, k.Kind, k.SubjectID, k.AgentID})
	return digest(raw)
}

type SessionState string

const (
	SessionCreating    SessionState = "creating"
	SessionReserved    SessionState = "reserved"
	SessionRunning     SessionState = "running"
	SessionFinishing   SessionState = "finishing"
	SessionIdle        SessionState = "idle"
	SessionDraining    SessionState = "draining"
	SessionQuarantined SessionState = "quarantined"
	SessionClosed      SessionState = "closed"
)

// WorkerSession owns one Pod incarnation and the writer lease between turns.
type WorkerSession struct {
	ProtocolVersion              int                `json:"protocolVersion"`
	ID                           string             `json:"id"`
	Conversation                 ConversationKey    `json:"conversation"`
	StorageID                    string             `json:"storageID"`
	WorkspaceAnchorTaskID        string             `json:"workspaceAnchorTaskID"`
	CompatibilityDigest          string             `json:"compatibilityDigest"`
	WorkspaceCompatibilityDigest string             `json:"workspaceCompatibilityDigest"`
	RuntimeID                    string             `json:"runtimeID"`
	RuntimeRef                   runtimeimage.Ref   `json:"runtimeRef"`
	Fingerprint                  string             `json:"fingerprint"`
	PodName                      string             `json:"podName"`
	PodUID                       string             `json:"podUID,omitempty"`
	PVCName                      string             `json:"pvcName"`
	PVCUID                       string             `json:"pvcUID"`
	NodeID                       string             `json:"nodeID,omitempty"`
	TaskRoot                     string             `json:"taskRoot"`
	Revision                     uint64             `json:"revision"`
	TurnSequence                 uint64             `json:"turnSequence"`
	ActiveAttempt                string             `json:"activeAttempt,omitempty"`
	LastAttemptID                string             `json:"lastAttemptID,omitempty"`
	InputDigest                  string             `json:"inputDigest,omitempty"`
	LastCompleteAttempt          string             `json:"lastCompleteAttempt,omitempty"`
	State                        SessionState       `json:"state"`
	SupervisorKey                []byte             `json:"supervisorKey,omitempty"`
	ControlToken                 string             `json:"controlToken"`
	Bootstrap                    json.RawMessage    `json:"bootstrap,omitempty"`
	BootstrapDigest              string             `json:"bootstrapDigest,omitempty"`
	Resources                    json.RawMessage    `json:"resources,omitempty"`
	CreatedAt                    time.Time          `json:"createdAt"`
	LastObservedAt               time.Time          `json:"lastObservedAt"`
	IdleSince                    time.Time          `json:"idleSince,omitempty"`
	IdleDeadline                 time.Time          `json:"idleDeadline,omitempty"`
	Retention                    time.Duration      `json:"retention"`
	ResourcesCleaned             bool               `json:"resourcesCleaned"`
	Stop                         *SessionStopRecord `json:"stop,omitempty"`
}

const SessionChallengeTTL = 30 * time.Second

// SessionChallenge authorizes one bounded control operation for the pinned key.
// The bootstrap token can request a challenge but cannot satisfy its signature.
type SessionChallenge struct {
	Operation       string    `json:"operation"`
	WorkerSessionID string    `json:"workerSessionID"`
	PodUID          string    `json:"podUID"`
	PVCUID          string    `json:"pvcUID"`
	TurnSequence    uint64    `json:"turnSequence"`
	AttemptID       string    `json:"attemptID,omitempty"`
	BodyDigest      string    `json:"bodyDigest"`
	Nonce           string    `json:"nonce"`
	ExpiresAt       time.Time `json:"expiresAt"`
}

type SessionProof struct {
	SessionChallenge
	Signature []byte `json:"signature"`
}

func SessionProofMessage(p SessionProof) []byte {
	raw, _ := json.Marshal([]any{"multica-session-control-v1", p.Operation, p.WorkerSessionID, p.PodUID, p.PVCUID,
		p.TurnSequence, p.AttemptID, p.BodyDigest, p.Nonce, p.ExpiresAt.UTC().Format(time.RFC3339Nano)})
	return raw
}

// TurnReceipt proves quiescence in this live incarnation, not Pod termination.
type TurnReceipt struct {
	WorkerSessionID string `json:"workerSessionID"`
	TaskID          string `json:"taskID"`
	AttemptID       string `json:"attemptID"`
	Generation      uint64 `json:"generation"`
	TurnSequence    uint64 `json:"turnSequence"`
	InputDigest     string `json:"inputDigest"`
	PodUID          string `json:"podUID"`
	PVCUID          string `json:"pvcUID"`
	ResultDigest    string `json:"resultDigest"`
	RequestDigest   string `json:"requestDigest"`
	Nonce           string `json:"nonce"`
	WritersStopped  bool   `json:"writersStopped"`
	FlushOK         bool   `json:"flushOK"`
	Signature       []byte `json:"signature"`
}

func TurnReceiptMessage(r TurnReceipt) []byte {
	raw, _ := json.Marshal([]any{"multica-turn-quiescence-v1", r.WorkerSessionID, r.TaskID, r.AttemptID, r.Generation, r.TurnSequence, r.InputDigest,
		r.PodUID, r.PVCUID, r.ResultDigest, r.RequestDigest, r.Nonce, r.WritersStopped, r.FlushOK})
	return raw
}

// TurnExecutionReceipt ends task authority while conversation apps can write.
// It does not certify a filesystem flush or final storage release.
type TurnExecutionReceipt struct {
	WorkerSessionID      string          `json:"workerSessionID"`
	Conversation         ConversationKey `json:"conversation"`
	StorageID            string          `json:"storageID"`
	TaskID               string          `json:"taskID"`
	AttemptID            string          `json:"attemptID"`
	Generation           uint64          `json:"generation"`
	TurnSequence         uint64          `json:"turnSequence"`
	InputDigest          string          `json:"inputDigest"`
	PodUID               string          `json:"podUID"`
	PVCUID               string          `json:"pvcUID"`
	ResultDigest         string          `json:"resultDigest"`
	RequestDigest        string          `json:"requestDigest"`
	Nonce                string          `json:"nonce"`
	TaskProcessesStopped bool            `json:"taskProcessesStopped"`
	LocalRequestsClosed  bool            `json:"localRequestsClosed"`
	PrivateStateCleared  bool            `json:"privateStateCleared"`
	Signature            []byte          `json:"signature"`
}

func TurnExecutionReceiptMessage(r TurnExecutionReceipt) []byte {
	raw, _ := json.Marshal([]any{"multica-turn-execution-v2", r.WorkerSessionID,
		r.Conversation.OwnerID, r.Conversation.WorkspaceID, r.Conversation.Kind, r.Conversation.SubjectID, r.Conversation.AgentID,
		r.StorageID, r.TaskID, r.AttemptID, r.Generation, r.TurnSequence, r.InputDigest, r.PodUID, r.PVCUID,
		r.ResultDigest, r.RequestDigest, r.Nonce, r.TaskProcessesStopped, r.LocalRequestsClosed, r.PrivateStateCleared})
	return raw
}

type SessionStopRecord struct {
	ActiveAttempt            string              `json:"activeAttempt,omitempty"`
	ControllerWritersStopped bool                `json:"controllerWritersStopped"`
	Reason                   string              `json:"reason"`
	Revision                 uint64              `json:"revision"`
	Nonce                    string              `json:"nonce"`
	RequestedAt              time.Time           `json:"requestedAt"`
	Receipt                  *SessionStopReceipt `json:"receipt,omitempty"`
	Evidence                 *StopEvidence       `json:"evidence,omitempty"`
	RequestProof             *SessionProof       `json:"requestProof,omitempty"`
	RequestReason            string              `json:"requestReason,omitempty"`
	ReceiptProof             *SessionProof       `json:"receiptProof,omitempty"`
}

// SessionStopReceipt accompanies actual Kubernetes termination evidence.
type SessionStopReceipt struct {
	WorkerSessionID string `json:"workerSessionID"`
	AttemptID       string `json:"attemptID,omitempty"`
	InputDigest     string `json:"inputDigest,omitempty"`
	TurnSequence    uint64 `json:"turnSequence"`
	PodUID          string `json:"podUID"`
	PVCUID          string `json:"pvcUID"`
	Revision        uint64 `json:"revision"`
	Nonce           string `json:"nonce"`
	WritersStopped  bool   `json:"writersStopped"`
	FlushOK         bool   `json:"flushOK"`
	Signature       []byte `json:"signature"`
}

func SessionStopReceiptMessage(r SessionStopReceipt) []byte {
	raw, _ := json.Marshal([]any{"multica-session-stop-v1", r.WorkerSessionID, r.AttemptID, r.InputDigest, r.TurnSequence, r.PodUID, r.PVCUID,
		r.Revision, r.Nonce, r.WritersStopped, r.FlushOK})
	return raw
}
