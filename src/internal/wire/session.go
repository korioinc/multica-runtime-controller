package wire

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

const (
	SessionProtocolVersion = workspace.SessionProtocolVersion
	MaxAssignmentBytes     = workspace.MaxAssignmentBytes
	// The poll adds bounded UUID/state/uint64/time/stop fields and a JSON
	// encoder newline. This allowance does not enlarge the assignment itself.
	MaxSessionPollResponseBytes = MaxAssignmentBytes + (1 << 10)
)

// SessionBootstrap is immutable for one Pod incarnation. Its readable control
// capability grants enrollment and challenges, never a task assignment.
type SessionBootstrap struct {
	Version                 int                       `json:"version"`
	WorkerSessionID         string                    `json:"workerSessionID"`
	Conversation            workspace.ConversationKey `json:"conversation"`
	StorageID               string                    `json:"storageID"`
	WorkspaceAnchorTaskID   string                    `json:"workspaceAnchorTaskID"`
	CompatibilityDigest     string                    `json:"compatibilityDigest"`
	TaskRoot                string                    `json:"taskRoot"`
	PVCName                 string                    `json:"pvcName"`
	PVCUID                  string                    `json:"pvcUID"`
	NFSServer               string                    `json:"nfsServer"`
	GatewayURL              string                    `json:"gatewayURL"`
	Provider                string                    `json:"provider"`
	RuntimeID               string                    `json:"runtimeID"`
	RuntimeRef              runtimeimage.Ref          `json:"runtimeRef"`
	ControlCapability       string                    `json:"controlCapability"`
	TerminationGraceSeconds int                       `json:"terminationGraceSeconds"`
}

func (b SessionBootstrap) Validate() error {
	if b.Version != SessionProtocolVersion && b.Version != workspace.LegacySessionProtocolVersion || !UUID(b.WorkerSessionID) || !b.Conversation.Valid() ||
		!UUID(b.StorageID) || !UUID(b.WorkspaceAnchorTaskID) || !UUID(b.RuntimeID) || b.PVCName == "" || b.PVCUID == "" ||
		!core.ValidSHA(b.CompatibilityDigest) || b.TerminationGraceSeconds < 1 {
		return errors.New("invalid worker session identity")
	}
	if workspace.ValidateTaskRoot(WorkspaceRoot, b.TaskRoot, b.Conversation.WorkspaceID, b.WorkspaceAnchorTaskID) != nil ||
		net.ParseIP(b.NFSServer) == nil || !workspace.SupportedProvider(b.Provider) {
		return errors.New("invalid conversation mount")
	}
	u, err := url.Parse(b.GatewayURL)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("worker gateway requires a fixed HTTP origin")
	}
	if len(b.ControlCapability) < 32 || strings.ContainsAny(b.ControlCapability, "\x00\r\n \t") {
		return errors.New("invalid session enrollment capability")
	}
	if err := b.RuntimeRef.Validate(); err != nil {
		return err
	}
	if _, ok := b.RuntimeRef.Providers[b.Provider]; !ok {
		return errors.New("session provider is not in the image")
	}
	return nil
}

func DecodeSessionBootstrap(raw []byte) (SessionBootstrap, error) {
	var b SessionBootstrap
	if len(raw) > MaxRequestBytes {
		return b, errors.New("session bootstrap exceeds Secret payload limit")
	}
	if err := runtimeimage.Decode(raw, &b); err != nil {
		return b, errors.New("invalid session bootstrap")
	}
	return b, b.Validate()
}

// TurnAssignment is published once. InputDigest binds all authority and input,
// including its execution deadline; retries cannot replace individual fields.
type TurnAssignment struct {
	WorkerSessionID string    `json:"workerSessionID"`
	PodUID          string    `json:"podUID"`
	TurnSequence    uint64    `json:"turnSequence"`
	InputDigest     string    `json:"inputDigest"`
	Bootstrap       Bootstrap `json:"bootstrap"`
	Run             Run       `json:"run"`
	Deadline        time.Time `json:"deadline"`
}

func DecodeTurnAssignment(raw []byte) (TurnAssignment, error) {
	var assignment TurnAssignment
	if len(raw) > MaxAssignmentBytes || runtimeimage.Decode(raw, &assignment) != nil {
		return assignment, errors.New("invalid worker turn assignment")
	}
	b := assignment.Bootstrap
	if b.Validate() != nil || !UUID(assignment.WorkerSessionID) || !UUID(assignment.PodUID) || assignment.TurnSequence == 0 ||
		b.WorkerSessionID != assignment.WorkerSessionID || b.TurnSequence != assignment.TurnSequence ||
		!core.ValidSHA(assignment.InputDigest) || assignment.InputDigest != assignment.Digest() || assignment.Deadline.IsZero() {
		return assignment, errors.New("invalid worker turn binding")
	}
	return assignment, nil
}

func (a TurnAssignment) Digest() string {
	raw, err := json.Marshal(a)
	if err != nil {
		return ""
	}
	digest, _ := workspace.TurnInputDigest(raw)
	return digest
}

func (a TurnAssignment) Validate(session SessionBootstrap, podUID string) error {
	if session.Version != SessionProtocolVersion {
		return errors.New("historical session cannot receive a new assignment")
	}
	if err := session.Validate(); err != nil {
		return err
	}
	b := a.Bootstrap
	if err := b.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(a)
	if err != nil || len(raw) > MaxAssignmentBytes {
		return errors.New("invalid worker turn assignment")
	}
	inputDigest, err := workspace.TurnInputDigest(raw)
	if err != nil {
		return errors.New("invalid worker turn assignment")
	}
	if !UUID(podUID) || a.PodUID != podUID || a.WorkerSessionID != session.WorkerSessionID || a.TurnSequence == 0 ||
		!core.ValidSHA(a.InputDigest) || a.InputDigest != inputDigest || a.Deadline.IsZero() ||
		b.WorkerSessionID != a.WorkerSessionID || b.TurnSequence != a.TurnSequence ||
		b.OwnerID != session.Conversation.OwnerID || b.WorkspaceID != session.Conversation.WorkspaceID || b.AgentID != session.Conversation.AgentID ||
		b.WorkspaceAnchorTaskID != session.WorkspaceAnchorTaskID || b.StorageID != session.StorageID || b.TaskRoot != session.TaskRoot ||
		b.PVCName != session.PVCName || b.PVCUID != session.PVCUID || b.NFSServer != session.NFSServer ||
		b.Provider != session.Provider || b.RuntimeID != session.RuntimeID || !b.RuntimeRef.Equal(session.RuntimeRef) ||
		b.GatewayURL != session.GatewayURL || b.TerminationGraceSeconds != session.TerminationGraceSeconds {
		return errors.New("assignment differs from the admitted session")
	}
	if session.Conversation.Kind == workspace.ConversationTask && b.TaskID != session.Conversation.SubjectID {
		return errors.New("task-scoped session cannot accept another task")
	}
	if a.Run.Provider != b.Provider || a.Run.Options.Cwd != b.TaskRoot+"/workdir" || a.Run.Options.Timeout <= 0 {
		return errors.New("assignment input differs from its prepared root")
	}
	expiry, err := time.Parse(time.RFC3339Nano, b.ExpiresAt)
	if err != nil || a.Deadline.After(expiry) {
		return errors.New("assignment exceeds its turn authority lifetime")
	}
	return nil
}

type SessionAdmission struct {
	WorkerSessionID string            `json:"workerSessionID"`
	PodUID          string            `json:"podUID"`
	PVCUID          string            `json:"pvcUID"`
	BootstrapDigest string            `json:"bootstrapDigest"`
	PublicKey       ed25519.PublicKey `json:"publicKey"`
}

type SessionChallengeRequest struct {
	Operation  string `json:"operation"`
	BodyDigest string `json:"bodyDigest"`
}

type SessionControlRequest struct {
	Proof workspace.SessionProof `json:"proof"`
	Body  json.RawMessage        `json:"body"`
}

type SessionPoll struct {
	TurnSequence uint64                 `json:"turnSequence"`
	State        workspace.SessionState `json:"state"`
}

type SessionAccept struct {
	TurnSequence uint64 `json:"turnSequence"`
	InputDigest  string `json:"inputDigest"`
}

type SessionStopCommand struct {
	Reason                   string `json:"reason,omitempty"`
	Revision                 uint64 `json:"revision"`
	Nonce                    string `json:"nonce"`
	TurnSequence             uint64 `json:"turnSequence"`
	AttemptID                string `json:"attemptID,omitempty"`
	InputDigest              string `json:"inputDigest,omitempty"`
	ControllerWritersStopped bool   `json:"controllerWritersStopped"`
}

type SessionStopRequest struct {
	Reason string `json:"reason"`
}

type SessionPollResponse struct {
	WorkerSessionID     string                 `json:"workerSessionID"`
	TurnSequence        uint64                 `json:"turnSequence"`
	SettledTurnSequence uint64                 `json:"settledTurnSequence"`
	State               workspace.SessionState `json:"state"`
	Assignment          *TurnAssignment        `json:"assignment,omitempty"`
	IdleDeadline        time.Time              `json:"idleDeadline,omitempty"`
	Stop                *SessionStopCommand    `json:"stop,omitempty"`
}
