package workspace

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

var ErrInvalidClaim = errors.New("invalid task claim")

type Repository struct {
	ID          string `json:"id,omitempty"`
	URL         string `json:"url"`
	Ref         string `json:"ref,omitempty"`
	Description string `json:"description,omitempty"`
}

// PreparationProcess identifies the controller container whose descendants own
// preparation writes. Pod absence alone does not prove that container stopped.
type PreparationProcess struct {
	PodName     string `json:"podName"`
	PodUID      string `json:"podUID"`
	ContainerID string `json:"containerID"`
}

func (p PreparationProcess) valid() bool {
	runtime, id, ok := strings.Cut(p.ContainerID, "://")
	return validOpaque(p.PodName) && canonicalUUID(p.PodUID) && ok && validOpaque(runtime) && validOpaque(id) && !strings.ContainsAny(p.ContainerID, " \t")
}

type TaskGrant struct {
	Legacy                       bool                         `json:"legacy,omitempty"`
	SessionProtocol              bool                         `json:"sessionProtocol,omitempty"`
	Conversation                 ConversationKey              `json:"conversation,omitempty"`
	WorkerSessionID              string                       `json:"workerSessionID,omitempty"`
	TurnSequence                 uint64                       `json:"turnSequence,omitempty"`
	WorkspaceAnchorTaskID        string                       `json:"workspaceAnchorTaskID,omitempty"`
	CompatibilityDigest          string                       `json:"compatibilityDigest,omitempty"`
	WorkspaceCompatibilityDigest string                       `json:"workspaceCompatibilityDigest,omitempty"`
	Compatibility                *ConversationCompatibilityV1 `json:"compatibility,omitempty"`
	Assignment                   json.RawMessage              `json:"assignment,omitempty"`
	InputDigest                  string                       `json:"inputDigest,omitempty"`
	TurnAccepted                 bool                         `json:"turnAccepted,omitempty"`
	TurnComplete                 bool                         `json:"turnComplete,omitempty"`
	TurnReceipt                  *TurnReceipt                 `json:"turnReceipt,omitempty"`
	TurnExecutionReceipt         *TurnExecutionReceipt        `json:"turnExecutionReceipt,omitempty"`
	TurnExecutionProof           *SessionProof                `json:"turnExecutionProof,omitempty"`
	AcceptProof                  *SessionProof                `json:"acceptProof,omitempty"`
	CompletionWitness            *CompletionWitness           `json:"completionWitness,omitempty"`
	Selection                    *Selection                   `json:"selection,omitempty"`
	BackendSelection             *Selection                   `json:"backendSelection,omitempty"`
	PendingResume                *ResumePointers              `json:"pendingResume,omitempty"`
	StartConfirmed               bool                         `json:"startConfirmed"`
	CheckoutClosed               bool                         `json:"checkoutClosed"`
	CheckoutNeedsFlush           bool                         `json:"checkoutNeedsFlush"`
	CheckoutProcess              *PreparationProcess          `json:"checkoutProcess,omitempty"`
	ExecutionRevoked             bool                         `json:"executionRevoked"`
	OwnerID                      string                       `json:"ownerID"`
	TaskID                       string                       `json:"taskID"`
	AttemptID                    string                       `json:"attemptID"`
	RuntimeID                    string                       `json:"runtimeID"`
	WorkspaceID                  string                       `json:"workspaceID"`
	AgentID                      string                       `json:"agentID"`
	Repositories                 []Repository                 `json:"repositories"`
	ResourceScope                []string                     `json:"resourceScope"`
	StorageID                    string                       `json:"storageID"`
	PVCName                      string                       `json:"pvcName"`
	PVCUID                       string                       `json:"pvcUID,omitempty"`
	PodName                      string                       `json:"podName"`
	PodUID                       string                       `json:"podUID,omitempty"`
	NodeID                       string                       `json:"nodeID,omitempty"`
	Fingerprint                  string                       `json:"fingerprint"`
	RuntimeRef                   runtimeimage.Ref             `json:"runtimeRef"`
	ScopeDigest                  string                       `json:"scopeDigest"`
	RequestDigest                string                       `json:"requestDigest"`
	Envelope                     json.RawMessage              `json:"envelope"`
	Metadata                     json.RawMessage              `json:"metadata"`
	Resources                    json.RawMessage              `json:"resources,omitempty"`
	Bootstrap                    json.RawMessage              `json:"bootstrap,omitempty"`
	BootstrapDigest              string                       `json:"bootstrapDigest,omitempty"`
	Generation                   uint64                       `json:"generation"`
	State                        string                       `json:"state"`
	CleanupComplete              bool                         `json:"cleanupComplete"`
	ResumeSession                string                       `json:"resumeSession,omitempty"`
	ResumeWorkDir                string                       `json:"resumeWorkDir,omitempty"`
	SupervisorKey                []byte                       `json:"supervisorKey,omitempty"`
	StopKey                      []byte                       `json:"stopKey,omitempty"`
	Stop                         *StopRecord                  `json:"stop,omitempty"`
	TaskRoot                     string                       `json:"taskRoot,omitempty"`
	Reuse                        bool                         `json:"reuse"`
	Prepared                     *Prepared                    `json:"prepared,omitempty"`
	Events                       []Event                      `json:"events,omitempty"`
	Execution                    json.RawMessage              `json:"execution,omitempty"`
	PreparationStarted           bool                         `json:"preparationStarted"`
	PreparationStopped           bool                         `json:"preparationStopped"`
	CreatedAt                    time.Time                    `json:"createdAt"`

	PreparationProcess *PreparationProcess `json:"preparationProcess,omitempty"`
}

type Storage struct {
	Conversation                 ConversationKey    `json:"conversation,omitempty"`
	WorkspaceAnchorTaskID        string             `json:"workspaceAnchorTaskID,omitempty"`
	CompatibilityDigest          string             `json:"compatibilityDigest,omitempty"`
	WorkspaceCompatibilityDigest string             `json:"workspaceCompatibilityDigest,omitempty"`
	WriterSessionID              string             `json:"writerSessionID,omitempty"`
	LatestWriter                 CheckpointSource   `json:"latestWriter,omitempty"`
	Checkpoint                   *SessionCheckpoint `json:"checkpoint,omitempty"`
	RetiredSessions              []string           `json:"retiredSessions,omitempty"`
	// Deprecated: retained only to load and preserve existing journal records.
	// Issue authorization belongs to the backend.
	CreatedIssueIDs []string  `json:"createdIssueIDs,omitempty"`
	EventSequence   uint64    `json:"eventSequence"`
	ID              string    `json:"id"`
	ScopeDigest     string    `json:"scopeDigest"`
	Fingerprint     string    `json:"fingerprint"`
	PVCName         string    `json:"pvcName"`
	PVCUID          string    `json:"pvcUID,omitempty"`
	WorkspaceID     string    `json:"workspaceID"`
	TaskID          string    `json:"taskID"`
	AgentID         string    `json:"agentID"`
	TaskRoot        string    `json:"taskRoot"`
	Prepared        *Prepared `json:"prepared,omitempty"`
	SessionID       string    `json:"sessionID,omitempty"`
	ActiveAttempt   string    `json:"activeAttempt,omitempty"`
	Quarantined     bool      `json:"quarantined"`
	Dirty           bool      `json:"dirty"`
}

func scopeDigest(g TaskGrant) string {
	raw, _ := json.Marshal([]any{g.OwnerID, g.WorkspaceID, g.TaskID, g.AgentID, g.Repositories, g.ResourceScope})
	return digest(raw)
}
func RuntimeFingerprint(ref runtimeimage.Ref) string { raw, _ := json.Marshal(ref); return digest(raw) }
func (s *Store) normalizeClaim(g TaskGrant) (result TaskGrant, err error) {
	defer func() {
		if err != nil {
			err = errors.Join(ErrInvalidClaim, err)
		}
	}()
	if g.OwnerID == "" {
		g.OwnerID = s.options.OwnerID
	}
	if g.AttemptID == "" {
		g.AttemptID = uuid.NewString()
	}
	if g.OwnerID != s.options.OwnerID || !canonicalUUID(g.TaskID) || !canonicalUUID(g.AttemptID) || g.RuntimeID == "" || g.WorkspaceID == "" || g.AgentID == "" || g.RuntimeRef.Validate() != nil || !json.Valid(g.Envelope) || int64(len(g.Envelope)) > s.options.MaxRecordBytes {
		return TaskGrant{}, invalid("task grant")
	}
	if g.StartConfirmed || g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil || g.ExecutionRevoked || g.CleanupComplete || g.StorageID != "" || g.PVCName != "" || g.PVCUID != "" || g.PodName != "" || g.PodUID != "" || g.NodeID != "" || g.State != "" || len(g.SupervisorKey) != 0 || len(g.StopKey) != 0 || g.Stop != nil || len(g.Bootstrap) != 0 || len(g.Resources) != 0 || g.ResumeSession != "" || g.ResumeWorkDir != "" || g.TaskRoot != "" || g.Prepared != nil || len(g.Execution) != 0 || len(g.Events) != 0 || g.PreparationStarted || g.PreparationStopped || g.PreparationProcess != nil || g.Reuse {
		return TaskGrant{}, errors.New("new grants cannot supply storage or execution authority")
	}
	if g.Legacy || g.WorkerSessionID != "" || g.Conversation != (ConversationKey{}) || g.TurnSequence != 0 || g.WorkspaceAnchorTaskID != "" || g.Compatibility != nil ||
		g.CompatibilityDigest != "" || g.WorkspaceCompatibilityDigest != "" || len(g.Assignment) != 0 || g.InputDigest != "" || g.TurnAccepted || g.TurnComplete ||
		g.TurnReceipt != nil || g.TurnExecutionReceipt != nil || g.TurnExecutionProof != nil || g.AcceptProof != nil || g.CompletionWitness != nil || g.Selection != nil || g.BackendSelection != nil || g.PendingResume != nil {
		return TaskGrant{}, errors.New("new grants cannot supply conversation authority")
	}
	g.Repositories = slices.Clone(g.Repositories)
	slices.SortFunc(g.Repositories, func(a, b Repository) int { return strings.Compare(a.URL, b.URL) })
	for i, r := range g.Repositories {
		if !validOpaque(r.URL) || (i > 0 && r.URL == g.Repositories[i-1].URL) || strings.ContainsAny(r.Ref, "\x00\r\n") {
			return TaskGrant{}, invalid("repository scope")
		}
	}
	g.ResourceScope = slices.Clone(g.ResourceScope)
	slices.Sort(g.ResourceScope)
	g.ResourceScope = slices.Compact(g.ResourceScope)
	for _, v := range g.ResourceScope {
		if !validOpaque(v) {
			return TaskGrant{}, invalid("resource scope")
		}
	}
	if g.Repositories == nil {
		g.Repositories = []Repository{}
	}
	if g.ResourceScope == nil {
		g.ResourceScope = []string{}
	}
	g.ScopeDigest = scopeDigest(g)
	calculated := RuntimeFingerprint(g.RuntimeRef)
	if g.Fingerprint != "" && g.Fingerprint != calculated {
		return TaskGrant{}, invalid("runtime fingerprint")
	}
	g.Envelope = compactJSON(g.Envelope)
	if len(g.Metadata) == 0 {
		g.Metadata = json.RawMessage(`{}`)
	}
	if !json.Valid(g.Metadata) || int64(len(g.Metadata)) > s.options.MaxRecordBytes {
		return TaskGrant{}, invalid("claim metadata")
	}
	g.Metadata = compactJSON(g.Metadata)
	g.Fingerprint = calculated
	g.RequestDigest = digest(g.Envelope)
	g.State = "waiting_storage"
	g.Generation = 1
	g.CreatedAt = time.Now().UTC()
	return g, nil
}

// QueueClaim takes durable responsibility for a claim without granting storage
// or process authority. Its creation time includes time waiting for a writer.
func (s *Store) QueueClaim(input TaskGrant) (TaskGrant, error) {
	g, err := s.normalizeClaim(input)
	if err != nil {
		return TaskGrant{}, err
	}
	err = s.transaction(func(st *registry) error {
		for _, old := range st.Grants {
			if old.TaskID != g.TaskID {
				continue
			}
			if old.State != "closed" {
				if old.State != "waiting_storage" {
					return ErrStorageBusy
				}
				candidate := g
				candidate.AttemptID = old.AttemptID
				candidate.CreatedAt = old.CreatedAt
				candidate.Generation = old.Generation
				candidate.Conversation, candidate.Selection, candidate.CompatibilityDigest = old.Conversation, old.Selection, old.CompatibilityDigest
				candidate.WorkspaceCompatibilityDigest = old.WorkspaceCompatibilityDigest
				candidate.Compatibility = old.Compatibility
				candidate.BackendSelection = old.BackendSelection
				if !sameJSON(candidate, old) {
					return ErrConflict
				}
				g = old
				return nil
			}
			if old.Generation >= g.Generation {
				g.Generation = old.Generation + 1
			}
		}
		if _, exists := st.Grants[g.AttemptID]; exists {
			return ErrConflict
		}
		st.Grants[g.AttemptID] = g
		return nil
	})
	if err != nil {
		return TaskGrant{}, err
	}
	return g, nil
}

// Create atomically activates a queued claim or creates new storage authority.
// Busy storage leaves a queued claim durable with its original attempt identity.
// Production supplies its resolved resident ceiling. Historical setup callers
// may omit it and use the Store's existing task admission bound.
func (s *Store) Create(input TaskGrant, residentLimit ...int) (TaskGrant, error) {
	limit := s.options.MaxTasks
	if len(residentLimit) > 1 || len(residentLimit) == 1 && residentLimit[0] < 1 {
		return TaskGrant{}, invalid("resident Pod limit")
	}
	if len(residentLimit) == 1 {
		limit = residentLimit[0]
	}
	if input.SessionProtocol {
		return TaskGrant{}, errors.New("session claims must use QueueClaim and ReserveTurn")
	}
	pending := input.State == "waiting_storage"
	if pending {
		input.State = ""
		input.Legacy = false
	}
	g, err := s.normalizeClaim(input)
	if err != nil {
		return TaskGrant{}, err
	}
	var deferred error
	err = s.transaction(func(st *registry) error {
		queued, exists := st.Grants[g.AttemptID]
		if exists {
			if !pending || queued.State != "waiting_storage" {
				return ErrConflict
			}
			candidate := g
			candidate.CreatedAt = queued.CreatedAt
			candidate.Generation = queued.Generation
			candidate.Legacy = queued.Legacy
			if !sameJSON(candidate, queued) {
				return ErrConflict
			}
			g.CreatedAt = queued.CreatedAt
			g.Generation = queued.Generation
			g.Legacy = queued.Legacy
		} else if pending {
			return ErrConflict
		}
		for _, old := range st.Grants {
			if old.TaskID != g.TaskID || old.AttemptID == g.AttemptID {
				continue
			}
			if old.State != "closed" || !old.CleanupComplete || !deliverySettled(st, old.AttemptID) {
				return ErrStorageBusy
			}
			if old.Generation >= g.Generation {
				g.Generation = old.Generation + 1
			}
		}
		if err := s.checkPendingBudget(st, 0); err != nil {
			return err
		}
		if st.WorkspaceClaim == "" || st.WorkspaceUID == "" {
			return errors.New("workspace volume has not been bound")
		}
		storage := Storage{}
		for _, candidate := range st.Storages {
			if candidate.WorkspaceID == g.WorkspaceID && candidate.TaskID == g.TaskID {
				if candidate.AgentID != g.AgentID || candidate.ScopeDigest != g.ScopeDigest || candidate.Fingerprint != g.Fingerprint {
					return ErrConflict
				}
				if candidate.ActiveAttempt != "" || candidate.Quarantined || candidate.Dirty {
					return ErrStorageBusy
				}
				for _, prior := range st.Grants {
					if prior.StorageID == candidate.ID && !prior.CleanupComplete {
						return ErrStorageBusy
					}
				}
				storage = candidate
				g.Reuse = candidate.Prepared != nil
				break
			}
		}
		if storage.ID == "" {
			if len(st.Storages) >= s.options.MaxTasks {
				return ErrAdmissionBudget
			}
			var labels struct {
				WorkspaceSlug   string `json:"workspace_slug"`
				IssueIdentifier string `json:"issue_identifier"`
			}
			if err := json.Unmarshal(g.Envelope, &labels); err != nil {
				return invalid("task naming input")
			}
			root, err := TaskRoot("/workspace", g.WorkspaceID, g.TaskID, labels.WorkspaceSlug, labels.IssueIdentifier)
			if err != nil {
				return err
			}
			for _, other := range st.Storages {
				if other.TaskRoot == root {
					return ErrConflict
				}
			}
			storage = Storage{ID: uuid.NewString(), WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, TaskRoot: root, ScopeDigest: g.ScopeDigest, Fingerprint: g.Fingerprint, PVCName: st.WorkspaceClaim, PVCUID: st.WorkspaceUID}
		}
		if !admitResident(st, limit, s.now()) {
			deferred = ErrResidentCapacity
			return nil
		}
		g.StorageID, g.PVCName, g.PVCUID, g.TaskRoot = storage.ID, storage.PVCName, storage.PVCUID, storage.TaskRoot
		g.ResumeSession = storage.SessionID
		if g.ResumeSession != "" {
			g.ResumeWorkDir = storage.TaskRoot + "/workdir"
		}
		g.PodName = "task-" + g.AttemptID
		g.State = "intent"
		storage.ActiveAttempt = g.AttemptID
		st.Storages[storage.ID] = storage
		st.Grants[g.AttemptID] = g
		return nil
	})
	if err != nil {
		return TaskGrant{}, err
	}
	if deferred != nil {
		return TaskGrant{}, deferred
	}
	return g, nil
}

// BindWorkspace pins the installation's new volume once; restarts cannot adopt a replacement.
func (s *Store) BindWorkspace(name, uid, server string) error {
	if !validOpaque(name) || !validOpaque(uid) || net.ParseIP(server) == nil {
		return invalid("workspace claim")
	}
	return s.transaction(func(st *registry) error {
		if st.WorkspaceClaim != "" && (st.WorkspaceClaim != name || st.WorkspaceUID != uid || st.NFSServer != server) {
			return ErrConflict
		}
		st.WorkspaceClaim, st.WorkspaceUID, st.NFSServer = name, uid, server
		return nil
	})
}

func (s *Store) PreviousPrepared(id string) (*Prepared, error) {
	st, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	g, ok := st.Grants[id]
	if !ok || g.StorageID == "" {
		return nil, ErrUnauthorized
	}
	return st.Storages[g.StorageID].Prepared, nil
}

func (s *Store) BeginPreparation(id string, process PreparationProcess) error {
	if !process.valid() {
		return invalid("preparation process")
	}
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.State != "intent" || g.ExecutionRevoked || g.PreparationStarted || g.Prepared != nil || g.WorkerSessionID == "" && g.PodUID != "" ||
			g.WorkerSessionID != "" && !currentSessionTurn(st, *g) {
			return ErrConflict
		}
		g.PreparationStarted = true
		g.PreparationProcess = &process
		return nil
	})
}

// CheckpointPreparation retains validated native initialization before eager
// checkout. A failed checkout can then reuse those files without granting a worker.
func (s *Store) CheckpointPreparation(id string, prepared Prepared) error {
	if ValidatePrepared(prepared) != nil {
		return invalid("preparation checkpoint")
	}
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.State != "intent" || !g.PreparationStarted || g.PreparationStopped || g.Prepared != nil || g.WorkerSessionID == "" && g.PodUID != "" ||
			prepared.OwnerID != g.OwnerID || prepared.WorkspaceID != g.WorkspaceID || prepared.TaskID != g.TaskID || prepared.AgentID != g.AgentID ||
			prepared.AttemptID != id || prepared.Generation != g.Generation || prepared.PVCUID != g.PVCUID || prepared.TaskRoot != g.TaskRoot ||
			prepared.RuntimeDigest != g.Fingerprint || prepared.ConfigurationDigest != g.RuntimeRef.ConfigurationDigest {
			return ErrConflict
		}
		if !preparedMatchesConversation(prepared, *g) {
			return ErrConflict
		}
		storage := st.Storages[g.StorageID]
		storage.Prepared = &prepared
		st.Storages[g.StorageID] = storage
		return nil
	})
}

// InterruptPreparation is called only after Kubernetes positively proves that
// the exact recorded controller container terminated or was replaced. It never
// releases worker authority, including an uncertain Pod or Secret creation.
func (s *Store) InterruptPreparation(id string, expected PreparationProcess) error {
	if !expected.valid() {
		return invalid("preparation process")
	}
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.WorkerSessionID != "" {
			if !currentSessionTurn(st, *g) || g.PreparationProcess == nil || *g.PreparationProcess != expected || !g.PreparationStarted ||
				g.PreparationStopped || g.Prepared != nil || (g.State != "intent" && g.State != "quarantined") {
				return ErrConflict
			}
			g.PreparationStopped, g.ExecutionRevoked = true, true
			session := st.Sessions[g.WorkerSessionID]
			requestSessionStop(st, &session, "preparation_interrupted", s.now())
			g.Stop = st.Grants[id].Stop
			st.Sessions[session.ID] = session
			return nil
		}
		if g.PreparationProcess == nil || *g.PreparationProcess != expected || !g.PreparationStarted || g.PreparationStopped ||
			(g.State != "intent" && g.State != "quarantined") || g.Prepared != nil || g.PodUID != "" || g.NodeID != "" ||
			len(g.Bootstrap) != 0 || g.BootstrapDigest != "" || len(g.Execution) != 0 || len(g.SupervisorKey) != 0 {
			return ErrConflict
		}
		var resources struct {
			PodCreateRequested    *bool `json:"podCreateRequested"`
			SecretCreateRequested *bool `json:"secretCreateRequested"`
			Reference             struct {
				PodUID        string `json:"podUID"`
				SecretUID     string `json:"secretUID"`
				NodeID        string `json:"nodeID"`
				RequestDigest string `json:"requestDigest"`
				PodDigest     string `json:"podDigest"`
			} `json:"reference"`
		}
		if json.Unmarshal(g.Resources, &resources) != nil || resources.PodCreateRequested == nil || *resources.PodCreateRequested ||
			resources.SecretCreateRequested == nil || *resources.SecretCreateRequested || resources.Reference.PodUID != "" ||
			resources.Reference.SecretUID != "" || resources.Reference.NodeID != "" || resources.Reference.RequestDigest != "" || resources.Reference.PodDigest != "" {
			return ErrConflict
		}
		if g.State == "quarantined" && g.Stop == nil {
			terminal, ok := st.Terminals[id]
			if !ok || terminal.Source != "controller" || terminal.Kind != "fail" || terminal.Seal != nil {
				return ErrConflict
			}
			g.State = "unexecuted"
			storage := st.Storages[g.StorageID]
			storage.Quarantined = false
			st.Storages[g.StorageID] = storage
		}
		g.PreparationStopped = true
		return nil
	})
}

// CompletePreparation is called only after the helper and every preparation writer have exited.
func (s *Store) CompletePreparation(id string, prepared *Prepared, execution json.RawMessage) error {
	if prepared != nil && (ValidatePrepared(*prepared) != nil || !json.Valid(execution) || int64(len(execution)) > s.options.MaxRecordBytes) {
		return invalid("prepared execution")
	}
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if (g.State != "intent" && g.State != "quarantined") || !g.PreparationStarted || g.PreparationStopped || g.WorkerSessionID == "" && g.PodUID != "" {
			return ErrConflict
		}
		g.PreparationStopped = true
		if g.State == "quarantined" && g.Stop == nil && g.WorkerSessionID == "" {
			terminal, ok := st.Terminals[id]
			if !ok || terminal.Source != "controller" || terminal.Kind != "fail" || terminal.Seal != nil ||
				g.Prepared != nil || len(g.Execution) != 0 || len(g.Bootstrap) != 0 || g.BootstrapDigest != "" || !preparationStoppedWithoutWorker(*g) {
				return ErrConflict
			}
			// Failure can settle while the preparation goroutine is stopping. Its
			// final stop proof releases the reservation, never late worker input.
			g.State = "unexecuted"
			storage := st.Storages[g.StorageID]
			storage.Quarantined = false
			st.Storages[g.StorageID] = storage
			return nil
		}
		if prepared == nil {
			return nil
		}
		p := *prepared
		if p.OwnerID != g.OwnerID || p.TaskID != g.TaskID || p.WorkspaceID != g.WorkspaceID || p.AgentID != g.AgentID || p.AttemptID != id || p.Generation != g.Generation || p.PVCUID != g.PVCUID || p.TaskRoot != g.TaskRoot || !preparedMatchesConversation(p, *g) {
			return ErrConflict
		}
		g.Prepared = &p
		g.Execution = compactJSON(execution)
		storage := st.Storages[g.StorageID]
		storage.Prepared = &p
		st.Storages[g.StorageID] = storage
		return nil
	})
}

func (s *Store) Get(attemptID string) (TaskGrant, error) {
	st, err := s.snapshot()
	if err != nil {
		return TaskGrant{}, err
	}
	g, ok := st.Grants[attemptID]
	if !ok {
		return TaskGrant{}, ErrUnauthorized
	}
	return g, nil
}
func (s *Store) List() ([]TaskGrant, error) {
	st, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	all := make([]TaskGrant, 0, len(st.Grants))
	for _, g := range st.Grants {
		all = append(all, g)
	}
	slices.SortFunc(all, func(a, b TaskGrant) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return all, nil
}

// ReconcileGrants selects only work that can still progress. Settled history
// and delivery outcomes that cannot safely be replayed remain in the journal
// without triggering an additional full-journal read for every retained grant.
func (s *Store) ReconcileGrants() ([]TaskGrant, error) {
	st, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	var pending []TaskGrant
	for _, g := range st.Grants {
		t := st.Terminals[g.AttemptID]
		cleaned := g.CleanupComplete
		if g.WorkerSessionID != "" {
			cleaned = g.TurnComplete
		}
		if g.State != "closed" || !cleaned || t.Source != "" && !deliverySettled(&st, g.AttemptID) {
			pending = append(pending, g)
		}
	}
	slices.SortFunc(pending, func(a, b TaskGrant) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return pending, nil
}

func (s *Store) updateGrant(id string, fn func(*registry, *TaskGrant) error) error {
	return s.transaction(func(st *registry) error {
		g, ok := st.Grants[id]
		if !ok {
			return ErrUnauthorized
		}
		if err := fn(st, &g); err != nil {
			return err
		}
		st.Grants[id] = g
		return nil
	})
}
func (s *Store) BindPod(id, name, uid, node string) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if (g.State != "intent" && !(g.Stop != nil && g.State == "quarantined")) || g.Prepared == nil || !g.PreparationStopped || g.PVCUID == "" || name != g.PodName || !validOpaque(uid) || (g.PodUID != "" && g.PodUID != uid) || (g.NodeID != "" && node != "" && g.NodeID != node) {
			return ErrConflict
		}
		if terminal, ok := st.Terminals[id]; ok && terminal.Source == "controller" && terminal.PodUID == "" {
			terminal.PodUID = uid
			st.Terminals[id] = terminal
		}
		g.PodUID = uid
		if node != "" {
			g.NodeID = node
		}
		for key, c := range st.Capabilities {
			if c.AttemptID == id {
				c.PodUID = uid
				st.Capabilities[key] = c
			}
		}
		return nil
	})
}
func (s *Store) Admit(id string, key ed25519.PublicKey) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.WorkerSessionID != "" {
			session := st.Sessions[g.WorkerSessionID]
			if !currentSessionTurn(st, *g) || session.Stop != nil || g.ExecutionRevoked || g.Prepared == nil || !g.PreparationStopped ||
				!slices.Equal(session.SupervisorKey, key) || len(key) != ed25519.PublicKeySize || g.PodUID == "" || !g.TurnAccepted ||
				(g.State != "assigned" && g.State != "offered" && g.State != "ready" && g.State != "starting" && g.State != "started") {
				return ErrConflict
			}
			g.SupervisorKey, g.StopKey = slices.Clone(key), slices.Clone(key)
			if g.State == "assigned" {
				g.State = "ready"
			}
			return nil
		}
		if (g.State != "intent" && g.State != "ready") || g.ExecutionRevoked || g.Prepared == nil || !g.PreparationStopped || g.PodUID == "" || g.PVCUID == "" || g.NodeID == "" || len(key) != ed25519.PublicKeySize {
			return ErrConflict
		}
		if len(g.SupervisorKey) != 0 && !slices.Equal(g.SupervisorKey, key) {
			return ErrConflict
		}
		if len(g.StopKey) != 0 && !slices.Equal(g.StopKey, key) {
			return ErrConflict
		}
		g.SupervisorKey = slices.Clone(key)
		g.State = "ready"
		return nil
	})
}
func (s *Store) Offer(id string) (TaskGrant, bool, error) {
	var g TaskGrant
	offered := false
	err := s.updateGrant(id, func(st *registry, current *TaskGrant) error {
		g = *current
		if current.State != "ready" || current.ExecutionRevoked || current.WorkerSessionID != "" && (!currentSessionTurn(st, *current) || !current.TurnAccepted) {
			return nil
		}
		current.State = "offered"
		g = *current
		offered = true
		return nil
	})
	return g, offered, err
}
func (s *Store) BeginStart(id string) error  { return s.transition(id, "offered", "starting") }
func (s *Store) MarkStarted(id string) error { return s.transition(id, "starting", "started") }
func (s *Store) transition(id, from, to string) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.State != from || g.ExecutionRevoked || g.WorkerSessionID != "" && (!currentSessionTurn(st, *g) || !g.TurnAccepted || to == "starting" && !turnBeforeDeadline(*g, s.now())) {
			return ErrConflict
		}
		g.State = to
		if to == "started" {
			g.StartConfirmed = true
			if g.WorkerSessionID != "" {
				session := st.Sessions[g.WorkerSessionID]
				session.State, session.Revision = SessionRunning, session.Revision+1
				st.Sessions[session.ID] = session
			}
		}
		return nil
	})
}
func (s *Store) Quarantine(id string) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.State == "closed" || g.State == "waiting_storage" {
			return ErrConflict
		}
		storage := st.Storages[g.StorageID]
		storage.Quarantined = true
		st.Storages[g.StorageID] = storage
		g.State = "quarantined"
		if g.WorkerSessionID != "" {
			session := st.Sessions[g.WorkerSessionID]
			requestSessionStop(st, &session, "turn_unproven", s.now())
			session.State = SessionQuarantined
			g.ExecutionRevoked, g.Stop = true, st.Grants[id].Stop
			st.Sessions[session.ID] = session
		}
		return nil
	})
}

// MarkCleaned records successful cleanup of the exact journaled Pod and Secret.
// Callers may retry cleanup after a crash; this does not delete or release data.
func (s *Store) MarkCleaned(id string) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.WorkerSessionID != "" {
			return ErrConflict
		}
		if g.State == "closed" && g.Stop != nil && g.Stop.Evidence != nil {
			g.CleanupComplete = true
			return nil
		}
		terminal, ok := st.Terminals[id]
		if g.State != "closed" || !ok || terminal.Source != "worker" || terminal.Seal == nil || (terminal.State != "delivered" && terminal.State != "rejected") {
			return ErrConflict
		}
		g.CleanupComplete = true
		return nil
	})
}

func publishResume(st *registry, g *TaskGrant, session, workdir string) error {
	if workdir != "" && workdir != g.TaskRoot+"/workdir" {
		return invalid("session workdir")
	}
	if session != "" {
		if !validOpaque(session) || g.Prepared == nil {
			return invalid("session")
		}
		if err := ValidateSession(*g.Prepared, session); err != nil {
			return err
		}
		g.ResumeSession = session
		storage := st.Storages[g.StorageID]
		storage.SessionID = session
		st.Storages[g.StorageID] = storage
	}
	if workdir != "" {
		g.ResumeWorkDir = workdir
	}
	return nil
}

func validOpaque(value string) bool {
	return value != "" && len(value) <= 8192 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func compactJSON(raw []byte) json.RawMessage {
	var b bytes.Buffer
	if json.Compact(&b, raw) != nil {
		return nil
	}
	return b.Bytes()
}
func (s *Store) SetBootstrap(id string, raw json.RawMessage) error {
	if !json.Valid(raw) || int64(len(raw)) > s.options.MaxRecordBytes {
		return invalid("worker bootstrap")
	}
	raw = compactJSON(raw)
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if g.State != "intent" || g.ExecutionRevoked || g.WorkerSessionID == "" && g.PodUID != "" {
			return ErrConflict
		}
		if len(g.Bootstrap) > 0 && !slices.Equal(g.Bootstrap, raw) {
			return ErrConflict
		}
		g.Bootstrap = raw
		g.BootstrapDigest = digest(raw)
		return nil
	})
}

func currentSessionTurn(st *registry, g TaskGrant) bool {
	session, ok := st.Sessions[g.WorkerSessionID]
	return ok && session.ActiveAttempt == g.AttemptID && session.TurnSequence == g.TurnSequence && session.StorageID == g.StorageID &&
		session.PodUID == g.PodUID && session.PVCUID == g.PVCUID && session.Conversation == g.Conversation && !g.TurnComplete
}

func preparedMatchesConversation(p Prepared, g TaskGrant) bool {
	if g.WorkerSessionID == "" {
		return p.Conversation == nil && p.WorkspaceAnchorTaskID == ""
	}
	return p.Conversation != nil && *p.Conversation == g.Conversation && p.WorkspaceAnchorTaskID == g.WorkspaceAnchorTaskID
}

// SetResources journals controller-owned Kubernetes creation intent and observed
// identities. The composition owner must preserve immutable resource identity.
func (s *Store) SetResources(id string, raw json.RawMessage) error {
	if !json.Valid(raw) || int64(len(raw)) > s.options.MaxRecordBytes {
		return invalid("resource journal")
	}
	raw = compactJSON(raw)
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if g.State != "intent" && !(g.Stop != nil && g.State != "closed") {
			return ErrConflict
		}
		if !preservesResourceCreation(g.Resources, raw, g.Stop != nil) {
			return ErrConflict
		}
		g.Resources = raw
		return nil
	})
}
