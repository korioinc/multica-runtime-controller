package workspace

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ReserveTurn persists the sequence and exclusive writer before preparation.
// The private journal supplies paths; backend hints are comparisons only.
func (s *Store) ReserveTurn(id string, selection Selection, compatibilityDigest string, retention time.Duration, residentLimit int) (TaskGrant, WorkerSession, error) {
	if !selection.Conversation.Valid() || !fingerprint(compatibilityDigest) || retention < 0 || residentLimit < 1 {
		return TaskGrant{}, WorkerSession{}, invalid("turn reservation")
	}
	if selection.Mode != SelectionResume && selection.Mode != SelectionFreshSession && selection.Mode != SelectionFreshWorkspace {
		return TaskGrant{}, WorkerSession{}, invalid("continuity selection")
	}
	if selection.EvidenceDigest != "" && !fingerprint(selection.EvidenceDigest) {
		return TaskGrant{}, WorkerSession{}, invalid("selection evidence")
	}
	if !selection.WorkspaceReuseEligible && selection.Mode != SelectionFreshWorkspace || selection.Mode == SelectionResume && !selection.ReuseEligible {
		return TaskGrant{}, WorkerSession{}, invalid("unproven conversation reuse")
	}
	var grant TaskGrant
	var session WorkerSession
	var deferred error
	err := s.transaction(func(st *registry) error {
		now := s.now()
		g, ok := st.Grants[id]
		if !ok || !g.SessionProtocol || g.Legacy || selection.Conversation.OwnerID != g.OwnerID ||
			selection.Conversation.WorkspaceID != g.WorkspaceID || selection.Conversation.AgentID != g.AgentID ||
			selection.Conversation.Kind == ConversationTask && selection.Conversation.SubjectID != g.TaskID {
			return ErrConflict
		}
		if g.WorkerSessionID != "" {
			if g.Selection == nil || !sameJSON(*g.Selection, selection) || g.CompatibilityDigest != compatibilityDigest {
				return ErrConflict
			}
			grant, session = g, st.Sessions[g.WorkerSessionID]
			return nil
		}
		if g.Selection != nil && (!sameJSON(*g.Selection, selection) || g.CompatibilityDigest != compatibilityDigest) {
			return ErrConflict
		}
		if !provenCompatibility(g, selection, compatibilityDigest) || !preservesBackendSelection(g, selection) {
			return ErrConflict
		}
		if err := validateLocalWorkspaceSelection(st, g, selection); err != nil {
			return err
		}
		if g.State != "waiting_storage" || g.ExecutionRevoked {
			return ErrConflict
		}
		if st.WorkspaceClaim == "" || st.WorkspaceUID == "" {
			return errors.New("workspace volume has not been bound")
		}
		if err := s.checkPendingBudget(st, 0); err != nil {
			return err
		}
		for _, other := range st.Grants {
			if other.AttemptID == id {
				continue
			}
			if other.TaskID == g.TaskID && !attemptSettled(st, other) {
				return ErrStorageBusy
			}
			if other.WorkerSessionID == "" && other.StorageID != "" && !legacyWriterReleased(st, other) && legacyConversationConflicts(other, selection.Conversation) {
				return ErrStorageBusy
			}
			if selection.Mode == SelectionFreshWorkspace && other.Conversation == selection.Conversation &&
				other.StorageID == st.Conversations[selection.Conversation.Digest()].CurrentStorageID &&
				other.PendingResume != nil && other.PendingResume.ResumeRejectedTransient && !transientResumeSettled(st, other) &&
				!resolvedCompatibilityChanged(g, other) {
				return ErrStorageBusy
			}
		}
		key := selection.Conversation.Digest()
		conversation := st.Conversations[key]
		if conversation.Revision == 0 {
			conversation = Conversation{Key: selection.Conversation, Revision: 1}
		}
		if conversation.Key != selection.Conversation {
			return ErrConflict
		}
		var storage Storage
		if selection.Mode != SelectionFreshWorkspace {
			var err error
			if candidate := st.Storages[selection.StorageID]; candidate.WriterSessionID != "" {
				storage, session, err = selectedWarmStorage(st, g, selection, compatibilityDigest)
			} else {
				storage, err = selectedStorage(st, selection, compatibilityDigest, g.WorkspaceCompatibilityDigest)
			}
			if err != nil {
				return err
			}
		} else if selection.StorageID != "" || selection.SessionID != "" || selection.WorkDir != "" ||
			selection.SessionSource != (CheckpointSource{}) || selection.WorkspaceSource != (CheckpointSource{}) || selection.LatestWriter != (CheckpointSource{}) {
			return invalid("fresh workspace source")
		}
		if conversation.CurrentSessionID != "" {
			current := st.Sessions[conversation.CurrentSessionID]
			if current.State != SessionClosed || !current.ResourcesCleaned {
				if current.State != SessionIdle {
					return ErrStorageBusy
				}
				deadline := current.IdleDeadline
				if shorter := current.IdleSince.Add(retention); shorter.Before(deadline) {
					deadline = shorter
				}
				if now.Before(current.LastObservedAt) || retention == 0 || !now.Before(deadline) ||
					current.WorkspaceCompatibilityDigest != g.WorkspaceCompatibilityDigest || current.StorageID != storage.ID ||
					!selection.WorkspaceReuseEligible || selection.Mode == SelectionFreshWorkspace {
					reason := "conversation_replacement"
					if !now.Before(deadline) || retention == 0 {
						reason = "idle_expired"
					}
					requestSessionStop(st, &current, reason, now)
					st.Sessions[current.ID] = current
					deferred = ErrStorageBusy
					return nil
				}
				session = current
			}
		}
		if storage.ID == "" {
			var err error
			storage, err = unusedTaskStorage(st, g, selection.Conversation, compatibilityDigest)
			if err != nil {
				return err
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
			if json.Unmarshal(g.Envelope, &labels) != nil {
				return invalid("task naming input")
			}
			root, err := TaskRoot("/workspace", g.WorkspaceID, g.TaskID, labels.WorkspaceSlug, labels.IssueIdentifier)
			if err != nil {
				return err
			}
			for _, old := range st.Storages {
				if old.TaskRoot == root {
					return ErrConflict
				}
			}
			storage = Storage{ID: uuid.NewString(), Conversation: selection.Conversation, WorkspaceAnchorTaskID: g.TaskID,
				CompatibilityDigest: compatibilityDigest, WorkspaceCompatibilityDigest: g.WorkspaceCompatibilityDigest,
				WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID,
				TaskRoot: root, ScopeDigest: g.ScopeDigest, Fingerprint: g.Fingerprint, PVCName: st.WorkspaceClaim, PVCUID: st.WorkspaceUID}
		}
		if storage.ActiveAttempt != "" || storage.Quarantined || storage.WriterSessionID != "" && storage.WriterSessionID != session.ID ||
			storage.Dirty && (session.ID == "" || session.ProtocolVersion != SessionProtocolVersion || storage.WriterSessionID != session.ID) {
			return ErrStorageBusy
		}
		if session.ID == "" {
			if !admitResident(st, residentLimit, now) {
				deferred = ErrResidentCapacity
				return nil
			}
			secret := make([]byte, 32)
			if _, err := rand.Read(secret); err != nil {
				return err
			}
			sessionID := uuid.NewString()
			session = WorkerSession{ID: sessionID, ProtocolVersion: SessionProtocolVersion, Conversation: selection.Conversation, StorageID: storage.ID,
				WorkspaceAnchorTaskID: storage.WorkspaceAnchorTaskID, CompatibilityDigest: compatibilityDigest,
				WorkspaceCompatibilityDigest: g.WorkspaceCompatibilityDigest,
				RuntimeID:                    g.RuntimeID, RuntimeRef: g.RuntimeRef, Fingerprint: g.Fingerprint,
				PodName: "worker-" + key[:16] + "-" + sessionID[:12], PVCName: storage.PVCName, PVCUID: storage.PVCUID,
				TaskRoot: storage.TaskRoot, Revision: 1, State: SessionCreating, ControlToken: "mts_" + base64.RawURLEncoding.EncodeToString(secret),
				CreatedAt: now, LastObservedAt: now, Retention: retention}
		} else {
			session.State = SessionReserved
			session.Revision++
		}
		if session.TurnSequence == ^uint64(0) {
			return ErrConflict
		}
		session.TurnSequence++
		session.ActiveAttempt = id
		session.Retention = retention
		session.LastObservedAt = now
		session.IdleSince, session.IdleDeadline = time.Time{}, time.Time{}
		delete(s.controlChallenges, session.ID)
		g.Conversation, g.WorkerSessionID, g.TurnSequence = selection.Conversation, session.ID, session.TurnSequence
		g.WorkspaceAnchorTaskID, g.CompatibilityDigest = storage.WorkspaceAnchorTaskID, compatibilityDigest
		g.StorageID, g.PVCName, g.PVCUID, g.TaskRoot = storage.ID, storage.PVCName, storage.PVCUID, storage.TaskRoot
		g.PodName, g.PodUID, g.NodeID = session.PodName, session.PodUID, session.NodeID
		g.Reuse = storage.Prepared != nil
		g.Selection = clonePointer(&selection)
		g.State = "intent"
		if selection.Mode == SelectionResume {
			g.ResumeSession, g.ResumeWorkDir = selection.SessionID, storage.TaskRoot+"/workdir"
		} else if selection.Mode == SelectionFreshSession {
			g.ResumeWorkDir = storage.TaskRoot + "/workdir"
		}
		storage.ActiveAttempt, storage.WriterSessionID = id, session.ID
		storage.LatestWriter = CheckpointSource{TaskID: g.TaskID, AttemptID: g.AttemptID}
		storage.Dirty = true
		conversation.CurrentStorageID, conversation.CurrentSessionID = storage.ID, session.ID
		conversation.Revision++
		st.Conversations[key], st.Storages[storage.ID], st.Sessions[session.ID], st.Grants[id] = conversation, storage, session, g
		grant = g
		return nil
	})
	if err != nil {
		return TaskGrant{}, WorkerSession{}, err
	}
	if deferred != nil {
		return TaskGrant{}, WorkerSession{}, deferred
	}
	return cloneGrant(grant), cloneSession(session), nil
}

// An unused reservation still freezes its full task identity and native path.
// Reclaim it only when every prior owner proves preparation and workers never ran.
func unusedTaskStorage(st *registry, g TaskGrant, conversation ConversationKey, compatibility string) (Storage, error) {
	var storage Storage
	for _, candidate := range st.Storages {
		if candidate.WorkspaceID != g.WorkspaceID || candidate.TaskID != g.TaskID {
			continue
		}
		if storage.ID != "" {
			return Storage{}, ErrConflict
		}
		storage = candidate
	}
	if storage.ID == "" {
		return storage, nil
	}
	if storage.WorkspaceAnchorTaskID != g.TaskID || storage.Conversation != conversation || storage.CompatibilityDigest != compatibility ||
		storage.AgentID != g.AgentID || storage.ScopeDigest != g.ScopeDigest || storage.Fingerprint != g.Fingerprint ||
		storage.Prepared != nil || storage.Checkpoint != nil || storage.SessionID != "" || len(storage.RetiredSessions) != 0 {
		return Storage{}, ErrConflict
	}
	if storage.ActiveAttempt != "" || storage.WriterSessionID != "" || storage.Dirty || storage.Quarantined {
		return Storage{}, ErrStorageBusy
	}
	for _, prior := range st.Grants {
		if prior.StorageID != storage.ID {
			continue
		}
		session := st.Sessions[prior.WorkerSessionID]
		if prior.Legacy || !prior.SessionProtocol || prior.TaskID != g.TaskID || prior.PreparationStarted ||
			session.Stop == nil || session.Stop.Evidence == nil || session.Stop.Evidence.Kind != "no-worker" {
			return Storage{}, ErrConflict
		}
		if !attemptSettled(st, prior) || session.State != SessionClosed || !session.ResourcesCleaned {
			return Storage{}, ErrStorageBusy
		}
	}
	return storage, nil
}

func transientResumeSettled(st *registry, g TaskGrant) bool {
	storage := st.Storages[g.StorageID]
	if session, err := warmStorageSession(st, storage); err == nil {
		return session.LastCompleteAttempt == g.AttemptID
	}
	return g.TurnComplete && g.CompletionWitness != nil && attemptSettled(st, g) && !storage.Dirty && !storage.Quarantined &&
		storage.Checkpoint != nil && storage.Checkpoint.Source == storage.LatestWriter
}

// Missing backend hints or unresolved defaults do not authorize a fork after
// a transient lock rejection. A proven static configuration change may do so.
func resolvedCompatibilityChanged(current, prior TaskGrant) bool {
	return current.CompatibilityDigest != prior.CompatibilityDigest && current.Compatibility != nil &&
		current.Compatibility.ProviderOptionsResolved && current.Compatibility.Authority.ValidForReuse()
}

// RecordSelection freezes the observed backend choice while its local writer
// is still finishing. Reservation subsequently proves quiescence and ownership.
func (s *Store) RecordSelection(id string, selection Selection, compatibilityDigest string) error {
	if !selection.Conversation.Valid() || !fingerprint(compatibilityDigest) ||
		(selection.Mode != SelectionResume && selection.Mode != SelectionFreshSession && selection.Mode != SelectionFreshWorkspace) {
		return invalid("continuity selection")
	}
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if !g.SessionProtocol || g.Legacy || g.State != "waiting_storage" || g.ExecutionRevoked || g.WorkerSessionID != "" ||
			selection.Conversation.OwnerID != g.OwnerID || selection.Conversation.WorkspaceID != g.WorkspaceID || selection.Conversation.AgentID != g.AgentID ||
			selection.Conversation.Kind == ConversationTask && selection.Conversation.SubjectID != g.TaskID ||
			!selection.WorkspaceReuseEligible && selection.Mode != SelectionFreshWorkspace || selection.Mode == SelectionResume && !selection.ReuseEligible {
			return ErrConflict
		}
		if !provenCompatibility(*g, selection, compatibilityDigest) || !preservesBackendSelection(*g, selection) {
			return ErrConflict
		}
		if err := validateLocalWorkspaceSelection(st, *g, selection); err != nil {
			return err
		}
		if g.Selection != nil {
			if !sameJSON(*g.Selection, selection) || g.CompatibilityDigest != compatibilityDigest {
				return ErrConflict
			}
			return nil
		}
		if err := validateBackendSelectionSources(*st, selection); err != nil {
			return err
		}
		if selection.Mode != SelectionFreshWorkspace && st.Storages[selection.StorageID].WorkspaceCompatibilityDigest != g.WorkspaceCompatibilityDigest {
			return ErrConflict
		}
		g.Conversation, g.CompatibilityDigest, g.Selection = selection.Conversation, compatibilityDigest, clonePointer(&selection)
		return nil
	})
}

// RecordBackendSelection preserves the proven upstream choice before waiting
// for its writer. It grants no filesystem access, provider defaults, or reuse.
func (s *Store) RecordBackendSelection(id string, selection Selection) error {
	if !selection.Conversation.Valid() || (selection.Mode != SelectionResume && selection.Mode != SelectionFreshSession && selection.Mode != SelectionFreshWorkspace) {
		return invalid("backend continuity selection")
	}
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if !g.SessionProtocol || g.Legacy || g.State != "waiting_storage" || g.ExecutionRevoked || g.WorkerSessionID != "" ||
			selection.Conversation.OwnerID != g.OwnerID || selection.Conversation.WorkspaceID != g.WorkspaceID || selection.Conversation.AgentID != g.AgentID ||
			g.Conversation != (ConversationKey{}) && g.Conversation != selection.Conversation || selection.Conversation.Kind == ConversationTask && selection.Conversation.SubjectID != g.TaskID {
			return ErrConflict
		}
		if g.BackendSelection != nil {
			if sameJSON(*g.BackendSelection, selection) {
				return nil
			}
			if g.Selection != nil || !sameBackendSources(*g.BackendSelection, selection) ||
				validateBackendSelectionSources(*st, selection) != nil || selection.Mode != SelectionFreshWorkspace && st.Storages[selection.StorageID].LatestWriter != selection.LatestWriter {
				return ErrConflict
			}
			g.BackendSelection = clonePointer(&selection)
			return nil
		}
		if err := validateBackendSelectionSources(*st, selection); err != nil {
			return err
		}
		if selection.Mode != SelectionFreshWorkspace && st.Storages[selection.StorageID].LatestWriter != selection.LatestWriter {
			return ErrConflict
		}
		if g.Selection != nil && !sameSelectedSources(*g.Selection, selection) && g.Selection.Mode != SelectionFreshWorkspace {
			return ErrConflict
		}
		g.BackendSelection, g.Conversation = clonePointer(&selection), selection.Conversation
		return nil
	})
}

func sameSelectedSources(a, b Selection) bool {
	return a.Conversation == b.Conversation && a.Mode == b.Mode && a.StorageID == b.StorageID && a.SessionSource == b.SessionSource &&
		a.WorkspaceSource == b.WorkspaceSource && a.LatestWriter == b.LatestWriter && a.SessionID == b.SessionID && a.WorkDir == b.WorkDir
}

func sameBackendSources(a, b Selection) bool {
	a.LatestWriter, b.LatestWriter = CheckpointSource{}, CheckpointSource{}
	return sameSelectedSources(a, b)
}

func preservesBackendSelection(g TaskGrant, selection Selection) bool {
	if selection.Mode == SelectionFreshWorkspace {
		return g.BackendSelection == nil || g.BackendSelection.Conversation == selection.Conversation
	}
	if g.BackendSelection == nil || g.BackendSelection.Conversation != selection.Conversation {
		return false
	}
	if selection.Mode == SelectionFreshSession && selection.SessionID == "" && selection.SessionSource == (CheckpointSource{}) {
		if g.BackendSelection.Mode == SelectionFreshWorkspace {
			// The current-root policy supplies physical evidence independently.
			// It cannot add a native source to the frozen backend observation.
			return true
		}
		backend := *g.BackendSelection
		backend.Mode, backend.SessionID, backend.SessionSource = SelectionFreshSession, "", CheckpointSource{}
		return sameSelectedSources(backend, selection)
	}
	return sameSelectedSources(*g.BackendSelection, selection)
}

func validateLocalWorkspaceSelection(st *registry, g TaskGrant, selection Selection) error {
	if selection.Mode != SelectionFreshSession || g.BackendSelection == nil || g.BackendSelection.Mode != SelectionFreshWorkspace {
		return nil
	}
	storage := st.Storages[selection.StorageID]
	if storage.WriterSessionID != "" {
		_, session, err := selectedWarmStorage(st, g, selection, g.CompatibilityDigest)
		if err != nil || selection.WorkspaceSource != storage.LatestWriter || session.LastCompleteAttempt != selection.LatestWriter.AttemptID {
			return ErrConflict
		}
		return nil
	}
	if st.Conversations[selection.Conversation.Digest()].CurrentStorageID != storage.ID || storage.Checkpoint == nil ||
		selection.WorkspaceSource != storage.Checkpoint.WorkspaceSource || selection.LatestWriter != storage.Checkpoint.Source {
		return ErrConflict
	}
	_, err := selectedStorage(st, selection, g.CompatibilityDigest, g.WorkspaceCompatibilityDigest)
	return err
}

func validateBackendSelectionSources(st registry, selection Selection) error {
	if !selection.Conversation.Valid() || (selection.Mode != SelectionResume && selection.Mode != SelectionFreshSession && selection.Mode != SelectionFreshWorkspace) {
		return invalid("backend selection identity")
	}
	if selection.EvidenceDigest != "" && !fingerprint(selection.EvidenceDigest) {
		return invalid("backend selection evidence")
	}
	if selection.Mode == SelectionFreshWorkspace {
		if selection.StorageID != "" || selection.SessionID != "" || selection.WorkDir != "" || selection.SessionSource != (CheckpointSource{}) ||
			selection.WorkspaceSource != (CheckpointSource{}) || selection.LatestWriter != (CheckpointSource{}) {
			return ErrConflict
		}
		return nil
	}
	storage, ok := st.Storages[selection.StorageID]
	if !ok || storage.Conversation != selection.Conversation || selection.WorkDir != storage.TaskRoot+"/workdir" {
		return ErrConflict
	}
	sources := []CheckpointSource{selection.WorkspaceSource, selection.LatestWriter}
	if selection.Mode == SelectionResume {
		if !validOpaque(selection.SessionID) {
			return ErrConflict
		}
		sources = append(sources, selection.SessionSource)
	} else if selection.SessionID != "" || selection.SessionSource != (CheckpointSource{}) {
		return ErrConflict
	}
	for _, source := range sources {
		prior, ok := st.Grants[source.AttemptID]
		if !source.valid() || !ok || prior.TaskID != source.TaskID || prior.StorageID != storage.ID || prior.Conversation != selection.Conversation {
			return ErrConflict
		}
	}
	return nil
}

func selectedStorage(st *registry, selection Selection, compatibility, workspaceCompatibility string) (Storage, error) {
	storage, ok := st.Storages[selection.StorageID]
	if !ok || !selection.WorkspaceReuseEligible || storage.Conversation != selection.Conversation ||
		storage.WorkspaceCompatibilityDigest != workspaceCompatibility || storage.Prepared == nil {
		return Storage{}, ErrConflict
	}
	if storage.ActiveAttempt != "" || storage.WriterSessionID != "" || storage.Quarantined || storage.Dirty {
		return Storage{}, ErrStorageBusy
	}
	if storage.Checkpoint == nil || storage.Checkpoint.Source != selection.LatestWriter || storage.LatestWriter != selection.LatestWriter {
		return Storage{}, ErrConflict
	}
	producer := st.Grants[storage.Checkpoint.Source.AttemptID]
	predecessor := st.Sessions[producer.WorkerSessionID]
	if predecessor.State != SessionClosed || !sessionStoppedClean(st, predecessor) || predecessor.Stop == nil || predecessor.Stop.Evidence == nil ||
		!validSessionStopEvidence(st, predecessor, *predecessor.Stop.Evidence) {
		return Storage{}, ErrConflict
	}
	if err := selectedSourceWitnesses(st, selection, storage, compatibility); err != nil {
		return Storage{}, err
	}
	if selection.Mode == SelectionResume && storage.Checkpoint.SessionID != selection.SessionID && ValidateSession(*storage.Prepared, selection.SessionID) != nil {
		return Storage{}, ErrConflict
	}
	return storage, nil
}

// Resolve the one exact live owner before considering cold checkpoint reuse.
// Task settlement permits this owner to continue; it never releases storage.
func selectedWarmStorage(st *registry, g TaskGrant, selection Selection, compatibility string) (Storage, WorkerSession, error) {
	storage, ok := st.Storages[selection.StorageID]
	if !ok || !selection.WorkspaceReuseEligible || selection.Conversation != storage.Conversation ||
		storage.WorkspaceCompatibilityDigest != g.WorkspaceCompatibilityDigest {
		return Storage{}, WorkerSession{}, ErrConflict
	}
	if storage.ActiveAttempt != "" {
		return Storage{}, WorkerSession{}, ErrStorageBusy
	}
	if storage.LatestWriter != selection.LatestWriter {
		return Storage{}, WorkerSession{}, ErrConflict
	}
	session, err := warmStorageSession(st, storage)
	if err != nil {
		return Storage{}, WorkerSession{}, err
	}
	if g.RuntimeID != session.RuntimeID || !g.RuntimeRef.Equal(session.RuntimeRef) || g.Fingerprint != session.Fingerprint {
		return Storage{}, WorkerSession{}, ErrConflict
	}
	if err := selectedSourceWitnesses(st, selection, storage, compatibility); err != nil {
		return Storage{}, WorkerSession{}, err
	}
	return storage, session, nil
}

func warmStorageSession(st *registry, storage Storage) (WorkerSession, error) {
	session, ok := st.Sessions[storage.WriterSessionID]
	if !ok || session.ProtocolVersion != SessionProtocolVersion || session.State != SessionIdle || session.Stop != nil ||
		session.ActiveAttempt != "" || storage.ActiveAttempt != "" || !storage.Dirty || storage.Quarantined || storage.Prepared == nil ||
		storage.Checkpoint != nil || session.StorageID != storage.ID || session.Conversation != storage.Conversation ||
		session.WorkspaceCompatibilityDigest != storage.WorkspaceCompatibilityDigest || session.TaskRoot != storage.TaskRoot ||
		session.PVCUID != storage.PVCUID || session.PVCName != storage.PVCName || session.Fingerprint != storage.Fingerprint ||
		!canonicalUUID(session.PodUID) || len(session.SupervisorKey) == 0 {
		return WorkerSession{}, ErrStorageBusy
	}
	conversation := st.Conversations[storage.Conversation.Digest()]
	g, ok := st.Grants[session.LastCompleteAttempt]
	terminal := st.Terminals[g.AttemptID]
	if conversation.CurrentStorageID != storage.ID || conversation.CurrentSessionID != session.ID || !ok ||
		storage.LatestWriter != (CheckpointSource{g.TaskID, g.AttemptID}) || g.WorkerSessionID != session.ID ||
		g.TurnSequence != session.TurnSequence || g.PodUID != session.PodUID || g.PVCUID != session.PVCUID ||
		g.Conversation != session.Conversation || g.StorageID != storage.ID || !g.RuntimeRef.Equal(session.RuntimeRef) ||
		g.Selection == nil || !g.Selection.WorkspaceReuseEligible || !g.TurnComplete || !turnExecutionSettled(st, g, terminal) ||
		g.CompletionWitness == nil || !witnessMatches(g, terminal, *g.CompletionWitness) || !checkpointOutcome(g, terminal) ||
		terminal.State != "delivered" || terminal.RecoveryFailure != nil || !attemptSettled(st, g) ||
		!g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil || !g.PreparationStopped {
		return WorkerSession{}, ErrConflict
	}
	return session, nil
}

// WarmSession exposes only durably settled same-owner workspace authority.
// Controller source selection still proves the current task's compatibility.
func (s *Store) WarmSession(storageID string) (WorkerSession, error) {
	st, err := s.snapshot()
	if err != nil {
		return WorkerSession{}, err
	}
	return warmStorageSession(&st, st.Storages[storageID])
}

func selectedSourceWitnesses(st *registry, selection Selection, storage Storage, compatibility string) error {
	workspaceSource, err := selectedWitness(st, selection.WorkspaceSource, storage.ID)
	if err != nil || workspaceSource.WorkDir != selection.WorkDir || selection.WorkDir != storage.TaskRoot+"/workdir" {
		return ErrConflict
	}
	if _, err := selectedWitness(st, selection.LatestWriter, storage.ID); err != nil {
		return err
	}
	if selection.Mode == SelectionResume {
		source, err := selectedWitness(st, selection.SessionSource, storage.ID)
		sourceGrant := st.Grants[selection.SessionSource.AttemptID]
		latestGrant := st.Grants[selection.LatestWriter.AttemptID]
		if err != nil || !selection.ReuseEligible || sourceGrant.Selection == nil || !sourceGrant.Selection.ReuseEligible ||
			latestGrant.Selection == nil || !latestGrant.Selection.ReuseEligible || !nativeCheckpointOutcome(latestGrant, st.Terminals[latestGrant.AttemptID]) ||
			sourceGrant.CompatibilityDigest != compatibility || !nativeCheckpointOutcome(sourceGrant, st.Terminals[sourceGrant.AttemptID]) ||
			selection.SessionID == "" || source.SessionID != selection.SessionID || slices.Contains(storage.RetiredSessions, selection.SessionID) {
			return ErrConflict
		}
	} else if selection.SessionID != "" || selection.SessionSource != (CheckpointSource{}) {
		return ErrConflict
	}
	return nil
}

func selectedWitness(st *registry, source CheckpointSource, storageID string) (CompletionWitness, error) {
	g, ok := st.Grants[source.AttemptID]
	if !source.valid() || !ok || g.TaskID != source.TaskID || g.StorageID != storageID || g.CompletionWitness == nil || !g.TurnComplete ||
		g.Selection == nil || !g.Selection.WorkspaceReuseEligible || !checkpointOutcome(g, st.Terminals[g.AttemptID]) || !attemptSettled(st, g) {
		return CompletionWitness{}, ErrConflict
	}
	return *g.CompletionWitness, nil
}

func attemptSettled(st *registry, g TaskGrant) bool {
	if g.WorkerSessionID == "" {
		return legacyWriterReleased(st, g)
	}
	return g.TurnComplete && deliverySettled(st, g.AttemptID) && eventsSettled(g)
}

func legacyWriterReleased(st *registry, g TaskGrant) bool {
	return g.State == "closed" && g.CleanupComplete && deliverySettled(st, g.AttemptID)
}

func legacyConversationConflicts(g TaskGrant, key ConversationKey) bool {
	if g.WorkspaceID != key.WorkspaceID || g.AgentID != key.AgentID {
		return false
	}
	var scope struct {
		Kind            string `json:"kind"`
		IssueID         string `json:"issue_id"`
		ChatSessionID   string `json:"chat_session_id"`
		AutopilotRunID  string `json:"autopilot_run_id"`
		ChatChannelType string `json:"chat_channel_type"`
		ChatType        string `json:"chat_type"`
	}
	if json.Unmarshal(g.Envelope, &scope) != nil {
		return true
	}
	if canonicalUUID(scope.IssueID) && (scope.Kind == "direct" || scope.Kind == "comment") && scope.ChatSessionID == "" && scope.AutopilotRunID == "" {
		return key.Kind == ConversationIssue && key.SubjectID == scope.IssueID
	}
	if canonicalUUID(scope.ChatSessionID) && scope.Kind == "chat" && scope.IssueID == "" && scope.AutopilotRunID == "" && scope.ChatChannelType == "" && scope.ChatType == "" {
		return key.Kind == ConversationAgentDM && key.SubjectID == scope.ChatSessionID
	}
	// Unknown legacy scope conservatively fences the recorded workspace and agent.
	return true
}

func eventsSettled(g TaskGrant) bool {
	for _, event := range g.Events {
		if event.State != "delivered" && event.State != "local" {
			return false
		}
	}
	return true
}

func (s *Store) GetSession(id string) (WorkerSession, error) {
	st, err := s.snapshot()
	if err != nil {
		return WorkerSession{}, err
	}
	session, ok := st.Sessions[id]
	if !ok {
		return WorkerSession{}, ErrUnauthorized
	}
	return session, nil
}

func (s *Store) GetConversation(key ConversationKey) (Conversation, error) {
	st, err := s.snapshot()
	if err != nil {
		return Conversation{}, err
	}
	conversation, ok := st.Conversations[key.Digest()]
	if !ok || conversation.Key != key {
		return Conversation{}, ErrUnauthorized
	}
	return conversation, nil
}

func (s *Store) ListSessions() ([]WorkerSession, error) {
	st, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	all := make([]WorkerSession, 0, len(st.Sessions))
	for _, session := range st.Sessions {
		all = append(all, session)
	}
	slices.SortFunc(all, func(a, b WorkerSession) int { return strings.Compare(a.ID, b.ID) })
	return all, nil
}

func (s *Store) GetStorage(id string) (Storage, error) {
	st, err := s.snapshot()
	if err != nil {
		return Storage{}, err
	}
	storage, ok := st.Storages[id]
	if !ok {
		return Storage{}, ErrUnauthorized
	}
	return storage, nil
}

func (s *Store) ListStorages() ([]Storage, error) {
	st, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	all := make([]Storage, 0, len(st.Storages))
	for _, storage := range st.Storages {
		all = append(all, storage)
	}
	slices.SortFunc(all, func(a, b Storage) int { return strings.Compare(a.ID, b.ID) })
	return all, nil
}
