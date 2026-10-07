package workspace

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"slices"
	"time"
)

func validTurnReceipt(g TaskGrant, terminal Terminal, receipt TurnReceipt) bool {
	return g.WorkerSessionID != "" && receipt.WorkerSessionID == g.WorkerSessionID && receipt.TaskID == g.TaskID &&
		receipt.AttemptID == g.AttemptID && receipt.Generation == g.Generation && receipt.TurnSequence == g.TurnSequence &&
		receipt.InputDigest == g.InputDigest && fingerprint(g.InputDigest) && receipt.PodUID == g.PodUID && receipt.PVCUID == g.PVCUID &&
		receipt.ResultDigest == terminal.ResultDigest && receipt.RequestDigest == terminal.RequestDigest && receipt.Nonce == terminal.Nonce &&
		receipt.WritersStopped && receipt.FlushOK && len(g.SupervisorKey) == ed25519.PublicKeySize &&
		ed25519.Verify(g.SupervisorKey, TurnReceiptMessage(receipt), receipt.Signature)
}

func (s *Store) RecordTurnReceipt(id string, receipt TurnReceipt) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if st.Sessions[g.WorkerSessionID].ProtocolVersion != LegacySessionProtocolVersion {
			return ErrConflict
		}
		terminal, ok := st.Terminals[id]
		if !ok || terminal.Source != "worker" || terminal.RecoveryFailure != nil || !validTurnReceipt(*g, terminal, receipt) {
			return ErrConflict
		}
		if g.TurnReceipt != nil {
			if !sameJSON(*g.TurnReceipt, receipt) {
				return ErrConflict
			}
			return nil
		}
		session := st.Sessions[g.WorkerSessionID]
		if g.State != "terminal_received" || session.ActiveAttempt != id || session.TurnSequence != g.TurnSequence ||
			!g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil || !g.PreparationStopped {
			return ErrConflict
		}
		receipt.Signature = slices.Clone(receipt.Signature)
		g.TurnReceipt = &receipt
		g.ExecutionRevoked = true
		if session.Stop == nil {
			session.State = SessionFinishing
		}
		st.Sessions[session.ID] = session
		return nil
	})
}

func validTurnExecutionReceipt(g TaskGrant, terminal Terminal, receipt TurnExecutionReceipt) bool {
	return g.WorkerSessionID != "" && g.Conversation.Valid() && receipt.WorkerSessionID == g.WorkerSessionID &&
		receipt.Conversation == g.Conversation && receipt.StorageID == g.StorageID && receipt.TaskID == g.TaskID &&
		receipt.AttemptID == g.AttemptID && receipt.Generation == g.Generation && receipt.TurnSequence == g.TurnSequence &&
		receipt.InputDigest == g.InputDigest && fingerprint(g.InputDigest) && receipt.PodUID == g.PodUID && receipt.PVCUID == g.PVCUID &&
		receipt.ResultDigest == terminal.ResultDigest && receipt.RequestDigest == terminal.RequestDigest && receipt.Nonce == terminal.Nonce &&
		receipt.TaskProcessesStopped && receipt.LocalRequestsClosed && receipt.PrivateStateCleared &&
		len(g.SupervisorKey) == ed25519.PublicKeySize && ed25519.Verify(g.SupervisorKey, TurnExecutionReceiptMessage(receipt), receipt.Signature)
}

func validStoredTurnExecutionProof(session WorkerSession, g TaskGrant, receipt TurnExecutionReceipt, proof SessionProof) bool {
	body, _ := json.Marshal(receipt)
	return session.ProtocolVersion == SessionProtocolVersion && proof.Operation == SessionOperationTurnExecutionReceipt &&
		proof.BodyDigest == digest(body) && proof.WorkerSessionID == session.ID && proof.PodUID == g.PodUID && proof.PVCUID == g.PVCUID &&
		proof.TurnSequence == g.TurnSequence && proof.AttemptID == g.AttemptID && canonicalUUID(proof.Nonce) && !proof.ExpiresAt.IsZero() &&
		len(session.SupervisorKey) == ed25519.PublicKeySize && ed25519.Verify(session.SupervisorKey, SessionProofMessage(proof), proof.Signature)
}

// RecordTurnExecutionReceipt commits the observed task stop and its fresh
// challenge together. Only an exact accepted retry survives challenge expiry.
func (s *Store) RecordTurnExecutionReceipt(id string, receipt TurnExecutionReceipt, proof SessionProof) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		session, ok := st.Sessions[g.WorkerSessionID]
		terminal, terminalOK := st.Terminals[id]
		if !ok || session.ProtocolVersion != SessionProtocolVersion || !terminalOK || terminal.Source != "worker" ||
			terminal.RecoveryFailure != nil || !validTurnExecutionReceipt(*g, terminal, receipt) {
			return ErrConflict
		}
		if g.TurnExecutionReceipt != nil {
			if !sameJSON(*g.TurnExecutionReceipt, receipt) || g.TurnExecutionProof == nil || !sameJSON(*g.TurnExecutionProof, proof) {
				return ErrConflict
			}
			return nil
		}
		body, _ := json.Marshal(receipt)
		if !s.validSessionProof(session, proof, SessionOperationTurnExecutionReceipt, s.now()) || proof.BodyDigest != digest(body) {
			return ErrUnauthorized
		}
		if g.State != "terminal_received" || session.ActiveAttempt != id || session.TurnSequence != g.TurnSequence ||
			!g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil || !g.PreparationStopped {
			return ErrConflict
		}
		receipt.Signature = slices.Clone(receipt.Signature)
		g.TurnExecutionReceipt, g.TurnExecutionProof = &receipt, cloneProof(&proof)
		g.ExecutionRevoked = true
		if session.Stop == nil {
			session.State = SessionFinishing
		}
		st.Sessions[session.ID] = session
		return nil
	})
}

func turnExecutionSettled(st *registry, g TaskGrant, terminal Terminal) bool {
	session := st.Sessions[g.WorkerSessionID]
	if session.ProtocolVersion == LegacySessionProtocolVersion {
		return g.TurnReceipt != nil && validTurnReceipt(g, terminal, *g.TurnReceipt)
	}
	return session.ProtocolVersion == SessionProtocolVersion && g.TurnExecutionReceipt != nil && g.TurnExecutionProof != nil &&
		validTurnExecutionReceipt(g, terminal, *g.TurnExecutionReceipt) &&
		validStoredTurnExecutionProof(session, g, *g.TurnExecutionReceipt, *g.TurnExecutionProof)
}

func witnessMatches(g TaskGrant, terminal Terminal, witness CompletionWitness) bool {
	status := map[string]string{"complete": "completed", "fail": "failed", "cancel-ack": "cancelled"}[terminal.Kind]
	if g.PendingResume == nil || witness.TaskID != g.TaskID || witness.AttemptID != g.AttemptID || witness.WorkerSessionID != g.WorkerSessionID ||
		witness.StorageID != g.StorageID || witness.TurnSequence != g.TurnSequence || witness.RequestDigest != terminal.RequestDigest ||
		witness.ResultDigest != terminal.ResultDigest || witness.Status != status || witness.AcknowledgedAt.IsZero() {
		return false
	}
	sessionID := effectiveSession(*g.PendingResume)
	if terminal.Kind == "fail" && !g.PendingResume.ResumeRejectedTransient {
		// Failed task observations do not expose native pointer side effects.
		sessionID = ""
	}
	return witness.SessionID == sessionID && witness.WorkDir == g.PendingResume.WorkDir
}

// RecordCompletionWitness follows the controller's exact comparison with the
// backend task result. Delivery settlement alone never creates this witness.
func (s *Store) RecordCompletionWitness(id string, witness CompletionWitness) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		terminal, ok := st.Terminals[id]
		if !ok || g.WorkerSessionID == "" || terminal.Source != "worker" || terminal.State == "rejected" || terminal.RecoveryFailure != nil ||
			terminal.ResultReceipt == nil || !validResultReceipt(*g, terminal, *terminal.ResultReceipt) || !witnessMatches(*g, terminal, witness) {
			return ErrConflict
		}
		if g.CompletionWitness != nil {
			// Re-observation may have a later wall time. Identity and accepted bytes stay fixed.
			original := *g.CompletionWitness
			witness.AcknowledgedAt = original.AcknowledgedAt
			if !sameJSON(original, witness) {
				return ErrConflict
			}
			return nil
		}
		terminal.State = "delivered"
		st.Terminals[id] = terminal
		g.CompletionWitness = clonePointer(&witness)
		return nil
	})
}

func effectiveSession(resume ResumePointers) string {
	if resume.SessionRolloutMissing || resume.SessionID == resume.RetiredSessionID {
		return ""
	}
	return resume.SessionID
}

// CompleteTurn ends the current task after independent result acceptance,
// event settlement and execution evidence. V2 keeps its dirty resident lease.
func (s *Store) CompleteTurn(id string) error {
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if g.TurnComplete {
			return recoverClosedCheckpoint(st, *g, s.now())
		}
		session, ok := st.Sessions[g.WorkerSessionID]
		terminal := st.Terminals[id]
		if !ok || session.ActiveAttempt != id || session.TurnSequence != g.TurnSequence ||
			!turnExecutionSettled(st, *g, terminal) || terminal.State != "delivered" || terminal.RecoveryFailure != nil ||
			g.CompletionWitness == nil || !witnessMatches(*g, terminal, *g.CompletionWitness) || !eventsSettled(*g) ||
			!g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil || g.PreparationStarted && !g.PreparationStopped {
			return ErrConflict
		}
		storage := st.Storages[g.StorageID]
		if storage.ActiveAttempt != id || storage.WriterSessionID != session.ID {
			return ErrConflict
		}
		now := s.now()
		eligible := g.Selection != nil && g.Selection.WorkspaceReuseEligible && checkpointOutcome(*g, terminal)
		storage.ActiveAttempt, storage.Dirty, storage.Quarantined = "", session.ProtocolVersion == SessionProtocolVersion, false
		storage.Checkpoint = nil
		storage.SessionID = ""
		if eligible && session.ProtocolVersion == LegacySessionProtocolVersion {
			storage.Checkpoint = completedCheckpoint(st, *g, storage, now, false)
			storage.SessionID = storage.Checkpoint.SessionID
		}
		g.TurnComplete, g.ExecutionRevoked, g.State = true, true, "closed"
		session.ActiveAttempt, session.LastCompleteAttempt = "", id
		session.Revision++
		if session.ProtocolVersion == SessionProtocolVersion && g.Stop == nil && session.Stop == nil && session.Retention > 0 && !now.Before(session.LastObservedAt) {
			session.State, session.IdleSince, session.IdleDeadline = SessionIdle, now, now.Add(session.Retention)
			session.LastObservedAt = now
		} else {
			requestSessionStop(st, &session, "retention_disabled", now)
		}
		st.Storages[storage.ID], st.Sessions[session.ID] = storage, session
		return nil
	})
}

func completedCheckpoint(st *registry, g TaskGrant, storage Storage, now time.Time, revalidate bool) *SessionCheckpoint {
	checkpoint := &SessionCheckpoint{Source: CheckpointSource{g.TaskID, g.AttemptID}, WorkerSessionID: g.WorkerSessionID,
		TurnSequence: g.TurnSequence, CompatibilityDigest: g.CompatibilityDigest, WorkspaceCompatibilityDigest: g.WorkspaceCompatibilityDigest,
		WorkDir: g.TaskRoot + "/workdir", PublishedAt: now}
	nativeEligible := g.Selection != nil && g.Selection.ReuseEligible && nativeCheckpointOutcome(g, st.Terminals[g.AttemptID])
	if session := g.CompletionWitness.SessionID; nativeEligible && session != "" && !slices.Contains(storage.RetiredSessions, session) &&
		(!revalidate || g.Prepared != nil && ValidateSession(*g.Prepared, session) == nil) {
		checkpoint.SessionID, checkpoint.SessionSource = session, checkpoint.Source
	} else if nativeEligible && g.CompletionWitness.SessionID == "" && g.Selection.Mode == SelectionResume && !slices.Contains(storage.RetiredSessions, g.Selection.SessionID) {
		selected, err := selectedWitness(st, g.Selection.SessionSource, g.StorageID)
		sourceGrant := st.Grants[g.Selection.SessionSource.AttemptID]
		if err == nil && sourceGrant.Selection != nil && sourceGrant.Selection.ReuseEligible && sourceGrant.CompatibilityDigest == g.CompatibilityDigest &&
			selected.SessionID == g.Selection.SessionID && g.Prepared != nil && ValidateSession(*g.Prepared, selected.SessionID) == nil {
			checkpoint.SessionID, checkpoint.SessionSource = selected.SessionID, g.Selection.SessionSource
		}
	}
	if g.CompletionWitness.WorkDir == checkpoint.WorkDir {
		checkpoint.WorkspaceSource = checkpoint.Source
	} else if selected, err := selectedWitness(st, g.Selection.WorkspaceSource, g.StorageID); err == nil && selected.WorkDir == checkpoint.WorkDir {
		checkpoint.WorkspaceSource = g.Selection.WorkspaceSource
	}
	return checkpoint
}

// A late backend acceptance or final receipt can prove a closed root reusable.
// This transition never reopens compute or moves the conversation's current root.
func recoverClosedCheckpoint(st *registry, g TaskGrant, now time.Time) error {
	session, ok := st.Sessions[g.WorkerSessionID]
	if !ok || session.State != SessionClosed || g.Selection == nil || !g.Selection.WorkspaceReuseEligible {
		return nil
	}
	storage := st.Storages[g.StorageID]
	if storage.LatestWriter != (CheckpointSource{g.TaskID, g.AttemptID}) || storage.ActiveAttempt != "" || storage.WriterSessionID != "" ||
		storage.Checkpoint != nil && storage.Checkpoint.Source == storage.LatestWriter && !storage.Dirty {
		return nil
	}
	terminal := st.Terminals[g.AttemptID]
	if !checkpointOutcome(g, terminal) {
		return nil
	}
	if g.CompletionWitness == nil || !witnessMatches(g, terminal, *g.CompletionWitness) || terminal.State != "delivered" || terminal.ResultReceipt == nil ||
		terminal.RecoveryFailure != nil || !eventsSettled(g) || !g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil ||
		g.Prepared == nil || !g.PreparationStopped || !sessionStoppedClean(st, session) || session.Stop == nil || session.Stop.Evidence == nil ||
		!validSessionStopEvidence(st, session, *session.Stop.Evidence) {
		return ErrConflict
	}
	storage.Checkpoint = completedCheckpoint(st, g, storage, now, true)
	if session.ProtocolVersion == SessionProtocolVersion {
		storage.Checkpoint.SealingSessionID, storage.Checkpoint.SealedAt = session.ID, session.Stop.Evidence.ObservedAt
	}
	storage.SessionID, storage.Dirty, storage.Quarantined = storage.Checkpoint.SessionID, false, false
	st.Storages[storage.ID] = storage
	return nil
}

func checkpointOutcome(g TaskGrant, terminal Terminal) bool {
	return terminal.Kind == "complete" || terminal.Kind == "fail" || terminal.Kind == "cancel-ack"
}

func nativeCheckpointOutcome(g TaskGrant, terminal Terminal) bool {
	return terminal.Kind == "complete" || terminal.Kind == "fail" && g.PendingResume != nil && g.PendingResume.ResumeRejectedTransient
}

func validSessionStopReceipt(session WorkerSession, receipt SessionStopReceipt) bool {
	return session.Stop != nil && receipt.WorkerSessionID == session.ID && receipt.AttemptID == session.LastAttemptID &&
		receipt.InputDigest == session.InputDigest && receipt.TurnSequence == session.TurnSequence && receipt.PodUID == session.PodUID &&
		receipt.PVCUID == session.PVCUID && receipt.Revision == session.Stop.Revision && receipt.Nonce == session.Stop.Nonce &&
		len(session.SupervisorKey) == ed25519.PublicKeySize && ed25519.Verify(session.SupervisorKey, SessionStopReceiptMessage(receipt), receipt.Signature)
}

func sessionWritersFenced(st *registry, session WorkerSession) bool {
	id := session.ActiveAttempt
	if id == "" && session.Stop != nil {
		id = session.Stop.ActiveAttempt
	}
	if id == "" {
		return true
	}
	g := st.Grants[id]
	return g.CheckoutClosed && !g.CheckoutNeedsFlush && g.CheckoutProcess == nil && (!g.PreparationStarted || g.PreparationStopped)
}

func (s *Store) RecordSessionStopReceipt(id string, receipt SessionStopReceipt, proof SessionProof) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || !validSessionStopReceipt(session, receipt) {
			return ErrConflict
		}
		if session.Stop.Receipt != nil {
			if !sameJSON(*session.Stop.Receipt, receipt) || session.Stop.ReceiptProof == nil || !sameJSON(*session.Stop.ReceiptProof, proof) {
				return ErrConflict
			}
			return nil
		}
		body, _ := json.Marshal(receipt)
		if !s.validSessionProof(session, proof, SessionOperationStopReceipt, s.now()) || proof.BodyDigest != digest(body) ||
			receipt.WritersStopped && receipt.FlushOK && (!session.Stop.ControllerWritersStopped || !sessionWritersFenced(st, session)) {
			return ErrUnauthorized
		}
		receipt.Signature = slices.Clone(receipt.Signature)
		session.Stop.Receipt, session.Stop.ReceiptProof = &receipt, cloneProof(&proof)
		st.Sessions[id] = session
		return nil
	})
}

func validSessionStopEvidence(st *registry, session WorkerSession, evidence StopEvidence) bool {
	if evidence.ObservedAt.IsZero() || evidence.PodUID != session.PodUID || evidence.PVCUID != session.PVCUID {
		return false
	}
	active := session.ActiveAttempt
	if active == "" && session.Stop != nil {
		active = session.Stop.ActiveAttempt
	}
	if active != "" {
		g := st.Grants[active]
		if g.CheckoutProcess != nil || g.PreparationStarted && !g.PreparationStopped {
			return false
		}
		if evidence.PreparedFlushOK && (g.Prepared == nil || !g.PreparationStopped) {
			return false
		}
	}
	switch evidence.Kind {
	case "terminated":
		return session.PodUID != "" && !evidence.PreparedFlushOK
	case "never-started":
		return session.PodUID != "" && len(session.SupervisorKey) == 0
	case "no-worker":
		if session.PodUID != "" || len(session.SupervisorKey) != 0 {
			return false
		}
		if len(session.Resources) == 0 {
			return true
		}
		var resources resourceCreation
		return json.Unmarshal(session.Resources, &resources) == nil && resources.PodCreateRequested != nil && !*resources.PodCreateRequested && resources.Reference.PodUID == ""
	}
	return false
}

func (s *Store) ObserveSessionStop(id string, evidence StopEvidence) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || session.Stop == nil || !validSessionStopEvidence(st, session, evidence) {
			return ErrConflict
		}
		if session.Stop.Evidence != nil {
			previous := *session.Stop.Evidence
			evidence.ObservedAt = previous.ObservedAt
			if !sameJSON(previous, evidence) {
				return ErrConflict
			}
			return nil
		}
		session.Stop.Evidence = clonePointer(&evidence)
		if session.ActiveAttempt != "" {
			g := st.Grants[session.ActiveAttempt]
			if g.Stop != nil {
				g.Stop.Evidence = clonePointer(&evidence)
				st.Grants[g.AttemptID] = g
			}
		}
		st.Sessions[id] = session
		return nil
	})
}

func sessionStoppedClean(st *registry, session WorkerSession) bool {
	if session.Stop == nil || session.Stop.Evidence == nil {
		return false
	}
	if receipt := session.Stop.Receipt; receipt != nil {
		return receipt.WritersStopped && receipt.FlushOK && session.Stop.ControllerWritersStopped && sessionWritersFenced(st, session)
	}
	if session.Stop.Evidence.Kind == "no-worker" || session.Stop.Evidence.Kind == "never-started" {
		if session.ActiveAttempt == "" {
			return true
		}
		g := st.Grants[session.ActiveAttempt]
		return !g.PreparationStarted || g.Prepared != nil && g.PreparationStopped && session.Stop.Evidence.PreparedFlushOK
	}
	return false
}

// RecordSessionControllerFlush records a completed controller-local flush after
// every controller writer stopped. Default false fields never imply this proof.
func (s *Store) RecordSessionControllerFlush(id string) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || session.Stop == nil || !sessionWritersFenced(st, session) {
			return ErrConflict
		}
		session.Stop.ControllerWritersStopped = true
		st.Sessions[id] = session
		return nil
	})
}

// CloseSession releases the incarnation's writer lease after positive final
// termination evidence. A missing Pod never satisfies this transition.
func (s *Store) CloseSession(id string) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || session.Stop == nil || session.Stop.Evidence == nil || !validSessionStopEvidence(st, session, *session.Stop.Evidence) {
			return ErrConflict
		}
		if session.State == SessionClosed {
			return nil
		}
		storage := st.Storages[session.StorageID]
		if storage.WriterSessionID != id {
			return ErrConflict
		}
		clean := sessionStoppedClean(st, session)
		if session.ActiveAttempt != "" {
			g := st.Grants[session.ActiveAttempt]
			terminal := st.Terminals[g.AttemptID]
			if terminal.Source == "worker" && terminal.State == "received" && terminal.RecoveryFailure == nil && terminal.ResultReceipt == nil {
				return ErrConflict
			}
			g.State, g.TurnComplete, g.ExecutionRevoked = "closed", true, true
			st.Grants[g.AttemptID] = g
			storage.ActiveAttempt = ""
			storage.Checkpoint, storage.SessionID = nil, ""
		}
		storage.WriterSessionID, storage.Quarantined = "", false
		storage.Dirty = !clean
		if !clean {
			storage.Checkpoint, storage.SessionID = nil, ""
		}
		session.ActiveAttempt, session.State = "", SessionClosed
		session.Revision++
		st.Storages[storage.ID], st.Sessions[id] = storage, session
		latest := st.Grants[storage.LatestWriter.AttemptID]
		if clean && latest.CompletionWitness != nil && latest.Selection != nil && latest.Selection.WorkspaceReuseEligible &&
			latest.Prepared != nil && latest.PreparationStopped && eventsSettled(latest) && st.Terminals[latest.AttemptID].State == "delivered" {
			// Closure and capacity release do not depend on producer acceptance.
			// Failed lineage leaves physically clean storage without a checkpoint.
			if err := recoverClosedCheckpoint(st, latest, s.now()); err != nil && !errors.Is(err, ErrConflict) {
				return err
			}
		}
		return nil
	})
}

func (s *Store) MarkSessionCleaned(id string) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || session.State != SessionClosed || session.Stop == nil || session.Stop.Evidence == nil {
			return ErrConflict
		}
		session.ResourcesCleaned = true
		st.Sessions[id] = session
		return nil
	})
}

func (s *Store) QuarantineSession(id string) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || session.State == SessionClosed {
			return ErrConflict
		}
		requestSessionStop(st, &session, "session_unproven", s.now())
		session.State = SessionQuarantined
		storage := st.Storages[session.StorageID]
		storage.Quarantined = true
		st.Storages[storage.ID], st.Sessions[id] = storage, session
		return nil
	})
}

// ExpireSession arbitrates with ReserveTurn under the same journal lock. A
// retention change may shorten an existing deadline but can never extend it.
func (s *Store) ExpireSession(id string, retention time.Duration) (bool, error) {
	if retention < 0 {
		return false, invalid("session retention")
	}
	stopped := false
	err := s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok {
			return ErrUnauthorized
		}
		if session.State != SessionIdle {
			return nil
		}
		now := s.now()
		if shorter := session.IdleSince.Add(retention); shorter.Before(session.IdleDeadline) {
			session.IdleDeadline = shorter
		}
		if now.Before(session.LastObservedAt) || !now.Before(session.IdleDeadline) {
			requestSessionStop(st, &session, "idle_expired", now)
			stopped = true
		} else {
			session.LastObservedAt = now
		}
		st.Sessions[id] = session
		return nil
	})
	return stopped, err
}

// DrainIdleSession consumes a controller timer only for the exact observed Idle
// revision. A stale monotonic timer cannot stop a reservation or a later turn.
func (s *Store) DrainIdleSession(id string, revision uint64, reason string) (bool, error) {
	if revision == 0 || !validOpaque(reason) || len(reason) > 256 {
		return false, invalid("idle timer")
	}
	drained := false
	err := s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok {
			return ErrUnauthorized
		}
		if session.Revision != revision || session.State != SessionIdle {
			return nil
		}
		requestSessionStop(st, &session, reason, s.now())
		st.Sessions[id] = session
		drained = true
		return nil
	})
	return drained, err
}
