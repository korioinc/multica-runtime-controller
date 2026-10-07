package workspace

import (
	"crypto/ed25519"
	"encoding/json"
	"slices"
)

func validatePendingSessionGrant(g TaskGrant) error {
	if g.WorkerSessionID != "" || g.TurnSequence != 0 || g.WorkspaceAnchorTaskID != "" || len(g.Assignment) != 0 || g.InputDigest != "" ||
		g.TurnAccepted || g.AcceptProof != nil || g.TurnReceipt != nil || g.TurnExecutionReceipt != nil || g.TurnExecutionProof != nil || g.CompletionWitness != nil || g.PendingResume != nil || g.TurnComplete {
		return invalid("pending session authority")
	}
	if g.Selection != nil {
		if !g.SessionProtocol || g.Legacy || !g.Conversation.Valid() || g.Conversation != g.Selection.Conversation ||
			g.Conversation.OwnerID != g.OwnerID || g.Conversation.WorkspaceID != g.WorkspaceID || g.Conversation.AgentID != g.AgentID ||
			!fingerprint(g.CompatibilityDigest) || !provenCompatibility(g, *g.Selection, g.CompatibilityDigest) {
			return invalid("pending continuity selection")
		}
	} else if g.Compatibility != nil {
		if !provenCompatibility(g, Selection{Conversation: g.Conversation}, g.CompatibilityDigest) {
			return invalid("pending compatibility")
		}
	} else if g.BackendSelection != nil {
		if !g.SessionProtocol || g.Legacy || g.Conversation != g.BackendSelection.Conversation || !g.Conversation.Valid() || g.CompatibilityDigest != "" || g.WorkspaceCompatibilityDigest != "" {
			return invalid("pending backend selection")
		}
	} else if g.Conversation != (ConversationKey{}) || g.CompatibilityDigest != "" || g.WorkspaceCompatibilityDigest != "" {
		return invalid("missing compatibility evidence")
	}
	return nil
}

func (s *Store) validateSessionGrant(st registry, g TaskGrant) error {
	session, ok := st.Sessions[g.WorkerSessionID]
	storage, storageOK := st.Storages[g.StorageID]
	if !ok || !storageOK || !g.SessionProtocol || g.Legacy || !g.Conversation.Valid() || g.Conversation.OwnerID != g.OwnerID ||
		g.Conversation.WorkspaceID != g.WorkspaceID || g.Conversation.AgentID != g.AgentID ||
		g.Conversation != session.Conversation || g.Conversation != storage.Conversation || g.TurnSequence == 0 || g.TurnSequence > session.TurnSequence ||
		g.WorkspaceAnchorTaskID != storage.WorkspaceAnchorTaskID || g.WorkspaceAnchorTaskID != session.WorkspaceAnchorTaskID ||
		g.WorkspaceCompatibilityDigest != storage.WorkspaceCompatibilityDigest || g.WorkspaceCompatibilityDigest != session.WorkspaceCompatibilityDigest ||
		g.TurnSequence == 1 && g.CompatibilityDigest != session.CompatibilityDigest ||
		g.TaskRoot != storage.TaskRoot || g.PVCName != storage.PVCName || g.PVCUID != storage.PVCUID ||
		g.PodName != session.PodName || g.PodUID != session.PodUID || g.NodeID != session.NodeID || g.RuntimeID != session.RuntimeID ||
		g.Fingerprint != session.Fingerprint || g.CleanupComplete || g.Selection == nil || g.Selection.Conversation != g.Conversation || !provenCompatibility(g, *g.Selection, g.CompatibilityDigest) ||
		g.Conversation.Kind == ConversationTask && g.Conversation.SubjectID != g.TaskID {
		return invalid("session grant binding")
	}
	if !preservesBackendSelection(g, *g.Selection) || g.BackendSelection != nil && validateBackendSelectionSources(st, *g.BackendSelection) != nil {
		return invalid("changed backend selection")
	}
	if len(g.SupervisorKey) != 0 && !slices.Equal(g.SupervisorKey, session.SupervisorKey) ||
		len(g.StopKey) != 0 && !slices.Equal(g.StopKey, session.SupervisorKey) {
		return invalid("session supervisor key")
	}
	if g.Prepared != nil {
		p := *g.Prepared
		if ValidatePrepared(p) != nil || !preparedMatchesConversation(p, g) || p.OwnerID != g.OwnerID || p.WorkspaceID != g.WorkspaceID ||
			p.TaskID != g.TaskID || p.AgentID != g.AgentID || p.AttemptID != g.AttemptID || p.Generation != g.Generation || p.TaskRoot != g.TaskRoot ||
			p.PVCUID != g.PVCUID || !g.PreparationStarted || !g.PreparationStopped || !json.Valid(g.Execution) || int64(len(g.Execution)) > s.options.MaxRecordBytes {
			return invalid("conversation prepared ownership")
		}
		if session.ProtocolVersion == SessionProtocolVersion {
			var run struct{ NativeMetadata *NativeMetadata }
			if p.NativeMetadata == nil || p.NativeMetadata.WorkerSessionID != session.ID || p.NativeMetadata.TurnSequence != g.TurnSequence ||
				json.Unmarshal(g.Execution, &run) != nil || run.NativeMetadata == nil || run.NativeMetadata.Digest() != p.NativeMetadata.Digest() {
				return invalid("resident preparation binding")
			}
		} else if p.NativeMetadata != nil {
			return invalid("historical resident metadata")
		}
	} else if len(g.Execution) != 0 {
		return invalid("unprepared conversation input")
	}
	if g.PreparationStopped && !g.PreparationStarted {
		return invalid("conversation preparation termination")
	}
	if len(g.Assignment) != 0 {
		inputDigest, err := TurnInputDigest(g.Assignment)
		var identity struct {
			WorkerSessionID, PodUID, InputDigest string
			TurnSequence                         uint64
			Bootstrap, Run                       json.RawMessage
		}
		if err != nil || inputDigest != g.InputDigest || !fingerprint(g.InputDigest) || int64(len(g.Assignment)) > s.options.MaxRecordBytes ||
			json.Unmarshal(g.Assignment, &identity) != nil || identity.WorkerSessionID != g.WorkerSessionID || identity.PodUID != g.PodUID ||
			identity.InputDigest != inputDigest || identity.TurnSequence != g.TurnSequence || g.Prepared == nil || len(g.SupervisorKey) != ed25519.PublicKeySize ||
			!sameJSON(identity.Bootstrap, g.Bootstrap) || !sameJSON(identity.Run, g.Execution) {
			return invalid("persisted turn assignment")
		}
		var bootstrap struct{ NativeMetadataDigest string }
		if json.Unmarshal(identity.Bootstrap, &bootstrap) != nil ||
			session.ProtocolVersion == SessionProtocolVersion && bootstrap.NativeMetadataDigest != g.Prepared.NativeMetadata.Digest() ||
			session.ProtocolVersion == LegacySessionProtocolVersion && bootstrap.NativeMetadataDigest != "" {
			return invalid("assignment native metadata binding")
		}
	} else if g.InputDigest != "" || g.TurnAccepted {
		return invalid("missing turn assignment")
	}
	if g.TurnAccepted != (g.AcceptProof != nil) {
		return invalid("turn acceptance")
	}
	if g.AcceptProof != nil {
		proof := *g.AcceptProof
		body, _ := json.Marshal(struct {
			TurnSequence uint64 `json:"turnSequence"`
			InputDigest  string `json:"inputDigest"`
		}{g.TurnSequence, g.InputDigest})
		if proof.Operation != SessionOperationAccept || proof.WorkerSessionID != session.ID || proof.PodUID != g.PodUID || proof.PVCUID != g.PVCUID ||
			proof.TurnSequence != g.TurnSequence || proof.AttemptID != g.AttemptID || proof.BodyDigest != digest(body) ||
			!canonicalUUID(proof.Nonce) || proof.ExpiresAt.IsZero() || !ed25519.Verify(session.SupervisorKey, SessionProofMessage(proof), proof.Signature) {
			return invalid("accepted session proof")
		}
	}
	terminal := st.Terminals[g.AttemptID]
	if session.ProtocolVersion == SessionProtocolVersion && g.TurnReceipt != nil || session.ProtocolVersion == LegacySessionProtocolVersion && (g.TurnExecutionReceipt != nil || g.TurnExecutionProof != nil) {
		return invalid("changed execution receipt domain")
	}
	if g.TurnReceipt != nil && (!validTurnReceipt(g, terminal, *g.TurnReceipt) || !g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil) {
		return invalid("turn quiescence")
	}
	if (g.TurnExecutionReceipt != nil) != (g.TurnExecutionProof != nil) || g.TurnExecutionReceipt != nil &&
		(!turnExecutionSettled(&st, g, terminal) || !g.CheckoutClosed || g.CheckoutNeedsFlush || g.CheckoutProcess != nil || !g.PreparationStopped) {
		return invalid("resident turn execution proof")
	}
	if g.CompletionWitness != nil && (terminal.State != "delivered" || terminal.ResultReceipt == nil || !witnessMatches(g, terminal, *g.CompletionWitness)) {
		return invalid("completion witness")
	}
	if g.PendingResume != nil && g.PendingResume.WorkDir != "" && g.PendingResume.WorkDir != g.TaskRoot+"/workdir" {
		return invalid("turn result workspace")
	}
	if g.PendingResume != nil && !validTransientResumeResult(g, terminal.Kind, *g.PendingResume) {
		return invalid("transient resume rejection")
	}
	if g.TurnComplete {
		if g.State != "closed" || !g.ExecutionRevoked || !(turnExecutionSettled(&st, g, terminal) && g.CompletionWitness != nil && eventsSettled(g)) &&
			!(session.State == SessionClosed && session.Stop != nil && session.Stop.Evidence != nil) {
			return invalid("completed turn")
		}
	} else if session.ActiveAttempt != g.AttemptID || session.TurnSequence != g.TurnSequence || storage.ActiveAttempt != g.AttemptID || storage.WriterSessionID != session.ID {
		return invalid("current turn writer")
	}
	switch g.State {
	case "intent":
	case "assigned", "ready", "offered", "starting", "started", "terminal_received":
		if g.PodUID == "" || g.NodeID == "" || len(g.SupervisorKey) != ed25519.PublicKeySize || g.Prepared == nil || len(g.Assignment) == 0 {
			return invalid("session turn admission")
		}
		if g.State == "terminal_received" && terminal.Source == "" {
			return invalid("turn terminal")
		}
	case "quarantined":
		if !storage.Quarantined || session.State != SessionQuarantined && session.State != SessionDraining {
			return invalid("turn quarantine")
		}
	case "unexecuted":
		if terminal.Source != "controller" || g.StartConfirmed {
			return invalid("unexecuted session turn")
		}
	case "closed":
		if !g.TurnComplete {
			return invalid("turn closure")
		}
	default:
		return invalid("session turn state")
	}
	return nil
}

func validateConversationStorage(st registry, storage Storage) error {
	if !storage.Conversation.Valid() || storage.Conversation.OwnerID != st.OwnerID || storage.Conversation.WorkspaceID != storage.WorkspaceID ||
		storage.Conversation.AgentID != storage.AgentID || storage.WorkspaceAnchorTaskID != storage.TaskID ||
		!fingerprint(storage.CompatibilityDigest) || !fingerprint(storage.WorkspaceCompatibilityDigest) {
		return invalid("conversation storage")
	}
	for i, session := range storage.RetiredSessions {
		if !validOpaque(session) || i > 0 && storage.RetiredSessions[i-1] >= session {
			return invalid("retired conversation session")
		}
	}
	writer, ok := st.Grants[storage.LatestWriter.AttemptID]
	if !ok || !storage.LatestWriter.valid() || writer.TaskID != storage.LatestWriter.TaskID || writer.StorageID != storage.ID || writer.Conversation != storage.Conversation {
		return invalid("latest conversation writer")
	}
	if p := storage.Prepared; p != nil {
		if ValidatePrepared(*p) != nil || p.Conversation == nil || *p.Conversation != storage.Conversation || p.WorkspaceAnchorTaskID != storage.WorkspaceAnchorTaskID ||
			p.OwnerID != st.OwnerID || p.WorkspaceID != storage.WorkspaceID || p.AgentID != storage.AgentID || p.TaskRoot != storage.TaskRoot || p.PVCUID != storage.PVCUID {
			return invalid("conversation retained preparation")
		}
	}
	if storage.WriterSessionID != "" {
		session, ok := st.Sessions[storage.WriterSessionID]
		if !ok || session.StorageID != storage.ID || session.State == SessionClosed || session.ActiveAttempt != storage.ActiveAttempt {
			return invalid("conversation writer lease")
		}
	} else if storage.ActiveAttempt != "" {
		return invalid("orphaned conversation turn")
	}
	if checkpoint := storage.Checkpoint; checkpoint != nil {
		g, ok := st.Grants[checkpoint.Source.AttemptID]
		sourceSession := st.Sessions[g.WorkerSessionID]
		storageProof := sourceSession.ProtocolVersion == LegacySessionProtocolVersion && g.TurnReceipt != nil || sourceSession.State == SessionClosed && sessionStoppedClean(&st, sourceSession)
		if !ok || g.TaskID != checkpoint.Source.TaskID || g.StorageID != storage.ID || !g.TurnComplete || g.CompletionWitness == nil ||
			g.Selection == nil || !g.Selection.WorkspaceReuseEligible || !checkpointOutcome(g, st.Terminals[g.AttemptID]) || !storageProof || checkpoint.WorkerSessionID != g.WorkerSessionID || checkpoint.TurnSequence != g.TurnSequence ||
			checkpoint.CompatibilityDigest != g.CompatibilityDigest || checkpoint.WorkspaceCompatibilityDigest != storage.WorkspaceCompatibilityDigest ||
			checkpoint.WorkspaceCompatibilityDigest != g.WorkspaceCompatibilityDigest || checkpoint.WorkDir != storage.TaskRoot+"/workdir" || checkpoint.PublishedAt.IsZero() ||
			storage.ActiveAttempt == "" && !storage.Dirty && checkpoint.Source != storage.LatestWriter || storage.SessionID != checkpoint.SessionID {
			return invalid("conversation checkpoint")
		}
		if sourceSession.ProtocolVersion == SessionProtocolVersion {
			if checkpoint.SealingSessionID != sourceSession.ID || sourceSession.Stop == nil || sourceSession.Stop.Evidence == nil ||
				!checkpoint.SealedAt.Equal(sourceSession.Stop.Evidence.ObservedAt) || sourceSession.State != SessionClosed || !sessionStoppedClean(&st, sourceSession) ||
				!validSessionStopEvidence(&st, sourceSession, *sourceSession.Stop.Evidence) {
				return invalid("resident final checkpoint seal")
			}
		} else if checkpoint.SealingSessionID != "" || !checkpoint.SealedAt.IsZero() {
			return invalid("historical checkpoint seal")
		}
		if checkpoint.SessionID != "" {
			source, err := selectedWitness(&st, checkpoint.SessionSource, storage.ID)
			sourceGrant := st.Grants[checkpoint.SessionSource.AttemptID]
			if err != nil || !g.Selection.ReuseEligible || !nativeCheckpointOutcome(g, st.Terminals[g.AttemptID]) ||
				sourceGrant.Selection == nil || !sourceGrant.Selection.ReuseEligible || sourceGrant.CompatibilityDigest != g.CompatibilityDigest ||
				source.SessionID != checkpoint.SessionID || slices.Contains(storage.RetiredSessions, checkpoint.SessionID) {
				return invalid("checkpoint session source")
			}
		} else if checkpoint.SessionSource != (CheckpointSource{}) {
			return invalid("empty checkpoint session source")
		}
		if checkpoint.WorkspaceSource != (CheckpointSource{}) {
			source, err := selectedWitness(&st, checkpoint.WorkspaceSource, storage.ID)
			if err != nil || source.WorkDir != checkpoint.WorkDir {
				return invalid("checkpoint workspace source")
			}
		}
	} else if storage.SessionID != "" {
		return invalid("unwitnessed conversation session")
	}
	return nil
}

func (s *Store) validateConversations(st registry, historicalMigration bool) error {
	live := map[string]string{}
	names := map[string]string{}
	for id, conversation := range st.Conversations {
		storage, storageOK := st.Storages[conversation.CurrentStorageID]
		session, sessionOK := st.Sessions[conversation.CurrentSessionID]
		if !conversation.Key.Valid() || conversation.Key.OwnerID != st.OwnerID || id != conversation.Key.Digest() || conversation.Revision == 0 ||
			!storageOK || !sessionOK || storage.Conversation != conversation.Key || session.Conversation != conversation.Key || session.StorageID != storage.ID {
			return invalid("conversation index")
		}
	}
	for id, session := range st.Sessions {
		storage, ok := st.Storages[session.StorageID]
		if !ok || !canonicalUUID(id) || id != session.ID || !session.Conversation.Valid() || session.Conversation != storage.Conversation ||
			(session.ProtocolVersion != SessionProtocolVersion && session.ProtocolVersion != LegacySessionProtocolVersion) ||
			session.Conversation.OwnerID != st.OwnerID || session.WorkspaceAnchorTaskID != storage.WorkspaceAnchorTaskID ||
			!fingerprint(session.CompatibilityDigest) || session.WorkspaceCompatibilityDigest != storage.WorkspaceCompatibilityDigest ||
			session.PVCName != storage.PVCName || session.PVCUID != storage.PVCUID ||
			session.TaskRoot != storage.TaskRoot || !canonicalUUID(session.RuntimeID) || session.RuntimeRef.Validate() != nil ||
			session.Fingerprint != RuntimeFingerprint(session.RuntimeRef) || session.Fingerprint != storage.Fingerprint || session.Revision == 0 || session.TurnSequence == 0 ||
			session.CreatedAt.IsZero() || session.LastObservedAt.IsZero() || session.Retention < 0 || !validOpaque(session.ControlToken) || len(session.ControlToken) < 32 ||
			len(session.SupervisorKey) != 0 && (len(session.SupervisorKey) != ed25519.PublicKeySize || session.PodUID == "" || session.NodeID == "") {
			return invalid("worker session")
		}
		if !historicalMigration && session.ProtocolVersion == LegacySessionProtocolVersion && session.State != SessionClosed && session.Stop == nil {
			return invalid("undrained historical session")
		}
		prefix := string(session.Conversation.Kind)
		if session.Conversation.Kind == ConversationAgentDM {
			prefix = "dm"
		}
		suffix := "-" + session.Conversation.Digest()[:16] + "-" + id[:12]
		if session.PodName != "worker"+suffix && session.PodName != prefix+suffix || names[session.PodName] != "" {
			return invalid("session Pod identity")
		}
		names[session.PodName] = id
		if len(session.Bootstrap) != 0 && (!json.Valid(session.Bootstrap) || !sessionBootstrapMatches(session, session.Bootstrap) || session.BootstrapDigest != digest(session.Bootstrap) || int64(len(session.Bootstrap)) > s.options.MaxRecordBytes) ||
			len(session.Bootstrap) == 0 && session.BootstrapDigest != "" || len(session.Resources) != 0 && (!json.Valid(session.Resources) || int64(len(session.Resources)) > s.options.MaxRecordBytes) {
			return invalid("session creation journal")
		}
		if session.PodUID != "" && len(session.Bootstrap) == 0 {
			return invalid("unpublished session bootstrap")
		}
		if session.LastAttemptID != "" {
			last, ok := st.Grants[session.LastAttemptID]
			if !ok || last.WorkerSessionID != id || last.InputDigest != session.InputDigest || !fingerprint(session.InputDigest) {
				return invalid("latest session assignment")
			}
		} else if session.InputDigest != "" {
			return invalid("session input without assignment")
		}
		if session.ActiveAttempt != "" {
			g, ok := st.Grants[session.ActiveAttempt]
			if !ok || g.WorkerSessionID != id || g.TurnSequence != session.TurnSequence || g.TurnComplete || storage.ActiveAttempt != g.AttemptID {
				return invalid("session active turn")
			}
		}
		if session.LastCompleteAttempt != "" {
			g, ok := st.Grants[session.LastCompleteAttempt]
			if !ok || g.WorkerSessionID != id || !g.TurnComplete || !turnExecutionSettled(&st, g, st.Terminals[g.AttemptID]) || g.CompletionWitness == nil {
				return invalid("session completed turn")
			}
		}
		switch session.State {
		case SessionCreating, SessionReserved, SessionRunning, SessionFinishing:
			if session.ActiveAttempt == "" || session.Stop != nil {
				return invalid("session execution state")
			}
		case SessionIdle:
			if session.ActiveAttempt != "" || session.Stop != nil || session.LastCompleteAttempt == "" || session.IdleSince.IsZero() ||
				session.IdleDeadline.IsZero() || session.IdleDeadline.Before(session.IdleSince) || session.Retention <= 0 || storage.Dirty != (session.ProtocolVersion == SessionProtocolVersion) || storage.Quarantined ||
				session.ProtocolVersion == SessionProtocolVersion && storage.Checkpoint != nil {
				return invalid("idle session authority")
			}
		case SessionDraining, SessionQuarantined:
			if session.Stop == nil {
				return invalid("session stop intent")
			}
		case SessionClosed:
			if session.ActiveAttempt != "" || session.Stop == nil || session.Stop.Evidence == nil || storage.WriterSessionID == id {
				return invalid("session writer release")
			}
		default:
			return invalid("session lifecycle state")
		}
		if session.ResourcesCleaned && session.State != SessionClosed {
			return invalid("session resource cleanup")
		}
		if session.State != SessionClosed && storage.WriterSessionID != id {
			return invalid("session exclusive writer")
		}
		if session.State != SessionClosed || !session.ResourcesCleaned {
			key := session.Conversation.Digest()
			if live[key] != "" {
				return invalid("multiple conversation residents")
			}
			live[key] = id
		}
		if stop := session.Stop; stop != nil {
			if !validOpaque(stop.Reason) || len(stop.Reason) > 256 || stop.Revision == 0 || stop.Revision > session.Revision || !canonicalUUID(stop.Nonce) || stop.RequestedAt.IsZero() {
				return invalid("session stop record")
			}
			if stop.ControllerWritersStopped && !sessionWritersFenced(&st, session) {
				return invalid("session controller writer proof")
			}
			if stop.Receipt != nil && stop.Receipt.WritersStopped && stop.Receipt.FlushOK && !stop.ControllerWritersStopped {
				return invalid("session flush ordering")
			}
			if stop.Receipt != nil && !validSessionStopReceipt(session, *stop.Receipt) || stop.Evidence != nil && !validSessionStopEvidence(&st, session, *stop.Evidence) {
				return invalid("session termination proof")
			}
			if stop.ActiveAttempt != "" {
				g, ok := st.Grants[stop.ActiveAttempt]
				if !ok || g.WorkerSessionID != id || g.TurnSequence != session.TurnSequence {
					return invalid("stopped turn identity")
				}
			}
			if stop.RequestProof != nil {
				body, _ := json.Marshal(struct {
					Reason string `json:"reason"`
				}{stop.RequestReason})
				if !validOpaque(stop.RequestReason) || len(stop.RequestReason) > 256 || !validStoredStopProof(session, *stop.RequestProof, SessionOperationStopRequest, body) {
					return invalid("accepted stop request")
				}
			} else if stop.RequestReason != "" {
				return invalid("unaccepted stop request")
			}
			if stop.Receipt != nil {
				body, _ := json.Marshal(stop.Receipt)
				if stop.ReceiptProof == nil || !validStoredStopProof(session, *stop.ReceiptProof, SessionOperationStopReceipt, body) {
					return invalid("accepted stop receipt")
				}
			} else if stop.ReceiptProof != nil {
				return invalid("missing accepted stop receipt")
			}
		}
	}
	return nil
}

func validStoredStopProof(session WorkerSession, proof SessionProof, operation string, body []byte) bool {
	return session.Stop != nil && proof.Operation == operation && proof.BodyDigest == digest(body) && proof.WorkerSessionID == session.ID &&
		proof.PodUID == session.PodUID && proof.PVCUID == session.PVCUID && proof.TurnSequence == session.TurnSequence && proof.AttemptID == session.Stop.ActiveAttempt &&
		!proof.ExpiresAt.IsZero() && canonicalUUID(proof.Nonce) && len(session.SupervisorKey) == ed25519.PublicKeySize &&
		ed25519.Verify(session.SupervisorKey, SessionProofMessage(proof), proof.Signature)
}
