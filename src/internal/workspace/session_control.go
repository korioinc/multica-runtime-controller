package workspace

import (
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/json"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	SessionOperationPoll                 = "poll"
	SessionOperationAccept               = "accept"
	SessionOperationStopControl          = "stop-control"
	SessionOperationStopRequest          = "stop-request"
	SessionOperationStopReceipt          = "stop-receipt"
	SessionOperationTurnExecutionReceipt = "turn-execution-receipt"
)

func validSessionOperation(operation string) bool {
	switch operation {
	case SessionOperationPoll, SessionOperationAccept, SessionOperationStopControl, SessionOperationStopRequest, SessionOperationStopReceipt, SessionOperationTurnExecutionReceipt:
		return true
	}
	return false
}

func challengeAttempt(session WorkerSession, operation string) string {
	if session.Stop != nil && (operation == SessionOperationStopControl || operation == SessionOperationStopReceipt || operation == SessionOperationStopRequest) {
		return session.Stop.ActiveAttempt
	}
	return session.ActiveAttempt
}

func sessionTokenMatches(session WorkerSession, token string) bool {
	return token != "" && subtle.ConstantTimeCompare([]byte(session.ControlToken), []byte(token)) == 1
}

func (s *Store) AuthorizeSessionEnrollment(id, token string) (WorkerSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, err := s.controlSession(id)
	if err != nil || !sessionTokenMatches(session, token) {
		return WorkerSession{}, ErrUnauthorized
	}
	return cloneSession(session), nil
}

// controlSession reads only the requested session under Store.mu. Polling does
// not copy or publish the full registry merely to inspect one supervisor.
func (s *Store) controlSession(id string) (WorkerSession, error) {
	if s.lock == nil || s.failed != nil {
		return WorkerSession{}, ErrUnauthorized
	}
	if err := s.checkJournal(); err != nil {
		return WorkerSession{}, err
	}
	session, ok := s.committed.Sessions[id]
	if !ok {
		return WorkerSession{}, ErrUnauthorized
	}
	return session, nil
}

// CreateSessionChallenge keeps at most one unaccepted challenge per operation.
// No task data or per-turn authority is returned to the enrollment capability.
func (s *Store) CreateSessionChallenge(id, token, operation, bodyDigest string) (SessionChallenge, error) {
	if !validSessionOperation(operation) || !fingerprint(bodyDigest) {
		return SessionChallenge{}, ErrUnauthorized
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, err := s.controlSession(id)
	if err != nil || !sessionTokenMatches(session, token) || session.PodUID == "" || len(session.SupervisorKey) != ed25519.PublicKeySize {
		return SessionChallenge{}, ErrUnauthorized
	}
	if operation == SessionOperationTurnExecutionReceipt && session.ProtocolVersion != SessionProtocolVersion {
		return SessionChallenge{}, ErrUnauthorized
	}
	now := s.now()
	if now.Before(session.LastObservedAt) {
		return SessionChallenge{}, ErrUnauthorized
	}
	if session.State == SessionIdle && !now.Before(session.IdleDeadline) {
		st := cloneRegistry(s.committed)
		session = cloneSession(session)
		requestSessionStop(&st, &session, "idle_expired", now)
		st.Sessions[id] = session
		if err := s.write(st); err != nil {
			return SessionChallenge{}, err
		}
	}
	if s.controlChallenges == nil {
		s.controlChallenges = map[string]map[string]SessionChallenge{}
	}
	if s.controlChallenges[id] == nil {
		s.controlChallenges[id] = map[string]SessionChallenge{}
	}
	challenges := s.controlChallenges[id]
	attempt := challengeAttempt(session, operation)
	if previous, exists := challenges[operation]; exists && previous.BodyDigest == bodyDigest &&
		previous.TurnSequence == session.TurnSequence && previous.AttemptID == attempt && now.Before(previous.ExpiresAt) && !previous.ExpiresAt.After(now.Add(SessionChallengeTTL)) {
		return previous, nil
	}
	challenge := SessionChallenge{Operation: operation, WorkerSessionID: id, PodUID: session.PodUID, PVCUID: session.PVCUID,
		TurnSequence: session.TurnSequence, AttemptID: attempt, BodyDigest: bodyDigest, Nonce: uuid.NewString(), ExpiresAt: now.Add(SessionChallengeTTL)}
	challenges[operation] = challenge
	return challenge, nil
}

func (s *Store) validSessionProof(session WorkerSession, proof SessionProof, operation string, now time.Time) bool {
	challenge, ok := s.controlChallenges[session.ID][operation]
	return ok && operation == proof.Operation && validSessionOperation(operation) && proof.SessionChallenge == challenge &&
		proof.WorkerSessionID == session.ID && proof.PodUID == session.PodUID && proof.PVCUID == session.PVCUID &&
		proof.TurnSequence == session.TurnSequence && proof.AttemptID == challengeAttempt(session, operation) &&
		now.Before(proof.ExpiresAt) && !proof.ExpiresAt.After(now.Add(SessionChallengeTTL)) && !now.Before(session.LastObservedAt) && len(session.SupervisorKey) == ed25519.PublicKeySize &&
		ed25519.Verify(session.SupervisorKey, SessionProofMessage(proof), proof.Signature)
}

// AuthorizeSession verifies read-only control. Mutations commit proof acceptance
// with their state change, so a crash cannot consume authority before the mutation.
func (s *Store) AuthorizeSession(proof SessionProof) (WorkerSession, error) {
	if proof.Operation != SessionOperationPoll && proof.Operation != SessionOperationStopControl {
		return WorkerSession{}, ErrUnauthorized
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	session, err := s.controlSession(proof.WorkerSessionID)
	if err != nil {
		return WorkerSession{}, err
	}
	if !s.validSessionProof(session, proof, proof.Operation, s.now()) {
		return WorkerSession{}, ErrUnauthorized
	}
	return cloneSession(session), nil
}

func (s *Store) SetSessionBootstrap(id string, raw json.RawMessage) error {
	if !json.Valid(raw) || int64(len(raw)) > s.options.MaxRecordBytes {
		return invalid("session bootstrap")
	}
	raw = compactJSON(raw)
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || session.ProtocolVersion != SessionProtocolVersion || session.Stop != nil || session.PodUID != "" {
			return ErrConflict
		}
		if len(session.Bootstrap) != 0 && !slices.Equal(session.Bootstrap, raw) {
			return ErrConflict
		}
		if !sessionBootstrapMatches(session, raw) {
			return invalid("session bootstrap identity")
		}
		session.Bootstrap, session.BootstrapDigest = slices.Clone(raw), digest(raw)
		st.Sessions[id] = session
		return nil
	})
}

func sessionBootstrapMatches(session WorkerSession, raw json.RawMessage) bool {
	var identity struct {
		Version                                                                                                                         int
		WorkerSessionID, StorageID, WorkspaceAnchorTaskID, CompatibilityDigest, TaskRoot, PVCName, PVCUID, RuntimeID, ControlCapability string
		Conversation                                                                                                                    ConversationKey
	}
	return json.Unmarshal(raw, &identity) == nil && identity.Version == session.ProtocolVersion && identity.WorkerSessionID == session.ID &&
		identity.Conversation == session.Conversation && identity.StorageID == session.StorageID &&
		identity.WorkspaceAnchorTaskID == session.WorkspaceAnchorTaskID && identity.CompatibilityDigest == session.CompatibilityDigest &&
		identity.TaskRoot == session.TaskRoot && identity.PVCName == session.PVCName && identity.PVCUID == session.PVCUID &&
		identity.RuntimeID == session.RuntimeID && identity.ControlCapability == session.ControlToken
}

func (s *Store) SetSessionResources(id string, raw json.RawMessage) error {
	if !json.Valid(raw) || int64(len(raw)) > s.options.MaxRecordBytes {
		return invalid("session resources")
	}
	raw = compactJSON(raw)
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || session.ResourcesCleaned || !preservesResourceCreation(session.Resources, raw, session.Stop != nil) {
			return ErrConflict
		}
		session.Resources = slices.Clone(raw)
		st.Sessions[id] = session
		return nil
	})
}

func (s *Store) BindSessionPod(id, name, uid, node string) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || name != session.PodName || !validOpaque(uid) || session.State == SessionClosed ||
			session.PodUID != "" && session.PodUID != uid || session.NodeID != "" && node != "" && session.NodeID != node {
			return ErrConflict
		}
		session.PodUID = uid
		if node != "" {
			session.NodeID = node
		}
		if session.ActiveAttempt != "" {
			g := st.Grants[session.ActiveAttempt]
			if g.Prepared == nil || !g.PreparationStopped {
				return ErrConflict
			}
			if terminal, ok := st.Terminals[g.AttemptID]; ok && terminal.Source == "controller" && terminal.PodUID == "" {
				terminal.PodUID = uid
				st.Terminals[g.AttemptID] = terminal
			}
			g.PodUID, g.NodeID = session.PodUID, session.NodeID
			st.Grants[g.AttemptID] = g
			for key, capability := range st.Capabilities {
				if capability.AttemptID == g.AttemptID {
					capability.PodUID = uid
					st.Capabilities[key] = capability
				}
			}
		}
		st.Sessions[id] = session
		return nil
	})
}

// AdmitSession pins the sole supervisor key. A readable bootstrap never permits
// key replacement, including after the current turn completes or is cancelled.
func (s *Store) AdmitSession(id, bootstrapDigest string, key ed25519.PublicKey) error {
	return s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || !fingerprint(bootstrapDigest) || bootstrapDigest != session.BootstrapDigest || session.PodUID == "" ||
			session.NodeID == "" || len(key) != ed25519.PublicKeySize || session.State == SessionClosed {
			return ErrConflict
		}
		if len(session.SupervisorKey) != 0 && !slices.Equal(session.SupervisorKey, key) {
			return ErrConflict
		}
		session.SupervisorKey = slices.Clone(key)
		st.Sessions[id] = session
		return nil
	})
}

func (s *Store) PublishTurn(id string, raw json.RawMessage) error {
	inputDigest, err := TurnInputDigest(raw)
	if err != nil || int64(len(raw)) > s.options.MaxRecordBytes {
		return invalid("turn assignment")
	}
	var assignment struct {
		WorkerSessionID string
		PodUID          string
		TurnSequence    uint64
		InputDigest     string
		Bootstrap       json.RawMessage
		Run             json.RawMessage
		Deadline        time.Time
	}
	if json.Unmarshal(raw, &assignment) != nil || assignment.InputDigest != inputDigest || assignment.Deadline.IsZero() {
		return invalid("turn assignment digest")
	}
	var bootstrap struct {
		OwnerID, WorkspaceID, TaskID, AgentID, AttemptID, WorkerSessionID, WorkspaceAnchorTaskID, StorageID, TaskRoot, PVCUID string
		NativeMetadataDigest                                                                                                  string
		TurnSequence, Generation                                                                                              uint64
	}
	if json.Unmarshal(assignment.Bootstrap, &bootstrap) != nil {
		return invalid("turn bootstrap")
	}
	raw = compactJSON(raw)
	return s.updateGrant(id, func(st *registry, g *TaskGrant) error {
		if len(g.Assignment) != 0 {
			if !slices.Equal(g.Assignment, raw) {
				return ErrConflict
			}
			return nil
		}
		session, ok := st.Sessions[g.WorkerSessionID]
		if !ok || session.ProtocolVersion != SessionProtocolVersion || session.ActiveAttempt != id || session.TurnSequence != g.TurnSequence || session.Stop != nil || g.ExecutionRevoked ||
			g.State != "intent" || g.Prepared == nil || !g.PreparationStopped || session.PodUID == "" || len(session.SupervisorKey) != ed25519.PublicKeySize ||
			assignment.WorkerSessionID != session.ID || assignment.PodUID != session.PodUID || assignment.TurnSequence != g.TurnSequence ||
			!assignment.Deadline.After(s.now()) || !sameJSON(assignment.Run, g.Execution) ||
			bootstrap.OwnerID != g.OwnerID || bootstrap.WorkspaceID != g.WorkspaceID || bootstrap.TaskID != g.TaskID || bootstrap.AgentID != g.AgentID ||
			bootstrap.AttemptID != id || bootstrap.WorkerSessionID != session.ID || bootstrap.TurnSequence != g.TurnSequence || bootstrap.Generation != g.Generation ||
			bootstrap.StorageID != g.StorageID || bootstrap.WorkspaceAnchorTaskID != g.WorkspaceAnchorTaskID || bootstrap.TaskRoot != g.TaskRoot || bootstrap.PVCUID != g.PVCUID ||
			g.Prepared.NativeMetadata == nil || bootstrap.NativeMetadataDigest != g.Prepared.NativeMetadata.Digest() {
			return ErrConflict
		}
		g.SupervisorKey, g.StopKey = slices.Clone(session.SupervisorKey), slices.Clone(session.SupervisorKey)
		g.Assignment, g.InputDigest = slices.Clone(raw), inputDigest
		g.Bootstrap, g.BootstrapDigest = compactJSON(assignment.Bootstrap), digest(compactJSON(assignment.Bootstrap))
		g.State = "assigned"
		session.State, session.LastAttemptID, session.InputDigest = SessionReserved, id, inputDigest
		session.Revision++
		st.Sessions[session.ID] = session
		return nil
	})
}

// AcceptTurn retains its signed acknowledgement on the original grant. A retry
// for an older accepted turn cannot accept, reveal, or change a successor turn.
func (s *Store) AcceptTurn(id string, sequence uint64, inputDigest string, proof SessionProof) (TaskGrant, error) {
	var accepted TaskGrant
	err := s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok || proof.WorkerSessionID != id || proof.Operation != SessionOperationAccept || sequence == 0 || !fingerprint(inputDigest) {
			return ErrUnauthorized
		}
		body, _ := json.Marshal(struct {
			TurnSequence uint64 `json:"turnSequence"`
			InputDigest  string `json:"inputDigest"`
		}{sequence, inputDigest})
		if proof.BodyDigest != digest(body) {
			return ErrUnauthorized
		}
		for _, old := range st.Grants {
			if old.WorkerSessionID == id && old.TurnSequence == sequence && old.InputDigest == inputDigest && old.AcceptProof != nil && sameJSON(*old.AcceptProof, proof) {
				accepted = old
				return nil
			}
		}
		now := s.now()
		if !s.validSessionProof(session, proof, SessionOperationAccept, now) || session.Stop != nil || session.State != SessionReserved || sequence != session.TurnSequence {
			return ErrUnauthorized
		}
		g := st.Grants[session.ActiveAttempt]
		if g.State != "assigned" || g.ExecutionRevoked || g.InputDigest != inputDigest || g.TurnAccepted || !turnBeforeDeadline(g, now) {
			return ErrConflict
		}
		g.TurnAccepted, g.AcceptProof = true, cloneProof(&proof)
		st.Grants[g.AttemptID] = g
		accepted = g
		return nil
	})
	return cloneGrant(accepted), err
}

func requestSessionStop(st *registry, session *WorkerSession, reason string, now time.Time) {
	if session.Stop == nil {
		session.Revision++
		session.Stop = &SessionStopRecord{ActiveAttempt: session.ActiveAttempt, Reason: reason, Revision: session.Revision, Nonce: uuid.NewString(), RequestedAt: now}
	}
	if session.State != SessionClosed && session.State != SessionQuarantined {
		session.State = SessionDraining
	}
	if session.ActiveAttempt != "" {
		g := st.Grants[session.ActiveAttempt]
		g.ExecutionRevoked = true
		if g.Stop == nil {
			g.Stop = &StopRecord{Reason: reason, Revision: 1, Nonce: uuid.NewString(), RequestedAt: now}
		}
		st.Grants[g.AttemptID] = g
	}
}

func (s *Store) RequestSessionStop(id, reason string) (WorkerSession, error) {
	if !validOpaque(reason) || len(reason) > 256 {
		return WorkerSession{}, invalid("session stop reason")
	}
	var result WorkerSession
	err := s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok {
			return ErrUnauthorized
		}
		if session.State == SessionClosed && session.Stop == nil {
			return ErrConflict
		}
		requestSessionStop(st, &session, reason, s.now())
		st.Sessions[id], result = session, session
		return nil
	})
	return cloneSession(result), err
}

func (s *Store) RequestSessionStopSigned(id, reason string, proof SessionProof) (WorkerSession, error) {
	if !validOpaque(reason) || len(reason) > 256 {
		return WorkerSession{}, invalid("session stop reason")
	}
	var result WorkerSession
	err := s.transaction(func(st *registry) error {
		session, ok := st.Sessions[id]
		if !ok {
			return ErrUnauthorized
		}
		if session.Stop != nil && session.Stop.RequestProof != nil && sameJSON(*session.Stop.RequestProof, proof) && session.Stop.RequestReason == reason {
			result = session
			return nil
		}
		body, _ := json.Marshal(struct {
			Reason string `json:"reason"`
		}{reason})
		if !s.validSessionProof(session, proof, SessionOperationStopRequest, s.now()) || proof.BodyDigest != digest(body) {
			return ErrUnauthorized
		}
		requestSessionStop(st, &session, reason, s.now())
		session.Stop.RequestProof = cloneProof(&proof)
		session.Stop.RequestReason = reason
		st.Sessions[id], result = session, session
		return nil
	})
	return cloneSession(result), err
}

func turnBeforeDeadline(g TaskGrant, now time.Time) bool {
	var assignment struct {
		Deadline time.Time `json:"deadline"`
	}
	return json.Unmarshal(g.Assignment, &assignment) == nil && !assignment.Deadline.IsZero() && now.Before(assignment.Deadline)
}
