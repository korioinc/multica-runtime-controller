package workspace

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

const sessionFixtureRetention = 10 * time.Minute

type sessionFixture struct {
	store         *Store
	options       Options
	clock         *atomic.Int64
	input         TaskGrant
	selection     Selection
	compatibility string
	grant         TaskGrant
	session       WorkerSession
	key           ed25519.PrivateKey
	token         string
	accept        SessionProof
}

func newSessionFixture(t *testing.T) *sessionFixture {
	return newSessionProviderFixture(t, "codex")
}

func newSessionProviderFixture(t *testing.T, provider string) *sessionFixture {
	return newSessionRetentionFixture(t, provider, sessionFixtureRetention)
}

func newSessionRetentionFixture(t *testing.T, provider string, retention time.Duration) *sessionFixture {
	t.Helper()
	s, options := testStore(t)
	clock := new(atomic.Int64)
	clock.Store(time.Now().UTC().UnixNano())
	options.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	s.options.Now = options.Now
	input := testGrant()
	if provider != "codex" {
		input.RuntimeRef.Providers[provider] = input.RuntimeRef.Providers["codex"]
		delete(input.RuntimeRef.Providers, "codex")
	}
	input.RuntimeID = uuid.NewString()
	input.SessionProtocol = true
	selection := Selection{Conversation: ConversationKey{OwnerID: options.OwnerID, WorkspaceID: input.WorkspaceID, AgentID: input.AgentID,
		Kind: ConversationIssue, SubjectID: uuid.NewString()}, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
	f := &sessionFixture{store: s, options: options, clock: clock, input: input, selection: selection}
	f.reserveWithRetention(t, selection, 4, retention)
	f.prepareAndStart(t)
	return f
}

func (f *sessionFixture) reserve(t *testing.T, selection Selection, limit int) {
	f.reserveWithRetention(t, selection, limit, sessionFixtureRetention)
}

func (f *sessionFixture) reserveWithRetention(t *testing.T, selection Selection, limit int, retention time.Duration) {
	t.Helper()
	g, err := f.store.QueueClaim(f.input)
	if err != nil {
		t.Fatal(err)
	}
	f.compatibility = recordTestCompatibility(t, f.store, g, selection)
	if err := f.store.RecordSelection(g.AttemptID, selection, f.compatibility); err != nil {
		t.Fatal(err)
	}
	f.grant, f.session, err = f.store.ReserveTurn(g.AttemptID, selection, f.compatibility, retention, limit)
	if err != nil {
		t.Fatal(err)
	}
}

func testConversationCompatibility(g TaskGrant, selection Selection) ConversationCompatibilityV1 {
	provider := "codex"
	if _, ok := g.RuntimeRef.Providers[provider]; !ok {
		provider = "pi"
	}
	return ConversationCompatibilityV1{Conversation: selection.Conversation, RuntimeID: g.RuntimeID,
		Authority: AuthorityEvidence{Version: AuthorityEvidenceVersion, WorkspaceID: g.WorkspaceID, RuntimeID: g.RuntimeID, AgentID: g.AgentID,
			PrincipalID: g.OwnerID, AgentOwnerID: g.OwnerID, DelegationKnown: true, Memberships: []AuthorityMembership{{UserID: g.OwnerID, Role: "owner"}},
			Integrations: IntegrationEvidence{State: IntegrationVerifiedEmpty}}, Repositories: g.Repositories, ResourceScope: g.ResourceScope,
		Provider: provider, ProviderOptionsResolved: true, RuntimeRef: g.RuntimeRef, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest,
		Model: "fixture-model", StableMCP: json.RawMessage(`{}`), StablePlugins: json.RawMessage(`{}`)}
}

func recordTestCompatibility(t *testing.T, s *Store, g TaskGrant, selection Selection) string {
	t.Helper()
	if err := s.RecordBackendSelection(g.AttemptID, selection); err != nil {
		t.Fatal(err)
	}
	compatibility := testConversationCompatibility(g, selection)
	calculated, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordCompatibility(g.AttemptID, compatibility); err != nil {
		t.Fatal(err)
	}
	return calculated
}

func (f *sessionFixture) prepareAndStart(t *testing.T) {
	t.Helper()
	f.prepareAssignment(t)
	g, session, s := f.grant, f.session, f.store
	body, _ := json.Marshal(struct {
		TurnSequence uint64 `json:"turnSequence"`
		InputDigest  string `json:"inputDigest"`
	}{g.TurnSequence, g.InputDigest})
	proof := f.proof(t, SessionOperationAccept, body)
	if _, err := s.AcceptTurn(session.ID, g.TurnSequence, g.InputDigest, proof); err != nil {
		t.Fatal(err)
	}
	if err := s.Admit(g.AttemptID, f.key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	startGrant(t, s, g)
	f.grant, _ = s.Get(g.AttemptID)
	f.session, _ = s.GetSession(session.ID)
	f.accept = proof
}

func (f *sessionFixture) prepareAssignment(t *testing.T) {
	t.Helper()
	g, session, s := f.grant, f.session, f.store
	provider := g.Compatibility.Provider
	if err := s.BeginPreparation(g.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://preparer"}); err != nil {
		t.Fatal(err)
	}
	p := Prepared{Conversation: clonePointer(&g.Conversation), WorkspaceAnchorTaskID: g.WorkspaceAnchorTaskID,
		OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID,
		Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, Provider: provider, Executable: "/opt/tools/runner",
		RuntimeDigest: g.Fingerprint, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, CreatedAt: s.now(),
		CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{},
		Environment: NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}}
	if provider != "codex" {
		p.Environment.CodexHome = ""
	}
	marker, _ := json.Marshal(map[string]string{"managed_by": "multica-daemon-task", "agent_id": g.AgentID})
	p.NativeMetadata = &NativeMetadata{WorkerSessionID: g.WorkerSessionID, TurnSequence: g.TurnSequence,
		ArtifactRoot: NativeArtifactRoot(g.TaskRoot, g.WorkerSessionID, g.TurnSequence), Artifacts: []NativeArtifact{}, TaskMarker: marker}
	p.AllowedLinks = NativeProjectionLinks(provider)
	if provider == "codex" {
		p.NativeMetadata.CodexConfig = []byte("model = \"fixture-model\"\n")
	}
	p.Digest = preparedDigest(p)
	var run json.RawMessage
	run, _ = json.Marshal(map[string]any{"provider": provider, "options": map[string]any{"timeout": 30000000000}, "nativeMetadata": p.NativeMetadata})
	if err := s.CompletePreparation(g.AttemptID, &p, run); err != nil {
		t.Fatal(err)
	}
	if session.PodUID == "" {
		bootstrap, _ := json.Marshal(map[string]any{"version": SessionProtocolVersion, "workerSessionID": session.ID, "conversation": session.Conversation,
			"storageID": session.StorageID, "workspaceAnchorTaskID": session.WorkspaceAnchorTaskID, "compatibilityDigest": session.CompatibilityDigest,
			"taskRoot": session.TaskRoot, "pvcName": session.PVCName, "pvcUID": session.PVCUID, "runtimeID": session.RuntimeID, "controlCapability": session.ControlToken})
		if err := s.SetSessionBootstrap(session.ID, bootstrap); err != nil {
			t.Fatal(err)
		}
		if err := s.BindSessionPod(session.ID, session.PodName, uuid.NewString(), "node-a"); err != nil {
			t.Fatal(err)
		}
		public, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		f.key = key
		if err := s.AdmitSession(session.ID, digest(compactJSON(bootstrap)), public); err != nil {
			t.Fatal(err)
		}
	}
	g, _ = s.Get(g.AttemptID)
	session, _ = s.GetSession(session.ID)
	token, err := s.IssueCapability(g.AttemptID, "daemon", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := map[string]any{"ownerID": g.OwnerID, "workspaceID": g.WorkspaceID, "taskID": g.TaskID, "agentID": g.AgentID,
		"attemptID": g.AttemptID, "workerSessionID": g.WorkerSessionID, "workspaceAnchorTaskID": g.WorkspaceAnchorTaskID,
		"storageID": g.StorageID, "taskRoot": g.TaskRoot, "pvcUID": g.PVCUID, "turnSequence": g.TurnSequence, "generation": g.Generation,
		"nativeMetadataDigest": p.NativeMetadata.Digest()}
	assignment := map[string]any{"workerSessionID": session.ID, "podUID": session.PodUID, "turnSequence": g.TurnSequence,
		"inputDigest": "", "bootstrap": bootstrap, "run": run, "deadline": s.now().Add(time.Hour)}
	raw, _ := json.Marshal(assignment)
	assignment["inputDigest"], err = TurnInputDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(assignment)
	if err := s.PublishTurn(g.AttemptID, raw); err != nil {
		t.Fatal(err)
	}
	f.grant, _ = s.Get(g.AttemptID)
	f.session, _ = s.GetSession(session.ID)
	f.token = token
}

func (f *sessionFixture) proof(t *testing.T, operation string, body []byte) SessionProof {
	t.Helper()
	challenge, err := f.store.CreateSessionChallenge(f.session.ID, f.session.ControlToken, operation, digest(body))
	if err != nil {
		t.Fatal(err)
	}
	proof := SessionProof{SessionChallenge: challenge}
	proof.Signature = ed25519.Sign(f.key, SessionProofMessage(proof))
	return proof
}

func (f *sessionFixture) receive(t *testing.T, sessionID string) Terminal {
	t.Helper()
	g := f.grant
	body, _ := json.Marshal(map[string]string{"result": "completed native turn", "session_id": sessionID, "work_dir": g.TaskRoot + "/workdir"})
	terminal, err := f.store.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{WorkDir: g.TaskRoot + "/workdir"}, digest([]byte("authenticated native outcome")))
	if err != nil {
		t.Fatal(err)
	}
	if sessionID != "" {
		// Native file validation has its own helper/provider checks. The journal
		// fixture supplies the already-validated native pointer at that boundary.
		if err := f.store.updateGrant(g.AttemptID, func(_ *registry, g *TaskGrant) error { g.PendingResume.SessionID = sessionID; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	f.authenticateResult(t, terminal)
	return terminal
}

func (f *sessionFixture) authenticateResult(t *testing.T, terminal Terminal) {
	t.Helper()
	g := f.grant
	receipt := ResultReceipt{WorkerSessionID: g.WorkerSessionID, TurnSequence: g.TurnSequence, TaskID: g.TaskID, AttemptID: g.AttemptID,
		PodUID: g.PodUID, PVCUID: g.PVCUID, RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}
	receipt.Signature = ed25519.Sign(f.key, ResultReceiptMessage(receipt))
	if err := f.store.RecordResultReceipt(g.AttemptID, receipt); err != nil {
		t.Fatal(err)
	}
}

func (f *sessionFixture) settle(t *testing.T, terminal Terminal, sessionID string) {
	t.Helper()
	g := f.grant
	if err := f.store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckoutsFlushed(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	receipt := TurnExecutionReceipt{WorkerSessionID: g.WorkerSessionID, Conversation: g.Conversation, StorageID: g.StorageID,
		TaskID: g.TaskID, AttemptID: g.AttemptID, Generation: g.Generation, TurnSequence: g.TurnSequence,
		InputDigest: g.InputDigest, PodUID: g.PodUID, PVCUID: g.PVCUID, ResultDigest: terminal.ResultDigest, RequestDigest: terminal.RequestDigest,
		Nonce: terminal.Nonce, TaskProcessesStopped: true, LocalRequestsClosed: true, PrivateStateCleared: true}
	receipt.Signature = ed25519.Sign(f.key, TurnExecutionReceiptMessage(receipt))
	body, _ := json.Marshal(receipt)
	proof := f.proof(t, SessionOperationTurnExecutionReceipt, body)
	if err := f.store.RecordTurnExecutionReceipt(g.AttemptID, receipt, proof); err != nil {
		t.Fatal(err)
	}
	witness := CompletionWitness{TaskID: g.TaskID, AttemptID: g.AttemptID, WorkerSessionID: g.WorkerSessionID, StorageID: g.StorageID, TurnSequence: g.TurnSequence,
		RequestDigest: terminal.RequestDigest, ResultDigest: terminal.ResultDigest, Status: map[string]string{"complete": "completed", "fail": "failed"}[terminal.Kind], SessionID: sessionID, WorkDir: g.TaskRoot + "/workdir", AcknowledgedAt: f.store.now()}
	if err := f.store.RecordCompletionWitness(g.AttemptID, witness); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CompleteTurn(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.grant, _ = f.store.Get(g.AttemptID)
	f.session, _ = f.store.GetSession(g.WorkerSessionID)
}

func (f *sessionFixture) followup() (TaskGrant, Selection) {
	input := f.input
	input.TaskID = uuid.NewString()
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	selection := Selection{Conversation: f.selection.Conversation, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshSession,
		StorageID: storage.ID, WorkspaceSource: storage.LatestWriter, LatestWriter: storage.LatestWriter, WorkDir: storage.TaskRoot + "/workdir"}
	if f.grant.CompletionWitness != nil && f.grant.CompletionWitness.SessionID != "" {
		selection.Mode, selection.SessionID, selection.SessionSource = SelectionResume, f.grant.CompletionWitness.SessionID, storage.LatestWriter
	} else if storage.Checkpoint != nil && storage.Checkpoint.SessionID != "" {
		selection.Mode, selection.SessionID, selection.SessionSource = SelectionResume, storage.Checkpoint.SessionID, storage.Checkpoint.SessionSource
	}
	return input, selection
}

func TestSessionReservationIsExclusiveAndSurvivesReopen(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	previous := f.grant
	idleRevision := f.session.Revision
	previousProof := f.accept
	previousToken := f.token
	pollBody := []byte(`{"turnSequence":1,"state":"idle"}`)
	capturedPoll := f.proof(t, SessionOperationPoll, pollBody)
	var claims []TaskGrant
	var selection Selection
	for range 12 {
		input, chosen := f.followup()
		g, err := f.store.QueueClaim(input)
		if err != nil {
			t.Fatal(err)
		}
		recordTestCompatibility(t, f.store, g, chosen)
		claims = append(claims, g)
		selection = chosen
	}
	var wins atomic.Int32
	var winner TaskGrant
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, claim := range claims {
		wg.Go(func() {
			g, _, err := f.store.ReserveTurn(claim.AttemptID, selection, f.compatibility, 10*time.Minute, 4)
			if err == nil {
				wins.Add(1)
				lock.Lock()
				winner = g
				lock.Unlock()
			} else if !errors.Is(err, ErrStorageBusy) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("reserved %d concurrent writers", wins.Load())
	}
	if winner.WorkerSessionID != previous.WorkerSessionID || winner.PodUID != previous.PodUID || winner.TaskRoot != previous.TaskRoot ||
		winner.WorkspaceAnchorTaskID != previous.TaskID || winner.TaskID == previous.TaskID || winner.TurnSequence != previous.TurnSequence+1 {
		t.Fatal("follow-up lost the exact Pod, anchor, or monotonic turn identity")
	}
	if _, err := f.store.Authorize(previousToken, "daemon"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old task capability remained valid: %v", err)
	}
	if _, err := f.store.AuthorizeSession(capturedPoll); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old signed poll disclosed successor authority: %v", err)
	}
	if drained, err := f.store.DrainIdleSession(f.session.ID, idleRevision, "old_timer"); err != nil || drained {
		t.Fatalf("old idle timer drained a new reservation: %v", err)
	}
	f.clock.Store(f.store.now().Add(2 * SessionChallengeTTL).UnixNano())
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	accepted, err := reopened.AcceptTurn(previous.WorkerSessionID, previous.TurnSequence, previous.InputDigest, previousProof)
	if err != nil || accepted.AttemptID != previous.AttemptID {
		t.Fatalf("lost old acceptance ACK: %v", err)
	}
	current, _ := reopened.GetSession(previous.WorkerSessionID)
	if current.ActiveAttempt != winner.AttemptID || current.TurnSequence != winner.TurnSequence {
		t.Fatal("old acceptance changed the next turn")
	}
	if _, _, err := reopened.ReserveTurn(winner.AttemptID, selection, f.compatibility, 10*time.Minute, 4); err != nil {
		t.Fatal("reservation retry changed identity", err)
	}
}

func (f *sessionFixture) stopAndClose(t *testing.T, receipt bool, cleanup bool) {
	t.Helper()
	session, err := f.store.RequestSessionStop(f.session.ID, "fixture_shutdown")
	if err != nil {
		t.Fatal(err)
	}
	if session.ActiveAttempt != "" {
		if err := f.store.CloseCheckouts(session.ActiveAttempt); err != nil {
			t.Fatal(err)
		}
		if err := f.store.CheckoutsFlushed(session.ActiveAttempt); err != nil {
			t.Fatal(err)
		}
	}
	if receipt {
		if err := f.store.RecordSessionControllerFlush(session.ID); err != nil {
			t.Fatal(err)
		}
		r := SessionStopReceipt{WorkerSessionID: session.ID, AttemptID: session.LastAttemptID, InputDigest: session.InputDigest,
			TurnSequence: session.TurnSequence, PodUID: session.PodUID, PVCUID: session.PVCUID, Revision: session.Stop.Revision, Nonce: session.Stop.Nonce, WritersStopped: true, FlushOK: true}
		r.Signature = ed25519.Sign(f.key, SessionStopReceiptMessage(r))
		body, _ := json.Marshal(r)
		proof := f.proof(t, SessionOperationStopReceipt, body)
		if err := f.store.RecordSessionStopReceipt(session.ID, r, proof); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.ObserveSessionStop(session.ID, StopEvidence{PodUID: session.PodUID, PVCUID: session.PVCUID, Kind: "terminated", ObservedAt: f.store.now()}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CloseSession(session.ID); err != nil {
		t.Fatal(err)
	}
	if cleanup {
		if err := f.store.MarkSessionCleaned(session.ID); err != nil {
			t.Fatal(err)
		}
	}
	f.session, _ = f.store.GetSession(session.ID)
}

func TestSessionFreshConversationKeepsWorkspaceAndPod(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	original := f.grant
	input, selection := f.followup()
	backend := selection
	selection.Mode, selection.SessionID, selection.SessionSource = SelectionFreshSession, "", CheckpointSource{}
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	recordTestCompatibility(t, f.store, queued, backend)
	grant, session, err := f.store.ReserveTurn(queued.AttemptID, selection, f.compatibility, 10*time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	if session.ID != original.WorkerSessionID || session.PodUID != original.PodUID || grant.StorageID != original.StorageID || grant.TaskRoot != original.TaskRoot || grant.ResumeSession != "" {
		t.Fatal("fresh native history replaced the compatible Pod or workspace")
	}
}

func TestSessionCapacityDoesNotRepeatEvictionWhileCleanupPending(t *testing.T) {
	first := newSessionFixture(t)
	firstID := uuid.NewString()
	first.settle(t, first.receive(t, firstID), firstID)
	first.clock.Store(first.store.now().Add(time.Minute).UnixNano())
	second := &sessionFixture{store: first.store, options: first.options, clock: first.clock, input: first.input, selection: first.selection, compatibility: first.compatibility}
	second.input.TaskID = uuid.NewString()
	second.selection.Conversation.SubjectID = uuid.NewString()
	second.reserve(t, second.selection, 2)
	second.prepareAndStart(t)
	secondID := uuid.NewString()
	second.settle(t, second.receive(t, secondID), secondID)
	input := first.input
	input.TaskID = uuid.NewString()
	selection := first.selection
	selection.Conversation.SubjectID = uuid.NewString()
	queued, err := first.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	thirdCompatibility := recordTestCompatibility(t, first.store, queued, selection)
	for range 3 {
		if _, _, err := first.store.ReserveTurn(queued.AttemptID, selection, thirdCompatibility, 10*time.Minute, 2); !errors.Is(err, ErrResidentCapacity) {
			t.Fatalf("capacity admission=%v", err)
		}
	}
	old, _ := first.store.GetSession(first.session.ID)
	kept, _ := first.store.GetSession(second.session.ID)
	if old.State != SessionDraining || kept.State != SessionIdle {
		t.Fatal("capacity retries evicted more than the one required resident")
	}
	first.stopAndClose(t, false, false)
	if _, _, err := first.store.ReserveTurn(queued.AttemptID, selection, thirdCompatibility, 10*time.Minute, 2); !errors.Is(err, ErrResidentCapacity) {
		t.Fatalf("uncleaned resources stopped counting: %v", err)
	}
	if err := first.store.MarkSessionCleaned(first.session.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.store.ReserveTurn(queued.AttemptID, selection, thirdCompatibility, 10*time.Minute, 2); err != nil {
		t.Fatal("cleaned resident did not release capacity", err)
	}
	storage, _ := first.store.GetStorage(first.grant.StorageID)
	if !storage.Dirty || storage.Checkpoint != nil {
		t.Fatal("receipt-less termination made old storage reusable")
	}
}

func TestSessionAbortedReservationConsumesSequenceAndFencesControllerWrites(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	original := f.grant
	input, selection := f.followup()
	f.input = input
	f.reserve(t, selection, 4)
	aborted := f.grant
	if aborted.TurnSequence != original.TurnSequence+1 {
		t.Fatal("reservation did not allocate a sequence")
	}
	if err := f.store.BeginPreparation(aborted.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://aborted"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RequestSessionStop(f.session.ID, "cancelled_before_assignment"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordSessionControllerFlush(f.session.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("live preparation gained a controller flush proof: %v", err)
	}
	if err := f.store.CompletePreparation(aborted.AttemptID, nil, nil); err != nil {
		t.Fatal(err)
	}
	f.stopAndClose(t, true, true)
	closed, _ := f.store.GetSession(original.WorkerSessionID)
	if closed.TurnSequence != aborted.TurnSequence || closed.ActiveAttempt != "" || closed.Stop.ActiveAttempt != aborted.AttemptID || !closed.Stop.ControllerWritersStopped {
		t.Fatal("aborted sequence or its final writer fence was lost")
	}
	old, _ := f.store.Get(aborted.AttemptID)
	if !old.TurnComplete || old.InputDigest != "" {
		t.Fatal("aborted reservation invented an assignment")
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	retained, _ := reopened.GetSession(closed.ID)
	if retained.TurnSequence != aborted.TurnSequence {
		t.Fatal("reopen made an aborted sequence available again")
	}
}

func TestSessionAcceptedWitnessSettlesLostTerminalAckAtomically(t *testing.T) {
	f := newSessionFixture(t)
	terminal := f.receive(t, "")
	if err := f.store.BeginForward(f.grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.FinishForward(f.grant.AttemptID, "uncertain"); err != nil {
		t.Fatal(err)
	}
	witness := CompletionWitness{TaskID: f.grant.TaskID, AttemptID: f.grant.AttemptID, WorkerSessionID: f.session.ID, StorageID: f.grant.StorageID, TurnSequence: f.grant.TurnSequence,
		RequestDigest: terminal.RequestDigest, ResultDigest: terminal.ResultDigest, Status: "completed", WorkDir: f.grant.TaskRoot + "/workdir", AcknowledgedAt: f.store.now()}
	wrong := witness
	wrong.ResultDigest = digest([]byte("another result"))
	if err := f.store.RecordCompletionWitness(f.grant.AttemptID, wrong); !errors.Is(err, ErrConflict) {
		t.Fatalf("another result settled uncertain delivery: %v", err)
	}
	if err := f.store.RecordCompletionWitness(f.grant.AttemptID, witness); err != nil {
		t.Fatal(err)
	}
	recorded, _ := f.store.Terminal(f.grant.AttemptID)
	g, _ := f.store.Get(f.grant.AttemptID)
	if recorded.State != "delivered" || g.CompletionWitness == nil || g.TurnComplete {
		t.Fatal("witness failed to settle delivery independently from writer release")
	}
	if err := f.store.CompleteTurn(g.AttemptID); !errors.Is(err, ErrConflict) {
		t.Fatal("lost-ACK recovery skipped quiescence")
	}
}

func TestSessionLegacyMigrationKeepsWriterFenceAndRejectsWarmAuthority(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	issueID := uuid.NewString()
	input.RuntimeID = uuid.NewString()
	input.Envelope, _ = json.Marshal(map[string]string{"kind": "direct", "issue_id": issueID})
	legacy, _, _ := readyGrant(t, s, input)
	startGrant(t, s, legacy)
	snapshot, err := s.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.SchemaVersion = 13
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(snapshot)
	path := filepath.Join(options.Directory, "journal.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	old, _ := reopened.Get(legacy.AttemptID)
	sessions, _ := reopened.ListSessions()
	if !old.Legacy || old.WorkerSessionID != "" || len(sessions) != 0 || old.PodUID != legacy.PodUID {
		t.Fatal("migration invented warm authority or lost legacy identity")
	}
	input.TaskID, input.SessionProtocol = uuid.NewString(), true
	queued, err := reopened.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	selection := Selection{Conversation: ConversationKey{OwnerID: options.OwnerID, WorkspaceID: input.WorkspaceID, Kind: ConversationIssue, SubjectID: issueID, AgentID: input.AgentID}, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
	compatibility := recordTestCompatibility(t, reopened, queued, selection)
	if _, _, err := reopened.ReserveTurn(queued.AttemptID, selection, compatibility, time.Minute, 4); !errors.Is(err, ErrStorageBusy) {
		t.Fatalf("legacy writer did not fence conversation: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	forged := snapshot.Grants[legacy.AttemptID]
	forged.SessionProtocol = true
	snapshot.Grants[legacy.AttemptID] = forged
	malformed, _ := json.Marshal(snapshot)
	if err := os.WriteFile(path, malformed, 0600); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(options); err == nil {
		opened.Close()
		t.Fatal("schema 13 imported future authority")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, malformed) {
		t.Fatal("failed migration rewrote rejected bytes", err)
	}
}

func TestSessionTurnDigestKeepsLargeIntegerInputs(t *testing.T) {
	left := json.RawMessage(`{"inputDigest":"","timeout":9007199254740992}`)
	right := json.RawMessage(`{"inputDigest":"","timeout":9007199254740993}`)
	a, err := TurnInputDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	b, err := TurnInputDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("assignment hashing rounded a distinct integer input")
	}
}

func TestSessionPollingDoesNotPublishJournalAndLosesOnlyUnusedChallenges(t *testing.T) {
	f := newSessionFixture(t)
	path := filepath.Join(f.options.Directory, "journal.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var proof SessionProof
	for range 20 {
		f.clock.Add(int64(time.Millisecond))
		proof = f.proof(t, SessionOperationPoll, []byte(`{"turnSequence":1,"state":"running"}`))
		if _, err := f.store.AuthorizeSession(proof); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || !os.SameFile(beforeInfo, afterInfo) {
		t.Fatal("read-only session polling rewrote the durable journal")
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.AuthorizeSession(proof); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("restart preserved an unconsumed challenge", err)
	}
	if _, err := reopened.AcceptTurn(f.session.ID, f.grant.TurnSequence, f.grant.InputDigest, f.accept); err != nil {
		t.Fatal("restart lost the accepted command ACK", err)
	}
}

func TestSessionAssignmentDeadlineFencesAcceptAndStart(t *testing.T) {
	for _, at := range []string{"accept", "start"} {
		t.Run(at, func(t *testing.T) {
			f := newSessionFixture(t)
			nativeID := uuid.NewString()
			f.settle(t, f.receive(t, nativeID), nativeID)
			input, selection := f.followup()
			f.input = input
			f.reserve(t, selection, 4)
			f.prepareAssignment(t)
			g := f.grant
			var assignment struct{ Deadline time.Time }
			if err := json.Unmarshal(g.Assignment, &assignment); err != nil {
				t.Fatal(err)
			}
			if at == "accept" {
				f.clock.Store(assignment.Deadline.UnixNano())
			}
			body, _ := json.Marshal(struct {
				TurnSequence uint64 `json:"turnSequence"`
				InputDigest  string `json:"inputDigest"`
			}{g.TurnSequence, g.InputDigest})
			proof := f.proof(t, SessionOperationAccept, body)
			_, err := f.store.AcceptTurn(f.session.ID, g.TurnSequence, g.InputDigest, proof)
			if at == "accept" {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("expired assignment was accepted: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := f.store.Admit(g.AttemptID, f.key.Public().(ed25519.PublicKey)); err != nil {
					t.Fatal(err)
				}
				if _, offered, err := f.store.Offer(g.AttemptID); err != nil || !offered {
					t.Fatal("turn was not offered", err)
				}
				f.clock.Store(assignment.Deadline.UnixNano())
				if err := f.store.BeginStart(g.AttemptID); !errors.Is(err, ErrConflict) {
					t.Fatalf("expired turn gained start authority: %v", err)
				}
			}
			actual, _ := f.store.Get(g.AttemptID)
			if actual.StartConfirmed || actual.State == "starting" || actual.State == "started" {
				t.Fatal("expired input started execution")
			}
		})
	}
}

func TestSessionStopRequestReplayKeepsOriginalCause(t *testing.T) {
	f := newSessionFixture(t)
	initial, err := f.store.RequestSessionStop(f.session.ID, "deadline")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"reason":"worker_cancelled"}`)
	proof := f.proof(t, SessionOperationStopRequest, body)
	if _, err := f.store.RequestSessionStopSigned(f.session.ID, "worker_cancelled", proof); err != nil {
		t.Fatal(err)
	}
	f.clock.Store(f.store.now().Add(2 * SessionChallengeTTL).UnixNano())
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replayed, err := reopened.RequestSessionStopSigned(f.session.ID, "worker_cancelled", proof)
	if err != nil {
		t.Fatal("lost stop ACK after restart", err)
	}
	if replayed.Stop.Reason != "deadline" || replayed.Stop.Revision != initial.Stop.Revision || replayed.Stop.RequestReason != "worker_cancelled" {
		t.Fatal("stop replay changed the original termination cause")
	}
}

func TestSessionLateProofRecoversClosedWorkspaceWithoutReopeningCompute(t *testing.T) {
	f := newSessionFixture(t)
	terminal := f.receive(t, "")
	if err := f.store.BeginForward(f.grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.FinishForward(f.grant.AttemptID, "uncertain"); err != nil {
		t.Fatal(err)
	}
	f.stopAndClose(t, false, true)
	closed := f.session
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	if !storage.Dirty || storage.Checkpoint != nil {
		t.Fatal("missing final proof did not preserve dirty data")
	}
	witness := CompletionWitness{TaskID: f.grant.TaskID, AttemptID: f.grant.AttemptID, WorkerSessionID: f.session.ID, StorageID: f.grant.StorageID, TurnSequence: f.grant.TurnSequence,
		RequestDigest: terminal.RequestDigest, ResultDigest: terminal.ResultDigest, Status: "completed", WorkDir: f.grant.TaskRoot + "/workdir", AcknowledgedAt: f.store.now()}
	if err := f.store.RecordCompletionWitness(f.grant.AttemptID, witness); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CompleteTurn(f.grant.AttemptID); !errors.Is(err, ErrConflict) {
		t.Fatal("late acceptance substituted for final storage proof", err)
	}
	if err := f.store.RecordSessionControllerFlush(f.session.ID); err != nil {
		t.Fatal(err)
	}
	r := SessionStopReceipt{WorkerSessionID: closed.ID, AttemptID: closed.LastAttemptID, InputDigest: closed.InputDigest, TurnSequence: closed.TurnSequence,
		PodUID: closed.PodUID, PVCUID: closed.PVCUID, Revision: closed.Stop.Revision, Nonce: closed.Stop.Nonce, WritersStopped: true, FlushOK: true}
	r.Signature = ed25519.Sign(f.key, SessionStopReceiptMessage(r))
	body, _ := json.Marshal(r)
	proof := f.proof(t, SessionOperationStopReceipt, body)
	if err := f.store.RecordSessionStopReceipt(closed.ID, r, proof); err != nil {
		t.Fatal("late final proof was lost", err)
	}
	if err := f.store.CompleteTurn(f.grant.AttemptID); err != nil {
		t.Fatal("conclusive late proof did not recover root", err)
	}
	after, _ := f.store.GetSession(closed.ID)
	storage, _ = f.store.GetStorage(f.grant.StorageID)
	if after.State != SessionClosed || !after.ResourcesCleaned || after.ActiveAttempt != "" || after.Revision != closed.Revision || after.IdleDeadline != closed.IdleDeadline {
		t.Fatal("late settlement reopened or extended compute")
	}
	if storage.Dirty || storage.Checkpoint == nil || storage.Checkpoint.Source.AttemptID != f.grant.AttemptID || storage.WriterSessionID != "" {
		t.Fatal("late evidence did not publish the exact closed workspace checkpoint")
	}
}

func TestSessionBackendSelectionWaitsWithoutDefaultOrWriterAuthority(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	terminal := f.receive(t, nativeID)
	source := CheckpointSource{TaskID: f.grant.TaskID, AttemptID: f.grant.AttemptID}
	selection := Selection{Conversation: f.selection.Conversation, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionResume, StorageID: f.grant.StorageID,
		SessionSource: source, WorkspaceSource: source, LatestWriter: source, SessionID: nativeID, WorkDir: f.grant.TaskRoot + "/workdir"}
	input := f.input
	input.TaskID = uuid.NewString()
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordBackendSelection(queued.AttemptID, selection); err != nil {
		t.Fatal(err)
	}
	waiting, _ := f.store.Get(queued.AttemptID)
	if waiting.BackendSelection == nil || waiting.Compatibility != nil || waiting.StorageID != "" || waiting.TurnSequence != 0 {
		t.Fatal("source staging granted execution/default authority")
	}
	changed := Selection{Conversation: selection.Conversation, Mode: SelectionFreshWorkspace}
	if err := f.store.RecordBackendSelection(queued.AttemptID, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("a waiting claim silently changed backend source", err)
	}
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, f.compatibility, time.Minute, 4); !errors.Is(err, ErrConflict) {
		t.Fatal("unresolved defaults granted a reservation", err)
	}
	compatibility := recordTestCompatibility(t, f.store, queued, selection)
	if err := f.store.RecordSelection(queued.AttemptID, selection, compatibility); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, time.Minute, 4); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("finishing predecessor did not hold writer", err)
	}
	f.settle(t, terminal, nativeID)
	grant, session, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	if session.ID != f.session.ID || grant.BackendSelection.SessionSource != source || grant.Selection.SessionSource != source {
		t.Fatal("writer settlement changed the selected lineage")
	}
}

func TestSessionTypedCompatibilityCannotBeReplacedByEligibilityFlag(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	input, selection := f.followup()
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordBackendSelection(queued.AttemptID, selection); err != nil {
		t.Fatal(err)
	}
	compatibility := testConversationCompatibility(queued, selection)
	compatibility.ProviderOptionsResolved = false
	calculated, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordCompatibility(queued.AttemptID, compatibility); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordSelection(queued.AttemptID, selection, calculated); !errors.Is(err, ErrConflict) {
		t.Fatal("boolean eligibility hid unresolved native defaults", err)
	}
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, calculated, time.Minute, 4); !errors.Is(err, ErrConflict) {
		t.Fatal("hash-only default evidence gained a writer", err)
	}
	actual, _ := f.store.GetSession(f.session.ID)
	if actual.State != SessionIdle {
		t.Fatal("rejected authority changed the prior resident")
	}
}

func TestSessionChangedResolvedOptionsKeepsBackendSourceButStartsFresh(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	input, backend := f.followup()
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordBackendSelection(queued.AttemptID, backend); err != nil {
		t.Fatal(err)
	}
	compatibility := testConversationCompatibility(queued, backend)
	compatibility.Model = "a-different-resolved-model"
	calculated, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordCompatibility(queued.AttemptID, compatibility); err != nil {
		t.Fatal(err)
	}
	fresh := Selection{Conversation: backend.Conversation, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace, Reason: "model_changed"}
	if err := f.store.RecordSelection(queued.AttemptID, fresh, calculated); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, fresh, calculated, time.Minute, 4); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("fresh configuration bypassed the old resident", err)
	}
	retained, _ := f.store.Get(queued.AttemptID)
	if retained.BackendSelection.SessionSource != backend.SessionSource || retained.Selection.Mode != SelectionFreshWorkspace {
		t.Fatal("configuration reset rewrote upstream selection evidence")
	}
}

func TestSessionStagedBackendSourceAllowsOnlyCurrentLocalWriterRefresh(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	input, selected := f.followup()
	waiting, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordBackendSelection(waiting.AttemptID, selected); err != nil {
		t.Fatal(err)
	}
	f.input, _ = f.followup()
	f.reserve(t, selected, 4)
	updated := selected
	updated.LatestWriter = CheckpointSource{TaskID: f.grant.TaskID, AttemptID: f.grant.AttemptID}
	if err := f.store.RecordBackendSelection(waiting.AttemptID, updated); err != nil {
		t.Fatal("known local writer could not refresh the wait fence", err)
	}
	actual, _ := f.store.Get(waiting.AttemptID)
	if actual.BackendSelection.SessionSource != selected.SessionSource || actual.BackendSelection.WorkspaceSource != selected.WorkspaceSource ||
		actual.BackendSelection.LatestWriter != updated.LatestWriter || actual.Compatibility != nil || actual.StorageID != "" {
		t.Fatal("writer refresh changed the backend choice or granted reuse")
	}
	switched := updated
	switched.SessionSource = updated.LatestWriter
	if err := f.store.RecordBackendSelection(waiting.AttemptID, switched); !errors.Is(err, ErrConflict) {
		t.Fatal("local writer refresh silently selected a different backend source", err)
	}
	if err := f.store.RecordBackendSelection(waiting.AttemptID, selected); !errors.Is(err, ErrConflict) {
		t.Fatal("a stale writer replaced the current wait fence", err)
	}
}

func TestSessionReservationSamplesExpiryAfterLockWait(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	input, selection := f.followup()
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	recordTestCompatibility(t, f.store, queued, selection)
	deadline := f.session.IdleDeadline
	f.clock.Store(deadline.Add(-time.Nanosecond).UnixNano())
	f.store.mu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, _, err := f.store.ReserveTurn(queued.AttemptID, selection, f.compatibility, 10*time.Minute, 4)
		done <- err
	}()
	<-started
	f.clock.Store(deadline.UnixNano())
	f.store.mu.Unlock()
	if err := <-done; !errors.Is(err, ErrStorageBusy) {
		t.Fatalf("equality reused expired Pod: %v", err)
	}
	session, _ := f.store.GetSession(f.session.ID)
	if session.State != SessionDraining || session.ActiveAttempt != "" {
		t.Fatal("expiry failed to retain the draining incarnation")
	}
	g, _ := f.store.Get(queued.AttemptID)
	if g.StorageID != "" || g.TurnSequence != 0 {
		t.Fatal("expired reservation acquired a writer")
	}
}

func TestSessionCompletionRequiresIndependentAcceptanceAndQuiescence(t *testing.T) {
	f := newSessionFixture(t)
	terminal := f.receive(t, "")
	if err := f.store.BeginForward(f.grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.FinishForward(f.grant.AttemptID, "delivered"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CompleteTurn(f.grant.AttemptID); !errors.Is(err, ErrConflict) {
		t.Fatalf("transport acceptance alone released writer: %v", err)
	}
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	if storage.Checkpoint != nil || storage.WriterSessionID != f.session.ID || storage.ActiveAttempt != f.grant.AttemptID {
		t.Fatal("unquiesced result became reusable")
	}
	f.settle(t, terminal, "")
	storage, _ = f.store.GetStorage(f.grant.StorageID)
	if storage.ActiveAttempt != "" || storage.WriterSessionID != f.session.ID || storage.Checkpoint != nil || !storage.Dirty {
		t.Fatal("safe idle lost the session writer lease")
	}
}

func TestSessionUnknownAuthorityPreservesCanonicalFence(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	input, _ := f.followup()
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	selection := Selection{Conversation: f.selection.Conversation, Mode: SelectionFreshWorkspace, Reason: "authority_unavailable"}
	compatibility := testConversationCompatibility(queued, selection)
	compatibility.Authority.Integrations = IntegrationEvidence{State: IntegrationUnknown, Reason: "fixture lookup unavailable"}
	unknownDigest, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordCompatibility(queued.AttemptID, compatibility); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, unknownDigest, time.Minute, 4); err == nil {
		t.Fatal("unknown authority retained a Pod")
	}
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, unknownDigest, 0, 4); !errors.Is(err, ErrStorageBusy) {
		t.Fatalf("unknown authority bypassed existing conversation: %v", err)
	}
	old, _ := f.store.GetSession(f.session.ID)
	if old.State != SessionDraining {
		t.Fatal("unknown authority failed to drain old conversation")
	}
}
