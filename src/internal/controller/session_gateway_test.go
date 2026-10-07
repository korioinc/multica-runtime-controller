package controller

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type sessionControllerFixture struct {
	C       *Controller
	Options workspace.Options
	Grant   workspace.TaskGrant
	Session workspace.WorkerSession
	Key     ed25519.PrivateKey
	Handler atomic.Value // http.HandlerFunc; tests can replace one observed API boundary.
	mu      sync.Mutex
	Rows    map[string]daemonapi.TaskObservation
	Starts  map[string]int
}

// The fixture exercises the real session HTTP handlers, Kubernetes client and
// journal. Native helper/provider execution has separate Linux/image proof.
func newSessionControllerFixture(t *testing.T, providers ...string) *sessionControllerFixture {
	t.Helper()
	provider := "codex"
	if len(providers) != 0 {
		provider = providers[0]
	}
	return newSessionControllerRepositoryFixture(t, provider)
}

func newSessionControllerRepositoryFixture(t *testing.T, provider string, repositories ...workspace.Repository) *sessionControllerFixture {
	t.Helper()
	return newSessionControllerContextFixture(t, provider, workspace.ConversationIssue, provider != "claude", repositories...)
}

func newSessionControllerContextFixture(t *testing.T, provider string, kind workspace.ConversationKind, nativeEligible bool, repositories ...workspace.Repository) *sessionControllerFixture {
	t.Helper()
	c, legacy, _, options, _ := controllerFixtureBeforeProvision(t, repositories...)
	if _, err := c.Store.RequestStop(legacy.AttemptID, "unused_fixture_seed"); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.CloseCheckouts(legacy.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.ObserveStop(legacy.AttemptID, workspace.StopEvidence{PVCUID: legacy.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.CloseStopped(legacy.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.MarkCleaned(legacy.AttemptID); err != nil {
		t.Fatal(err)
	}
	if !workspace.SupportedProvider(provider) {
		t.Fatal("unsupported fixture provider", provider)
	}
	if provider != "codex" {
		c.RuntimeRef.Providers = map[string]runtimeimage.Executable{provider: c.RuntimeRef.Providers["codex"]}
		c.Descriptor.Providers = c.RuntimeRef.Providers
	}
	c.Capacity, c.MaxResidentPods, c.ConversationIdleTimeout = 4, 4, DefaultConversationIdleTimeout
	f := &sessionControllerFixture{C: c, Options: options, Rows: map[string]daemonapi.TaskObservation{}, Starts: map[string]int{}}
	f.Handler.Store(http.HandlerFunc(f.serveBackend))
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.Handler.Load().(http.HandlerFunc)(w, r) }))
	t.Cleanup(backend.Close)
	var err error
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	id, agentID, issueID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	at := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	row := daemonapi.TaskObservation{ID: id, WorkspaceID: legacy.WorkspaceID, AgentID: agentID, RuntimeID: legacy.RuntimeID,
		Kind: "direct", IssueID: issueID, Status: "dispatched", CreatedAt: at.Add(-time.Second).Format(time.RFC3339), DispatchedAt: at.Format(time.RFC3339)}
	scope := []string{"issue:" + issueID}
	if kind == workspace.ConversationAgentDM {
		row.Kind, row.IssueID, row.ChatSessionID = "chat", "", uuid.NewString()
		scope = []string{"chat:" + row.ChatSessionID}
	} else if kind == workspace.ConversationTask {
		row.Kind, row.IssueID = "quick_create", ""
		scope = nil
	}
	f.Rows[id] = row
	fields := map[string]any{"id": id, "kind": row.Kind, "workspace_id": row.WorkspaceID, "agent_id": agentID,
		"runtime_id": row.RuntimeID, "issue_id": row.IssueID, "chat_session_id": row.ChatSessionID, "dispatched_at": at.Format(time.RFC3339Nano), "created_at": row.CreatedAt,
		"auth_token": "mat_" + id, "start_claim_supported": true, "agent": map[string]string{"id": agentID, "model": "fixture-model"},
		"attribution": daemonapi.TaskAttribution{Source: "unattributed"}}
	if kind == workspace.ConversationAgentDM {
		fields["chat_message"] = "the current task request"
	} else if kind == workspace.ConversationTask {
		fields["quick_create_prompt"] = "the current quick-create request"
	}
	envelope, _ := json.Marshal(fields)
	metadata, _ := json.Marshal(daemonapi.Bootstrap{Workspace: daemonapi.Workspace{ID: row.WorkspaceID}, Runtime: daemonapi.Runtime{ID: row.RuntimeID, Provider: provider}})
	g, err := c.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, AgentID: agentID, WorkspaceID: row.WorkspaceID,
		RuntimeID: row.RuntimeID, RuntimeRef: c.RuntimeRef, Envelope: envelope, Metadata: metadata, Repositories: repositories, ResourceScope: scope})
	if err != nil {
		t.Fatal(err)
	}
	f.Grant, f.Session = reserveConversationFixtureTurn(t, c, g, nativeEligible)
	prepareConversationFixtureTurn(t, c, f.Grant)
	f.refresh(t)
	if err := c.provisionTurn(t.Context(), f.Grant); !errors.Is(err, errPreparePending) {
		t.Fatalf("first provision = %v, want session enrollment wait", err)
	}
	f.refresh(t)
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(t.Context(), f.Session.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "worker-node"
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: c.RuntimeRef.Image}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: c.RuntimeRef.Image}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.Key = private
	admission := wire.SessionAdmission{WorkerSessionID: f.Session.ID, PodUID: f.Session.PodUID, PVCUID: f.Session.PVCUID,
		BootstrapDigest: f.Session.BootstrapDigest, PublicKey: public}
	response := f.request(http.MethodPost, "/internal/worker-sessions/"+f.Session.ID+"/admit", f.Session.ControlToken, admission)
	if response.Code != http.StatusOK {
		t.Fatalf("session enrollment: %d %s", response.Code, response.Body.String())
	}
	f.refresh(t)
	if err := c.provisionTurn(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	f.acceptAndStart(t)
	return f
}

func (f *sessionControllerFixture) acceptAndStart(t *testing.T) {
	t.Helper()
	accept := wire.SessionAccept{TurnSequence: f.Grant.TurnSequence, InputDigest: f.Grant.InputDigest}
	response := f.signed(t, workspace.SessionOperationAccept, accept)
	if response.Code != http.StatusOK {
		t.Fatalf("turn acceptance: %d %s", response.Code, response.Body.String())
	}
	f.refresh(t)
	admit := map[string]any{"publicKey": f.Key.Public().(ed25519.PublicKey), "podUID": f.Grant.PodUID, "pvcUID": f.Grant.PVCUID, "bootstrapDigest": f.Grant.BootstrapDigest}
	for _, action := range []string{"stop-admit", "admit", "start"} {
		audience := "supervisor"
		if action == "stop-admit" {
			audience = "stop"
		}
		token, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, audience)
		if err != nil {
			t.Fatal(err)
		}
		var body any = admit
		if action == "start" {
			body = struct{}{}
		}
		response = f.request(http.MethodPost, "/internal/attempts/"+f.Grant.AttemptID+"/"+action, token, body)
		if response.Code != http.StatusOK {
			t.Fatalf("turn %s: %d %s", action, response.Code, response.Body.String())
		}
		f.refresh(t)
	}
}

func selectConversationFixtureTurn(t *testing.T, c *Controller, g workspace.TaskGrant, eligible bool) (workspace.Selection, string) {
	t.Helper()
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	key := daemonapi.ConversationIdentity(g.OwnerID, claim)
	metadata, err := c.sessionMetadata(g)
	if err != nil {
		t.Fatal(err)
	}
	selection := freshSelection(key, eligible, "fixture_first_turn")
	selection.WorkspaceReuseEligible = true
	compatibility := workspace.ConversationCompatibilityV1{Conversation: key, RuntimeID: g.RuntimeID, Repositories: g.Repositories,
		ResourceScope: g.ResourceScope, Provider: metadata.Runtime.Provider, ProviderOptionsResolved: eligible, Model: "fixture-model", RuntimeRef: g.RuntimeRef,
		ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, StableMCP: json.RawMessage(`null`), StablePlugins: json.RawMessage(`[]`),
		Authority: workspace.AuthorityEvidence{Version: workspace.AuthorityEvidenceVersion, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID,
			RuntimeID: g.RuntimeID, PrincipalID: g.OwnerID, AgentOwnerID: g.OwnerID, DelegationKnown: true,
			Memberships: []workspace.AuthorityMembership{{UserID: g.OwnerID, Role: "owner"}}, Integrations: workspace.IntegrationEvidence{State: workspace.IntegrationVerifiedEmpty}}}
	if err := c.Store.RecordBackendSelection(g.AttemptID, selection); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.RecordCompatibility(g.AttemptID, compatibility); err != nil {
		t.Fatal(err)
	}
	digest, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Store.RecordSelection(g.AttemptID, selection, digest); err != nil {
		t.Fatal(err)
	}
	return selection, digest
}

func reserveConversationFixtureTurn(t *testing.T, c *Controller, g workspace.TaskGrant, eligible bool) (workspace.TaskGrant, workspace.WorkerSession) {
	t.Helper()
	selection, digest := selectConversationFixtureTurn(t, c, g, eligible)
	retention := c.ConversationIdleTimeout
	resident := c.MaxResidentPods
	if resident == 0 {
		resident = c.Capacity
	}
	g, session, err := c.Store.ReserveTurn(g.AttemptID, selection, digest, retention, resident)
	if err != nil {
		t.Fatal(err)
	}
	return g, session
}

func prepareConversationFixtureTurn(t *testing.T, c *Controller, g workspace.TaskGrant) {
	t.Helper()
	metadata, err := c.sessionMetadata(g)
	if err != nil {
		t.Fatal(err)
	}
	provider := metadata.Runtime.Provider
	prepared := workspace.Prepared{Conversation: &g.Conversation, WorkspaceAnchorTaskID: g.WorkspaceAnchorTaskID,
		OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID,
		Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, Provider: provider, Executable: g.RuntimeRef.Providers[provider].Path,
		RuntimeDigest: g.Fingerprint, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, CreatedAt: time.Now().UTC(),
		CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{},
		Environment: workspace.NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}}
	if provider != "codex" {
		prepared.Environment.CodexHome = ""
	}
	marker, _ := json.Marshal(map[string]string{"managed_by": "multica-daemon-task", "agent_id": g.AgentID})
	prepared.NativeMetadata = &workspace.NativeMetadata{WorkerSessionID: g.WorkerSessionID, TurnSequence: g.TurnSequence,
		ArtifactRoot: workspace.NativeArtifactRoot(g.TaskRoot, g.WorkerSessionID, g.TurnSequence), Artifacts: []workspace.NativeArtifact{}, TaskMarker: marker}
	prepared.AllowedLinks = workspace.NativeProjectionLinks(provider)
	if provider == "codex" {
		prepared.NativeMetadata.CodexConfig = []byte("model = \"fixture-model\"\n")
	}
	raw, _ := json.Marshal(prepared)
	prepared.Digest = core.Digest(raw)
	if err := c.Store.BeginPreparation(g.AttemptID, workspace.PreparationProcess{PodName: c.Owner.Name, PodUID: c.Owner.UID, ContainerID: "containerd://fixture-preparation"}); err != nil {
		t.Fatal(err)
	}
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	taskConfig, _ := json.Marshal(map[string]string{"token": claim.AuthToken, "workspace_id": g.WorkspaceID, "server_url": wire.RelayURL})
	run := wire.Run{Provider: provider, Options: agent.ExecOptions{Cwd: prepared.Environment.WorkDir, Timeout: time.Minute, Model: "fixture-model"},
		Prompt: "this turn's current request", TaskConfig: taskConfig, NativeMetadata: prepared.NativeMetadata,
		Environment: map[string]string{"MULTICA_TASK_ID": g.TaskID, "MULTICA_TOKEN": claim.AuthToken}}
	run.Options.ResumeSessionID, run.Options.ResumeExpected = g.ResumeSession, g.ResumeSession != ""
	if run.Options.ResumeExpected {
		run.Options.ResumeContinuityNotice = "Restore context if native resume is unavailable."
	}
	raw, _ = json.Marshal(run)
	if err := c.Store.CompletePreparation(g.AttemptID, &prepared, raw); err != nil {
		t.Fatal(err)
	}
}

func (f *sessionControllerFixture) refresh(t *testing.T) {
	t.Helper()
	var err error
	f.Grant, err = f.C.Store.Get(f.Grant.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	f.Session, err = f.C.Store.GetSession(f.Grant.WorkerSessionID)
	if err != nil {
		t.Fatal(err)
	}
}

func (f *sessionControllerFixture) request(method, path, token string, value any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(value)
	r := httptest.NewRequest(method, path, bytes.NewReader(raw))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	f.C.Handler().ServeHTTP(w, r)
	return w
}

func (f *sessionControllerFixture) proof(t *testing.T, operation string, raw []byte) workspace.SessionProof {
	t.Helper()
	response := f.request(http.MethodPost, "/internal/worker-sessions/"+f.Session.ID+"/challenge", f.Session.ControlToken,
		wire.SessionChallengeRequest{Operation: operation, BodyDigest: wire.Digest(raw)})
	var challenge workspace.SessionChallenge
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &challenge) != nil {
		t.Fatalf("control challenge: %d %s", response.Code, response.Body.String())
	}
	proof := workspace.SessionProof{SessionChallenge: challenge}
	proof.Signature = ed25519.Sign(f.Key, workspace.SessionProofMessage(proof))
	return proof
}

func (f *sessionControllerFixture) signed(t *testing.T, operation string, value any) *httptest.ResponseRecorder {
	t.Helper()
	raw, _ := json.Marshal(value)
	return f.request(http.MethodPost, "/internal/worker-sessions/"+f.Session.ID+"/"+operation, "",
		wire.SessionControlRequest{Body: raw, Proof: f.proof(t, operation, raw)})
}

func (f *sessionControllerFixture) serveBackend(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.HasPrefix(r.URL.Path, "/api/agents/") && strings.HasSuffix(r.URL.Path, "/tasks") ||
		strings.HasPrefix(r.URL.Path, "/api/issues/") && strings.HasSuffix(r.URL.Path, "/task-runs") {
		rows := make([]daemonapi.TaskObservation, 0, len(f.Rows))
		for _, row := range f.Rows {
			rows = append(rows, row)
		}
		writeJSON(w, rows)
		return
	}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) == 6 && parts[1] == "api" && parts[2] == "daemon" && parts[3] == "tasks" {
		row, ok := f.Rows[parts[4]]
		if !ok {
			http.NotFound(w, r)
			return
		}
		switch parts[5] {
		case "start":
			var start struct {
				RuntimeID, DispatchedAt string
			}
			// The API contract's snake-case body is asserted by dedicated start tests.
			_ = json.NewDecoder(r.Body).Decode(&start)
			f.Starts[row.ID]++
			row.Status, row.StartedAt = "running", time.Now().UTC().Format(time.RFC3339)
		case "complete":
			var result daemonapi.TaskCompleteRequest
			if json.NewDecoder(r.Body).Decode(&result) != nil {
				http.Error(w, "invalid result", http.StatusBadRequest)
				return
			}
			row.Status, row.Result, row.WorkDir, row.CompletedAt = "completed", &result, result.WorkDir, time.Now().UTC().Format(time.RFC3339)
		default:
			writeJSON(w, struct{}{})
			return
		}
		f.Rows[row.ID] = row
		writeJSON(w, row)
		return
	}
	writeJSON(w, struct{}{})
}

func TestSessionControlRequiresPinnedKeyAndExactAssignment(t *testing.T) {
	testSessionControlRequiresPinnedKeyAndExactAssignment(t, "codex")
}

func TestClaudeSessionControlRequiresPinnedKeyAndExactAssignment(t *testing.T) {
	testSessionControlRequiresPinnedKeyAndExactAssignment(t, "claude")
}

func testSessionControlRequiresPinnedKeyAndExactAssignment(t *testing.T, provider string) {
	t.Helper()
	f := newSessionControllerFixture(t, provider)
	if provider == "claude" && (f.Grant.Selection.ReuseEligible || f.Session.Retention != f.C.ConversationIdleTimeout) {
		t.Fatal("Claude fixture coupled Pod retention to native defaults")
	}
	pod, err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), f.Session.PodName, metav1.GetOptions{})
	if err != nil || pod.Spec.ActiveDeadlineSeconds != nil {
		t.Fatal("reusable Pod retained the per-task lifetime", err)
	}
	secret, err := f.C.Kube.API.CoreV1().Secrets(f.C.Kube.Namespace).Get(t.Context(), f.Session.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wire.DecodeSessionBootstrap(secret.Data[wire.RequestKey])
	claim, claimErr := daemonapi.ParseClaim(f.Grant.Envelope)
	if err != nil || claimErr != nil || bootstrap.WorkerSessionID != f.Session.ID || secret.Immutable == nil || !*secret.Immutable ||
		bytes.Contains(secret.Data[wire.RequestKey], []byte(claim.AuthToken)) || bytes.Contains(secret.Data[wire.RequestKey], []byte("this turn's current request")) {
		t.Fatal("immutable session bootstrap contains task authority", err, claimErr)
	}
	poll := wire.SessionPoll{TurnSequence: f.Grant.TurnSequence, State: workspace.SessionRunning}
	raw, _ := json.Marshal(poll)
	proof := f.proof(t, workspace.SessionOperationPoll, raw)
	request := wire.SessionControlRequest{Proof: proof, Body: raw}
	path := "/internal/worker-sessions/" + f.Session.ID + "/poll"
	for _, value := range []any{poll, wire.SessionControlRequest{Body: raw}} {
		response := f.request(http.MethodPost, path, f.Session.ControlToken, value)
		if response.Code != http.StatusForbidden || strings.Contains(response.Body.String(), "this turn's current request") {
			t.Fatal("bootstrap bearer disclosed a signed-control response", response.Code)
		}
	}
	if response := f.request(http.MethodPost, path, "", request); response.Code != http.StatusOK {
		t.Fatalf("pinned session key cannot poll: %d %s", response.Code, response.Body.String())
	}
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	request.Proof.Signature = ed25519.Sign(other, workspace.SessionProofMessage(proof))
	if response := f.request(http.MethodPost, path, "", request); response.Code != http.StatusForbidden {
		t.Fatal("foreign signing key was accepted", response.Code)
	}
	token, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	if response := f.request(http.MethodPost, "/internal/attempts/"+f.Grant.AttemptID+"/start", token, struct{}{}); response.Code < 400 {
		t.Fatal("consumed turn started twice")
	}
	f.mu.Lock()
	starts := f.Starts[f.Grant.TaskID]
	f.mu.Unlock()
	if starts != 1 {
		t.Fatalf("backend start count = %d", starts)
	}
}

func TestSessionRejectedDeliveryDoesNotWakeItselfDuringShutdown(t *testing.T) {
	f := newSessionControllerFixture(t)
	queues := newDispatchQueues()
	f.C.dispatch = queues
	t.Cleanup(func() { queues.attempts.ShutDown(); queues.deliveries.ShutDown(); queues.sessions.ShutDown() })
	if err := f.C.stopUnacceptedTurn(f.Grant); err != nil {
		t.Fatal(err)
	}
	select {
	case <-queues.claim:
	default:
		t.Fatal("new terminal rejection did not wake eligible claims")
	}
	for range 8 {
		if err := f.C.stopUnacceptedTurn(f.Grant); err != nil {
			t.Fatal(err)
		}
	}
	if len(queues.pending) != 0 || len(queues.claim) != 0 {
		t.Fatal("unchanged rejected delivery created a recovery feedback loop")
	}
}
