//go:build linux

package controller

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Execute this test in the disposable Linux fixture with an owned /workspace
// tmpfs. It proves HTTP/journal/native-file binding, not NFS or provider behavior.
func TestConversationHTTPWarmReuseFencesOldTurn(t *testing.T) {
	if os.Getenv("MULTICA_CONVERSATION_FS_TEST") != "1" {
		t.Skip("requires an explicitly owned Linux /workspace fixture")
	}
	repository := workspace.Repository{URL: "https://example.invalid/retained.git", Ref: "main"}
	f := newSessionControllerRepositoryFixture(t, "codex", repository)
	first := f.Grant
	if _, err := os.Lstat(first.TaskRoot); !os.IsNotExist(err) {
		t.Fatal("fixture native root already exists", err)
	}
	for _, directory := range []string{"workdir", "codex-home/sessions"} {
		if err := os.MkdirAll(filepath.Join(first.TaskRoot, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = os.RemoveAll(first.TaskRoot) })
	nativeID := uuid.NewString()
	header, _ := json.Marshal(map[string]any{"type": "session_meta", "payload": map[string]string{"id": nativeID, "cwd": first.TaskRoot + "/workdir"}})
	nativePath := filepath.Join(first.TaskRoot, "codex-home/sessions/rollout-fixture-"+nativeID+".jsonl")
	if err := os.WriteFile(nativePath, append(header, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(first.TaskRoot, "workdir/retained-user-file")
	if err := os.WriteFile(sentinel, []byte("user edits survive a new task"), 0600); err != nil {
		t.Fatal(err)
	}
	checkRetainedCheckout := retainedCheckoutProof(t, f, repository)
	checkRetainedCheckout()
	supervisor, _ := f.C.Store.CapabilityToken(first.AttemptID, "supervisor")
	daemon, _ := f.C.Store.CapabilityToken(first.AttemptID, "daemon")
	result := wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "first native outcome", SessionID: nativeID}}
	response := f.request(http.MethodPost, "/internal/attempts/"+first.AttemptID+"/result", supervisor, result)
	var command wire.SealCommand
	if json.Unmarshal(response.Body.Bytes(), &command) != nil {
		t.Fatal("result challenge could not be decoded")
	}
	resultReceipt := workspace.ResultReceipt{WorkerSessionID: first.WorkerSessionID, TurnSequence: first.TurnSequence,
		TaskID: first.TaskID, AttemptID: first.AttemptID, PodUID: first.PodUID, PVCUID: first.PVCUID,
		RequestDigest: command.RequestDigest, Nonce: command.Nonce}
	resultReceipt.Signature = ed25519.Sign(f.Key, workspace.ResultReceiptMessage(resultReceipt))
	f.request(http.MethodPost, "/internal/attempts/"+first.AttemptID+"/result-receipt", supervisor, resultReceipt)
	if err := f.C.reconcileDelivery(t.Context(), first.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if f.Grant.CompletionWitness == nil || f.Session.State != workspace.SessionFinishing || f.Grant.TurnComplete {
		t.Fatal("accepted result became reusable before quiescence")
	}
	stop, _ := f.C.Store.CapabilityToken(first.AttemptID, "stop")
	f.request(http.MethodPost, "/internal/attempts/"+first.AttemptID+"/checkout-stop", stop, struct{}{})
	f.refresh(t)
	if !f.Grant.CheckoutClosed || f.Grant.CheckoutNeedsFlush {
		t.Fatal("controller task writers were not fenced")
	}
	if err := workspace.SyncTaskFilesystem(first.TaskRoot); err != nil {
		t.Fatal(err)
	}
	terminal, err := f.C.Store.Terminal(first.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	turnReceipt := workspace.TurnExecutionReceipt{WorkerSessionID: first.WorkerSessionID, Conversation: first.Conversation, StorageID: first.StorageID,
		TaskID: first.TaskID, AttemptID: first.AttemptID,
		Generation: first.Generation, TurnSequence: first.TurnSequence, InputDigest: first.InputDigest, PodUID: first.PodUID, PVCUID: first.PVCUID,
		ResultDigest: terminal.ResultDigest, RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce,
		TaskProcessesStopped: true, LocalRequestsClosed: true, PrivateStateCleared: true}
	turnReceipt.Signature = ed25519.Sign(f.Key, workspace.TurnExecutionReceiptMessage(turnReceipt))
	turnBody, _ := json.Marshal(turnReceipt)
	turnProof := f.proof(t, workspace.SessionOperationTurnExecutionReceipt, turnBody)
	turnRequest := wire.SessionControlRequest{Body: turnBody, Proof: turnProof}
	f.request(http.MethodPost, "/internal/worker-sessions/"+first.WorkerSessionID+"/"+workspace.SessionOperationTurnExecutionReceipt, "", turnRequest)
	if err := f.C.reconcileDelivery(t.Context(), first.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if f.Session.State != workspace.SessionIdle || !f.Grant.TurnComplete || f.Grant.CleanupComplete {
		t.Fatal("safe idle incorrectly claimed Pod resource cleanup")
	}
	oldPoll, _ := json.Marshal(wire.SessionPoll{TurnSequence: first.TurnSequence, State: workspace.SessionIdle})
	oldProof := f.proof(t, workspace.SessionOperationPoll, oldPoll)
	originalBootstrap, err := wire.DecodeSessionBootstrap(f.Session.Bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	originalResources, err := sessionRecord(f.Session)
	if err != nil {
		t.Fatal(err)
	}
	f.C.GatewayURL = "http://replacement-controller-route:8080"
	f.C.Policy.Worker.TerminationGraceSeconds = 2
	f.C.Policy.Worker.TaskDeadlineSeconds += 60
	var envelope map[string]any
	if json.Unmarshal(first.Envelope, &envelope) != nil {
		t.Fatal("fixture claim disappeared")
	}
	nextID := uuid.NewString()
	dispatched := time.Now().UTC().Add(time.Second).Truncate(time.Microsecond)
	envelope["id"], envelope["auth_token"], envelope["dispatched_at"] = nextID, "mat_"+nextID, dispatched.Format(time.RFC3339Nano)
	envelope["prior_session_id"], envelope["prior_work_dir"], envelope["new_comments_delta_known"] = nativeID, first.TaskRoot+"/workdir", true
	raw, _ := json.Marshal(envelope)
	claim, err := daemonapi.ParseClaim(raw)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.Rows[nextID] = daemonapi.TaskObservation{ID: nextID, WorkspaceID: claim.WorkspaceID, RuntimeID: claim.RuntimeID, AgentID: claim.AgentID,
		Kind: claim.Kind, IssueID: claim.IssueID, Status: "dispatched", CreatedAt: dispatched.Format(time.RFC3339), DispatchedAt: dispatched.Format(time.RFC3339)}
	f.mu.Unlock()
	queued, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: nextID, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID,
		RuntimeID: claim.RuntimeID, RuntimeRef: first.RuntimeRef, Envelope: raw, Metadata: first.Metadata, Repositories: first.Repositories, ResourceScope: first.ResourceScope})
	if err != nil {
		t.Fatal(err)
	}
	compatibility := *first.Compatibility
	compatibility.ProviderOptionsResolved = false
	selection, err := f.C.backendSelection(t.Context(), claim, compatibility, true)
	if err != nil || selection.Mode != workspace.SelectionResume || selection.SessionSource.TaskID != first.TaskID {
		t.Fatal("supported history lost the exact completed producer", selection.Reason, err)
	}
	if err := f.C.Store.RecordBackendSelection(queued.AttemptID, selection); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.RecordCompatibility(queued.AttemptID, *first.Compatibility); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.RecordSelection(queued.AttemptID, selection, first.CompatibilityDigest); err != nil {
		t.Fatal(err)
	}
	f.Grant, f.Session, err = f.C.Store.ReserveTurn(queued.AttemptID, selection, first.CompatibilityDigest, f.C.ConversationIdleTimeout, f.C.MaxResidentPods)
	if err != nil {
		t.Fatal(err)
	}
	prepareConversationFixtureTurn(t, f.C, f.Grant)
	f.refresh(t)
	if err := f.C.provisionTurn(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	assignment, err := wire.DecodeTurnAssignment(f.Grant.Assignment)
	if err != nil || assignment.Validate(originalBootstrap, first.PodUID) != nil || assignment.Bootstrap.GatewayURL != originalBootstrap.GatewayURL ||
		assignment.Bootstrap.TerminationGraceSeconds != originalBootstrap.TerminationGraceSeconds ||
		!assignment.Deadline.Equal(f.Grant.CreatedAt.Add(time.Duration(f.C.Policy.Worker.PreparationTimeoutSeconds+f.C.Policy.Worker.TaskDeadlineSeconds)*time.Second)) {
		t.Fatal("new controller policy altered the admitted Pod or lost the current turn deadline", err)
	}
	currentResources, err := sessionRecord(f.Session)
	originalRequest, _ := json.Marshal(originalResources.PodRequest)
	currentRequest, _ := json.Marshal(currentResources.PodRequest)
	if err != nil || string(originalRequest) != string(currentRequest) {
		t.Fatal("a warm turn changed the durable Pod create payload", err)
	}
	if f.Grant.PodUID != first.PodUID || f.Grant.WorkerSessionID != first.WorkerSessionID || f.Grant.StorageID != first.StorageID ||
		f.Grant.TaskRoot != first.TaskRoot || f.Grant.WorkspaceAnchorTaskID != first.TaskID || f.Grant.TaskID == first.TaskID ||
		f.Grant.AttemptID == first.AttemptID || f.Grant.TurnSequence != first.TurnSequence+1 || f.Grant.ResumeSession != nativeID {
		t.Fatal("follow-up replaced the anchored native session or live Pod")
	}
	if _, err := f.C.Store.Authorize(daemon, "daemon"); err == nil {
		t.Fatal("old task capability survived reservation")
	}
	var protectedReads atomic.Int64
	priorHandler := f.Handler.Load().(http.HandlerFunc)
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/issues/"+claim.IssueID {
			protectedReads.Add(1)
		}
		priorHandler(w, r)
	}))
	oldClaim, _ := daemonapi.ParseClaim(first.Envelope)
	apiRequest := httptest.NewRequest(http.MethodGet, "/api/issues/"+claim.IssueID, nil)
	apiRequest.Header.Set("Authorization", "Bearer "+oldClaim.AuthToken)
	apiRequest.Header.Set("X-Multica-Attempt-Capability", daemon)
	apiResponse := httptest.NewRecorder()
	f.C.Handler().ServeHTTP(apiResponse, apiRequest)
	if protectedReads.Load() != 0 {
		t.Fatal("old token reached the backend after reservation")
	}
	f.request(http.MethodPost, "/internal/worker-sessions/"+f.Session.ID+"/poll", "", wire.SessionControlRequest{Body: oldPoll, Proof: oldProof})
	for _, action := range []string{"start", "event"} {
		f.request(http.MethodPost, "/internal/attempts/"+first.AttemptID+"/"+action, supervisor, struct{}{})
	}
	for action, body := range map[string]any{"result": result, "result-receipt": resultReceipt} {
		f.request(http.MethodPost, "/internal/attempts/"+first.AttemptID+"/"+action, supervisor, body)
	}
	f.request(http.MethodPost, "/internal/worker-sessions/"+first.WorkerSessionID+"/"+workspace.SessionOperationTurnExecutionReceipt, "", turnRequest)
	retained, err := f.C.Store.Terminal(first.AttemptID)
	if err != nil || retained.ResultDigest != terminal.ResultDigest || !bytes.Equal(retained.Body, terminal.Body) {
		t.Fatal("old retries replaced the authenticated result", err)
	}
	f.acceptAndStart(t)
	checkRetainedCheckout()
	var run wire.Run
	if json.Unmarshal(f.Grant.Execution, &run) != nil || run.Environment["MULTICA_TOKEN"] != claim.AuthToken || run.Environment["MULTICA_TASK_ID"] != nextID || run.Options.ResumeSessionID != nativeID {
		t.Fatal("follow-up reused an earlier task's input or credential")
	}
	if err := f.C.reconcileDelivery(t.Context(), first.AttemptID); err != nil {
		t.Fatal(err)
	}
	pod, err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), f.Session.PodName, metav1.GetOptions{})
	if err != nil || string(pod.UID) != first.PodUID || pod.DeletionTimestamp != nil {
		t.Fatal("old delivery removed a Pod serving another task", err)
	}
	if raw, err := os.ReadFile(sentinel); err != nil || string(raw) != "user edits survive a new task" {
		t.Fatal("warm handling changed retained user data", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Starts[first.TaskID] != 1 || f.Starts[nextID] != 1 {
		t.Fatal("replayed traffic duplicated a provider start", f.Starts)
	}
}

// The real checkout handler has no cache to copy from. Its Git executable
// wrapper records every command so a no-op fetch or reset cannot hide behind
// equal final files. Git setup and observations use the original executable.
func retainedCheckoutProof(t *testing.T, f *sessionControllerFixture, repository workspace.Repository) func() {
	t.Helper()
	controller, err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), f.C.Owner.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controller.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "controller", ContainerID: "containerd://fixture-preparation",
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}
	if _, err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).UpdateStatus(t.Context(), controller, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	name, err := checkout.DirectoryName(repository.URL)
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(f.Grant.TaskRoot, "workdir", name)
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	gitCommand := func(arguments ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), git, append([]string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid"}, arguments...)...)
		command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=/nonexistent", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0"}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatal("local Git proof command failed", err, string(output))
		}
		return output
	}
	gitCommand("init", "--initial-branch=main")
	gitCommand("remote", "add", "origin", repository.URL)
	tracked, untracked := filepath.Join(directory, "tracked.txt"), filepath.Join(directory, "untracked.txt")
	if err := os.WriteFile(tracked, []byte("committed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gitCommand("add", "tracked.txt")
	gitCommand("commit", "--no-gpg-sign", "-m", "fixture base")
	for path, content := range map[string]string{tracked: "uncommitted user edit\n", untracked: "untracked sentinel\n"} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	gitIdentity, err := os.Stat(filepath.Join(directory, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	head, status := gitCommand("rev-parse", "HEAD"), gitCommand("status", "--porcelain")
	tools := t.TempDir()
	trace := filepath.Join(tools, "git-commands.jsonl")
	wrapper := fmt.Sprintf("#!/usr/bin/python3\nimport json,os,sys\nwith open(%q,'a') as output: output.write(json.dumps(sys.argv[1:])+'\\n')\nos.execv(%q,[%q]+sys.argv[1:])\n", trace, git, git)
	if err := os.WriteFile(filepath.Join(tools, "git"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tools+":"+os.Getenv("PATH"))
	calls := 0
	return func() {
		t.Helper()
		if f.C.Cache != nil {
			t.Fatal("retained checkout proof unexpectedly has a cache")
		}
		g, err := f.C.Store.Get(f.Grant.AttemptID)
		if err != nil {
			t.Fatal(err)
		}
		claim, err := daemonapi.ParseClaim(g.Envelope)
		if err != nil {
			t.Fatal(err)
		}
		input, _ := json.Marshal(wire.CheckoutRequest{TaskID: g.TaskID, WorkspaceID: g.WorkspaceID,
			WorkDir: g.Prepared.Environment.WorkDir, URL: repository.URL, Ref: repository.Ref})
		token, err := f.C.Store.CapabilityToken(g.AttemptID, "supervisor")
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/internal/attempts/"+g.AttemptID+"/checkout", bytes.NewReader(input))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set(wire.CheckoutTaskAuthorizationHeader, "Bearer "+claim.AuthToken)
		response := httptest.NewRecorder()
		f.C.Handler().ServeHTTP(response, request)
		var result wire.CheckoutResult
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || result.Path != directory || result.Kept != "local_work" {
			t.Fatal("the current turn could not retain the existing checkout without a cache", response.Code, response.Body.String())
		}
		calls++
		raw, err := os.ReadFile(trace)
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSpace(raw), []byte{'\n'})
		if len(lines) != calls {
			t.Fatal("warm handling issued an unexpected Git command")
		}
		for _, line := range lines {
			var arguments []string
			if json.Unmarshal(line, &arguments) != nil || !reflect.DeepEqual(arguments, []string{"config", "--no-includes", "--file", "-", "--get", "remote.origin.url"}) {
				t.Fatal("retained checkout invoked Git beyond origin validation")
			}
		}
		identity, err := os.Stat(filepath.Join(directory, ".git"))
		if err != nil || !os.SameFile(gitIdentity, identity) || !bytes.Equal(head, gitCommand("rev-parse", "HEAD")) || !bytes.Equal(status, gitCommand("status", "--porcelain")) {
			t.Fatal("warm handling replaced or changed the existing Git checkout", err)
		}
		for path, content := range map[string]string{tracked: "uncommitted user edit\n", untracked: "untracked sentinel\n"} {
			if actual, err := os.ReadFile(path); err != nil || string(actual) != content {
				t.Fatal("warm handling changed retained user bytes", err)
			}
		}
	}
}
