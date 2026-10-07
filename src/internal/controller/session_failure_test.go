package controller

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestStoppedSessionWithoutResultPersistsFailureBeforeClosure(t *testing.T) {
	f := newUnstartedSessionFailureFixture(t)
	id, nextID := f.Grant.TaskID, uuid.NewString()
	var next map[string]any
	if err := json.Unmarshal(f.Grant.Envelope, &next); err != nil {
		t.Fatal(err)
	}
	next["id"], next["auth_token"], next["issue_id"] = nextID, "mat_"+nextID, uuid.NewString()
	var failures, claims atomic.Int32
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/daemon/tasks/" + id + "/fail":
			failures.Add(1)
			f.mu.Lock()
			row := f.Rows[id]
			row.Status = "failed"
			f.Rows[id] = row
			f.mu.Unlock()
			writeJSON(w, struct{}{})
		case "/api/daemon/tasks/claim":
			claims.Add(1)
			writeJSON(w, map[string]any{"tasks": []any{next}})
		default:
			f.serveBackend(w, r)
		}
	}))
	metadata, err := f.C.sessionMetadata(f.Grant)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture retains one unrelated running turn plus this failed startup.
	f.C.Capacity = 2
	f.C.runtimes = map[string]daemonapi.Bootstrap{f.Grant.RuntimeID: metadata}
	if err := f.C.claim(t.Context()); err != nil || claims.Load() != 0 {
		t.Fatal("unproven startup released execution capacity", err)
	}
	signSessionFailureStop(t, f)
	if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
	if err != nil || f.Session.State == workspace.SessionClosed || storage.WriterSessionID != f.Session.ID {
		t.Fatal("signed stop released a worker without Kubernetes termination proof", err)
	}
	if _, err := f.C.Store.Terminal(f.Grant.AttemptID); !errors.Is(err, workspace.ErrUnauthorized) {
		t.Fatal("an unproven writer acquired an inferred failure", err)
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	finishTaskPod(t, f.C, f.Grant)
	if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil || terminal.Source != "controller" || terminal.Kind != "fail" || terminal.State != "received" ||
		!bytes.Contains(terminal.Body, []byte("Worker stopped before execution admission")) {
		t.Fatal("proven stopped startup lost its durable failure outbox", err)
	}
	if f.Session.State != workspace.SessionClosed || !f.Session.ResourcesCleaned || f.Session.Stop.Evidence.Kind != "terminated" ||
		f.Grant.State != "closed" || !f.Grant.TurnComplete || f.Grant.StartConfirmed || f.Starts[id] != 0 {
		t.Fatal("stopped startup did not close once without starting execution")
	}
	if _, err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), f.Session.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("proven stopped session retained its Pod", err)
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	pending, err := f.C.Store.ReconcileGrants()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, grant := range pending {
		found = found || grant.AttemptID == f.Grant.AttemptID
	}
	if !found {
		t.Fatal("restart omitted the closed grant's undelivered failure")
	}
	for range 2 {
		if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
			t.Fatal(err)
		}
	}
	delivered, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil || delivered.State != "delivered" || failures.Load() != 1 || !bytes.Equal(delivered.Body, terminal.Body) {
		t.Fatal("recovery did not deliver the original failure exactly once", err)
	}
	if err := f.C.claim(t.Context()); err != nil || claims.Load() != 1 {
		t.Fatal("closed startup did not release capacity for an unrelated task", err)
	}
	grants, err := f.C.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, grant := range grants {
		if grant.TaskID == nextID {
			return
		}
	}
	t.Fatal("reclaimed capacity did not admit the next task")
}

func TestSessionFailureObservationPreservesConcurrentNativeResult(t *testing.T) {
	f := newSessionControllerFixture(t)
	signSessionFailureStop(t, f)
	finishTaskPod(t, f.C, f.Grant)
	entered, release := make(chan struct{}), make(chan struct{})
	var observeOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	f.C.Kube.API.(*fake.Clientset).PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.(clienttesting.GetAction).GetName() == f.Session.PodName {
			observeOnce.Do(func() { close(entered); <-release })
		}
		return false, nil, nil
	})
	done := make(chan error, 1)
	go func() { done <- f.C.reconcileSession(t.Context(), f.Session.ID) }()
	t.Cleanup(unblock)
	select {
	case <-entered:
	case err := <-done:
		t.Fatal("stop did not reach the in-flight Pod observation", err)
	}
	command, err := f.C.receiveResult(f.Grant, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "native result during stop observation"}})
	if err != nil {
		t.Fatal(err)
	}
	receipt := workspace.ResultReceipt{WorkerSessionID: f.Session.ID, TurnSequence: f.Grant.TurnSequence,
		TaskID: f.Grant.TaskID, AttemptID: f.Grant.AttemptID, PodUID: f.Grant.PodUID, PVCUID: f.Grant.PVCUID,
		RequestDigest: command.RequestDigest, Nonce: command.Nonce}
	receipt.Signature = ed25519.Sign(f.Key, workspace.ResultReceiptMessage(receipt))
	if err := f.C.Store.RecordResultReceipt(f.Grant.AttemptID, receipt); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	f.refresh(t)
	terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil || terminal.Source != "worker" || terminal.Kind != "complete" || terminal.ResultReceipt == nil || terminal.RecoveryFailure != nil ||
		!bytes.Contains(terminal.Body, []byte("native result during stop observation")) {
		t.Fatal("stopped session replaced an authenticated native result", err)
	}
	var failures, completions atomic.Int32
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/daemon/tasks/" + f.Grant.TaskID + "/fail":
			failures.Add(1)
		case "/api/daemon/tasks/" + f.Grant.TaskID + "/complete":
			completions.Add(1)
		}
		f.serveBackend(w, r)
	}))
	for range 2 {
		if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
			t.Fatal(err)
		}
	}
	if failures.Load() != 0 || completions.Load() != 1 {
		t.Fatal("stopped session did not retain the native delivery owner")
	}
}

func signSessionFailureStop(t *testing.T, f *sessionControllerFixture) {
	t.Helper()
	// This journal/backend fixture supplies trusted local stop/flush observations.
	// Actual task-root syncfs is covered by the native filesystem proof.
	if _, err := f.C.Store.RequestSessionStop(f.Session.ID, "worker_shutdown"); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.CloseCheckouts(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.CheckoutsFlushed(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.RecordSessionControllerFlush(f.Session.ID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	response := f.signed(t, workspace.SessionOperationStopRequest, wire.SessionStopRequest{Reason: "worker_shutdown"})
	var command wire.SessionStopCommand
	if err := json.Unmarshal(response.Body.Bytes(), &command); err != nil {
		t.Fatal(err)
	}
	receipt := workspace.SessionStopReceipt{WorkerSessionID: f.Session.ID, AttemptID: command.AttemptID, InputDigest: command.InputDigest,
		TurnSequence: command.TurnSequence, PodUID: f.Session.PodUID, PVCUID: f.Session.PVCUID, Revision: command.Revision,
		Nonce: command.Nonce, WritersStopped: true, FlushOK: true}
	receipt.Signature = ed25519.Sign(f.Key, workspace.SessionStopReceiptMessage(receipt))
	f.signed(t, workspace.SessionOperationStopReceipt, receipt)
	f.refresh(t)
	if f.Session.Stop.Receipt == nil {
		t.Fatal("trusted stop proof did not commit")
	}
}

// Keep the shared running fixture unchanged and create a second incarnation
// through normal reserve, prepare, provision, enrollment, and acceptance APIs.
func newUnstartedSessionFailureFixture(t *testing.T) *sessionControllerFixture {
	t.Helper()
	f := newSessionControllerFixture(t)
	id, issueID := uuid.NewString(), uuid.NewString()
	var fields map[string]any
	if err := json.Unmarshal(f.Grant.Envelope, &fields); err != nil {
		t.Fatal(err)
	}
	fields["id"], fields["issue_id"], fields["auth_token"] = id, issueID, "mat_"+id
	envelope, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	row := f.Rows[f.Grant.TaskID]
	row.ID, row.IssueID, row.Status, row.StartedAt = id, issueID, "dispatched", ""
	f.Rows[id] = row
	g, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, AgentID: f.Grant.AgentID,
		WorkspaceID: f.Grant.WorkspaceID, RuntimeID: f.Grant.RuntimeID, RuntimeRef: f.Grant.RuntimeRef,
		Metadata: f.Grant.Metadata, Envelope: envelope, ResourceScope: []string{"issue:" + issueID}})
	if err != nil {
		t.Fatal(err)
	}
	f.Grant, f.Session = reserveConversationFixtureTurn(t, f.C, g, true)
	prepareConversationFixtureTurn(t, f.C, f.Grant)
	f.refresh(t)
	if err := f.C.provisionTurn(t.Context(), f.Grant); !errors.Is(err, errPreparePending) {
		t.Fatal("new session did not wait for worker enrollment", err)
	}
	f.refresh(t)
	pods := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace)
	pod, err := pods.Get(t.Context(), f.Session.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "worker-node"
	if _, err := pods.Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: f.C.RuntimeRef.Image}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: f.C.RuntimeRef.Image}}
	if _, err := pods.UpdateStatus(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
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
	if err := f.C.provisionTurn(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	response = f.signed(t, workspace.SessionOperationAccept, wire.SessionAccept{TurnSequence: f.Grant.TurnSequence, InputDigest: f.Grant.InputDigest})
	if response.Code != http.StatusOK {
		t.Fatalf("turn acceptance: %d %s", response.Code, response.Body.String())
	}
	f.refresh(t)
	return f
}
