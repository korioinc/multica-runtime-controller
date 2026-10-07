package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type unfinishedTaskBody struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *unfinishedTaskBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return 0, io.EOF
}
func (b *unfinishedTaskBody) Close() error { return nil }

func TestCancellationCommitsWhileTaskRequestIsUnfinished(t *testing.T) {
	c, g, _, _, _ := controllerFixture(t)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Store.BindPod(g.AttemptID, g.PodName, g.PodUID, "worker-node"); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.Admit(g.AttemptID, pub); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Store.Offer(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.BeginStart(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.MarkStarted(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(context.Background(), g.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "worker-node"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: g.RuntimeRef.Image}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: g.RuntimeRef.Image}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	token, err := c.Store.CapabilityToken(g.AttemptID, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	body := &unfinishedTaskBody{entered: make(chan struct{}), release: make(chan struct{})}
	r := httptest.NewRequest(http.MethodPost, "/api/agents/"+g.AgentID+"/tasks/"+g.TaskID+"/messages", nil)
	r.Body = body
	r.Header.Set("Authorization", "Bearer mat_controller_fixture")
	r.Header.Set("X-Multica-Attempt-Capability", token)
	done := make(chan struct{})
	go func() { defer close(done); c.Handler().ServeHTTP(httptest.NewRecorder(), r) }()
	defer func() { close(body.release); <-done }()
	select {
	case <-body.entered:
	case <-done:
		t.Fatal("request did not reach the in-flight body read")
	}
	stopped := make(chan error, 1)
	go func() { _, err := c.requestStop(g, "backend_cancelled"); stopped <- err }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("unfinished task request prevented cancellation from committing")
	}
	if _, err := c.Store.Authorize(token, "daemon"); err == nil {
		t.Fatal("cancelled task retained authority while an old request drained")
	}
}

func TestCancelledBeforeInputSealsThroughGatewayAndCanRetry(t *testing.T) {
	c, g, _, options, assignment := controllerFixture(t)
	ctx := context.Background()
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, g.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "worker-node"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: g.RuntimeRef.Image}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: g.RuntimeRef.Image, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	stopToken, err := c.Store.CapabilityToken(g.AttemptID, "stop")
	if err != nil {
		t.Fatal(err)
	}
	request := func(action string, body any) *httptest.ResponseRecorder {
		t.Helper()
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, "/internal/attempts/"+g.AttemptID+"/"+action, bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+stopToken)
		w := httptest.NewRecorder()
		c.Handler().ServeHTTP(w, r)
		return w
	}
	request("stop-admit", map[string]any{"publicKey": pub, "podUID": g.PodUID, "pvcUID": g.PVCUID, "bootstrapDigest": g.BootstrapDigest})
	current := *assignment.Load()
	current.Status = "cancelled"
	assignment.Store(&current)
	if err := reconcileFixture(ctx, c); err != nil {
		t.Fatal(err)
	}
	response := request("stop-request", map[string]string{"reason": "worker_shutdown"})
	var command wire.StopCommand
	if err := json.Unmarshal(response.Body.Bytes(), &command); err != nil {
		t.Fatal(err)
	}
	receipt := workspace.StopReceipt{TaskID: g.TaskID, AttemptID: g.AttemptID, Generation: g.Generation, PodUID: g.PodUID, PVCUID: g.PVCUID, Revision: command.Revision, Nonce: command.Nonce, WritersStopped: true, FlushOK: true}
	receipt.Signature = ed25519.Sign(key, workspace.StopReceiptMessage(receipt))
	request("stop-receipt", receipt)
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	request("stop-receipt", receipt) // ACK loss after journal commit.
	finishTaskPod(t, c, g)
	if err := reconcileFixture(ctx, c); err != nil {
		t.Fatal(err)
	}
	next, err := c.Store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope})
	if err != nil {
		t.Fatal("clean cancelled initialization could not be retried", err)
	}
	if next.TaskRoot != g.TaskRoot || next.Generation <= g.Generation {
		t.Fatal("retry lost the task's data or attempt fencing")
	}
}

// A cancelled startup must relinquish stopped compute while preserving data
// that has no flush receipt. The real journal is reopened before reconciliation.
func TestCancelledStartupClosesStoppedComputeAndRetainsDirtyData(t *testing.T) {
	c, g, _, options, _ := controllerFixture(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/"+g.AgentID+"/tasks" {
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": "cancelled"}})
			return
		}
		if r.URL.Path == "/api/daemon/tasks/"+g.TaskID+"/status" {
			_, _ = w.Write([]byte(`{"status":"cancelled"}`))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer backend.Close()
	var err error
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcileFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	finishTaskPod(t, c, g)
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := reconcileFixture(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	current, err := c.Store.Get(g.AttemptID)
	if err != nil || current.State != "closed" || !current.CleanupComplete {
		t.Fatal("cancelled startup retained stopped compute", current.State, err)
	}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(context.Background(), g.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("cancelled worker resources were not reclaimed", err)
	}
	if _, err := c.Store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}); !errors.Is(err, workspace.ErrStorageBusy) {
		t.Fatal("unflushed task data was admitted for reuse", err)
	}
}

func finishTaskPod(t *testing.T, c *Controller, g workspace.TaskGrant) {
	t.Helper()
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(context.Background(), g.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	started, finished := metav1.NewTime(time.Now().Add(-time.Second)), metav1.Now()
	terminated := func(name string, code int32) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: name, Image: g.RuntimeRef.Image, ImageID: g.RuntimeRef.Image, ContainerID: "containerd://" + name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: "Completed", ContainerID: "containerd://" + name, StartedAt: started, FinishedAt: finished}}}
	}
	pod.Status.Phase = corev1.PodFailed
	pod.Spec.NodeName = "worker-node"
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated("task-layout", 0)}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{terminated("worker", 1)}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}
