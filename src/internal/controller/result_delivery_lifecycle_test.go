package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestAuthenticatedResultDeliveryProgressesWhileShutdownObservationIsBlocked(t *testing.T) {
	c, g, _, _, _ := controllerFixture(t)
	backend := installResultLifecycleBackend(t, c, g)
	command, key := receiveRunningResult(t, c, g)
	if err := c.Store.RecordResultReceipt(g.AttemptID, signResultReceipt(g, command, key)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.requestStop(g, "execution_finished"); err != nil {
		t.Fatal(err)
	}
	observing, release := make(chan struct{}), make(chan struct{})
	var entered sync.Once
	c.Kube.API.(*fake.Clientset).PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.(clienttesting.GetAction).GetName() == g.PodName {
			entered.Do(func() { close(observing) })
			<-release
		}
		return false, nil, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stopped := make(chan error, 1)
	var deliveryDone chan struct{}
	go func() { stopped <- c.reconcileAttempt(ctx, g.AttemptID) }()
	defer func() {
		close(release)
		<-stopped
		if deliveryDone != nil {
			<-deliveryDone
		}
	}()
	select {
	case <-observing:
	case <-ctx.Done():
		t.Fatal("shutdown did not reach the blocked Kubernetes observation")
	}
	// The real attempt reconciler holds its lock while Kubernetes is blocked.
	// Delivery must commit the authenticated outcome before observation resumes.
	delivered := make(chan error, 1)
	deliveryDone = make(chan struct{})
	go func() { defer close(deliveryDone); delivered <- c.reconcileDelivery(ctx, g.AttemptID) }()
	select {
	case err := <-delivered:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("blocked shutdown prevented authenticated result delivery")
	}
	backend.assertCompleted(t)
}

func TestAuthenticatedResultCompletesBeforeCleanupWithoutReleasingLiveStorage(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same-controller"
		if restart {
			name = "reopened-journal"
		}
		t.Run(name, func(t *testing.T) {
			c, g, _, options, _ := controllerFixture(t)
			backend := installResultLifecycleBackend(t, c, g)
			command, key := receiveRunningResult(t, c, g)
			if err := c.Store.RecordResultReceipt(g.AttemptID, signResultReceipt(g, command, key)); err != nil {
				t.Fatal(err)
			}
			if err := c.reconcileDelivery(t.Context(), g.AttemptID); err != nil {
				t.Fatal(err)
			}
			backend.assertCompleted(t)
			if restart {
				reopenStopRegressionStore(t, c, options)
				if err := reconcileFixture(t.Context(), c); err != nil {
					t.Fatal(err)
				}
			}
			pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(t.Context(), g.PodName, metav1.GetOptions{})
			if err != nil || pod.Status.Phase != corev1.PodRunning {
				t.Fatal("result delivery removed the worker before termination was observed", err)
			}
			if _, err := c.Store.Create(retryResultGrant(g)); !errors.Is(err, workspace.ErrStorageBusy) {
				t.Fatal("result delivery permitted reuse while the original writer was alive", err)
			}
			if err := c.claim(t.Context()); err != nil {
				t.Fatal(err)
			}
			if claimTaskAccepted(t, c, backend.nextID) {
				t.Fatal("result delivery released occupied worker capacity")
			}

			finishTaskPod(t, c, g)
			if err := reconcileFixture(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			// The controller has seen process exit, but the authentic flush seal
			// is still in flight. Recording it must remain valid after delivery.
			if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := c.Store.Seal(g.AttemptID, nativeStopRegressionReceipt(g, command, key)); err != nil {
				t.Fatal("late shutdown proof was rejected after result delivery", err)
			}
			if err := reconcileFixture(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err := c.claim(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !claimTaskAccepted(t, c, backend.nextID) {
				t.Fatal("confirmed worker shutdown did not release execution capacity")
			}
			retry, err := c.Store.Create(retryResultGrant(g))
			if err != nil || retry.TaskRoot != g.TaskRoot {
				t.Fatal("late authentic shutdown proof did not preserve reusable task storage", err)
			}
			backend.assertCompleted(t)
		})
	}
}

func TestDeliveredResultWithoutShutdownSealPreservesCompletionAndDirtyStorage(t *testing.T) {
	c, g, _, options, _ := controllerFixture(t)
	backend := installResultLifecycleBackend(t, c, g)
	command, key := receiveRunningResult(t, c, g)
	if err := c.Store.RecordResultReceipt(g.AttemptID, signResultReceipt(g, command, key)); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileDelivery(t.Context(), g.AttemptID); err != nil {
		t.Fatal(err)
	}
	backend.assertCompleted(t)
	original, err := c.Store.Terminal(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	reopenStopRegressionStore(t, c, options)
	finishTaskPod(t, c, g)
	if _, err := c.requestStop(g, "worker_shutdown_unproven"); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	// The worker exited without flushing; its receipt window already elapsed.
	if err := c.Store.ObserveStop(g.AttemptID, workspace.StopEvidence{Kind: "terminated", PodUID: g.PodUID, PVCUID: g.PVCUID, ObservedAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := reconcileFixture(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	closed, err := c.Store.Get(g.AttemptID)
	if err != nil || closed.State != "closed" || !closed.CleanupComplete {
		t.Fatal("missing shutdown seal retained terminated compute", err)
	}
	if _, err := c.Store.Create(retryResultGrant(g)); !errors.Is(err, workspace.ErrStorageBusy) {
		t.Fatal("missing shutdown proof permitted reuse of unflushed task data", err)
	}
	retained, err := c.Store.Terminal(g.AttemptID)
	if err != nil || retained.State != "delivered" || retained.RecoveryFailure != nil || !bytes.Equal(retained.Body, original.Body) {
		t.Fatal("shutdown failure replaced an already committed native result", err)
	}
	backend.assertCompleted(t)
}

func TestUntrustedResultReceiptCannotCompleteRunningTask(t *testing.T) {
	for _, invalid := range []string{"unsigned", "different-signer", "altered-result"} {
		t.Run(invalid, func(t *testing.T) {
			c, g, _, _, _ := controllerFixture(t)
			backend := installResultLifecycleBackend(t, c, g)
			command, key := receiveRunningResult(t, c, g)
			receipt := signResultReceipt(g, command, key)
			switch invalid {
			case "unsigned":
				receipt.Signature = nil
			case "different-signer":
				_, other, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				receipt = signResultReceipt(g, command, other)
			case "altered-result":
				receipt.RequestDigest = wire.Digest([]byte("a different result"))
			}
			_ = c.Store.RecordResultReceipt(g.AttemptID, receipt)
			if err := c.reconcileDelivery(t.Context(), g.AttemptID); err != nil {
				t.Fatal(err)
			}
			backend.mu.Lock()
			state, output := backend.state, backend.output
			backend.mu.Unlock()
			if state != "running" || output != "" {
				t.Fatal("an unauthenticated result completed the backend task")
			}
			if _, err := c.Store.Create(retryResultGrant(g)); !errors.Is(err, workspace.ErrStorageBusy) {
				t.Fatal("an unauthenticated result released live task storage", err)
			}
		})
	}
}

func receiveRunningResult(t *testing.T, c *Controller, g workspace.TaskGrant) (wire.SealCommand, ed25519.PrivateKey) {
	t.Helper()
	key := admitStopRegressionWorker(t, c, g)
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(t.Context(), g.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "worker-node"
	pod.Status.Phase = corev1.PodRunning
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: g.RuntimeRef.Image, ContainerID: "containerd://task-layout", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{StartedAt: metav1.Now(), FinishedAt: metav1.Now(), ContainerID: "containerd://task-layout"}}}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: g.RuntimeRef.Image, ContainerID: "containerd://worker", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	command, err := c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "completed native work"}})
	if err != nil {
		t.Fatal(err)
	}
	return command, key
}

func signResultReceipt(g workspace.TaskGrant, command wire.SealCommand, key ed25519.PrivateKey) workspace.ResultReceipt {
	r := workspace.ResultReceipt{TaskID: g.TaskID, AttemptID: g.AttemptID, PodUID: g.PodUID, PVCUID: g.PVCUID, RequestDigest: command.RequestDigest, Nonce: command.Nonce}
	r.Signature = ed25519.Sign(key, workspace.ResultReceiptMessage(r))
	return r
}

func retryResultGrant(g workspace.TaskGrant) workspace.TaskGrant {
	return workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}
}

type resultLifecycleBackend struct {
	mu            sync.Mutex
	state, output string
	nextID        string
}

func (b *resultLifecycleBackend) assertCompleted(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != "completed" || b.output != "completed native work" {
		t.Fatal("authenticated provider completion was not committed upstream", b.state)
	}
}

func installResultLifecycleBackend(t *testing.T, c *Controller, g workspace.TaskGrant) *resultLifecycleBackend {
	t.Helper()
	b := &resultLifecycleBackend{state: "running", nextID: uuid.NewString()}
	var next map[string]any
	if err := json.Unmarshal(g.Envelope, &next); err != nil {
		t.Fatal(err)
	}
	next["id"] = b.nextID
	pending := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		defer b.mu.Unlock()
		switch r.URL.Path {
		case "/api/agents/" + g.AgentID + "/tasks":
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": b.state}})
		case "/api/daemon/tasks/" + g.TaskID + "/complete":
			var result struct {
				Output string `json:"output"`
			}
			if json.NewDecoder(r.Body).Decode(&result) != nil {
				http.Error(w, "invalid result", http.StatusBadRequest)
				return
			}
			if b.state == "running" {
				b.state, b.output = "completed", result.Output
			}
			_, _ = w.Write([]byte(`{}`))
		case "/api/daemon/tasks/" + g.TaskID + "/fail":
			if b.state == "running" {
				b.state = "failed"
			}
			_, _ = w.Write([]byte(`{}`))
		case "/api/daemon/tasks/claim":
			batch := []any{}
			if pending && b.state == "completed" {
				batch, pending = []any{next}, false
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": batch})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	var err error
	c.API, err = daemonapi.NewClient(server.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	c.Capacity = 1
	c.runtimes = map[string]daemonapi.Bootstrap{g.RuntimeID: {Workspace: daemonapi.Workspace{ID: g.WorkspaceID}}}
	return b
}
