package controller

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
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

func TestFailedStartupEndsTaskAndReleasesCapacityBeforePreparationExpires(t *testing.T) {
	for _, failure := range []string{"init", "worker"} {
		t.Run(failure, func(t *testing.T) {
			c, g, _, _, _ := controllerFixture(t)
			finishTaskPod(t, c, g)
			pods := c.Kube.API.CoreV1().Pods(c.Kube.Namespace)
			if failure == "init" {
				pod, err := pods.Get(t.Context(), g.PodName, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 1
				pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodInitialized, Status: corev1.ConditionFalse}}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}}
				if _, err := pods.UpdateStatus(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			nextID := uuid.NewString()
			var mu sync.Mutex
			backendState := "dispatched"
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/agents/" + g.AgentID + "/tasks":
					mu.Lock()
					status := backendState
					mu.Unlock()
					_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": status}})
				case "/api/daemon/tasks/" + g.TaskID + "/fail":
					mu.Lock()
					backendState = "failed"
					mu.Unlock()
					_, _ = w.Write([]byte(`{}`))
				case "/api/daemon/tasks/claim":
					_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{"id": nextID, "runtime_id": g.RuntimeID, "workspace_id": g.WorkspaceID, "agent_id": g.AgentID, "agent": map[string]string{"id": g.AgentID}, "auth_token": "mat_local_task", "dispatched_at": "2026-09-12T00:00:00Z"}}})
				default:
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer backend.Close()
			var err error
			c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := reconcileFixture(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			settled := backendState == "failed"
			mu.Unlock()
			if !settled {
				t.Fatal("observed startup failure left the backend task active")
			}
			if _, err := pods.Get(t.Context(), g.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatal("confirmed terminated startup retained its Pod", err)
			}
			if failure == "worker" {
				if _, err := c.Store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}); !errors.Is(err, workspace.ErrStorageBusy) {
					t.Fatal("failed worker without a flush receipt released its task data", err)
				}
			}
			c.Capacity = 1
			c.runtimes = map[string]daemonapi.Bootstrap{g.RuntimeID: {Workspace: daemonapi.Workspace{ID: g.WorkspaceID}, Runtime: daemonapi.Runtime{ID: g.RuntimeID}}}
			if err := c.claim(t.Context()); err != nil {
				t.Fatal(err)
			}
			grants, err := c.Store.List()
			if err != nil {
				t.Fatal(err)
			}
			for _, grant := range grants {
				if grant.TaskID == nextID {
					return
				}
			}
			t.Fatal("failed startup kept an unrelated task from acquiring capacity")
		})
	}
}

func TestStartupFailureObservationPreservesConcurrentAuthority(t *testing.T) {
	for _, progress := range []string{"admission", "result", "cancellation"} {
		t.Run(progress, func(t *testing.T) {
			c, g, _, _, _ := controllerFixture(t)
			finishTaskPod(t, c, g)
			entered, release := make(chan struct{}), make(chan struct{})
			var observeOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			c.Kube.API.(*fake.Clientset).PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if action.(clienttesting.GetAction).GetName() == g.PodName {
					observeOnce.Do(func() { close(entered); <-release })
				}
				return false, nil, nil
			})
			finished := make(chan struct{})
			var reconcileErr error
			go func() { reconcileErr = c.reconcileAttempt(t.Context(), g.AttemptID); close(finished) }()
			t.Cleanup(func() { unblock(); <-finished })
			select {
			case <-entered:
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			switch progress {
			case "admission":
				public, _, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Store.BindPod(g.AttemptID, g.PodName, g.PodUID, "worker-node"); err != nil {
					t.Fatal(err)
				}
				if err := c.Store.Admit(g.AttemptID, public); err != nil {
					t.Fatal(err)
				}
			case "result":
				key := admitStopRegressionWorker(t, c, g)
				command, err := c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "completed before supervisor shutdown"}})
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
					t.Fatal(err)
				}
				if err := c.Store.Seal(g.AttemptID, nativeStopRegressionReceipt(g, command, key)); err != nil {
					t.Fatal(err)
				}
			case "cancellation":
				if _, err := c.requestStop(g, "backend_cancelled"); err != nil {
					t.Fatal(err)
				}
			}
			unblock()
			<-finished
			if reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			switch progress {
			case "admission":
				if _, allowed, err := c.Store.Offer(g.AttemptID); err != nil || !allowed {
					t.Fatal("stale startup observation revoked the admitted execution", err)
				}
			case "result":
				if err := c.Store.BeginForward(g.AttemptID); err != nil {
					t.Fatal("stale startup failure displaced the worker's sealed result", err)
				}
			case "cancellation":
				if _, err := c.Store.Terminal(g.AttemptID); !errors.Is(err, workspace.ErrUnauthorized) {
					t.Fatal("stale startup failure fabricated a result for a cancelled task", err)
				}
			}
		})
	}
}
