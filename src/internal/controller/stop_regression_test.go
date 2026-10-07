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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestCancelledResourceCreateLostACKResolvesAndCleansAfterRestart(t *testing.T) {
	for _, resource := range []string{"secrets", "pods"} {
		t.Run(resource, func(t *testing.T) {
			c, g, _, options, assignment := controllerFixtureBeforeProvision(t)
			ctx := context.Background()
			api := c.Kube.API.(*fake.Clientset)
			var committed metav1.Object
			api.PrependReactor("create", resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
				if committed != nil {
					return false, nil, nil
				}
				object := action.(clienttesting.CreateAction).GetObject().DeepCopyObject()
				metadata := object.(metav1.Object)
				metadata.SetUID(types.UID(uuid.NewString()))
				metadata.SetResourceVersion("1")
				if err := api.Tracker().Create(action.GetResource(), object, action.GetNamespace()); err != nil {
					return true, nil, err
				}
				committed = metadata
				return true, nil, io.ErrUnexpectedEOF
			})
			if err := c.provision(ctx, g); err == nil {
				t.Fatal("fixture failed to lose the acknowledged creation response")
			}
			if committed == nil {
				t.Fatal("fixture did not persist the attempt resource before losing its response")
			}
			if resource == "pods" {
				finishTaskPod(t, c, g)
			}
			current := *assignment.Load()
			current.Status = "cancelled"
			assignment.Store(&current)
			reopenStopRegressionStore(t, c, options)
			if err := reconcileFixture(ctx, c); err != nil {
				t.Fatal("cancellation could not recover the committed resource after restart", err)
			}
			closed, err := c.Store.Get(g.AttemptID)
			if err != nil || closed.State != "closed" || !closed.CleanupComplete {
				t.Fatal("cancelled creation retained stopped compute or unfinished cleanup", err)
			}
			resources, err := record(closed)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Kube.API.CoreV1().Secrets(c.Kube.Namespace).Get(ctx, resources.Reference.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatal("unacknowledged bootstrap authority survived cancellation cleanup", err)
			}
			if resource == "pods" {
				if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, committed.GetName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Fatal("unacknowledged worker survived cancellation cleanup", err)
				}
			}
		})
	}
}

func TestLostPodCreateRetainsAuthorityAfterTemplateChange(t *testing.T) {
	c, g, _, options, _ := controllerFixtureBeforeProvision(t)
	ctx := context.Background()
	api := c.Kube.API.(*fake.Clientset)
	var committed *corev1.Pod
	api.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		pod := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
		pod.UID = types.UID(uuid.NewString())
		if err := api.Tracker().Create(action.GetResource(), pod, action.GetNamespace()); err != nil {
			return true, nil, err
		}
		committed = pod
		return true, nil, io.ErrUnexpectedEOF
	})
	if err := c.provision(ctx, g); !errors.Is(err, io.ErrUnexpectedEOF) || committed == nil {
		t.Fatal("fixture did not commit a Pod and lose its creation response", err)
	}
	reopenStopRegressionStore(t, c, options)
	// A rollout changes the desired template, but cannot change the authority
	// already issued to a Pod whose creation response was lost.
	c.Policy.NodeSelector = map[string]string{"workload-pool": "next"}
	if err := c.provision(ctx, g); err != nil {
		t.Fatal("template change prevented recovery of the already-created worker", err)
	}
	reopenStopRegressionStore(t, c, options)
	recovered, err := c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.PodUID != string(committed.UID) || recovered.StorageID != g.StorageID {
		t.Fatal("recovery did not preserve the original worker and storage authority")
	}
	r, err := record(recovered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Kube.PodState(ctx, r.Reference); err != nil {
		t.Fatal("recovered worker no longer matches its durable creation authority", err)
	}
}

func TestSealedNativeResultReleasesComputeDuringBackendOutage(t *testing.T) {
	c, g, _, options, _ := controllerFixture(t)
	ctx := context.Background()
	key := admitStopRegressionWorker(t, c, g)
	output := "Completed work remains available after backend recovery"
	command, err := c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: output}})
	if err != nil {
		t.Fatal(err)
	}
	receipt := nativeStopRegressionReceipt(g, command, key)
	if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.Seal(g.AttemptID, receipt); err != nil {
		t.Fatal(err)
	}
	finishTaskPod(t, c, g)
	var unavailable atomic.Bool
	unavailable.Store(true)
	var backendMu sync.Mutex
	var completedOutputs []string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if unavailable.Load() {
			http.Error(w, "backend unavailable", http.StatusServiceUnavailable)
			return
		}
		backendMu.Lock()
		defer backendMu.Unlock()
		switch r.URL.Path {
		case "/api/agents/" + g.AgentID + "/tasks":
			status := "running"
			if len(completedOutputs) > 0 {
				status = "completed"
			}
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": status}})
		case "/api/daemon/tasks/" + g.TaskID + "/complete":
			var result struct {
				Output string `json:"output"`
			}
			if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
				http.Error(w, "invalid result", http.StatusBadRequest)
				return
			}
			// Preserve completed task output as the backend's effect. Replaying
			// an accepted result would create another completion output here.
			completedOutputs = append(completedOutputs, result.Output)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = reconcileFixture(ctx, c) // Delivery is unavailable; compute cleanup must still commit.
	closed, err := c.Store.Get(g.AttemptID)
	if err != nil || closed.State != "closed" || !closed.CleanupComplete {
		t.Fatal("backend outage retained a sealed and stopped worker", err)
	}
	reopenStopRegressionStore(t, c, options)
	terminal, err := c.Store.Terminal(g.AttemptID)
	if err != nil || terminal.State != "sealed" {
		t.Fatal("cleanup or restart lost the undelivered native result", err)
	}
	if _, err := c.Store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}); !errors.Is(err, workspace.ErrStorageBusy) {
		t.Fatal("task was re-executed while its native completion remained unresolved", err)
	}
	unavailable.Store(false)
	if err := reconcileFixture(ctx, c); err != nil {
		t.Fatal("backend recovery did not resolve the preserved completion", err)
	}
	reopenStopRegressionStore(t, c, options)
	if err := reconcileFixture(ctx, c); err != nil {
		t.Fatal("completed delivery did not survive controller restart", err)
	}
	terminal, err = c.Store.Terminal(g.AttemptID)
	if err != nil || terminal.State != "delivered" {
		t.Fatal("native completion was not durably delivered", err)
	}
	backendMu.Lock()
	defer backendMu.Unlock()
	if len(completedOutputs) != 1 || completedOutputs[0] != output {
		t.Fatal("backend recovery lost or duplicated the completed task output", completedOutputs)
	}
}

func TestTerminatingPodPreservesAuthenticatedNativeResultAndSeal(t *testing.T) {
	for name, replaced := range map[string]bool{"original worker": false, "replacement Pod": true} {
		t.Run(name, func(t *testing.T) {
			c, g, _, options, _ := controllerFixture(t)
			ctx := context.Background()
			key := admitStopRegressionWorker(t, c, g)
			finishTaskPod(t, c, g)
			pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, g.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// The provider has finished but its supervisor is reporting during normal
			// Kubernetes deletion, before the worker container's actual termination.
			pod.Status.Phase = corev1.PodRunning
			pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}
			stamp := metav1.Now()
			pod.DeletionTimestamp = &stamp
			if replaced {
				pod.UID = types.UID(uuid.NewString())
			}
			if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			token, err := c.Store.CapabilityToken(g.AttemptID, "supervisor")
			if err != nil {
				t.Fatal(err)
			}
			post := func(action string, body any) {
				t.Helper()
				raw, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				r := httptest.NewRequest(http.MethodPost, "/internal/attempts/"+g.AttemptID+"/"+action, bytes.NewReader(raw))
				r.Header.Set("Authorization", "Bearer "+token)
				c.Handler().ServeHTTP(httptest.NewRecorder(), r)
			}
			result := wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "Native result before termination"}}
			if err := c.admission(ctx, g); err == nil {
				t.Fatal("terminating Pod retained execution authority")
			}
			post("result", result)
			if replaced {
				if _, err := c.Store.Terminal(g.AttemptID); !errors.Is(err, workspace.ErrUnauthorized) {
					t.Fatal("replacement Pod acquired the original worker's result authority", err)
				}
				return
			}
			terminal, err := c.Store.Terminal(g.AttemptID)
			if err != nil || terminal.Source != "worker" || terminal.Kind != "complete" {
				t.Fatal("terminating worker could not preserve its actual provider result", err)
			}
			if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			post("receipt", nativeStopRegressionReceipt(g, wire.SealCommand{RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}, key))
			if err := c.recordFailure(g, errors.New("worker terminating")); err != nil {
				t.Fatal(err)
			}
			reopenStopRegressionStore(t, c, options)
			terminal, err = c.Store.Terminal(g.AttemptID)
			if err != nil || terminal.State != "sealed" || terminal.Source != "worker" || terminal.Kind != "complete" {
				t.Fatal("shutdown observation replaced or unsealed the authenticated native result", err)
			}
		})
	}
}

func admitStopRegressionWorker(t *testing.T, c *Controller, g workspace.TaskGrant) ed25519.PrivateKey {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
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
	return key
}

func nativeStopRegressionReceipt(g workspace.TaskGrant, command wire.SealCommand, key ed25519.PrivateKey) workspace.SignedReceipt {
	r := workspace.SignedReceipt{TaskID: g.TaskID, AttemptID: g.AttemptID, PodUID: g.PodUID, PVCUID: g.PVCUID, RequestDigest: command.RequestDigest, Nonce: command.Nonce, WritersStopped: true, FlushOK: true}
	r.Signature = ed25519.Sign(key, workspace.ReceiptMessage(r))
	return r
}

func reopenStopRegressionStore(t *testing.T, c *Controller, options workspace.Options) {
	t.Helper()
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
}
