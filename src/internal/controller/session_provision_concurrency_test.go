package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubeapi "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	clienttesting "k8s.io/client-go/testing"
)

func TestSessionCreationAndCleanupShareOneOwner(t *testing.T) {
	f := newPendingSessionProvisionFixture(t)
	grant, session := f.Grant, f.Session
	api := f.C.Kube.API
	barrier := &sessionCreateBarrier{name: session.PodName, entered: make(chan struct{}), release: make(chan struct{})}
	f.C.Kube.API = sessionCreateAPI{Interface: api, barrier: barrier}
	done := make(chan struct{})
	var provisionErr error
	go func() {
		provisionErr = f.C.reconcileAttempt(t.Context(), grant.AttemptID)
		close(done)
	}()
	var releaseOnce sync.Once
	finish := func() {
		releaseOnce.Do(func() { close(barrier.release) })
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("session provisioning did not finish after releasing the Pod POST")
		}
	}
	t.Cleanup(finish)
	select {
	case <-barrier.entered:
	case <-done:
		t.Fatal("session did not reach its original Pod POST", provisionErr)
	case <-time.After(5 * time.Second):
		t.Fatal("session did not issue its original Pod POST")
	}
	before := observeProvisionCapacity(t, f)
	if _, err := f.C.requestStop(grant, "cancelled_during_creation"); err != nil {
		t.Fatal("slow creation prevented durable stop recording", err)
	}
	for range 2 {
		if err := f.C.reconcileSession(t.Context(), session.ID); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := f.C.Store.GetSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	prematureCleanup := pending.ResourcesCleaned || pending.State == workspace.SessionClosed
	pendingCapacity := observeProvisionCapacity(t, f)
	finish()
	if provisionErr != nil {
		t.Fatal("stopped creation did not return to reconciliation", provisionErr)
	}
	if prematureCleanup || pendingCapacity != before {
		t.Error("cleanup or capacity release overtook the original Pod POST")
	}
	pod, err := api.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), session.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal("original Pod was not available for exact-UID cleanup", err)
	}
	createdUID := string(pod.UID)
	for range 2 {
		if err := f.C.reconcileSession(t.Context(), session.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.C.provisionTurn(t.Context(), grant); err != nil {
		t.Error("stale provisioning did not observe the closed session", err)
	}
	closed, err := f.C.Store.GetSession(session.ID)
	if err != nil || closed.State != workspace.SessionClosed || !closed.ResourcesCleaned || closed.PodUID != createdUID {
		t.Error("creation and cleanup did not retain one exact Pod incarnation", err)
	}
	if _, err := api.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), session.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Error("a Pod remained after the session recorded resource cleanup", err)
	}
	if _, err := api.CoreV1().Secrets(f.C.Kube.Namespace).Get(t.Context(), session.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Error("session cleanup left its bootstrap Secret", err)
	}
}

// Pause before entering client-go's globally locked fake reactor chain, so the
// real stop reconciler can independently observe, create, and clean resources.
type sessionCreateBarrier struct {
	name             string
	entered, release chan struct{}
	creates          atomic.Int32
}

type sessionCreateAPI struct {
	kubeapi.Interface
	barrier *sessionCreateBarrier
}

func (a sessionCreateAPI) CoreV1() typedcorev1.CoreV1Interface {
	return sessionCreateCore{CoreV1Interface: a.Interface.CoreV1(), barrier: a.barrier}
}

type sessionCreateCore struct {
	typedcorev1.CoreV1Interface
	barrier *sessionCreateBarrier
}

func (c sessionCreateCore) Pods(namespace string) typedcorev1.PodInterface {
	return sessionCreatePods{PodInterface: c.CoreV1Interface.Pods(namespace), barrier: c.barrier}
}

type sessionCreatePods struct {
	typedcorev1.PodInterface
	barrier *sessionCreateBarrier
}

func (p sessionCreatePods) Create(ctx context.Context, pod *corev1.Pod, options metav1.CreateOptions) (*corev1.Pod, error) {
	if pod.Name == p.barrier.name && p.barrier.creates.Add(1) == 1 {
		close(p.barrier.entered)
		select {
		case <-p.barrier.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.PodInterface.Create(ctx, pod, options)
}

func observeProvisionCapacity(t *testing.T, f *sessionControllerFixture) [2]int {
	t.Helper()
	grants, err := f.C.Store.ReconcileGrants()
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := f.C.Store.ListSessions()
	if err != nil {
		t.Fatal(err)
	}
	f.C.observeCapacity(grants, sessions)
	return [2]int{f.C.lastCapacity.active, f.C.lastCapacity.resident}
}

func TestTurnAnnotationConflictsPreserveTheCurrentGrant(t *testing.T) {
	for _, scenario := range []string{"one conflict", "persistent conflict", "replacement", "forbidden"} {
		t.Run(scenario, func(t *testing.T) {
			f := newPendingSessionProvisionFixture(t)
			enrollPendingSessionProvisionFixture(t, f)
			before := f.Grant
			api := f.C.Kube.API.(*fake.Clientset)
			var patches atomic.Int32
			var contentionEnded atomic.Bool
			api.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				patch := action.(clienttesting.PatchAction)
				if patch.GetName() != before.PodName || patch.GetPatchType() != types.MergePatchType {
					return false, nil, nil
				}
				attempt := patches.Add(1)
				if scenario == "forbidden" {
					return true, nil, apierrors.NewForbidden(corev1.Resource("pods"), before.PodName, errors.New("patch denied"))
				}
				object, err := api.Tracker().Get(action.GetResource(), action.GetNamespace(), patch.GetName())
				if err != nil {
					return true, nil, err
				}
				pod := object.(*corev1.Pod)
				if attempt == 1 || scenario == "persistent conflict" && !contentionEnded.Load() {
					pod.ResourceVersion = uuid.NewString()
					if pod.Annotations == nil {
						pod.Annotations = make(map[string]string)
					}
					pod.Annotations["fixture-external"] = "retained"
					if scenario == "replacement" {
						pod.UID = types.UID(uuid.NewString())
					}
					if err := api.Tracker().Update(action.GetResource(), pod, action.GetNamespace()); err != nil {
						return true, nil, err
					}
					return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name, errors.New("resource version changed"))
				}
				var body struct {
					Metadata struct {
						UID             types.UID `json:"uid"`
						ResourceVersion string    `json:"resourceVersion"`
					} `json:"metadata"`
				}
				if json.Unmarshal(patch.GetPatch(), &body) != nil || body.Metadata.UID != pod.UID || body.Metadata.ResourceVersion != pod.ResourceVersion {
					return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name, errors.New("stale annotation precondition"))
				}
				return false, nil, nil
			})
			if err := f.C.reconcileAttempt(t.Context(), before.AttemptID); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			if scenario == "replacement" || scenario == "forbidden" {
				terminal, err := f.C.Store.Terminal(before.AttemptID)
				if err != nil || terminal.Source != "controller" || !f.Grant.ExecutionRevoked || len(f.Grant.Assignment) != 0 {
					t.Fatal("annotation retry bypassed a permanent identity or permission failure", err)
				}
				return
			}
			if f.Grant.ExecutionRevoked || f.Grant.Stop != nil || f.Session.Stop != nil || !bytes.Equal(f.Grant.Execution, before.Execution) {
				t.Fatal("annotation contention failed or changed the prepared turn")
			}
			if _, err := f.C.Store.Terminal(before.AttemptID); !errors.Is(err, workspace.ErrUnauthorized) {
				t.Fatal("annotation contention created a failure outbox", err)
			}
			if scenario == "persistent conflict" {
				if f.Grant.State != "intent" || len(f.Grant.Assignment) != 0 {
					t.Fatal("exhausted annotation conflicts did not remain retryable")
				}
				contentionEnded.Store(true)
				if err := f.C.reconcileAttempt(t.Context(), before.AttemptID); err != nil {
					t.Fatal(err)
				}
				f.refresh(t)
			}
			assignment, err := wire.DecodeTurnAssignment(f.Grant.Assignment)
			if err != nil || f.Grant.State != "assigned" || f.Grant.PodUID != before.PodUID || f.Grant.WorkerSessionID != before.WorkerSessionID ||
				f.Grant.TurnSequence != before.TurnSequence || assignment.Bootstrap.AttemptID != before.AttemptID {
				t.Fatal("annotation retry replaced or lost the prepared assignment", err)
			}
			pod, err := api.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), before.PodName, metav1.GetOptions{})
			if err != nil || pod.Annotations["fixture-external"] != "retained" {
				t.Fatal("annotation retry lost a concurrent metadata change", err)
			}
		})
	}
}

func newPendingSessionProvisionFixture(t *testing.T) *sessionControllerFixture {
	t.Helper()
	f := newSessionControllerFixture(t)
	id, issueID := uuid.NewString(), uuid.NewString()
	var envelope map[string]any
	if err := json.Unmarshal(f.Grant.Envelope, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["id"], envelope["issue_id"], envelope["auth_token"] = id, issueID, "mat_"+id
	raw, _ := json.Marshal(envelope)
	f.mu.Lock()
	row := f.Rows[f.Grant.TaskID]
	row.ID, row.IssueID, row.Status, row.StartedAt = id, issueID, "dispatched", ""
	f.Rows[id] = row
	f.mu.Unlock()
	grant, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, AgentID: f.Grant.AgentID,
		WorkspaceID: f.Grant.WorkspaceID, RuntimeID: f.Grant.RuntimeID, RuntimeRef: f.Grant.RuntimeRef,
		Metadata: f.Grant.Metadata, Envelope: raw, ResourceScope: []string{"issue:" + issueID}})
	if err != nil {
		t.Fatal(err)
	}
	f.Grant, f.Session = reserveConversationFixtureTurn(t, f.C, grant, true)
	prepareConversationFixtureTurn(t, f.C, f.Grant)
	f.refresh(t)
	return f
}

func enrollPendingSessionProvisionFixture(t *testing.T, f *sessionControllerFixture) {
	t.Helper()
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
	f.request(http.MethodPost, "/internal/worker-sessions/"+f.Session.ID+"/admit", f.Session.ControlToken, admission)
	f.refresh(t)
	if !bytes.Equal(f.Session.SupervisorKey, public) || f.Session.PodUID != string(pod.UID) ||
		f.Session.NodeID != pod.Spec.NodeName || f.Grant.PodUID != f.Session.PodUID {
		t.Fatal("session enrollment did not commit the observed Pod and supervisor key")
	}
}
