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
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestSessionCreationInterruptionKeepsExactIncarnation(t *testing.T) {
	for _, resource := range []string{"secrets", "pods"} {
		for _, committed := range []bool{false, true} {
			name := resource + "/before_commit"
			if committed {
				name = resource + "/lost_response"
			}
			t.Run(name, func(t *testing.T) {
				f := newSessionControllerFixture(t)
				id, issueID := uuid.NewString(), uuid.NewString()
				var envelope map[string]any
				if err := json.Unmarshal(f.Grant.Envelope, &envelope); err != nil {
					t.Fatal(err)
				}
				envelope["id"], envelope["issue_id"], envelope["auth_token"] = id, issueID, "mat_"+id
				raw, err := json.Marshal(envelope)
				if err != nil {
					t.Fatal(err)
				}
				row := f.Rows[f.Grant.TaskID]
				row.ID, row.IssueID, row.Status = id, issueID, "dispatched"
				f.Rows[id] = row
				grant, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, AgentID: f.Grant.AgentID,
					WorkspaceID: f.Grant.WorkspaceID, RuntimeID: f.Grant.RuntimeID, RuntimeRef: f.Grant.RuntimeRef,
					Metadata: f.Grant.Metadata, Envelope: raw, ResourceScope: []string{"issue:" + issueID}})
				if err != nil {
					t.Fatal(err)
				}
				grant, session := reserveConversationFixtureTurn(t, f.C, grant, true)
				prepareConversationFixtureTurn(t, f.C, grant)
				api := f.C.Kube.API.(*fake.Clientset)
				interrupted, committedUID := false, ""
				api.PrependReactor("create", resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
					if interrupted {
						return false, nil, nil
					}
					interrupted = true
					if !committed {
						return true, nil, context.DeadlineExceeded
					}
					object := action.(clienttesting.CreateAction).GetObject().DeepCopyObject()
					metadata := object.(metav1.Object)
					committedUID = uuid.NewString()
					metadata.SetUID(types.UID(committedUID))
					if err := api.Tracker().Create(action.GetResource(), object, action.GetNamespace()); err != nil {
						return true, nil, err
					}
					return true, nil, io.ErrUnexpectedEOF
				})
				if err := f.C.provisionTurn(t.Context(), grant); err == nil || !interrupted {
					t.Fatal("fixture did not interrupt the requested resource creation")
				}
				pending, err := f.C.Store.GetSession(session.ID)
				if err != nil {
					t.Fatal(err)
				}
				before, err := sessionRecord(pending)
				if err != nil || !before.SecretCreateRequested || resource == "pods" && !before.PodCreateRequested {
					t.Fatal("creation intent was not durable before the API call", err)
				}
				var admission wire.SessionAdmission
				if resource == "pods" && committed {
					public, _, err := ed25519.GenerateKey(rand.Reader)
					if err != nil {
						t.Fatal(err)
					}
					admission = wire.SessionAdmission{WorkerSessionID: session.ID, PodUID: committedUID,
						PVCUID: pending.PVCUID, BootstrapDigest: pending.BootstrapDigest, PublicKey: public}
					response := f.request(http.MethodPost, "/internal/worker-sessions/"+session.ID+"/admit", pending.ControlToken, admission)
					if response.Code != http.StatusServiceUnavailable {
						t.Fatalf("unobserved Pod UID must remain retryable: got HTTP %d", response.Code)
					}
					unchanged, err := f.C.Store.GetSession(session.ID)
					if err != nil || unchanged.PodUID != "" || len(unchanged.SupervisorKey) != 0 {
						t.Fatal("request identity supplied missing Pod or key authority", err)
					}
					for _, invalid := range []string{"pod", "pvc", "bootstrap", "key"} {
						bad := admission
						switch invalid {
						case "pod":
							bad.PodUID = ""
						case "pvc":
							bad.PVCUID = uuid.NewString()
						case "bootstrap":
							bad.BootstrapDigest = "invalid"
						case "key":
							bad.PublicKey = nil
						}
						response := f.request(http.MethodPost, "/internal/worker-sessions/"+session.ID+"/admit", pending.ControlToken, bad)
						if response.Code != http.StatusForbidden {
							t.Fatal("pending Pod observation masked invalid enrollment", invalid, response.Code)
						}
					}
				}
				reopenStopRegressionStore(t, f.C, f.Options)
				f.C.Policy.NodeSelector = map[string]string{"workload-pool": "changed-after-crash"}
				if err := f.C.provisionTurn(t.Context(), grant); !errors.Is(err, errPreparePending) {
					t.Fatal("recovery did not reach the original incarnation's enrollment", err)
				}
				recovered, err := f.C.Store.GetSession(session.ID)
				if err != nil {
					t.Fatal(err)
				}
				after, err := sessionRecord(recovered)
				if err != nil || !reflect.DeepEqual(before.PodRequest, after.PodRequest) || !reflect.DeepEqual(before.SecretRequest, after.SecretRequest) {
					t.Fatal("recovery rewrote the persisted Pod or Secret request", err)
				}
				if recovered.ID != session.ID || recovered.PodName != session.PodName || recovered.TurnSequence != session.TurnSequence || recovered.ActiveAttempt != grant.AttemptID {
					t.Fatal("recovery allocated another session or turn")
				}
				uid := after.Reference.SecretUID
				if resource == "pods" {
					uid = after.Reference.PodUID
				}
				if committed && uid != committedUID {
					t.Fatal("lost response caused the original object to be replaced")
				}
				pod, err := api.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), session.PodName, metav1.GetOptions{})
				if err != nil || string(pod.UID) != recovered.PodUID || pod.Spec.ActiveDeadlineSeconds != nil ||
					!reflect.DeepEqual(pod.Spec.NodeSelector, before.PodRequest.Spec.NodeSelector) {
					t.Fatal("recovery did not preserve the durable reusable Pod", err)
				}
				pods, err := api.CoreV1().Pods(f.C.Kube.Namespace).List(t.Context(), metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				matches := 0
				for _, observed := range pods.Items {
					if observed.Name == session.PodName {
						matches++
					}
				}
				if matches != 1 || f.Starts[id] != 0 || recovered.State != workspace.SessionCreating {
					t.Fatal("creation recovery duplicated or prematurely started the turn")
				}
				if admission.WorkerSessionID != "" {
					pod.Spec.NodeName = "worker-node"
					if _, err := api.CoreV1().Pods(f.C.Kube.Namespace).Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
					pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: f.C.RuntimeRef.Image}}
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: f.C.RuntimeRef.Image}}
					if _, err := api.CoreV1().Pods(f.C.Kube.Namespace).UpdateStatus(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
					wrongPod := admission
					wrongPod.PodUID = uuid.NewString()
					if response := f.request(http.MethodPost, "/internal/worker-sessions/"+session.ID+"/admit", recovered.ControlToken, wrongPod); response.Code != http.StatusForbidden {
						t.Fatal("an observed Pod UID mismatch became retryable", response.Code)
					}
					for range 2 {
						response := f.request(http.MethodPost, "/internal/worker-sessions/"+session.ID+"/admit", recovered.ControlToken, admission)
						if response.Code != http.StatusOK {
							t.Fatal("the original worker could not enroll after UID recovery", response.Code, response.Body.String())
						}
					}
					otherKey, _, err := ed25519.GenerateKey(rand.Reader)
					if err != nil {
						t.Fatal(err)
					}
					changed := admission
					changed.PublicKey = otherKey
					if response := f.request(http.MethodPost, "/internal/worker-sessions/"+session.ID+"/admit", recovered.ControlToken, changed); response.Code != http.StatusForbidden {
						t.Fatal("recovered enrollment allowed supervisor key replacement", response.Code)
					}
				}
			})
		}
	}
}

func TestSessionLostCreateWithPendingFailureReleasesCapacity(t *testing.T) {
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
	queued, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, AgentID: f.Grant.AgentID,
		WorkspaceID: f.Grant.WorkspaceID, RuntimeID: f.Grant.RuntimeID, RuntimeRef: f.Grant.RuntimeRef,
		Metadata: f.Grant.Metadata, Envelope: envelope, ResourceScope: []string{"issue:" + issueID}})
	if err != nil {
		t.Fatal(err)
	}
	grant, session := reserveConversationFixtureTurn(t, f.C, queued, true)
	prepareConversationFixtureTurn(t, f.C, grant)
	api := f.C.Kube.API.(*fake.Clientset)
	committedUID, creates := uuid.NewString(), 0
	api.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.CreateAction).GetObject().DeepCopyObject()
		metadata := object.(metav1.Object)
		if metadata.GetName() != session.PodName {
			return false, nil, nil
		}
		creates++
		if creates > 1 {
			return false, nil, nil
		}
		metadata.SetUID(types.UID(committedUID))
		metadata.SetResourceVersion("1")
		if err := api.Tracker().Create(action.GetResource(), object, action.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, nil, io.ErrUnexpectedEOF
	})
	if err := f.C.provisionTurn(t.Context(), grant); err == nil || creates != 1 {
		t.Fatal("fixture did not lose the committed Pod creation response", err)
	}
	if err := f.C.recordFailure(grant, context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	before, err := f.C.Store.Terminal(grant.AttemptID)
	if err != nil || before.Source != "controller" || before.PodUID != "" || before.State != "received" {
		t.Fatal("failure did not precede Pod UID observation", err)
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	for range 2 {
		if err := f.C.ensureSessionResources(t.Context(), session.ID); err != nil {
			t.Fatal("late UID recovery blocked the pending failure", err)
		}
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	recovered, err := f.C.Store.GetSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := f.C.Store.Terminal(grant.AttemptID)
	if err != nil || recovered.PodUID != committedUID || after.PodUID != committedUID ||
		after.RequestDigest != before.RequestDigest || after.ReceiptID != before.ReceiptID || !bytes.Equal(after.Body, before.Body) {
		t.Fatal("UID recovery replaced the original failure or Pod", err)
	}
	grants, err := f.C.Store.ReconcileGrants()
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := f.C.Store.ListSessions()
	if err != nil {
		t.Fatal(err)
	}
	f.C.observeCapacity(grants, sessions)
	if f.C.lastCapacity.active != 2 || f.C.lastCapacity.resident != 2 {
		t.Fatal("unproven Pod termination released capacity")
	}
	// The existing fake Kubernetes API retains the finalizer after deletion.
	// The second observation proves that this unbound Pod never reached a node.
	for range 2 {
		if err := f.C.reconcileSession(t.Context(), session.ID); err != nil {
			t.Fatal(err)
		}
	}
	recovered, err = f.C.Store.GetSession(session.ID)
	if err != nil || recovered.State != workspace.SessionClosed || !recovered.ResourcesCleaned ||
		recovered.Stop.Evidence == nil || recovered.Stop.Evidence.Kind != "never-started" || f.Starts[id] != 0 {
		t.Fatal("recovered failed startup did not reach proven cleanup", err)
	}
	if _, err := api.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), session.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("recovered Pod remained after cleanup", err)
	}
	if _, err := api.CoreV1().Secrets(f.C.Kube.Namespace).Get(t.Context(), session.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("recovered Secret remained after cleanup", err)
	}
	if err := f.C.reconcileDelivery(t.Context(), grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	after, err = f.C.Store.Terminal(grant.AttemptID)
	if err != nil || after.State != "delivered" || after.PodUID != committedUID || !bytes.Equal(after.Body, before.Body) {
		t.Fatal("cleanup lost or changed the pending failure", err)
	}
	grants, err = f.C.Store.ReconcileGrants()
	if err != nil {
		t.Fatal(err)
	}
	sessions, err = f.C.Store.ListSessions()
	if err != nil {
		t.Fatal(err)
	}
	f.C.observeCapacity(grants, sessions)
	if f.C.lastCapacity.active != 1 || f.C.lastCapacity.resident != 1 {
		t.Fatal("proven cleanup retained execution or resident capacity")
	}
}

func TestSessionConsumedStartCannotRepeatAfterReopen(t *testing.T) {
	f := newSessionControllerFixture(t)
	before := f.Grant
	token, err := f.C.Store.CapabilityToken(before.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	f.refresh(t)
	response := f.request(http.MethodPost, "/internal/attempts/"+before.AttemptID+"/start", token, struct{}{})
	if response.Code >= 200 && response.Code < 300 || f.Starts[before.TaskID] != 1 || !f.Grant.StartConfirmed ||
		f.Grant.InputDigest != before.InputDigest || f.Grant.WorkerSessionID != before.WorkerSessionID || f.Grant.TurnSequence != before.TurnSequence {
		t.Fatal("a consumed start changed identity or invoked the backend after recovery")
	}
	pod, err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), f.Session.PodName, metav1.GetOptions{})
	if err != nil || pod.Status.Phase != corev1.PodRunning || string(pod.UID) != before.PodUID {
		t.Fatal("start replay changed the admitted Pod", err)
	}
}
