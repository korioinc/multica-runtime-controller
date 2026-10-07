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
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// Drive one recovery sweep through the production attempt worker. These tests
// exercise durable transitions; live event interleavings are covered separately.
func reconcileFixture(ctx context.Context, c *Controller) error {
	grants, err := c.Store.ReconcileGrants()
	if err != nil {
		return err
	}
	q := newDispatchQueues()
	c.mu.Lock()
	prior := c.dispatch
	c.dispatch = q
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.dispatch = prior
		c.mu.Unlock()
	}()
	for _, g := range grants {
		if g.State == "closed" {
			q.deliveries.Add(g.AttemptID)
		} else {
			q.attempts.Add(g.AttemptID)
		}
	}
	q.attempts.ShutDown()
	c.runQueue(ctx, q.attempts, c.reconcileAttempt)
	q.deliveries.ShutDown()
	c.runQueue(ctx, q.deliveries, c.reconcileDelivery)
	return nil
}

func controllerFixture(t *testing.T, repositories ...workspace.Repository) (*Controller, workspace.TaskGrant, *atomic.Bool, workspace.Options, *atomic.Pointer[daemonapi.TaskAssignment]) {
	t.Helper()
	c, g, completed, options, assignment := controllerFixtureBeforeProvision(t, repositories...)
	if err := c.provision(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	g, err := c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return c, g, completed, options, assignment
}

func controllerFixtureBeforeProvision(t *testing.T, repositories ...workspace.Repository) (*Controller, workspace.TaskGrant, *atomic.Bool, workspace.Options, *atomic.Pointer[daemonapi.TaskAssignment]) {
	t.Helper()
	c, g, completed, options, assignment, prepared := controllerFixturePreparing(t, repositories...)
	if err := c.Store.CompletePreparation(g.AttemptID, &prepared, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	g, err := c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return c, g, completed, options, assignment
}

func controllerFixturePreparing(t *testing.T, repositories ...workspace.Repository) (*Controller, workspace.TaskGrant, *atomic.Bool, workspace.Options, *atomic.Pointer[daemonapi.TaskAssignment], workspace.Prepared) {
	t.Helper()
	completed := new(atomic.Bool)
	taskID := uuid.NewString()
	runtimeID, workspaceID, agentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	assignment := new(atomic.Pointer[daemonapi.TaskAssignment])
	assignment.Store(&daemonapi.TaskAssignment{RuntimeID: runtimeID, DispatchedAt: "2026-09-12T00:00:00Z", Status: "running"})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/"+agentID+"/tasks" {
			current := assignment.Load()
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": taskID, "agent_id": agentID, "workspace_id": workspaceID, "runtime_id": current.RuntimeID, "dispatched_at": current.DispatchedAt, "status": current.Status}})
			return
		}
		if r.URL.Path == "/api/daemon/tasks/"+taskID+"/complete" {
			completed.Store(true)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(backend.Close)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := workspace.Options{Directory: filepath.Join(root, "journal"), OwnerID: uuid.NewString()}
	store, err := workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	owner := kubernetes.Owner{Name: "controller", UID: uuid.NewString()}
	kubeAPI := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: owner.Name, Namespace: "fixture", UID: types.UID(owner.UID)}})
	kubeAPI.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		action.(clienttesting.CreateAction).GetObject().(metav1.Object).SetUID(types.UID(uuid.NewString()))
		action.(clienttesting.CreateAction).GetObject().(metav1.Object).SetResourceVersion("1")
		return false, nil, nil
	})
	// The client-go tracker deletes immediately despite Pod finalizers. Model
	// the API's retained termination evidence until cleanup releases it.
	kubeAPI.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deletion := action.(clienttesting.DeleteAction)
		object, err := kubeAPI.Tracker().Get(action.GetResource(), action.GetNamespace(), deletion.GetName())
		if err != nil {
			return true, nil, err
		}
		pod := object.(*corev1.Pod)
		if len(pod.Finalizers) == 0 {
			return false, nil, nil
		}
		if pod.DeletionTimestamp == nil {
			stamp := metav1.Now()
			pod.DeletionTimestamp = &stamp
		}
		return true, nil, kubeAPI.Tracker().Update(action.GetResource(), pod, action.GetNamespace())
	})
	kubeAPI.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		patch := action.(clienttesting.PatchAction)
		var changes []struct {
			Op, Path string
			Value    json.RawMessage
		}
		if patch.GetPatchType() != types.JSONPatchType || json.Unmarshal(patch.GetPatch(), &changes) != nil {
			return false, nil, nil
		}
		object, err := kubeAPI.Tracker().Get(action.GetResource(), action.GetNamespace(), patch.GetName())
		if err != nil {
			return true, nil, err
		}
		pod := object.(*corev1.Pod)
		for _, change := range changes {
			switch change.Path {
			case "/metadata/uid", "/metadata/resourceVersion":
				var expected string
				if json.Unmarshal(change.Value, &expected) != nil || change.Path == "/metadata/uid" && expected != string(pod.UID) || change.Path == "/metadata/resourceVersion" && expected != pod.ResourceVersion {
					return true, nil, errors.New("Pod cleanup precondition failed")
				}
			case "/metadata/finalizers":
				if json.Unmarshal(change.Value, &pod.Finalizers) != nil {
					return true, nil, errors.New("invalid finalizers")
				}
			default:
				return false, nil, nil
			}
		}
		if pod.DeletionTimestamp != nil && len(pod.Finalizers) == 0 {
			return true, pod, kubeAPI.Tracker().Delete(action.GetResource(), action.GetNamespace(), pod.Name)
		}
		return true, pod, kubeAPI.Tracker().Update(action.GetResource(), pod, action.GetNamespace())
	})
	bundle := configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest([]configuration.Group{})}
	sha := core.Digest([]byte("installed fixture"))
	executable := runtimeimage.Executable{Path: "/opt/tools/runner", Version: "1.0.0", SHA256: sha}
	ref := runtimeimage.Ref{Image: "registry.example/runtime@sha256:" + sha, Platform: "linux/amd64", ImageBuildID: uuid.NewString(), DescriptorDigest: sha, ConfigurationDigest: configuration.ExecutionDigest(bundle, nil), Controller: core.Contract{BuildID: sha, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.6"}, Daemon: runtimeimage.Daemon{Executable: executable, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"codex": executable}}
	policy := kubernetes.Config{Platform: ref.Platform, ImagePullPolicy: corev1.PullIfNotPresent, ServiceAccount: "worker", Storage: kubernetes.Storage{ClaimName: "workspace-new", MaxTasks: 100, MaxBytes: 1 << 20, MaxConcurrentPreparations: 1}, NFS: kubernetes.NFS{Server: "controller-nfs", Image: "registry.example/nfs@sha256:" + sha}, Worker: kubernetes.Worker{PreparationTimeoutSeconds: 60, TaskDeadlineSeconds: 60, TerminationGraceSeconds: 10, TemporarySizeLimit: "1Gi", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("2Gi")}}}}
	pvcUID := uuid.NewString()
	if err := store.BindWorkspace(policy.Storage.ClaimName, pvcUID, "10.43.0.20"); err != nil {
		t.Fatal(err)
	}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: policy.Storage.ClaimName, Namespace: "fixture", UID: types.UID(pvcUID), Labels: map[string]string{"multica.ai/owner-id": options.OwnerID, "multica.ai/storage-layout": "controller-nfs-v1", "app.kubernetes.io/component": "storage"}}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, VolumeName: "new-volume"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	if err := kubeAPI.Tracker().Add(claim); err != nil {
		t.Fatal(err)
	}
	api, err := daemonapi.NewClient(backend.URL, "local-owner-token", ref.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := &Controller{Store: store, API: api, Kube: &kubernetes.Client{API: kubeAPI, Namespace: "fixture"}, Owner: owner, OwnerID: options.OwnerID, Policy: policy, Capacity: policy.Storage.MaxTasks, RuntimeRef: ref, Configuration: bundle, Descriptor: runtimeimage.Descriptor{Providers: ref.Providers}, GatewayURL: "http://gateway:8080", NFSServer: "10.43.0.20"}
	t.Cleanup(func() { c.Store.Close() })
	dispatchedAt, err := time.Parse(time.RFC3339, assignment.Load().DispatchedAt)
	if err != nil {
		t.Fatal(err)
	}
	// Claims retain subsecond precision; task history projects the same dispatch to seconds.
	envelope, _ := json.Marshal(map[string]any{"kind": "direct", "id": taskID, "runtime_id": runtimeID, "workspace_id": workspaceID, "agent_id": agentID, "issue_id": uuid.NewString(), "auth_token": "mat_controller_fixture", "agent": map[string]string{"id": agentID}, "dispatched_at": dispatchedAt.Add(326469 * time.Microsecond).Format(time.RFC3339Nano)})
	g, err := store.Create(workspace.TaskGrant{TaskID: taskID, RuntimeID: runtimeID, WorkspaceID: workspaceID, AgentID: agentID, RuntimeRef: ref, Envelope: envelope, Repositories: repositories})
	if err != nil {
		t.Fatal(err)
	}
	p := workspace.Prepared{OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID, Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, Provider: "codex", Executable: executable.Path, RuntimeDigest: g.Fingerprint, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{}, Environment: workspace.NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}}
	raw, _ := json.Marshal(p)
	p.Digest = core.Digest(raw)
	if err := store.BeginPreparation(g.AttemptID, workspace.PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://fixture"}); err != nil {
		t.Fatal(err)
	}
	g, err = store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return c, g, completed, options, assignment, p
}

func TestChangedDispatchStopsTaskAuthority(t *testing.T) {
	c, g, _, _, assignment := controllerFixtureBeforeProvision(t)
	current := *assignment.Load()
	dispatchedAt, err := time.Parse(time.RFC3339, current.DispatchedAt)
	if err != nil {
		t.Fatal(err)
	}
	current.DispatchedAt = dispatchedAt.Add(time.Second).Format(time.RFC3339)
	assignment.Store(&current)
	if _, err := c.observeBackendStop(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	stopped, err := c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Stop == nil {
		t.Fatal("a superseded dispatch retained task authority")
	}
}

func TestAcknowledgedUIDsSurviveResourceRecordPublicationCrash(t *testing.T) {
	c, g, _, options, _ := controllerFixture(t)
	r, err := record(g)
	if err != nil {
		t.Fatal(err)
	}
	r.Reference.PVCUID = ""
	r.Reference.PodUID = ""
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	// Recover the durable image from the crash window, rather than using the
	// live store API to roll resource evidence backwards.
	path := filepath.Join(options.Directory, "journal.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot map[string]json.RawMessage
	var grants map[string]map[string]json.RawMessage
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(snapshot["grants"], &grants); err != nil {
		t.Fatal(err)
	}
	grants[g.AttemptID]["resources"], err = json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	snapshot["grants"], err = json.Marshal(grants)
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.provision(context.Background(), recovered); err != nil {
		t.Fatal(err)
	}
	recovered, err = c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	r, err = record(recovered)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.StorageID != g.StorageID || r.Reference.PodUID != g.PodUID || r.Reference.PVCUID != g.PVCUID {
		t.Fatal("recovery replaced acknowledged writer or storage")
	}
}

func TestStartupWaitsForLiveImageAndPinsOneSupervisorKey(t *testing.T) {
	c, g, _, _, _ := controllerFixture(t)
	token, err := c.Store.CapabilityToken(g.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, action string, body any) int {
		raw, _ := json.Marshal(body)
		r := httptest.NewRequest(method, "/internal/attempts/"+g.AttemptID+"/"+action, bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		c.Handler().ServeHTTP(w, r)
		return w.Code
	}
	if code := request(http.MethodGet, "input", nil); code < 400 {
		t.Fatal("input arrived before kubelet published image identity")
	}
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(context.Background(), g.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "worker-node"
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: g.RuntimeRef.Image}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: g.RuntimeRef.Image}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if code := request(http.MethodGet, "input", nil); code != http.StatusOK {
		t.Fatal("published current image could not fetch prepared input", code)
	}
	key, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"publicKey": key, "podUID": g.PodUID, "pvcUID": g.PVCUID, "bootstrapDigest": g.BootstrapDigest}
	for range 2 {
		if code := request(http.MethodPost, "admit", body); code != http.StatusOK {
			t.Fatal("same-key admission could not recover a lost acknowledgement", code)
		}
	}
	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	body["publicKey"] = other
	if code := request(http.MethodPost, "admit", body); code < 400 {
		t.Fatal("admission retry replaced the supervisor")
	}
	body["publicKey"], body["podUID"] = key, uuid.NewString()
	if code := request(http.MethodPost, "admit", body); code < 400 {
		t.Fatal("admission retry changed the worker UID")
	}
	pod.Status.ContainerStatuses[0].ImageID = "example.invalid/other@sha256:" + core.Digest([]byte("other image"))
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if code := request(http.MethodGet, "input", nil); code < 400 {
		t.Fatal("startup retry admitted a different image")
	}
	current, err := c.Store.Get(g.AttemptID)
	if err != nil || !bytes.Equal(current.SupervisorKey, key) || current.PodUID != g.PodUID {
		t.Fatal("startup retries changed current worker authority", err)
	}
}

func TestUnsupportedJournalPreservesStoredData(t *testing.T) {
	c, _, _, options, _ := controllerFixture(t)
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(options.Directory, "journal.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]json.RawMessage
	if err := json.Unmarshal(raw, &journal); err != nil {
		t.Fatal(err)
	}
	journal["schemaVersion"] = json.RawMessage(`5`)
	raw, err = json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := workspace.Open(options); err == nil {
		reopened.Close()
		t.Fatal("previous journal schema was admitted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, raw) {
		t.Fatal("refusing old metadata changed existing journal bytes", err)
	}
}

func TestExhaustedTaskBudgetReportsAdmissionFailure(t *testing.T) {
	c, first, _, options, _ := controllerFixture(t)
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	options.MaxTasks = 1
	var err error
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := c.Store.QueueClaim(workspace.TaskGrant{TaskID: uuid.NewString(), RuntimeID: first.RuntimeID, WorkspaceID: first.WorkspaceID, AgentID: first.AgentID, RuntimeRef: first.RuntimeRef, Envelope: first.Envelope})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	terminal, err := c.Store.Terminal(queued.AttemptID)
	if err != nil || !bytes.Contains(terminal.Body, []byte("admission budget exhausted")) {
		t.Fatal("task budget was hidden as a worker failure", err)
	}
	retained, err := c.Store.Get(first.AttemptID)
	if err != nil || retained.PodUID != first.PodUID || retained.TaskRoot != first.TaskRoot {
		t.Fatal("budget refusal changed existing task authority", err)
	}
}

func TestFullOutboxStillReconcilesLaterTerminal(t *testing.T) {
	c, first, _, options, _ := controllerFixture(t)
	later, err := c.Store.QueueClaim(workspace.TaskGrant{TaskID: uuid.NewString(), RuntimeID: first.RuntimeID, WorkspaceID: first.WorkspaceID, AgentID: first.AgentID, RuntimeRef: first.RuntimeRef, Envelope: first.Envelope})
	if err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": later.TaskID, "agent_id": later.AgentID, "workspace_id": later.WorkspaceID, "runtime_id": uuid.NewString(), "dispatched_at": "2026-09-12T00:00:00Z", "status": "running"}})
	}))
	t.Cleanup(backend.Close)
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.ReceiveFailure(later.AttemptID, []byte(`{"error":"unexecuted task"}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	options.MaxPendingResults = 1
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	c.Policy.Worker.PreparationTimeoutSeconds = 1
	time.Sleep(time.Until(first.CreatedAt.Add(time.Second)))
	if err := reconcileFixture(context.Background(), c); err != nil {
		t.Fatal("full outbox prevented independent reconciliation", err)
	}
	terminal, err := c.Store.Terminal(later.AttemptID)
	if err != nil || terminal.State != "rejected" {
		t.Fatal("expired earlier attempt starved later obsolete terminal", err)
	}
}

func TestNativeCompletionWaitsForSealAndActualPodTermination(t *testing.T) {
	c, g, completed, _, assignment := controllerFixture(t)
	ctx := context.Background()
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
	// A failed observation must not revoke a writer whose Pod is still present.
	observationFailed := true
	c.Kube.API.(*fake.Clientset).PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if observationFailed {
			return true, nil, apierrors.NewServiceUnavailable("temporary control plane outage")
		}
		return false, nil, nil
	})
	if err := reconcileFixture(ctx, c); err != nil {
		t.Fatal(err)
	}
	observationFailed = false
	token, err := c.Store.CapabilityToken(g.AttemptID, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.Authorize(token, "daemon"); err != nil {
		t.Fatal("temporary Pod observation revoked a live writer", err)
	}
	task, err := c.Store.ReceiveTerminal(g.AttemptID, "complete", []byte(`{"result":"done"}`), workspace.ResumePointers{}, core.Digest([]byte("fixture provider result")))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.deliver(ctx, g); err != nil {
		t.Fatal(err)
	}
	if completed.Load() {
		t.Fatal("unsealed task was completed upstream")
	}
	receipt := workspace.SignedReceipt{TaskID: g.TaskID, AttemptID: g.AttemptID, PodUID: g.PodUID, PVCUID: g.PVCUID, RequestDigest: task.RequestDigest, Nonce: task.Nonce, WritersStopped: true, FlushOK: true}
	receipt.Signature = ed25519.Sign(key, workspace.ReceiptMessage(receipt))
	if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.Seal(g.AttemptID, receipt); err != nil {
		t.Fatal(err)
	}
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, g.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: g.RuntimeRef.Image, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: g.RuntimeRef.Image, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.deliver(ctx, g); err != nil {
		t.Fatal(err)
	}
	if completed.Load() {
		t.Fatal("live Pod was completed upstream")
	}
	finishTaskPod(t, c, g)
	current := *assignment.Load()
	assignment.Store(&current)
	for _, resource := range []string{"pods", "persistentvolumeclaims"} {
		unavailable := true
		c.Kube.API.(*fake.Clientset).PrependReactor("get", resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
			if unavailable {
				return true, nil, apierrors.NewServiceUnavailable("temporary control plane outage")
			}
			return false, nil, nil
		})
		_ = c.deliver(ctx, g)
		if completed.Load() {
			t.Fatal("unobserved writer was completed upstream")
		}
		unavailable = false
	}
	if err := c.deliver(ctx, g); err != nil {
		t.Fatal(err)
	}
	// The assignment read succeeds, then the callback cannot open a connection.
	// Restoring the backend must allow this definitely unsent result to finish.
	var disconnected *httptest.Server
	disconnected = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": current.RuntimeID, "dispatched_at": current.DispatchedAt, "status": current.Status}})
		_ = disconnected.Listener.Close()
	}))
	defer disconnected.Close()
	originalAPI := c.API
	c.API, err = daemonapi.NewClient(disconnected.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.reconcileDelivery(ctx, g.AttemptID)
	c.API = originalAPI
	if err := c.reconcileDelivery(ctx, g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if !completed.Load() {
		t.Fatal("sealed stopped task never completed upstream")
	}
}

func TestRefusedPreparationLeaseRevokesEvenWhenTaskRemainsDispatched(t *testing.T) {
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
	token, err := c.Store.CapabilityToken(g.AttemptID, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.Authorize(token, "daemon"); err != nil {
		t.Fatal(err)
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"dispatched"}`))
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcileFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.Authorize(token, "daemon"); err == nil {
		t.Fatal("refused preparation lease retained execution authority")
	}
}

func TestCancelledUnprovenWriterRetainsCapacityAndTaskExclusion(t *testing.T) {
	c, cancelled, _, options, _ := controllerFixture(t)
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(t.Context(), cancelled.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeName = "worker-node"
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Update(t.Context(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	nextID := uuid.NewString()
	nextIssueID := uuid.NewString()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/daemon/runtimes/" + cancelled.RuntimeID + "/tasks/" + cancelled.TaskID + "/prepare-lease":
			w.WriteHeader(http.StatusBadRequest)
		case "/api/agents/" + cancelled.AgentID + "/tasks":
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": cancelled.TaskID, "agent_id": cancelled.AgentID, "workspace_id": cancelled.WorkspaceID, "runtime_id": cancelled.RuntimeID, "kind": "direct", "created_at": "2026-09-12T00:00:00Z", "dispatched_at": "2026-09-12T00:00:00Z", "status": "cancelled"}, map[string]string{"id": nextID, "agent_id": cancelled.AgentID, "workspace_id": cancelled.WorkspaceID, "runtime_id": cancelled.RuntimeID, "issue_id": nextIssueID, "kind": "direct", "created_at": "2026-09-12T00:00:00Z", "dispatched_at": "2026-09-12T00:00:00Z", "status": "dispatched"}})
		case "/api/daemon/tasks/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{"kind": "direct", "issue_id": nextIssueID, "id": nextID, "runtime_id": cancelled.RuntimeID, "workspace_id": cancelled.WorkspaceID, "agent_id": cancelled.AgentID, "agent": map[string]string{"id": cancelled.AgentID}, "auth_token": "mat_local_task", "dispatched_at": "2026-09-12T00:00:00Z"}}})
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := reconcileFixture(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	c.runtimes = map[string]daemonapi.Bootstrap{cancelled.RuntimeID: {Workspace: daemonapi.Workspace{ID: cancelled.WorkspaceID}, Runtime: daemonapi.Runtime{ID: cancelled.RuntimeID, Provider: "codex"}}}
	c.Capacity = 1
	if err := c.claim(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := c.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range before {
		if candidate.TaskID == nextID {
			t.Fatal("uncertain writer's capacity was reused")
		}
	}
	c.Capacity = 2
	if err := c.claim(context.Background()); err != nil {
		t.Fatal("cancelled worker halted unrelated dispatch", err)
	}
	queued, err := c.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range queued {
		if g.TaskID == nextID {
			// Freeze selection inputs so this capacity test does not inspect
			// the host's provider HOME or execute its native tools.
			selectConversationFixtureTurn(t, c, g, false)
		}
	}
	if err := reconcileFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	grants, err := c.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, g := range grants {
		if g.TaskID == nextID {
			found = true
			if g.StorageID == "" || g.StorageID == cancelled.StorageID || g.TaskRoot == cancelled.TaskRoot {
				t.Fatal("following task did not receive isolated storage")
			}
		}
	}
	if !found {
		t.Fatal("unrelated task was not admitted")
	}
	current, err := c.Store.Get(cancelled.AttemptID)
	if err != nil || current.State == "closed" || current.CleanupComplete {
		t.Fatal("uncertain writer was released", current.State, err)
	}
	if _, err := c.Store.Create(workspace.TaskGrant{TaskID: cancelled.TaskID, RuntimeID: cancelled.RuntimeID, WorkspaceID: cancelled.WorkspaceID, AgentID: cancelled.AgentID, RuntimeRef: cancelled.RuntimeRef, Envelope: cancelled.Envelope}); !errors.Is(err, workspace.ErrStorageBusy) {
		t.Fatal("quarantined task storage could be reused", err)
	}
	if _, allowed, err := c.Store.Offer(cancelled.AttemptID); err != nil || allowed {
		t.Fatal("cancelled worker regained execution authority", err)
	}
}

func TestUnexecutedFailureSettlesAndAllowsFollowingClaim(t *testing.T) {
	c, previous, _, options, _ := controllerFixture(t)
	failedID, nextID := uuid.NewString(), uuid.NewString()
	envelope := func(id string) map[string]any {
		return map[string]any{"id": id, "runtime_id": previous.RuntimeID, "workspace_id": previous.WorkspaceID, "agent_id": previous.AgentID, "agent": map[string]string{"id": previous.AgentID}, "auth_token": "mat_local_task", "dispatched_at": "2026-09-12T00:00:00Z"}
	}
	var failureRecorded atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/daemon/runtimes/" + previous.RuntimeID + "/tasks/" + failedID + "/prepare-lease":
			w.WriteHeader(http.StatusConflict)
		case "/api/agents/" + previous.AgentID + "/tasks":
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": failedID, "agent_id": previous.AgentID, "workspace_id": previous.WorkspaceID, "runtime_id": previous.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": "dispatched"}})
		case "/api/daemon/tasks/" + failedID + "/fail":
			failureRecorded.Store(true)
			_, _ = w.Write([]byte(`{}`))
		case "/api/daemon/tasks/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{envelope(nextID)}})
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
	raw, _ := json.Marshal(envelope(failedID))
	failed, err := c.Store.QueueClaim(workspace.TaskGrant{TaskID: failedID, RuntimeID: previous.RuntimeID, WorkspaceID: previous.WorkspaceID, AgentID: previous.AgentID, RuntimeRef: previous.RuntimeRef, Envelope: raw})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	if err := reconcileFixture(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if !failureRecorded.Load() {
		t.Fatal("unexecuted claim failure was lost")
	}
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	c.Capacity = 2
	c.runtimes = map[string]daemonapi.Bootstrap{previous.RuntimeID: {Workspace: daemonapi.Workspace{ID: previous.WorkspaceID}, Runtime: daemonapi.Runtime{ID: previous.RuntimeID}}}
	if err := c.claim(context.Background()); err != nil {
		t.Fatal("never-executed failure halted dispatch", err)
	}
	grants, err := c.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range grants {
		if g.TaskID == nextID {
			return
		}
	}
	t.Fatal("settled unexecuted claim retained worker capacity")
}

func TestSlowTaskAPIKeepsEventPersistenceAvailable(t *testing.T) {
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
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: g.RuntimeRef.Image}}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: g.RuntimeRef.Image}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/issues/") {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer slow.Close()
	defer close(release)
	c.API, err = daemonapi.NewClient(slow.URL, "fixture-owner", c.RuntimeRef.Daemon.Version, slow.Client())
	if err != nil {
		t.Fatal(err)
	}
	router := c.Handler()
	apiToken, err := c.Store.CapabilityToken(g.AttemptID, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/issues/"+claim.IssueID, nil)
	request.Header.Set("Authorization", "Bearer "+claim.AuthToken)
	request.Header.Set("X-Multica-Attempt-Capability", apiToken)
	go router.ServeHTTP(httptest.NewRecorder(), request)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("task API did not enter its live upstream request")
	}
	raw, _ := json.Marshal(wire.ProviderEvent{Sequence: 1, Message: &agent.Message{Type: agent.MessageText, Content: "progress remains durable"}})
	event := httptest.NewRequest(http.MethodPost, "/internal/attempts/"+g.AttemptID+"/event", bytes.NewReader(raw))
	supervisor, err := c.Store.CapabilityToken(g.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	event.Header.Set("Authorization", "Bearer "+supervisor)
	persisted := make(chan struct{})
	go func() { router.ServeHTTP(httptest.NewRecorder(), event); close(persisted) }()
	select {
	case <-persisted:
	case <-time.After(10 * time.Second):
		t.Fatal("slow task API blocked execution event persistence")
	}
	recorded, err := c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range recorded.Events {
		if bytes.Contains(entry.Body, []byte("progress remains durable")) {
			found = true
		}
	}
	if !found {
		t.Fatal("running task lost its acknowledged progress")
	}
}

func TestInterruptedPreparationRequiresItsStoppedContainer(t *testing.T) {
	for _, scenario := range []string{"still running", "missing Pod", "unknown termination", "started here", "replaced container"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			c, previous, _, options, _ := controllerFixture(t)
			if err := c.Store.Close(); err != nil {
				t.Fatal(err)
			}
			options.Directory = filepath.Join(filepath.Dir(options.Directory), "interrupted")
			var err error
			c.Store, err = workspace.Open(options)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Store.BindWorkspace(c.Policy.Storage.ClaimName, previous.PVCUID, c.NFSServer); err != nil {
				t.Fatal(err)
			}
			input := workspace.TaskGrant{TaskID: uuid.NewString(), RuntimeID: previous.RuntimeID, WorkspaceID: previous.WorkspaceID, AgentID: previous.AgentID, RuntimeRef: previous.RuntimeRef, Envelope: []byte(`{}`)}
			g, err := c.Store.Create(input)
			if err != nil {
				t.Fatal(err)
			}
			r := resourceRecord{Reference: kubernetes.Reference{Namespace: c.Kube.Namespace, Owner: c.Owner, OwnerID: g.OwnerID, TaskID: g.TaskID, StorageID: g.StorageID, AttemptID: g.AttemptID, PodName: g.PodName, SecretName: g.PodName, RuntimeRef: g.RuntimeRef, PVCName: g.PVCName, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, NFSServer: c.NFSServer}}
			if err := c.saveResources(g.AttemptID, r); err != nil {
				t.Fatal(err)
			}
			process := workspace.PreparationProcess{PodName: c.Owner.Name, PodUID: c.Owner.UID, ContainerID: "containerd://old"}
			if err := c.Store.BeginPreparation(g.AttemptID, process); err != nil {
				t.Fatal(err)
			}
			failure := []byte(`{"error":"preparation interrupted"}`)
			if _, err := c.Store.ReceiveFailure(g.AttemptID, failure); err != nil {
				t.Fatal(err)
			}
			if err := c.Store.BeginFailureForward(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := c.Store.FinishForward(g.AttemptID, "delivered"); err != nil {
				t.Fatal(err)
			}
			pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, c.Owner.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			status := corev1.ContainerStatus{Name: "controller", ContainerID: process.ContainerID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}
			if scenario == "replaced container" || scenario == "started here" {
				status.ContainerID = "containerd://new"
				status.RestartCount = 1
				status.LastTerminationState.Terminated = &corev1.ContainerStateTerminated{ContainerID: process.ContainerID, FinishedAt: metav1.Now(), Reason: "Error"}
			}
			if scenario == "unknown termination" {
				status.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "ContainerStatusUnknown"}}
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{status}
			if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if scenario == "missing Pod" {
				if err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Delete(ctx, c.Owner.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "started here" {
				c.preparing = map[string]bool{g.AttemptID: false}
			}
			if err := c.Store.Close(); err != nil {
				t.Fatal(err)
			}
			c.Store, err = workspace.Open(options)
			if err != nil {
				t.Fatal(err)
			}
			if err := reconcileFixture(ctx, c); err != nil {
				t.Fatal(err)
			}
			_, err = c.Store.Create(input)
			if err == nil {
				t.Fatal("unproven writer or unflushed preparation was reused")
			}
			retained, readErr := c.Store.Get(g.AttemptID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if scenario == "replaced container" && (retained.State != "closed" || !retained.CleanupComplete) {
				t.Fatal("observed stopped preparation retained execution capacity")
			}
			terminal, err := c.Store.Terminal(g.AttemptID)
			if err != nil || !bytes.Equal(terminal.Body, failure) {
				t.Fatal("automatic interruption changed the settled result", err)
			}
		})
	}
}
