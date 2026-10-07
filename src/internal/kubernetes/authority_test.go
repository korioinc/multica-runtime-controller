package kubernetes

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func currentTaskFixture(t *testing.T) (*Client, Reference, wire.Bootstrap, Config) {
	t.Helper()
	owner := Owner{Name: "controller", UID: uuid.NewString()}
	controller := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: owner.Name, Namespace: "fixture", UID: types.UID(owner.UID)}}
	api := fake.NewClientset(controller)
	api.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		action.(clienttesting.CreateAction).GetObject().(metav1.Object).SetUID(types.UID(uuid.NewString()))
		return false, nil, nil
	})
	client := &Client{API: api, Namespace: "fixture"}
	bundle := configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest([]configuration.Group{})}
	sha := core.Digest([]byte("fixture installed files"))
	contract := core.Contract{BuildID: sha, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.1"}
	selected := runtimeimage.Ref{Image: "registry.example/runtime@sha256:" + sha, Platform: contract.Platform, ImageBuildID: uuid.NewString(), DescriptorDigest: sha, Controller: contract, Daemon: runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/opt/tools/multica", Version: "0.4.43", SHA256: sha}, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"codex": {Path: "/opt/tools/codex", Version: "1.0.0", SHA256: sha}}, ConfigurationDigest: configuration.ExecutionDigest(bundle, nil)}
	task, storage, attempt := uuid.NewString(), uuid.NewString(), uuid.NewString()
	cfg := Config{Platform: selected.Platform, ImagePullPolicy: corev1.PullIfNotPresent, ServiceAccount: "worker", Worker: Worker{TaskDeadlineSeconds: 60, TerminationGraceSeconds: 10, TemporarySizeLimit: "1Gi", PreparationTimeoutSeconds: 60, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("1Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi"), corev1.ResourceEphemeralStorage: resource.MustParse("4Gi")}}}, Storage: Storage{ClaimName: "workspace-v3", MaxTasks: 20, MaxBytes: 1 << 30, MaxConcurrentPreparations: 1}, NFS: NFS{Server: "controller-nfs", Image: "registry.example/nfs@sha256:" + sha}}
	ref := Reference{Namespace: client.Namespace, Owner: owner, OwnerID: uuid.NewString(), TaskID: task, StorageID: storage, AttemptID: attempt, PodName: "task-" + attempt, SecretName: "request-" + attempt, PVCName: cfg.Storage.ClaimName, RuntimeRef: selected}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: ref.PVCName, Namespace: client.Namespace, Labels: map[string]string{ownerLabel: ref.OwnerID, storageLayoutLabel: storageLayout, "app.kubernetes.io/component": "storage"}}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, VolumeMode: ptr.To(corev1.PersistentVolumeFilesystem), VolumeName: "fixture-volume", Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")}}}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	if _, err := api.CoreV1().PersistentVolumeClaims(client.Namespace).Create(context.Background(), pvc, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cfg.NFS.Server, Namespace: client.Namespace, Labels: map[string]string{ownerLabel: ref.OwnerID}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.43.0.50", PublishNotReadyAddresses: true, Selector: map[string]string{ownerLabel: ref.OwnerID, "app.kubernetes.io/component": "controller"}, Ports: []corev1.ServicePort{{Port: 2049, TargetPort: intstr.FromInt32(2049), Protocol: corev1.ProtocolTCP}}}}
	if _, err := api.CoreV1().Services(client.Namespace).Create(context.Background(), service, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var err error
	ref.PVCUID, ref.NFSServer, err = client.ResolveWorkspace(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	workspaceID := uuid.NewString()
	ref.TaskRoot, err = workspace.TaskRoot(wire.WorkspaceRoot, workspaceID, task, "", "")
	if err != nil {
		t.Fatal(err)
	}
	request := wire.Bootstrap{TaskID: task, AttemptID: attempt, StorageID: storage, OwnerID: ref.OwnerID, RuntimeID: uuid.NewString(), WorkspaceID: workspaceID, TaskRoot: ref.TaskRoot, NFSServer: ref.NFSServer, PreparedDigest: sha, Provider: "codex", AgentID: uuid.NewString(), Generation: 1, PVCName: ref.PVCName, PVCUID: ref.PVCUID, RuntimeRef: selected, GatewayURL: "http://controller:8080", APICapability: strings.Repeat("a", 32), SupervisorCapability: strings.Repeat("s", 32), CacheCapability: strings.Repeat("c", 32), ExpiresAt: "2099-01-01T00:00:00Z", Configuration: bundle, TerminationGraceSeconds: 10}
	request.StopCapability = strings.Repeat("t", 32)
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wire.DecodeBootstrap(raw); err != nil {
		t.Fatal(err)
	}
	ref.RequestDigest = wire.Digest(raw)
	ref.PodDigest, err = PodFingerprint(cfg, ref, request)
	if err != nil {
		t.Fatal(err)
	}
	return client, ref, request, cfg
}

func ensureTaskPod(ctx context.Context, client *Client, cfg Config, ref Reference, request wire.Bootstrap) (string, error) {
	want, err := PodRequest(cfg, ref, request)
	if err != nil {
		return "", err
	}
	return client.EnsurePod(ctx, ref, want)
}

func TestCleanupCanResolveLostCreatesAfterControllerDeletion(t *testing.T) {
	ctx := context.Background()
	client, ref, request, cfg := currentTaskFixture(t)
	var err error
	ref.SecretUID, err = client.EnsureSecret(ctx, ref, request)
	if err != nil {
		t.Fatal(err)
	}
	ref.PodUID, err = ensureTaskPod(ctx, client, cfg, ref, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.API.CoreV1().Pods(client.Namespace).Delete(ctx, ref.Owner.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	lost := ref
	lost.SecretUID, lost.PodUID = "", ""
	lost.SecretUID, err = client.ResolveCleanupSecret(ctx, lost)
	if err != nil {
		t.Fatal("controller deletion blocked request teardown", err)
	}
	lost.PodUID, err = client.ResolveCleanupPod(ctx, lost)
	if err != nil {
		t.Fatal("controller deletion blocked worker teardown", err)
	}
	if err = client.Cleanup(ctx, lost); err != nil {
		t.Fatal("journaled resources could not be removed after owner GC", err)
	}
	if _, err = client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("completed worker retained storage authority", err)
	}
	if _, err = client.API.CoreV1().Secrets(client.Namespace).Get(ctx, ref.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("completed attempt retained its request", err)
	}
	if _, err := client.ResolveCleanupPod(ctx, ref); err != nil {
		t.Fatal("already-collected worker could not be reconciled", err)
	}
	// A known prior UID cannot be used to delete a later object of the same
	// name, even when all of its non-UID fields resemble the old request.
	replacement, err := secretObject(ref, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.API.CoreV1().Secrets(client.Namespace).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = client.Cleanup(ctx, ref); err == nil {
		t.Fatal("old recovery authority deleted a replacement request")
	}
	if _, err = client.API.CoreV1().Secrets(client.Namespace).Get(ctx, ref.SecretName, metav1.GetOptions{}); err != nil {
		t.Fatal("replacement request was lost", err)
	}
}

func TestImageBindingRejectsMixedInitMainBuilds(t *testing.T) {
	image := "registry.example/runtime@sha256:" + core.Digest([]byte("initial build"))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "fixture", UID: types.UID(uuid.NewString())}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64"}, Containers: []corev1.Container{{Name: "controller"}, {Name: "nfs", Image: image}}, InitContainers: []corev1.Container{{Name: "home-layout"}, {Name: "nfs-layout", Image: image}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "controller", ImageID: image}, {Name: "nfs", ImageID: image}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "home-layout", ImageID: image}, {Name: "nfs-layout", ImageID: image}}}}
	if _, pending, err := boundImage(pod, pod.Namespace, pod.Name, string(pod.UID), "controller", "", "linux/amd64", NFS{Image: image}); err != nil || pending {
		t.Fatal("consistent image could not be admitted", err)
	}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "cache", Image: "registry.example/cache:stable"})
	if _, pending, err := boundImage(pod, pod.Namespace, pod.Name, string(pod.UID), "controller", "", "linux/amd64", NFS{Image: image}); err != nil || pending {
		t.Fatal("independent sidecar prevented controller image admission", err)
	}
	pod.Status.ContainerStatuses[0].ImageID = "registry.example/runtime@sha256:" + core.Digest([]byte("replacement build"))
	if _, _, err := boundImage(pod, pod.Namespace, pod.Name, string(pod.UID), "controller", "", "linux/amd64", NFS{Image: image}); err == nil {
		t.Fatal("different main build acquired initialization authority")
	}
}

func TestUnprovenImageCannotAcquireControllerAuthority(t *testing.T) {
	digest := core.Digest([]byte("running image"))
	image := "registry.example/runtime@sha256:" + digest
	original := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "fixture", UID: types.UID(uuid.NewString())}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64"}, Containers: []corev1.Container{{Name: "controller", Image: "registry.example/runtime:latest"}, {Name: "nfs", Image: image}}, InitContainers: []corev1.Container{{Name: "home-layout", Image: "registry.example/runtime:latest"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "controller", ImageID: "docker-pullable://" + image}, {Name: "nfs", ImageID: image}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "home-layout", ImageID: image}, {Name: "nfs-layout", ImageID: image}}}}
	bind := func(pod *corev1.Pod) (string, bool, error) {
		return boundImage(pod, original.Namespace, original.Name, string(original.UID), "controller", "", "linux/amd64", NFS{Image: image})
	}
	if _, pending, err := bind(original); err != nil || pending {
		t.Fatal("the observed repository digest did not authorize the matching Pod", err)
	}
	for name, change := range map[string]func(*corev1.Pod){
		"bare config digest cannot inherit the tag repository": func(pod *corev1.Pod) {
			pod.Status.ContainerStatuses[0].ImageID = "sha256:" + digest
			pod.Status.InitContainerStatuses[0].ImageID = "sha256:" + digest
		},
		"recreated Pod cannot inherit prior process identity": func(pod *corev1.Pod) {
			pod.UID = types.UID(uuid.NewString())
		},
		"same digest cannot authorize another execution platform": func(pod *corev1.Pod) {
			pod.Spec.NodeSelector["kubernetes.io/arch"] = "arm64"
		},
	} {
		t.Run(name, func(t *testing.T) {
			pod := original.DeepCopy()
			change(pod)
			if _, _, err := bind(pod); err == nil {
				t.Fatal("unproven image acquired controller authority")
			}
		})
	}
}
