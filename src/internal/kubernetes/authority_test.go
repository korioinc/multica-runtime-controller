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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func currentTaskFixture(t *testing.T) (*Client, Reference, wire.Request, Config) {
	t.Helper()
	owner := Owner{Name: "controller", UID: uuid.NewString()}
	content := []byte("operator configuration")
	group := configuration.Group{Name: "provider", Directories: []string{".codex"}, Files: []configuration.File{{Target: ".codex/config.toml", Mode: 0600, SHA256: core.Digest(content), Content: content}}}
	bundle := configuration.Bundle{SchemaVersion: 1, Groups: []configuration.Group{group}, Digest: configuration.Digest([]configuration.Group{group})}
	controller := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: owner.Name, Namespace: "fixture", UID: types.UID(owner.UID)}, Spec: corev1.PodSpec{NodeName: "fixture-node"}}
	api := fake.NewClientset(controller)
	// The real API allocates UIDs on creation. Preserve that behavior in the
	// in-memory API owner without implying real-cluster transaction coverage.
	api.PrependReactor("create", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		object := action.(clienttesting.CreateAction).GetObject().(metav1.Object)
		object.SetUID(types.UID(uuid.NewString()))
		return false, nil, nil
	})
	client := &Client{API: api, Namespace: "fixture"}
	sha := core.Digest([]byte("fixture installed files"))
	contract := core.Contract{SchemaVersion: core.Version, ControllerABI: core.ABI, BuildID: sha, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.1", ShimPaths: map[string]string{}}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		contract.ShimPaths[alias] = core.Root + "/shims/" + alias
	}
	selected := runtimeimage.Ref{SchemaVersion: 2, Image: "registry.example/runtime@sha256:" + sha, Platform: contract.Platform, ImageBuildID: uuid.NewString(), DescriptorDigest: sha, Controller: contract, Daemon: runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/opt/tools/multica", Version: "0.4.40", SHA256: sha}, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"codex": {Path: "/opt/tools/codex", Version: "1.0.0", SHA256: sha}}, ConfigurationDigest: bundle.Digest}
	task, storage, attempt := uuid.NewString(), uuid.NewString(), uuid.NewString()
	root := "/workspace/fixture/" + task
	request := wire.Request{SchemaVersion: wire.RequestSchemaVersion, TaskID: task, Provider: "codex", Args: []string{"--version"}, Env: []string{"MULTICA_TASK_ID=" + task, "MULTICA_TASK_CONFIG_ROOT=" + root + "/multica-config", "MULTICA_DAEMON_PORT=4321"}, WorkDir: root + "/workdir", WorkerSubPath: ".multica-runtime/workers/" + storage, RuntimeRef: selected, AttemptID: attempt, OwnerID: uuid.NewString(), HomeDigest: sha, BrokerPort: 3210, BrokerToken: strings.Repeat("t", 32), TerminationGraceSeconds: 10}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wire.Decode(raw); err != nil {
		t.Fatal(err)
	}
	ref := Reference{Namespace: client.Namespace, Owner: owner, TaskID: task, StorageID: storage, AttemptID: attempt, PodName: "worker-" + storage, SecretName: "request-" + attempt, RequestDigest: wire.Digest(raw), RuntimeRef: selected, FixedNode: controller.Spec.NodeName}
	cfg := Config{Platform: selected.Platform, ImagePullPolicy: corev1.PullIfNotPresent, WorkspaceClaim: "workspace", WorkspaceAccessMode: corev1.ReadWriteMany, SingleNodeName: ref.FixedNode, ServiceAccount: "worker", TaskDeadlineSeconds: 60, TerminationGraceSeconds: 10}
	ref.PodDigest, err = PodFingerprint(cfg, ref, request, "http://controller:8080")
	if err != nil {
		t.Fatal(err)
	}
	return client, ref, request, cfg
}

func TestCleanupCanResolveLostCreatesAfterControllerDeletion(t *testing.T) {
	ctx := context.Background()
	client, ref, request, cfg := currentTaskFixture(t)
	var err error
	ref.SecretUID, err = client.CreateSecret(ctx, ref, request)
	if err != nil {
		t.Fatal(err)
	}
	ref.PodUID, err = client.CreatePod(ctx, cfg, ref, request, "http://controller:8080")
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
	if uid, err := client.ResolveCleanupPod(ctx, ref); err != nil || uid != "" {
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
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "fixture", UID: types.UID(uuid.NewString())}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64"}, Containers: []corev1.Container{{Name: "controller"}}, InitContainers: []corev1.Container{{Name: "home-layout"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "controller", ImageID: image}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "home-layout", ImageID: image}}}}
	if _, pending, err := boundImage(pod, pod.Namespace, pod.Name, string(pod.UID), "controller", "", "linux/amd64"); err != nil || pending {
		t.Fatal("consistent image could not be admitted", err)
	}
	pod.Status.ContainerStatuses[0].ImageID = "registry.example/runtime@sha256:" + core.Digest([]byte("replacement build"))
	if _, _, err := boundImage(pod, pod.Namespace, pod.Name, string(pod.UID), "controller", "", "linux/amd64"); err == nil {
		t.Fatal("different main build acquired initialization authority")
	}
}

func TestUnprovenImageCannotAcquireControllerAuthority(t *testing.T) {
	digest := core.Digest([]byte("running image"))
	image := "registry.example/runtime@sha256:" + digest
	original := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller", Namespace: "fixture", UID: types.UID(uuid.NewString())}, Spec: corev1.PodSpec{NodeSelector: map[string]string{"kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64"}, Containers: []corev1.Container{{Name: "controller", Image: "registry.example/runtime:latest"}}, InitContainers: []corev1.Container{{Name: "home-layout", Image: "registry.example/runtime:latest"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "controller", ImageID: "docker-pullable://" + image}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "home-layout", ImageID: image}}}}
	bind := func(pod *corev1.Pod) (string, bool, error) {
		return boundImage(pod, original.Namespace, original.Name, string(original.UID), "controller", "", "linux/amd64")
	}
	if got, pending, err := bind(original); err != nil || pending || got != image {
		t.Fatal("the observed repository digest did not authorize the matching Pod", got, pending, err)
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
			if got, pending, err := bind(pod); err == nil || pending || got != "" {
				t.Fatal("unproven image acquired controller authority", got, pending, err)
			}
		})
	}
}
