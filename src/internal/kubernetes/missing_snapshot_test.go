package kubernetes

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMissingSnapshotCannotAuthorizeNewResourcesOrReadyWorker(t *testing.T) {
	ctx := context.Background()
	client, owner, bundle, snapshots := snapshotFixture(t)
	ref, request, cfg := taskFixture(t, client, owner, bundle, snapshots)
	var err error
	ref.SecretUID, err = client.CreateSecret(ctx, ref, request)
	if err != nil {
		t.Fatal(err)
	}
	ref.PodUID, err = client.CreatePod(ctx, cfg, ref, request, "http://controller:8080")
	if err != nil {
		t.Fatal(err)
	}
	pod, err := client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", Ready: true}}
	if _, err = client.API.CoreV1().Pods(client.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if uid, err := client.ResolvePod(ctx, ref); err != nil || uid != ref.PodUID {
		t.Fatal("valid snapshot authority could not recover its worker", uid, err)
	}
	if err = client.API.CoreV1().ConfigMaps(client.Namespace).Delete(ctx, snapshots[0].Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err = client.Execute(ctx, ref, time.Second, Streams{}); !apierrors.IsNotFound(err) {
		t.Fatal("a ready worker was not denied for its lost snapshot authority", err)
	}
	// Existing GC teardown behavior is separately covered by
	// TestCleanupCanResolveLostCreatesAfterSnapshotOwnerGC.
	if err = client.Cleanup(ctx, ref); err != nil {
		t.Fatal("snapshot loss prevented exact owned resource cleanup", err)
	}
	if _, err = client.CreateSecret(ctx, ref, request); err == nil {
		t.Fatal("missing snapshot authorized a new request")
	}
	if _, err = client.CreatePod(ctx, cfg, ref, request, "http://controller:8080"); err == nil {
		t.Fatal("missing snapshot authorized a new worker")
	}
	if _, err = client.API.CoreV1().Secrets(client.Namespace).Get(ctx, ref.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("unapproved request persisted", err)
	}
	if _, err = client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("unapproved worker persisted", err)
	}
}
