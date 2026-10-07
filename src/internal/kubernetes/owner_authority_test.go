package kubernetes

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestLostControllerCannotAuthorizeTaskResourcesOrExecution(t *testing.T) {
	for _, state := range []string{"deleted", "replaced", "terminating", "unavailable"} {
		t.Run(state, func(t *testing.T) {
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
			pod, err := client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod.Spec.NodeName = "independent-worker-node"
			if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", Ready: true, ImageID: ref.RuntimeRef.Image}}
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: ref.RuntimeRef.Image}}
			if _, err = client.API.CoreV1().Pods(client.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err = client.Authorize(ctx, ref); err != nil {
				t.Fatal("live controller could not authorize its worker", err)
			}
			controller, err := client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.Owner.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "deleted", "replaced":
				if err = client.API.CoreV1().Pods(client.Namespace).Delete(ctx, controller.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				if state == "replaced" {
					controller.UID, controller.ResourceVersion = "", ""
					if _, err = client.API.CoreV1().Pods(client.Namespace).Create(ctx, controller, metav1.CreateOptions{}); err != nil {
						t.Fatal(err)
					}
				}
			case "terminating":
				// Model the live state returned while a finalizer delays deletion.
				stamp := metav1.Now()
				controller.DeletionTimestamp = &stamp
				controller.Finalizers = []string{"fixture.test/hold"}
				if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, controller, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				retained, err := client.API.CoreV1().Pods(client.Namespace).Get(ctx, controller.Name, metav1.GetOptions{})
				if err != nil || retained.UID != controller.UID {
					t.Fatal("terminating controller lost the fence for unresolved creates", err)
				}
			case "unavailable":
				client.API.(*fake.Clientset).PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if action.(clienttesting.GetAction).GetName() == ref.Owner.Name {
						return true, nil, errors.New("controller lookup unavailable")
					}
					return false, nil, nil
				})
			}
			if _, err = client.Authorize(ctx, ref); err == nil {
				t.Fatal("a ready worker acquired execution authority without a live controller")
			}
			unacknowledged := ref
			unacknowledged.SecretUID = ""
			if _, err = client.EnsureSecret(ctx, unacknowledged, request); err == nil {
				t.Fatal("lost controller adopted an existing bootstrap for execution")
			}
			if err = client.Cleanup(ctx, ref); err != nil {
				t.Fatal("lost execution authority prevented owned resource cleanup", err)
			}
			if _, err = client.EnsureSecret(ctx, ref, request); err == nil {
				t.Fatal("lost controller authorized a new request")
			}
			if _, err = ensureTaskPod(ctx, client, cfg, ref, request); err == nil {
				t.Fatal("lost controller authorized a new worker")
			}
			if _, err = client.API.CoreV1().Secrets(client.Namespace).Get(ctx, ref.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatal("unapproved request persisted", err)
			}
			if _, err = client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatal("unapproved worker persisted", err)
			}
		})
	}
}
