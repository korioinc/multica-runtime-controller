package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"

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
			pod.Spec.NodeName = cfg.SingleNodeName
			if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", Ready: true}}
			if _, err = client.API.CoreV1().Pods(client.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err = client.authorizeExecution(ctx, ref); err != nil {
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
				if exists, err := client.OwnerExists(ctx, ref); err != nil || !exists {
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
			if _, err = client.ResolveSecret(ctx, ref); err == nil {
				t.Fatal("lost controller reauthorized an existing request")
			}
			if _, err = client.ResolvePod(ctx, ref); err == nil {
				t.Fatal("lost controller reauthorized an existing worker")
			}
			if err = client.Execute(ctx, ref, time.Second, Streams{}); err == nil {
				t.Fatal("a ready worker accepted execution without a live controller")
			}
			if err = client.Cleanup(ctx, ref); err != nil {
				t.Fatal("lost execution authority prevented owned resource cleanup", err)
			}
			if _, err = client.CreateSecret(ctx, ref, request); err == nil {
				t.Fatal("lost controller authorized a new request")
			}
			if _, err = client.CreatePod(ctx, cfg, ref, request, "http://controller:8080"); err == nil {
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

func TestControllerOutsideFixedNodeCannotAuthorizeNewAttempt(t *testing.T) {
	ctx := context.Background()
	client, ref, request, cfg := currentTaskFixture(t)
	ref.FixedNode = "another-node"
	cfg.SingleNodeName = ref.FixedNode
	var err error
	ref.PodDigest, err = PodFingerprint(cfg, ref, request, "http://controller:8080")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.CreateSecret(ctx, ref, request); err == nil {
		t.Fatal("controller outside the selected storage node authorized a request")
	}
	if _, err = client.CreatePod(ctx, cfg, ref, request, "http://controller:8080"); err == nil {
		t.Fatal("controller outside the selected storage node authorized a worker")
	}
	if _, err = client.API.CoreV1().Secrets(client.Namespace).Get(ctx, ref.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("unapproved request persisted", err)
	}
	if _, err = client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("unapproved worker persisted", err)
	}
}
