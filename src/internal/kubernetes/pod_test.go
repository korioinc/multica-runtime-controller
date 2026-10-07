package kubernetes

import (
	"context"
	"testing"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestEnsurePodAdoptsCommittedWorker(t *testing.T) {
	for _, terminating := range []bool{false, true} {
		name := "live"
		if terminating {
			name = "terminating"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client, ref, request, cfg := currentTaskFixture(t)
			var err error
			ref.SecretUID, err = client.EnsureSecret(ctx, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			originalUID, err := ensureTaskPod(ctx, client, cfg, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			if terminating {
				pod, err := client.API.CoreV1().Pods(client.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				stamp := metav1.Now()
				pod.DeletionTimestamp = &stamp
				if _, err = client.API.CoreV1().Pods(client.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			uid, err := ensureTaskPod(ctx, client, cfg, ref, request)
			if err != nil || uid != originalUID {
				t.Fatal("unacknowledged worker could not recover its original storage authority", err)
			}
		})
	}
}

func TestEnsurePodAdoptsLateCreation(t *testing.T) {
	for _, at := range []string{"storage lookup", "creation"} {
		t.Run(at, func(t *testing.T) {
			ctx := context.Background()
			client, ref, request, cfg := currentTaskFixture(t)
			var err error
			ref.SecretUID, err = client.EnsureSecret(ctx, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			want, err := PodRequest(cfg, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			late := want.DeepCopy()
			late.UID = types.UID(uuid.NewString())
			api := client.API.(*fake.Clientset)
			verb := "create"
			if at == "storage lookup" {
				verb = "list"
			}
			committed := false
			api.PrependReactor(verb, "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if committed {
					return false, nil, nil
				}
				// A previously sent POST commits after the first Pod lookup.
				// The tracker exposes that worker to the list or competing POST.
				if err := api.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), late, client.Namespace); err != nil {
					t.Fatal(err)
				}
				committed = true
				return false, nil, nil
			})
			uid, err := client.EnsurePod(ctx, ref, want)
			if err != nil || uid != string(late.UID) {
				t.Fatal("late creation stranded or replaced the original worker", err)
			}
		})
	}
}

func TestEnsurePodCannotReplaceAcknowledgedWorker(t *testing.T) {
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
	if err = client.API.CoreV1().Pods(client.Namespace).Delete(ctx, ref.PodName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = ensureTaskPod(ctx, client, cfg, ref, request); err == nil {
		t.Fatal("replacement worker inherited acknowledged storage authority")
	}
	if pod, err := client.pod(ctx, ref.PodName); err != nil || pod != nil {
		t.Fatal("rejected replacement still started another task writer", err)
	}
	replacement, err := PodRequest(cfg, ref, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.API.CoreV1().Pods(client.Namespace).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = ensureTaskPod(ctx, client, cfg, ref, request); err == nil {
		t.Fatal("replacement worker inherited a prior worker's identity")
	}
}

func TestEnsurePodRejectsAlteredCreationRequest(t *testing.T) {
	for name, change := range map[string]func(*corev1.Pod){
		"another task": func(pod *corev1.Pod) {
			pod.Labels[taskLabel] = uuid.NewString()
		},
		"another image": func(pod *corev1.Pod) {
			pod.Spec.Containers[0].Image = "registry.example/unapproved:latest"
		},
		"missing termination evidence": func(pod *corev1.Pod) {
			pod.Finalizers = nil
		},
		"unproven fixed node": func(pod *corev1.Pod) {
			pod.Spec.NodeName = "unproven-node"
		},
		"existing worker identity": func(pod *corev1.Pod) {
			pod.UID = types.UID(uuid.NewString())
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client, ref, request, cfg := currentTaskFixture(t)
			var err error
			ref.SecretUID, err = client.EnsureSecret(ctx, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			want, err := PodRequest(cfg, ref, request)
			if err != nil {
				t.Fatal(err)
			}
			change(want)
			if _, err = client.EnsurePod(ctx, ref, want); err == nil {
				t.Fatal("altered creation request acquired storage authority")
			}
			if pod, err := client.pod(ctx, ref.PodName); err != nil || pod != nil {
				t.Fatal("rejected request still started an unauthorized writer", err)
			}
		})
	}
}
