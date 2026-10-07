package kubernetes

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestEnsureSecretAdoptsConcurrentBootstrap(t *testing.T) {
	ctx := context.Background()
	client, ref, request, cfg := currentTaskFixture(t)
	api := client.API.(*fake.Clientset)
	concurrent, err := secretObject(ref, request)
	if err != nil {
		t.Fatal(err)
	}
	concurrent.UID = types.UID(uuid.NewString())
	committed := false
	api.PrependReactor("create", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if committed {
			return false, nil, nil
		}
		// Another request commits after our lookup and before our create. The
		// tracker then produces the real AlreadyExists result for our write.
		if err := api.Tracker().Create(corev1.SchemeGroupVersion.WithResource("secrets"), concurrent, client.Namespace); err != nil {
			t.Fatal(err)
		}
		committed = true
		return false, nil, nil
	})
	ref.SecretUID, err = client.EnsureSecret(ctx, ref, request)
	if err != nil {
		t.Fatal("concurrent creation stranded the prepared worker", err)
	}
	if ref.SecretUID != string(concurrent.UID) {
		t.Fatal("recovery replaced the original bootstrap authority")
	}
	if _, err = ensureTaskPod(ctx, client, cfg, ref, request); err != nil {
		t.Fatal("recovered bootstrap did not authorize its worker", err)
	}
}

func TestEnsureSecretRejectsUntrustedBootstrap(t *testing.T) {
	for name, change := range map[string]func(*corev1.Secret){
		"another task": func(secret *corev1.Secret) {
			secret.Labels[taskLabel] = uuid.NewString()
		},
		"altered credentials": func(secret *corev1.Secret) {
			secret.Data[wire.RequestKey] = []byte("unapproved credentials")
		},
		"mutable request": func(secret *corev1.Secret) {
			secret.Immutable = ptr.To(false)
		},
		"terminating request": func(secret *corev1.Secret) {
			stamp := metav1.Now()
			secret.DeletionTimestamp = &stamp
			secret.Finalizers = []string{"fixture.test/hold"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client, ref, request, _ := currentTaskFixture(t)
			secret, err := secretObject(ref, request)
			if err != nil {
				t.Fatal(err)
			}
			change(secret)
			// Seed the live API state, including an object retained by a
			// finalizer, without pretending immutable payloads can be edited.
			secret.UID = types.UID(uuid.NewString())
			if err = client.API.(*fake.Clientset).Tracker().Create(corev1.SchemeGroupVersion.WithResource("secrets"), secret, client.Namespace); err != nil {
				t.Fatal(err)
			}
			if _, err = client.EnsureSecret(ctx, ref, request); err == nil {
				t.Fatal("untrusted bootstrap acquired worker provisioning authority")
			}
		})
	}
}

func TestEnsureSecretCannotReplaceAcknowledgedBootstrap(t *testing.T) {
	ctx := context.Background()
	client, ref, request, _ := currentTaskFixture(t)
	var err error
	ref.SecretUID, err = client.EnsureSecret(ctx, ref, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.API.CoreV1().Secrets(client.Namespace).Delete(ctx, ref.SecretName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.EnsureSecret(ctx, ref, request); err == nil {
		t.Fatal("lost bootstrap was replaced under its acknowledged authority")
	}
	if secret, err := client.secret(ctx, ref.SecretName); err != nil || secret != nil {
		t.Fatal("rejected replacement still published new execution credentials", err)
	}
	// A separately created replacement also cannot inherit the old UID's
	// authority, even when it carries the same request bytes.
	replacement, err := secretObject(ref, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.API.CoreV1().Secrets(client.Namespace).Create(ctx, replacement, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.EnsureSecret(ctx, ref, request); err == nil {
		t.Fatal("replacement bootstrap inherited prior execution authority")
	}
}

func TestEnsureSecretValidatesRequestBeforeAdoption(t *testing.T) {
	ctx := context.Background()
	client, ref, request, _ := currentTaskFixture(t)
	if _, err := client.EnsureSecret(ctx, ref, request); err != nil {
		t.Fatal(err)
	}
	request.APICapability = "different-unapproved-capability-value"
	if _, err := client.EnsureSecret(ctx, ref, request); err == nil {
		t.Fatal("mismatched request inherited existing bootstrap authority")
	}
}
