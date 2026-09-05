package intercept

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

// ControllerGrantKey is stored outside the workspace PVC: legacy workers could
// write every byte of that PVC, so a file there cannot establish provenance.
// The key survives controller replacement but is never delivered to workers.
func ControllerGrantKey(ctx context.Context) (string, error) {
	cfg, _, err := LoadConfig()
	if err != nil {
		return "", err
	}
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return "", fmt.Errorf("load controller grant-key Kubernetes config: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return "", fmt.Errorf("create controller grant-key Kubernetes client: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// One signing authority per PVC, independent of the ephemeral Pod name.
	digest := sha256.Sum256([]byte(cfg.WorkspacePVCName))
	name := fmt.Sprintf("multica-repo-grants-%x", digest[:12])
	secrets := client.CoreV1().Secrets(cfg.Namespace)
	secret, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return "", fmt.Errorf("generate controller grant key: %w", err)
		}
		secret, err = secrets.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cfg.Namespace, Labels: map[string]string{ManagedByLabel: ManagedByValue}},
			Immutable:  ptr.To(true), Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{"key": key},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			secret, err = secrets.Get(ctx, name, metav1.GetOptions{})
		}
	}
	if err != nil {
		return "", fmt.Errorf("load controller grant key: %w", err)
	}
	if secret.Immutable == nil || !*secret.Immutable || secret.Labels[ManagedByLabel] != ManagedByValue || len(secret.Data["key"]) != 32 {
		return "", errors.New("controller grant-key Secret is invalid; refusing to trust workspace reuse records")
	}
	return hex.EncodeToString(secret.Data["key"]), nil
}
