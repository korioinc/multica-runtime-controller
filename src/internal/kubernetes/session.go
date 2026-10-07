package kubernetes

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
)

func sessionSecretObject(ref Reference, b wire.SessionBootstrap) (*corev1.Secret, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	if b.WorkerSessionID != ref.WorkerSessionID || b.StorageID != ref.StorageID || b.Conversation.OwnerID != ref.OwnerID ||
		b.Conversation.WorkspaceID != ref.WorkspaceID || b.WorkspaceAnchorTaskID != ref.WorkspaceAnchorTaskID ||
		b.PVCName != ref.PVCName || b.PVCUID != ref.PVCUID || b.TaskRoot != ref.TaskRoot || b.NFSServer != ref.NFSServer ||
		!b.RuntimeRef.Equal(ref.RuntimeRef) || ref.TaskID != "" || ref.AttemptID != "" {
		return nil, errors.New("session bootstrap differs from its resource authority")
	}
	raw, err := json.Marshal(b)
	if err != nil || wire.Digest(raw) != ref.RequestDigest || len(raw) > wire.MaxRequestBytes {
		return nil, errors.New("session bootstrap digest or size mismatch")
	}
	return &corev1.Secret{ObjectMeta: ref.metadata(ref.SecretName), Immutable: ptr.To(true), Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{wire.RequestKey: raw}}, nil
}

func sessionPodObject(cfg Config, ref Reference, b wire.SessionBootstrap) (*corev1.Pod, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Storage.ClaimName != ref.PVCName || cfg.Platform != ref.RuntimeRef.Platform || b.TerminationGraceSeconds != int(cfg.Worker.TerminationGraceSeconds) {
		return nil, errors.New("session deployment selection mismatch")
	}
	if _, err := sessionSecretObject(ref, b); err != nil {
		return nil, err
	}
	return workerPod(cfg, ref)
}

func SessionPodFingerprint(cfg Config, ref Reference, b wire.SessionBootstrap) (string, error) {
	pod, err := sessionPodObject(cfg, ref, b)
	if err != nil {
		return "", err
	}
	return specFingerprint(pod.Spec), nil
}

func SessionPodRequest(cfg Config, ref Reference, b wire.SessionBootstrap) (*corev1.Pod, error) {
	pod, err := sessionPodObject(cfg, ref, b)
	if err != nil {
		return nil, err
	}
	if !podRequestMatches(pod, ref) {
		return nil, errors.New("session Pod request differs from journaled authority")
	}
	return pod, nil
}

func SessionSecretRequest(ref Reference, b wire.SessionBootstrap) (*corev1.Secret, error) {
	return sessionSecretObject(ref, b)
}

func (c *Client) EnsureSessionSecret(ctx context.Context, ref Reference, want *corev1.Secret) (string, error) {
	if err := c.validateReferenceAuthority(ctx, ref); err != nil {
		return "", err
	}
	if want == nil || want.UID != "" || want.ResourceVersion != "" || want.DeletionTimestamp != nil ||
		!metadataRequestMatches(want.ObjectMeta, ref, ref.SecretName) || want.Immutable == nil || !*want.Immutable ||
		want.Type != corev1.SecretTypeOpaque || len(want.Data) != 1 || len(want.StringData) != 0 || wire.Digest(want.Data[wire.RequestKey]) != ref.RequestDigest {
		return "", errors.New("session Secret request differs from journaled authority")
	}
	b, err := wire.DecodeSessionBootstrap(want.Data[wire.RequestKey])
	if err != nil {
		return "", err
	}
	if _, err := sessionSecretObject(ref, b); err != nil {
		return "", err
	}
	return c.ensureSecret(ctx, ref, want.DeepCopy())
}

// AnnotateTurn updates observational attribution, never admission identity.
func (c *Client) AnnotateTurn(ctx context.Context, ref Reference, taskID, attemptID string) error {
	if !wire.UUID(ref.WorkerSessionID) || !wire.UUID(taskID) || !wire.UUID(attemptID) {
		return errors.New("invalid turn observation")
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pod, err := c.PodState(ctx, ref)
		if err != nil {
			return err
		}
		if pod.Annotations[taskLabel] == taskID && pod.Annotations[attemptLabel] == attemptID {
			return nil
		}
		patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"uid": pod.UID, "resourceVersion": pod.ResourceVersion,
			"annotations": map[string]string{taskLabel: taskID, attemptLabel: attemptID}}})
		_, err = c.API.CoreV1().Pods(c.Namespace).Patch(ctx, ref.PodName, types.MergePatchType, patch, metav1.PatchOptions{})
		return err
	})
}

func validSessionMount(ref Reference) bool {
	return wire.UUID(ref.WorkerSessionID) && workspace.ValidateTaskRoot(wire.WorkspaceRoot, ref.TaskRoot, ref.WorkspaceID, ref.WorkspaceAnchorTaskID) == nil
}
