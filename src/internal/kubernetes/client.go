package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Client struct {
	API       clientset.Interface
	Namespace string
	watchAPI  clientset.Interface
}

func InCluster(namespace string, capacity int) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	cfg.Timeout = 10 * time.Second
	// Live admission performs several reads per worker. The client-go default
	// of five requests/second cannot serve the controller's configured capacity.
	cfg.QPS = float32(max(20, capacity*5))
	cfg.Burst = int(cfg.QPS) * 2
	api, err := clientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	// Keep bounded live reads without cutting each watch stream off after ten
	// seconds. The watch owns its request context and server-side timeout.
	watchConfig := rest.CopyConfig(cfg)
	watchConfig.Timeout = 0
	watchAPI, err := clientset.NewForConfig(watchConfig)
	if err != nil {
		return nil, err
	}
	return &Client{API: api, Namespace: namespace, watchAPI: watchAPI}, nil
}
func (c *Client) Controller(ctx context.Context, name, expectedUID, node string) (Owner, error) {
	p, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return Owner{}, err
	}
	if expectedUID == "" || string(p.UID) != expectedUID || p.DeletionTimestamp != nil || node != "" && p.Spec.NodeName != node {
		return Owner{}, errors.New("controller identity or fixed Node mismatch")
	}
	return Owner{Name: p.Name, UID: string(p.UID)}, nil
}
func (c *Client) pod(ctx context.Context, name string) (*corev1.Pod, error) {
	p, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return p, err
}
func (c *Client) secret(ctx context.Context, name string) (*corev1.Secret, error) {
	s, err := c.API.CoreV1().Secrets(c.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return s, err
}

func metadataRequestMatches(m metav1.ObjectMeta, r Reference, name string) bool {
	if m.Name != name || m.Namespace != r.Namespace || m.Labels[managedLabel] != managedValue || m.Labels[ownerLabel] != r.OwnerID || m.Labels[storageLabel] != r.StorageID || m.Annotations[digestAnnotation] != r.RequestDigest {
		return false
	}
	if r.WorkerSessionID != "" {
		if !wire.UUID(r.WorkerSessionID) || m.Labels[sessionLabel] != r.WorkerSessionID {
			return false
		}
	} else if m.Labels[taskLabel] != r.TaskID || m.Labels[attemptLabel] != r.AttemptID || m.Labels[sessionLabel] != "" {
		return false
	}
	return len(m.OwnerReferences) == 0
}
func metadataMatches(m metav1.ObjectMeta, r Reference, name, uid string) bool {
	return m.UID != "" && (uid == "" || string(m.UID) == uid) && metadataRequestMatches(m, r, name)
}
func secretMatches(s *corev1.Secret, r Reference) bool {
	return s != nil && metadataMatches(s.ObjectMeta, r, r.SecretName, r.SecretUID) && s.Immutable != nil && *s.Immutable && len(s.Data) == 1 && wire.Digest(s.Data[wire.RequestKey]) == r.RequestDigest
}
func podMatches(p *corev1.Pod, r Reference) bool {
	return p != nil && p.UID != "" && (r.PodUID == "" || string(p.UID) == r.PodUID) && (r.NodeID == "" || p.Spec.NodeName == r.NodeID) && podRequestMatches(p, r)
}
func podRequestMatches(p *corev1.Pod, r Reference) bool {
	if p == nil || r.RuntimeRef.Validate() != nil || !sha256String.MatchString(r.PodDigest) || specFingerprint(p.Spec) != r.PodDigest {
		return false
	}
	if r.WorkerSessionID != "" && p.Spec.ActiveDeadlineSeconds != nil {
		return false
	}
	if !metadataRequestMatches(p.ObjectMeta, r, r.PodName) || len(p.Spec.Containers) != 1 || len(p.Spec.InitContainers) != 1 || p.Spec.AutomountServiceAccountToken == nil || *p.Spec.AutomountServiceAccountToken {
		return false
	}
	worker, init := p.Spec.Containers[0], p.Spec.InitContainers[0]
	if worker.Name != "worker" || worker.Image != r.RuntimeRef.Image || len(worker.Command) != 0 || !slices.Equal(worker.Args, []string{"worker", "serve"}) || init.Name != "task-layout" || init.Image != r.RuntimeRef.Image || !slices.Equal(init.Command, []string{wire.ControllerRoot + "/runtime", "worker", "layout", "--private-root=" + wire.PrivateRoot, "--request=" + wire.RequestPath}) {
		return false
	}
	if p.Spec.NodeSelector["kubernetes.io/os"] != "linux" || p.Spec.NodeSelector["kubernetes.io/arch"] != strings.TrimPrefix(r.RuntimeRef.Platform, "linux/") {
		return false
	}
	if unprivilegedPodSecurity(p.Spec) != nil || p.Spec.HostNetwork || p.Spec.HostPID || p.Spec.HostIPC || len(p.Spec.EphemeralContainers) != 0 || p.Spec.ShareProcessNamespace != nil && *p.Spec.ShareProcessNamespace {
		return false
	}
	if p.Spec.NodeSelector["kubernetes.io/hostname"] != "" || p.Spec.Affinity != nil {
		return false
	}
	taskVolume, secretVolume := false, false
	for _, v := range p.Spec.Volumes {
		if v.Name == "runtime-task" && v.NFS != nil && v.NFS.Server == r.NFSServer && v.NFS.Path == r.TaskRoot && !v.NFS.ReadOnly {
			taskVolume = true
		}
		if v.Name == "runtime-request" && v.Secret != nil && v.Secret.SecretName == r.SecretName {
			secretVolume = true
		}
	}
	return taskVolume && secretVolume && r.PVCUID != "" && validTaskMount(r)

}

func (c *Client) validateReferenceAuthority(ctx context.Context, r Reference) error {
	if c.Namespace != r.Namespace {
		return errors.New("task reference namespace mismatch")
	}
	if err := r.RuntimeRef.Validate(); err != nil {
		return err
	}
	_, err := c.Controller(ctx, r.Owner.Name, r.Owner.UID, "")
	return diagnostics.Wrap("controller_authority_changed", err)
}

var ErrSecretPending = errors.New("bootstrap Secret creation outcome is pending")

// EnsureSecret recovers an unacknowledged create under the same immutable
// request authority. A journaled UID can be verified but never replaced.
func (c *Client) EnsureSecret(ctx context.Context, r Reference, request wire.Bootstrap) (string, error) {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return "", err
	}
	want, err := secretObject(r, request)
	if err != nil {
		return "", err
	}
	return c.ensureSecret(ctx, r, want)
}

func (c *Client) ensureSecret(ctx context.Context, r Reference, want *corev1.Secret) (string, error) {
	s, err := c.secret(ctx, r.SecretName)
	if err != nil {
		return "", err
	}
	if s == nil {
		if r.SecretUID != "" {
			return "", errors.New("journaled bootstrap Secret is missing")
		}
		s, err = c.API.CoreV1().Secrets(c.Namespace).Create(ctx, want, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			s, err = c.secret(ctx, r.SecretName)
			if err == nil && s == nil {
				// The competing object disappeared before lookup. Leave the
				// next bounded attempt to the reconciler with a fresh context.
				return "", ErrSecretPending
			}
		}
		if err != nil {
			return "", err
		}
	}
	if !secretMatches(s, r) || s.DeletionTimestamp != nil {
		return "", errors.New("bootstrap Secret identity mismatch or terminating")
	}
	return string(s.UID), nil
}

var ErrPodPending = errors.New("worker Pod creation outcome is pending")

// EnsurePod retries the journaled creation request. Finding an existing worker
// recovers its UID, including a terminating worker whose stop must be resolved.
func (c *Client) EnsurePod(ctx context.Context, r Reference, want *corev1.Pod) (string, error) {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return "", err
	}
	if !podRequestMatches(want, r) || want.UID != "" || want.ResourceVersion != "" || want.DeletionTimestamp != nil || want.Spec.NodeName != "" || !slices.Contains(want.Finalizers, terminationFinalizer) {
		return "", errors.New("Pod creation request differs from journaled authority")
	}
	p, err := c.pod(ctx, r.PodName)
	if err != nil {
		return "", err
	}
	if p == nil {
		if r.PodUID != "" {
			return "", errors.New("journaled worker Pod is missing")
		}
		if err := c.StorageAvailable(ctx, r); err != nil {
			// A prior POST can commit between our lookup and the consumer
			// list. Recover that worker instead of treating it as a new writer.
			var lookupErr error
			p, lookupErr = c.pod(ctx, r.PodName)
			if lookupErr != nil {
				return "", lookupErr
			}
			if p == nil {
				return "", err
			}
		}
		if p == nil {
			secret, err := c.secret(ctx, r.SecretName)
			if err != nil {
				return "", err
			}
			if r.SecretUID == "" || !secretMatches(secret, r) || secret.DeletionTimestamp != nil {
				return "", errors.New("journaled bootstrap Secret UID required")
			}
			p, err = c.API.CoreV1().Pods(c.Namespace).Create(ctx, want.DeepCopy(), metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				p, err = c.pod(ctx, r.PodName)
				if err == nil && p == nil {
					return "", ErrPodPending
				}
			}
			if err != nil {
				return "", err
			}
		}
	}
	if !podMatches(p, r) || !slices.Contains(p.Finalizers, terminationFinalizer) {
		return "", errors.New("worker Pod identity mismatch")
	}
	return string(p.UID), nil
}

// ResolveCleanupSecret identifies a journaled resource for deletion only.
// The controller may already be terminating or absent.
func (c *Client) ResolveCleanupSecret(ctx context.Context, r Reference) (string, error) {
	if c.Namespace != r.Namespace {
		return "", errors.New("cleanup reference namespace mismatch")
	}
	s, err := c.secret(ctx, r.SecretName)
	if err != nil || s == nil {
		return "", err
	}
	if !secretMatches(s, r) {
		return "", errors.New("Secret identity mismatch")
	}
	return string(s.UID), nil
}

// ResolveCleanupPod cannot grant execution authority; reuse and execution keep
// the live controller checks in Authorize.
func (c *Client) ResolveCleanupPod(ctx context.Context, r Reference) (string, error) {
	if c.Namespace != r.Namespace {
		return "", errors.New("cleanup reference namespace mismatch")
	}
	p, err := c.pod(ctx, r.PodName)
	if err != nil || p == nil {
		return "", err
	}
	if !podMatches(p, r) {
		return "", errors.New("Pod identity mismatch")
	}
	return string(p.UID), nil
}

func (c *Client) Pods(ctx context.Context) ([]corev1.Pod, error) {
	var result []corev1.Pod
	opts := metav1.ListOptions{Limit: 500}
	for {
		page, err := c.API.CoreV1().Pods(c.Namespace).List(ctx, opts)
		if err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if page.Continue == "" {
			return result, nil
		}
		opts.Continue = page.Continue
	}
}

var ErrCleanupPending = errors.New("attempt resource cleanup is pending")

// Cleanup removes only journaled Pod and Secret objects after the caller has
// durably recorded stop evidence. Removing our finalizer releases that evidence;
// this function cannot decide whether a storage grant is safe to reuse. Absence
// is never fencing, and PVCs are never deleted. Pending deletion is retried by
// the attempt reconciler without blocking other attempts.
func (c *Client) Cleanup(ctx context.Context, r Reference) error {
	if c.Namespace != r.Namespace {
		return errors.New("cleanup namespace mismatch")
	}
	p, err := c.pod(ctx, r.PodName)
	if err != nil {
		return err
	}
	if p != nil {
		if r.PodUID == "" || !podMatches(p, r) {
			return errors.New("cleanup Pod identity is unresolved or replaced")
		}
		if p.DeletionTimestamp == nil {
			uid := types.UID(r.PodUID)
			if err := c.API.CoreV1().Pods(c.Namespace).Delete(ctx, r.PodName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		p, err = c.pod(ctx, r.PodName)
		if err != nil {
			return err
		}
		if p != nil {
			if !podMatches(p, r) {
				return errors.New("Pod replaced during cleanup")
			}
			if err := c.releaseTerminationFinalizer(ctx, p); err != nil {
				return err
			}
		}
		p, err = c.pod(ctx, r.PodName)
		if err != nil {
			return err
		}
		if p != nil {
			if string(p.UID) != r.PodUID {
				return errors.New("Pod replaced during cleanup")
			}
			return ErrCleanupPending
		}
	}
	pods, err := c.Pods(ctx)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if ReferencesSecret(&pod, r.SecretName) {
			return errors.New("request Secret remains referenced")
		}
	}
	s, err := c.secret(ctx, r.SecretName)
	if err != nil {
		return err
	}
	if s != nil {
		if r.SecretUID == "" || !secretMatches(s, r) {
			return errors.New("cleanup Secret identity is unresolved or replaced")
		}
		uid := types.UID(r.SecretUID)
		if err := c.API.CoreV1().Secrets(c.Namespace).Delete(ctx, r.SecretName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	if s, err := c.secret(ctx, r.SecretName); err != nil {
		return err
	} else if s != nil {
		return errors.New("Secret deletion unconfirmed")
	}
	return nil
}

func (c *Client) releaseTerminationFinalizer(ctx context.Context, pod *corev1.Pod) error {
	if !slices.Contains(pod.Finalizers, terminationFinalizer) {
		return nil
	}
	if pod.ResourceVersion == "" {
		return errors.New("Pod resource version required to release termination evidence")
	}
	finalizers := slices.DeleteFunc(slices.Clone(pod.Finalizers), func(value string) bool { return value == terminationFinalizer })
	patch, err := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": string(pod.UID)},
		{"op": "test", "path": "/metadata/resourceVersion", "value": pod.ResourceVersion},
		{"op": "replace", "path": "/metadata/finalizers", "value": finalizers},
	})
	if err != nil {
		return err
	}
	_, err = c.API.CoreV1().Pods(c.Namespace).Patch(ctx, pod.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// Kubernetes' typed Pod schema includes several historical CSI/volume secret
// references. Walk those actual reference fields without assuming only the
// worker's own Secret volume can hold an attempt Secret alive.
func ReferencesSecret(p *corev1.Pod, name string) bool {
	raw, _ := json.Marshal(p.Spec)
	var spec any
	if json.Unmarshal(raw, &spec) != nil {
		return true
	}
	var visit func(any, string) bool
	visit = func(value any, key string) bool {
		switch v := value.(type) {
		case map[string]any:
			if strings.Contains(strings.ToLower(key), "secret") {
				if v["name"] == name || v["secretName"] == name {
					return true
				}
			}
			for k, child := range v {
				if (k == "secretName" || k == "secretFile") && child == name {
					return true
				}
				if visit(child, k) {
					return true
				}
			}
		case []any:
			for _, child := range v {
				if visit(child, key) {
					return true
				}
			}
		}
		return false
	}
	return visit(spec, "")
}
