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
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Client struct {
	API       clientset.Interface
	Transport *rest.Config
	Namespace string
}

func InCluster(namespace string) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	api, err := clientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{API: api, Transport: cfg, Namespace: namespace}, nil
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
func (c *Client) OwnerExists(ctx context.Context, r Reference) (bool, error) {
	p, err := c.pod(ctx, r.Owner.Name)
	return p != nil && string(p.UID) == r.Owner.UID, err
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

func metadataMatches(m metav1.ObjectMeta, r Reference, name, uid string) bool {
	if m.Name != name || m.Namespace != r.Namespace || m.UID == "" || uid != "" && string(m.UID) != uid || m.Labels[managedLabel] != managedValue || m.Labels[taskLabel] != r.TaskID || m.Labels[storageLabel] != r.StorageID || m.Labels[attemptLabel] != r.AttemptID || m.Annotations[digestAnnotation] != r.RequestDigest {
		return false
	}
	if len(m.OwnerReferences) != 1 {
		return false
	}
	o := m.OwnerReferences[0]
	return o.Kind == "Pod" && o.APIVersion == "v1" && o.Name == r.Owner.Name && string(o.UID) == r.Owner.UID && o.Controller != nil && *o.Controller && o.BlockOwnerDeletion != nil && *o.BlockOwnerDeletion
}
func secretMatches(s *corev1.Secret, r Reference) bool {
	return s != nil && metadataMatches(s.ObjectMeta, r, r.SecretName, r.SecretUID) && s.Immutable != nil && *s.Immutable && len(s.Data) == 1 && wire.Digest(s.Data[wire.RequestKey]) == r.RequestDigest
}
func podMatches(p *corev1.Pod, r Reference) bool {
	if p == nil || r.RuntimeRef.Validate() != nil || !sha256String.MatchString(r.PodDigest) || specFingerprint(p.Spec) != r.PodDigest || r.FixedNode != "" && p.Spec.NodeName != "" && p.Spec.NodeName != r.FixedNode {
		return false
	}
	if !metadataMatches(p.ObjectMeta, r, r.PodName, r.PodUID) || len(p.Spec.Containers) != 1 || len(p.Spec.InitContainers) != 1 || p.Spec.AutomountServiceAccountToken == nil || *p.Spec.AutomountServiceAccountToken {
		return false
	}
	worker, init := p.Spec.Containers[0], p.Spec.InitContainers[0]
	if worker.Name != "worker" || worker.Image != r.RuntimeRef.Image || len(worker.Command) != 0 || !slices.Equal(worker.Args, []string{"worker", "serve"}) || init.Name != "home-layout" || init.Image != r.RuntimeRef.Image || !slices.Equal(init.Command, []string{wire.ControllerRoot + "/runtime", "home", "layout", "--private-root=" + wire.PrivateRoot, "--request=" + wire.RequestPath}) {
		return false
	}
	if p.Spec.NodeSelector["kubernetes.io/os"] != "linux" || p.Spec.NodeSelector["kubernetes.io/arch"] != strings.TrimPrefix(r.RuntimeRef.Platform, "linux/") {
		return false
	}
	secretVolume, storage := false, false
	private := map[string]bool{}
	for _, v := range p.Spec.Volumes {
		if v.Name == "runtime-request" && v.Secret != nil && v.Secret.SecretName == r.SecretName {
			secretVolume = true
		}
	}
	for _, m := range worker.VolumeMounts {
		switch m.Name {
		case "runtime-workspace":
			if m.SubPath == ".multica-runtime/workers/"+r.StorageID && m.SubPathExpr == "" && !m.ReadOnly {
				storage = true
			}
		case "runtime-private":
			if !m.ReadOnly && m.SubPathExpr == "" {
				switch m.MountPath {
				case wire.Home:
					private["agents"] = m.SubPath == "agents"
				case "/tmp":
					private["tmp"] = m.SubPath == "tmp"
				case wire.ControlRoot:
					private["run"] = m.SubPath == "run"
				}
			}
		}
	}
	digest, task := "", ""
	for _, v := range worker.Env {
		if v.Name == "MULTICA_REQUEST_DIGEST" {
			digest = v.Value
		}
		if v.Name == "MULTICA_TASK_ID" {
			task = v.Value
		}
	}
	return secretVolume && storage && private["agents"] && private["tmp"] && private["run"] && digest == r.RequestDigest && task == r.TaskID
}

func (c *Client) validateReferenceAuthority(ctx context.Context, r Reference) error {
	if c.Namespace != r.Namespace {
		return errors.New("task reference namespace mismatch")
	}
	if err := r.RuntimeRef.Validate(); err != nil {
		return err
	}
	_, err := c.Controller(ctx, r.Owner.Name, r.Owner.UID, r.FixedNode)
	return diagnostics.Wrap("controller_authority_changed", err)
}
func (c *Client) CreateSecret(ctx context.Context, r Reference, request wire.Request) (string, error) {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return "", err
	}
	want, err := secretObject(r, request)
	if err != nil {
		return "", err
	}
	s, err := c.API.CoreV1().Secrets(c.Namespace).Create(ctx, want, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	if !secretMatches(s, r) {
		return "", errors.New("created Secret identity mismatch")
	}
	return string(s.UID), nil
}
func (c *Client) CreatePod(ctx context.Context, cfg Config, r Reference, request wire.Request, gateway string) (string, error) {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return "", err
	}
	want, err := podObject(cfg, r, request, gateway)
	if err != nil {
		return "", err
	}
	p, err := c.API.CoreV1().Pods(c.Namespace).Create(ctx, want, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	if !podMatches(p, r) {
		return "", errors.New("created Pod identity mismatch")
	}
	return string(p.UID), nil
}
func (c *Client) ResolveSecret(ctx context.Context, r Reference) (string, error) {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return "", err
	}
	return c.ResolveCleanupSecret(ctx, r)
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
func (c *Client) ResolvePod(ctx context.Context, r Reference) (string, error) {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return "", err
	}
	return c.ResolveCleanupPod(ctx, r)
}

// ResolveCleanupPod cannot grant execution authority; reuse and execution keep
// the live controller checks in ResolvePod and Execute.
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

func (c *Client) Request(ctx context.Context, name, task string) (wire.Request, error) {
	s, err := c.secret(ctx, name)
	if err != nil {
		return wire.Request{}, err
	}
	if s == nil || s.DeletionTimestamp != nil || s.Immutable == nil || !*s.Immutable || s.Labels[managedLabel] != managedValue || s.Labels[taskLabel] != task || wire.Digest(s.Data[wire.RequestKey]) != s.Annotations[digestAnnotation] {
		return wire.Request{}, errors.New("request authority unavailable")
	}
	r, err := wire.Decode(s.Data[wire.RequestKey])
	if err != nil {
		return r, err
	}
	if r.TaskID != task || r.AttemptID != s.Labels[attemptLabel] {
		return r, errors.New("request identity mismatch")
	}
	return r, nil
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

// StorageAvailable treats unknown mounts conservatively. The controller's own
// namespace-wide mount is the only exception, identified by its exact Pod UID.
func (c *Client) StorageAvailable(ctx context.Context, claim, storage string, owner Owner) error {
	active, err := c.ActiveStorage(ctx, claim, owner)
	if err != nil {
		return err
	}
	if active[storage] {
		return errors.New("worker storage has an unresolved Pod consumer")
	}
	return nil
}

func (c *Client) ActiveStorage(ctx context.Context, claim string, owner Owner) (map[string]bool, error) {
	pods, err := c.Pods(ctx)
	if err != nil {
		return nil, err
	}
	active := map[string]bool{}
	for _, p := range pods {
		if p.Name == owner.Name && string(p.UID) == owner.UID {
			continue
		}
		volumes := map[string]bool{}
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == claim {
				volumes[v.Name] = true
			}
		}
		containers := append(slices.Clone(p.Spec.Containers), p.Spec.InitContainers...)
		for _, e := range p.Spec.EphemeralContainers {
			containers = append(containers, corev1.Container{VolumeMounts: e.VolumeMounts, VolumeDevices: e.VolumeDevices})
		}
		for _, c := range containers {
			for _, d := range c.VolumeDevices {
				if volumes[d.Name] {
					return nil, errors.New("workspace has an unknown block-volume consumer")
				}
			}
			for _, m := range c.VolumeMounts {
				if !volumes[m.Name] {
					continue
				}
				const prefix = ".multica-runtime/workers/"
				if session, ok := strings.CutPrefix(m.SubPath, ".multica-runtime/sessions/"); ok && m.SubPathExpr == "" && session != "" && !strings.Contains(session, "/") && !strings.HasPrefix(session, ".") && strings.HasSuffix(session, ".jsonl") {
					continue
				}
				if m.SubPathExpr != "" || !strings.HasPrefix(m.SubPath, prefix) {
					return nil, errors.New("workspace has an unbounded Pod mount")
				}
				id, _, _ := strings.Cut(strings.TrimPrefix(m.SubPath, prefix), "/")
				if !wire.UUID(id) {
					return nil, errors.New("workspace has an unknown Pod mount")
				}
				active[id] = true
			}
		}
	}
	return active, nil
}

// Cleanup requires a durable UID before it deletes, and verifies absence before
// releasing the storage lease. The caller resolves uncertain creates first.
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
		uid := types.UID(r.PodUID)
		if err := c.API.CoreV1().Pods(c.Namespace).Delete(ctx, r.PodName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
			p, err := c.pod(ctx, r.PodName)
			if err != nil {
				return false, err
			}
			if p == nil {
				return true, nil
			}
			if string(p.UID) != r.PodUID {
				return false, errors.New("Pod replaced during cleanup")
			}
			return false, nil
		}); err != nil {
			return err
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
