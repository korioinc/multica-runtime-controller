package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/fixturehome"
	runtimekube "github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

// Faults wrap a real API transport. POST loss happens only after the API response
// body has arrived, so the recovery check observes an actually committed object.
var (
	errCreateReplyLost = errors.New("fixture discarded successful create response")
	errDeleteOutage    = errors.New("fixture injected delete transport outage")
	errReplacementExec = errors.New("fixture refused execution against replaced UID")
)

type transportFault struct {
	base          http.RoundTripper
	mutex         sync.Mutex
	losePost      map[string]bool
	denyDelete    bool
	forbidExec    bool
	execAttempted bool
	ownerRead     *ownerReadObservation
}

func (t *transportFault) RoundTrip(request *http.Request) (*http.Response, error) {
	t.mutex.Lock()
	if t.forbidExec && strings.HasSuffix(request.URL.Path, "/exec") {
		t.execAttempted = true
		t.mutex.Unlock()
		return nil, errReplacementExec
	}
	if t.denyDelete && request.Method == http.MethodDelete {
		t.mutex.Unlock()
		return nil, errDeleteOutage
	}
	lose := request.Method == http.MethodPost && t.losePost[filepath.Base(request.URL.Path)]
	if lose {
		delete(t.losePost, filepath.Base(request.URL.Path))
	}
	t.mutex.Unlock()
	if t.ownerRead != nil && request.Method == http.MethodGet && request.URL.Path == t.ownerRead.path {
		return t.ownerRead.roundTrip(t.base, request)
	}
	response, err := t.base.RoundTrip(request)
	if err != nil || !lose {
		return response, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response, nil
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if err = errors.Join(readErr, closeErr); err != nil {
		return nil, err
	}
	return nil, errCreateReplyLost
}
func (t *transportFault) attemptedExecution() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.execAttempted
}

func (f *fixture) faultClient(fault *transportFault) (*runtimekube.Client, error) {
	cfg := rest.CopyConfig(f.config)
	previous := cfg.WrapTransport
	cfg.WrapTransport = func(base http.RoundTripper) http.RoundTripper {
		if previous != nil {
			base = previous(base)
		}
		fault.base = base
		return fault
	}
	api, err := clientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &runtimekube.Client{API: api, Transport: cfg, Namespace: f.selection.Namespace}, nil
}
func (f *fixture) attempt() (runtimekube.Reference, wire.Request, error) {
	return f.attemptWithImage("", nil)
}

func (f *fixture) attemptWithImage(image string, index []byte) (runtimekube.Reference, wire.Request, error) {
	request := f.request
	request.AttemptID = uuid.NewString()
	request.Env = append(slices.Clone(request.Env), fixturehome.PreserveSampleEnv+"=true")
	if image != "" {
		if index == nil {
			return runtimekube.Reference{}, request, errors.New("fixture index execution requires its captured registry bytes")
		}
		request.RuntimeRef.Image = image
	}
	storage := filepath.Base(request.WorkerSubPath)
	podName := "task-worker-" + storage
	if _, err := f.api.CoreV1().Pods(f.selection.Namespace).Get(f.ctx, podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		if err != nil {
			return runtimekube.Reference{}, request, err
		}
		return runtimekube.Reference{}, request, errors.New("fixture worker name is still occupied; refusing to adopt it")
	}
	var err error
	request.HomeDigest, err = f.home.Publish(f.ctx, request, index)
	if err != nil {
		return runtimekube.Reference{}, request, err
	}
	raw, err := json.Marshal(request)
	if err != nil {
		return runtimekube.Reference{}, request, err
	}
	if _, err = wire.Decode(raw); err != nil {
		return runtimekube.Reference{}, request, err
	}
	ref := runtimekube.Reference{Namespace: f.selection.Namespace, Owner: f.selection.Controller, TaskID: request.TaskID, StorageID: storage, AttemptID: request.AttemptID, PodName: podName, SecretName: "task-request-" + request.AttemptID, RequestDigest: wire.Digest(raw), RuntimeRef: request.RuntimeRef, FixedNode: f.selection.Worker.SingleNodeName}
	ref.PodDigest, err = runtimekube.PodFingerprint(f.selection.Worker, ref, request, f.selection.Gateway)
	return ref, request, err
}
func (f *fixture) createSecret(ref *runtimekube.Reference, request wire.Request) error {
	uid, err := f.client.CreateSecret(f.ctx, *ref, request)
	if err != nil {
		return err
	}
	ref.SecretUID = uid
	secret, err := f.api.CoreV1().Secrets(ref.Namespace).Get(f.ctx, ref.SecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepSecret(secret)
	return nil
}
func (f *fixture) createPair() (runtimekube.Reference, wire.Request, error) {
	ref, request, err := f.attempt()
	if err != nil {
		return ref, request, err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return ref, request, err
	}
	ref.PodUID, err = f.client.CreatePod(f.ctx, f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return ref, request, err
	}
	pod, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err == nil {
		f.keepPod(pod)
	}
	return ref, request, err
}
func (f *fixture) responseLoss() error {
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	fault := &transportFault{losePost: map[string]bool{"pods": true, "secrets": true}}
	client, err := f.faultClient(fault)
	if err != nil {
		return err
	}
	if uid, err := client.CreateSecret(f.ctx, ref, request); !errors.Is(err, errCreateReplyLost) || uid != "" {
		return errors.New("create response loss was not exercised for Secret")
	}
	ref.SecretUID, err = f.client.ResolveSecret(f.ctx, ref)
	if err != nil || ref.SecretUID == "" {
		return errors.Join(err, errors.New("committed Secret was not resolved"))
	}
	secret, err := f.api.CoreV1().Secrets(ref.Namespace).Get(f.ctx, ref.SecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepSecret(secret)
	foreign := ref
	foreign.Owner.UID = uuid.NewString()
	if _, err = f.client.ResolveSecret(f.ctx, foreign); err == nil {
		return errors.New("foreign owner adopted committed Secret")
	}
	changed := ref
	changed.RequestDigest = strings.Repeat("0", 64)
	if _, err = f.client.ResolveSecret(f.ctx, changed); err == nil {
		return errors.New("wrong request adopted committed Secret")
	}
	if uid, err := client.CreatePod(f.ctx, f.selection.Worker, ref, request, f.selection.Gateway); !errors.Is(err, errCreateReplyLost) || uid != "" {
		return errors.New("create response loss was not exercised for Pod")
	}
	ref.PodUID, err = f.client.ResolvePod(f.ctx, ref)
	if err != nil || ref.PodUID == "" {
		return errors.Join(err, errors.New("committed Pod was not resolved"))
	}
	pod, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepPod(pod)
	foreign = ref
	foreign.Owner.UID = uuid.NewString()
	if _, err = f.client.ResolvePod(f.ctx, foreign); err == nil {
		return errors.New("foreign owner adopted committed Pod")
	}
	changed = ref
	changed.RuntimeRef.Image = "invalid@sha256:" + strings.Repeat("0", 64)
	if _, err = f.client.ResolvePod(f.ctx, changed); err == nil {
		return errors.New("wrong environment adopted committed Pod")
	}
	return f.client.Cleanup(f.ctx, ref)
}
func resetPod(p *corev1.Pod) {
	p.ResourceVersion = ""
	p.UID = ""
	p.Generation = 0
	p.CreationTimestamp = metav1.Time{}
	p.ManagedFields = nil
	p.DeletionTimestamp = nil
	p.DeletionGracePeriodSeconds = nil
	p.Status = corev1.PodStatus{}
	p.Spec.NodeName = ""
}
func (f *fixture) podReplacement() error {
	ref, _, err := f.createPair()
	if err != nil {
		return err
	}
	original, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err = f.deletePod(original.Name, original.UID); err != nil {
		return err
	}
	replacement := original.DeepCopy()
	resetPod(replacement)
	replacement, err = f.api.CoreV1().Pods(ref.Namespace).Create(f.ctx, replacement, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	f.keepPod(replacement)
	if string(replacement.UID) == ref.PodUID {
		return errors.New("API did not produce a replacement Pod identity")
	}
	if _, err = f.client.Controller(f.ctx, ref.PodName, ref.PodUID, ""); err == nil {
		return errors.New("stale controller identity adopted same-name replacement")
	}
	if _, err = f.client.Controller(f.ctx, ref.PodName, string(replacement.UID), ""); err != nil {
		return errors.Join(err, errors.New("live replacement identity was not observable"))
	}
	guard := &transportFault{forbidExec: true}
	client, err := f.faultClient(guard)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	err = client.Execute(ctx, ref, 4*time.Second, runtimekube.Streams{Stdout: io.Discard, Stderr: io.Discard})
	cancel()
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errReplacementExec) || guard.attemptedExecution() {
		return errors.New("old attempt did not reject replacement before exec")
	}
	if err = f.client.Cleanup(f.ctx, ref); err == nil {
		return errors.New("old attempt cleaned up a replacement Pod")
	}
	found, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil || found.UID != replacement.UID {
		return errors.Join(err, errors.New("replacement Pod was lost"))
	}
	if err = f.deletePod(found.Name, found.UID); err != nil {
		return err
	}
	ref.PodUID = ""
	return f.client.Cleanup(f.ctx, ref)
}
func (f *fixture) deleteSecret(name string, uid types.UID) error {
	return f.api.CoreV1().Secrets(f.selection.Namespace).Delete(f.ctx, name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}
func (f *fixture) secretReplacement() error {
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return err
	}
	original, err := f.api.CoreV1().Secrets(ref.Namespace).Get(f.ctx, ref.SecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err = f.deleteSecret(original.Name, original.UID); err != nil {
		return err
	}
	replacement := original.DeepCopy()
	replacement.UID = ""
	replacement.ResourceVersion = ""
	replacement.CreationTimestamp = metav1.Time{}
	replacement.ManagedFields = nil
	replacement, err = f.api.CoreV1().Secrets(ref.Namespace).Create(f.ctx, replacement, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	f.keepSecret(replacement)
	if string(replacement.UID) == ref.SecretUID {
		return errors.New("API did not produce a replacement Secret identity")
	}
	if _, err = f.client.ResolveSecret(f.ctx, ref); err == nil {
		return errors.New("old attempt resolved replacement Secret")
	}
	if err = f.client.Cleanup(f.ctx, ref); err == nil {
		return errors.New("old attempt cleaned replacement Secret")
	}
	found, err := f.api.CoreV1().Secrets(ref.Namespace).Get(f.ctx, ref.SecretName, metav1.GetOptions{})
	if err != nil || found.UID != replacement.UID {
		return errors.Join(err, errors.New("replacement Secret was lost"))
	}
	return f.deleteSecret(found.Name, found.UID)
}
func (f *fixture) simplePod(prefix string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix, Namespace: f.selection.Namespace, Labels: map[string]string{"multica.io/verification": "resource-scheduling"}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: ptr.To(false), TerminationGracePeriodSeconds: ptr.To[int64](0), Containers: []corev1.Container{{Name: "probe", Image: f.selection.RuntimeRef.Image, ImagePullPolicy: f.selection.Worker.ImagePullPolicy, Command: []string{"/bin/bash", "-ec", "true"}}}}}
}
func (f *fixture) referencedSecret() error {
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return err
	}
	holder := f.simplePod("verify-secret-holder-")
	holder.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "verification.multica.io/hold"}}
	holder.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: ref.SecretName}}}}
	holder, err = f.api.CoreV1().Pods(ref.Namespace).Create(f.ctx, holder, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	f.keepPod(holder)
	if err = f.client.Cleanup(f.ctx, ref); err == nil {
		return errors.New("cleanup ignored another Pod's Secret reference")
	}
	if _, err = f.api.CoreV1().Secrets(ref.Namespace).Get(f.ctx, ref.SecretName, metav1.GetOptions{}); err != nil {
		return errors.Join(err, errors.New("referenced Secret was deleted"))
	}
	if err = f.deletePod(holder.Name, holder.UID); err != nil {
		return err
	}
	return f.client.Cleanup(f.ctx, ref)
}
func (f *fixture) storageProbe(name, content string, write bool) error {
	pod := f.simplePod("verify-storage-")
	pod.Spec.NodeSelector = f.selection.Worker.NodeSelector
	pod.Spec.Affinity = f.fixedAffinity(f.selection.Worker.SingleNodeName)
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: ptr.To[int64](65532), RunAsGroup: ptr.To[int64](65532), FSGroup: ptr.To[int64](65532), FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch)}
	pod.Spec.Volumes = []corev1.Volume{{Name: "storage", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: f.selection.Worker.WorkspaceClaim, ReadOnly: !write}}}}
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "storage", MountPath: "/fixture", SubPath: f.request.WorkerSubPath, ReadOnly: !write}}
	pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "MARKER", Value: name}, {Name: "CONTENT", Value: content}}
	if write {
		pod.Spec.Containers[0].Command = []string{"/bin/bash", "-ec", `printf '%s' "$CONTENT" > "/fixture/$MARKER"; sync`}
	} else {
		pod.Spec.Containers[0].Command = []string{"/bin/bash", "-ec", `cat "/fixture/$MARKER"`}
	}
	actual, err := f.api.CoreV1().Pods(f.selection.Namespace).Create(f.ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	f.keepPod(actual)
	if err = wait.PollUntilContextTimeout(f.ctx, 250*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		p, err := f.api.CoreV1().Pods(f.selection.Namespace).Get(ctx, actual.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if p.Status.Phase == corev1.PodFailed {
			return false, errors.New("storage probe failed")
		}
		return p.Status.Phase == corev1.PodSucceeded, nil
	}); err != nil {
		return err
	}
	if !write {
		raw, err := f.api.CoreV1().Pods(f.selection.Namespace).GetLogs(actual.Name, &corev1.PodLogOptions{Container: "probe"}).DoRaw(f.ctx)
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, []byte(content)) {
			return errors.New("worker file changed during failed cleanup and recovery")
		}
	}
	return f.deletePod(actual.Name, actual.UID)
}
func (f *fixture) cleanupRecovery() error {
	marker, content := "verification-"+uuid.NewString(), uuid.NewString()
	if err := f.storageProbe(marker, content, true); err != nil {
		return fmt.Errorf("persist worker file: %w", err)
	}
	ref, _, err := f.createPair()
	if err != nil {
		return err
	}
	fault := &transportFault{denyDelete: true}
	client, err := f.faultClient(fault)
	if err != nil {
		return err
	}
	if err = client.Cleanup(f.ctx, ref); !errors.Is(err, errDeleteOutage) {
		return errors.New("cleanup did not expose delete transport failure")
	}
	if _, err = f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{}); err != nil {
		return errors.New("failed delete lost the original Pod")
	}
	// Persist only ownership metadata, then reconstruct a new client and reference.
	// This proves cleanup retry, not the production Runner journal's crash protocol.
	raw, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	file, err := os.OpenFile("/verification-evidence/cleanup-reference.json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	if err = errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	ref = runtimekube.Reference{}
	raw, err = os.ReadFile("/verification-evidence/cleanup-reference.json")
	if err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &ref); err != nil {
		return err
	}
	freshAPI, err := clientset.NewForConfig(f.config)
	if err != nil {
		return err
	}
	fresh := &runtimekube.Client{API: freshAPI, Transport: f.config, Namespace: f.selection.Namespace}
	if err = fresh.Cleanup(f.ctx, ref); err != nil {
		return err
	}
	return f.storageProbe(marker, content, false)
}

func (f *fixture) payloadSubstitution() error {
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return err
	}
	fault := &transportFault{losePost: map[string]bool{"pods": true}}
	client, err := f.faultClient(fault)
	if err != nil {
		return err
	}
	if _, err = client.CreatePod(f.ctx, f.selection.Worker, ref, request, f.selection.Gateway); !errors.Is(err, errCreateReplyLost) {
		return errors.New("lost create response was not exercised")
	}
	original, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepPod(original)
	if err = f.deletePod(original.Name, original.UID); err != nil {
		return err
	}
	modified := original.DeepCopy()
	resetPod(modified)
	// Existing validated mounts, owner, attempt and request metadata stay intact;
	// an extra read-only root mount exposes unrelated workspace authority.
	modified.Spec.Volumes = append(modified.Spec.Volumes, corev1.Volume{Name: "fixture-rerouted-workspace", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: f.selection.Worker.WorkspaceClaim, ReadOnly: true}}})
	modified.Spec.Containers[0].VolumeMounts = append(modified.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "fixture-rerouted-workspace", MountPath: "/unauthorized-workspace", ReadOnly: true})
	modified, err = f.api.CoreV1().Pods(ref.Namespace).Create(f.ctx, modified, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	f.keepPod(modified)
	if _, err = f.client.ResolvePod(f.ctx, ref); err == nil {
		return errors.New("lost-create recovery adopted an expanded workspace mount")
	}
	ref.PodUID = string(modified.UID)
	if err = f.client.Cleanup(f.ctx, ref); err == nil {
		return errors.New("cleanup deleted substituted Pod using metadata alone")
	}
	found, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil || found.UID != modified.UID {
		return errors.Join(err, errors.New("substituted Pod was not preserved for operator resolution"))
	}
	if err = f.deletePod(found.Name, found.UID); err != nil {
		return err
	}
	return f.client.Cleanup(f.ctx, ref)
}

func (f *fixture) limitsDefaults() error {
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	cfg := f.selection.Worker
	if len(cfg.Resources.Limits) == 0 {
		return errors.New("limits-only fixture requires resource limits")
	}
	cfg.Resources.Requests = nil
	ref.PodDigest, err = runtimekube.PodFingerprint(cfg, ref, request, f.selection.Gateway)
	if err != nil {
		return err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return err
	}
	ref.PodUID, err = f.client.CreatePod(f.ctx, cfg, ref, request, f.selection.Gateway)
	if err != nil {
		return fmt.Errorf("API-defaulted limits-only Pod was not accepted: %w", err)
	}
	actual, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepPod(actual)
	if _, err = f.client.ResolvePod(f.ctx, ref); err != nil {
		return fmt.Errorf("API-defaulted Pod was not recoverable: %w", err)
	}
	return f.client.Cleanup(f.ctx, ref)
}

type disconnectedOutput struct {
	mu       sync.Mutex
	received bool
}

func (w *disconnectedOutput) Write(data []byte) (int, error) {
	w.mu.Lock()
	w.received = w.received || len(data) > 0
	w.mu.Unlock()
	return 0, io.EOF
}
func (w *disconnectedOutput) observedBytes() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.received
}
func (f *fixture) streamDrop() (result error) {
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	session, err := wire.PiSession(f.request)
	if err != nil || session == "" {
		return errors.Join(err, errors.New("stream fixture requires the existing Pi session binding"))
	}
	request.Provider = "pi"
	request.Args = []string{"--version", "--session", session}
	raw, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if _, err = wire.Decode(raw); err != nil {
		return err
	}
	ref.RequestDigest = wire.Digest(raw)
	ref.PodDigest, err = runtimekube.PodFingerprint(f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		result = errors.Join(result, f.client.Cleanup(cleanup, ref))
	}()
	ref.PodUID, err = f.client.CreatePod(f.ctx, f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return err
	}
	pod, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepPod(pod)
	downstream := &disconnectedOutput{}
	err = f.client.Execute(f.ctx, ref, 2*time.Minute, runtimekube.Streams{Stdout: downstream, Stderr: io.Discard})
	if !downstream.observedBytes() {
		return errors.Join(err, errors.New("downstream failure did not receive actual provider bytes"))
	}
	if err == nil {
		return errors.New("broken downstream endpoint was reported as successful execution")
	}
	_, exited := runtimekube.ExitCode(err)
	if exited {
		return errors.New("broken transport was reduced to a provider exit result")
	}
	return nil
}
