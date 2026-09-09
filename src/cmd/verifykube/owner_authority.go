package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	runtimekube "github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/retry"
)

const ownerHoldFinalizer = "verification.multica.io/hold-owner"

// The barrier changes real API objects immediately before the production
// owner's GET. Its original response is observed and passed through unchanged.
// This keeps an orphaned worker alive without mistaking GC for an authority gate.
type ownerReadObservation struct {
	path      string
	before    func(context.Context) error
	once      sync.Once
	beforeErr error
	mutex     sync.Mutex
	pod       *corev1.Pod
	err       error
}

func (o *ownerReadObservation) roundTrip(base http.RoundTripper, request *http.Request) (*http.Response, error) {
	o.once.Do(func() {
		if o.before != nil {
			o.beforeErr = o.before(request.Context())
		}
	})
	if o.beforeErr != nil {
		return nil, o.beforeErr
	}
	response, err := base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	response.Body = io.NopCloser(bytes.NewReader(raw))
	object, _, decodeErr := scheme.Codecs.UniversalDeserializer().Decode(raw, nil, nil)
	pod, ok := object.(*corev1.Pod)
	if !ok {
		decodeErr = errors.Join(decodeErr, errors.New("owner GET did not return a Pod"))
	}
	o.mutex.Lock()
	o.pod, o.err = pod, errors.Join(readErr, closeErr, decodeErr)
	o.mutex.Unlock()
	return response, nil
}

func (o *ownerReadObservation) result() (*corev1.Pod, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.pod == nil || o.err != nil {
		return nil, errors.Join(o.err, o.beforeErr, errors.New("the production owner GET was not observed successfully"))
	}
	return o.pod.DeepCopy(), nil
}

func (f *fixture) ownerAuthority() error {
	for _, state := range []string{"terminating", "replaced"} {
		if err := f.ownerAuthorityState(state); err != nil {
			return fmt.Errorf("%s owner: %w", state, err)
		}
	}
	return nil
}

func (f *fixture) ownerAuthorityState(state string) (result error) {
	owner := f.simplePod("verify-owner-")
	owner.Spec.NodeName = f.selection.Worker.SingleNodeName
	owner.Spec.Containers[0].Command = []string{"/bin/bash", "-ec", "sleep 600"}
	if state == "terminating" {
		owner.Finalizers = []string{ownerHoldFinalizer}
	}
	owner, err := f.api.CoreV1().Pods(f.selection.Namespace).Create(f.ctx, owner, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, f.releaseFixtureOwner(owner)) }()
	ref, request, err := f.attempt()
	if err != nil {
		return err
	}
	ref.Owner = runtimekube.Owner{Name: owner.Name, UID: string(owner.UID)}
	ref.PodDigest, err = runtimekube.PodFingerprint(f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return err
	}
	if err = f.createSecret(&ref, request); err != nil {
		return err
	}
	ref.PodUID, err = f.client.CreatePod(f.ctx, f.selection.Worker, ref, request, f.selection.Gateway)
	if err != nil {
		return err
	}
	pod, err := f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	f.keepPod(pod)
	if err = wait.PollUntilContextTimeout(f.ctx, 250*time.Millisecond, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pod, err := f.api.CoreV1().Pods(ref.Namespace).Get(ctx, ref.PodName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if string(pod.UID) != ref.PodUID || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			return false, errors.New("owner fixture worker ended or changed identity before readiness")
		}
		return workerIsReady(pod), nil
	}); err != nil {
		return err
	}
	if uid, err := f.client.ResolvePod(f.ctx, ref); err != nil || uid != ref.PodUID {
		return errors.Join(err, errors.New("live owner could not authorize its ready worker"))
	}
	if err = f.workerHasNotExecuted(ref); err != nil {
		return err
	}
	observation := &ownerReadObservation{path: "/api/v1/namespaces/" + ref.Namespace + "/pods/" + owner.Name}
	expectedUID := owner.UID
	if state == "terminating" {
		background := metav1.DeletePropagationBackground
		if err = f.api.CoreV1().Pods(ref.Namespace).Delete(f.ctx, owner.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &owner.UID}, GracePeriodSeconds: int64Pointer(0), PropagationPolicy: &background}); err != nil {
			return err
		}
	} else {
		observation.before = func(ctx context.Context) error {
			orphan := metav1.DeletePropagationOrphan
			if err := f.api.CoreV1().Pods(ref.Namespace).Delete(ctx, owner.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &owner.UID}, GracePeriodSeconds: int64Pointer(0), PropagationPolicy: &orphan}); err != nil {
				return err
			}
			if err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
				_, err := f.api.CoreV1().Pods(ref.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
				if apierrors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}); err != nil {
				return err
			}
			replacement := owner.DeepCopy()
			resetPod(replacement)
			replacement.GenerateName = ""
			replacement.Spec.NodeName = ref.FixedNode
			replacement, err := f.api.CoreV1().Pods(ref.Namespace).Create(ctx, replacement, metav1.CreateOptions{})
			if err != nil {
				return err
			}
			f.keepPod(replacement)
			if replacement.UID == owner.UID {
				return errors.New("owner recreation did not change its UID")
			}
			expectedUID = replacement.UID
			return nil
		}
	}
	guard := &transportFault{forbidExec: true, ownerRead: observation}
	client, err := f.faultClient(guard)
	if err != nil {
		return err
	}
	var stdout, stderr bytes.Buffer
	executionErr := client.Execute(f.ctx, ref, 30*time.Second, runtimekube.Streams{Stdout: &stdout, Stderr: &stderr})
	observed, err := observation.result()
	if err != nil {
		return err
	}
	if observed.Name != owner.Name || observed.UID != expectedUID || ref.FixedNode != "" && observed.Spec.NodeName != ref.FixedNode || (observed.DeletionTimestamp != nil) != (state == "terminating") {
		return errors.New("production owner GET did not observe the intended live identity change")
	}
	var diagnostic *diagnostics.Error
	if !errors.As(executionErr, &diagnostic) || diagnostic.Reason != "controller_authority_changed" || guard.attemptedExecution() || stdout.Len() != 0 || stderr.Len() != 0 {
		return errors.New("changed owner did not deny execution before any provider output or exec request")
	}
	pod, err = f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{})
	if err != nil || string(pod.UID) != ref.PodUID || pod.DeletionTimestamp != nil || !workerIsReady(pod) {
		return errors.Join(err, errors.New("worker did not survive the owner authority rejection"))
	}
	if err = f.workerHasNotExecuted(ref); err != nil {
		return err
	}
	secret, err := f.api.CoreV1().Secrets(ref.Namespace).Get(f.ctx, ref.SecretName, metav1.GetOptions{})
	if err != nil || string(secret.UID) != ref.SecretUID || secret.DeletionTimestamp != nil {
		return errors.Join(err, errors.New("request Secret was lost before owner-independent cleanup"))
	}
	if state == "terminating" {
		if err = f.client.Cleanup(f.ctx, ref); err != nil {
			return fmt.Errorf("cleanup with terminating owner: %w", err)
		}
	} else {
		// Orphan propagation removed ownerReferences. Preserve production cleanup's
		// metadata checks and remove only the fixture's original UIDs directly.
		if err = f.deletePod(pod.Name, pod.UID); err != nil {
			return err
		}
		if err = f.deleteSecret(secret.Name, secret.UID); err != nil {
			return err
		}
	}
	if _, err = f.api.CoreV1().Pods(ref.Namespace).Get(f.ctx, ref.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return errors.Join(err, errors.New("worker remains after owner fixture cleanup"))
	}
	if _, err = f.api.CoreV1().Secrets(ref.Namespace).Get(f.ctx, ref.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		return errors.Join(err, errors.New("request remains after owner fixture cleanup"))
	}
	if _, err = f.client.Controller(f.ctx, f.selection.Controller.Name, f.selection.Controller.UID, ref.FixedNode); err != nil {
		return errors.Join(err, errors.New("the real controller identity changed during the isolated owner fixture"))
	}
	f.evidence.Checks = append(f.evidence.Checks, check{Name: "ready-worker-owner-" + state, Passed: true, Details: map[string]string{"ownerName": owner.Name, "beforeOwnerUID": string(owner.UID), "observedOwnerUID": string(observed.UID), "workerUID": ref.PodUID, "workerSurvivedReady": "true", "workerExecutionMarkerAbsent": "true", "reason": diagnostic.Reason}})
	return nil
}

func workerIsReady(pod *corev1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "worker" && status.Ready {
			return true
		}
	}
	return false
}

func (f *fixture) workerHasNotExecuted(ref runtimekube.Reference) error {
	// Read the actual worker's execution side effect through an independent SDK
	// client; the guarded production Execute call cannot affect this observation.
	options := &corev1.PodExecOptions{Container: "worker", Command: []string{"/bin/bash", "-ec", `test ! -e "$1/executed"`, "owner-authority-probe", wire.ControlRoot}, Stderr: true}
	endpoint := f.api.CoreV1().RESTClient().Post().Namespace(ref.Namespace).Resource("pods").Name(ref.PodName).SubResource("exec").VersionedParams(options, scheme.ParameterCodec).URL()
	executor, err := remotecommand.NewSPDYExecutor(f.config, http.MethodPost, endpoint)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	if err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stderr: &stderr}); err != nil {
		return errors.Join(err, errors.New("ready worker's execution marker was not absent"))
	}
	return nil
}

func (f *fixture) releaseFixtureOwner(owner *corev1.Pod) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sameOwner := false
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		sameOwner = false
		current, err := f.api.CoreV1().Pods(owner.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) || err == nil && current.UID != owner.UID {
			return nil
		}
		if err != nil {
			return err
		}
		sameOwner = true
		if !slices.Contains(current.Finalizers, ownerHoldFinalizer) {
			return nil
		}
		current.Finalizers = slices.DeleteFunc(current.Finalizers, func(value string) bool { return value == ownerHoldFinalizer })
		_, err = f.api.CoreV1().Pods(owner.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		if apierrors.IsNotFound(err) {
			sameOwner = false
			return nil
		}
		return err
	})
	if err != nil || !sameOwner {
		return err
	}
	err = f.api.CoreV1().Pods(owner.Namespace).Delete(ctx, owner.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &owner.UID}, GracePeriodSeconds: int64Pointer(0)})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
