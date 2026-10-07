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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func terminatedTaskFixture(t *testing.T) (*Client, Reference, *corev1.Pod) {
	t.Helper()
	ctx := context.Background()
	c, r, b, cfg := currentTaskFixture(t)
	var err error
	r.SecretUID, err = c.EnsureSecret(ctx, r, b)
	if err != nil {
		t.Fatal(err)
	}
	r.PodUID, err = ensureTaskPod(ctx, c, cfg, r, b)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.PodState(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	p.ResourceVersion = "1"
	p.Spec.NodeName = "worker-node"
	p.Status.Phase = corev1.PodFailed
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodInitialized, Status: corev1.ConditionTrue}}
	status := func(name string, exit int32) corev1.ContainerStatus {
		id := "containerd://" + name + "-container"
		return corev1.ContainerStatus{Name: name, ContainerID: id, ImageID: r.RuntimeRef.Image, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ContainerID: id, ExitCode: exit, Reason: "Completed", StartedAt: metav1.NewTime(time.Unix(100, 0)), FinishedAt: metav1.NewTime(time.Unix(110, 0)),
		}}}
	}
	p.Status.InitContainerStatuses = []corev1.ContainerStatus{status("task-layout", 0)}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{status("worker", 1)}
	storeTaskPod(t, c, p)
	return c, r, p
}

func storeTaskPod(t *testing.T, c *Client, p *corev1.Pod) {
	t.Helper()
	if _, err := c.API.CoreV1().Pods(c.Namespace).Update(context.Background(), p, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestTerminationRequiresEvidenceForEveryPossibleWriter(t *testing.T) {
	ctx := context.Background()
	c, r, original := terminatedTaskFixture(t)
	if stop, err := c.ObserveTermination(ctx, r); err != nil || !stop.Stopped || stop.NeverStarted {
		t.Fatal("exact completed containers did not establish a stopped, potentially dirty worker", err)
	}
	for name, mutate := range map[string]func(*corev1.Pod){
		"terminal phase with worker still running": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
		},
		"worker runtime identity lost": func(p *corev1.Pod) { p.Status.ContainerStatuses[0].ContainerID = "" },
		"kubelet synthesized a missing container": func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Terminated = &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "ContainerStatusUnknown"}
		},
		"init runtime identity lost":               func(p *corev1.Pod) { p.Status.InitContainerStatuses[0].State.Terminated.ContainerID = "" },
		"app status missing after successful init": func(p *corev1.Pod) { p.Status.ContainerStatuses = nil },
		"node lost":        func(p *corev1.Pod) { p.Status.Reason = "NodeLost" },
		"replacement pod":  func(p *corev1.Pod) { p.UID = types.UID("replacement") },
		"unadmitted image": func(p *corev1.Pod) { p.Status.ContainerStatuses[0].ImageID = "containerd://unrelated" },
		"unexpected writable container": func(p *corev1.Pod) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debugger"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := original.DeepCopy()
			mutate(p)
			storeTaskPod(t, c, p)
			if stop, _ := c.ObserveTermination(ctx, r); stop.Stopped || stop.NeverStarted {
				t.Fatal("unproven writer termination released storage execution authority")
			}
		})
	}
}

func TestFailedInitCanProveTaskWriterNeverStarted(t *testing.T) {
	ctx := context.Background()
	c, r, p := terminatedTaskFixture(t)
	p.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 1
	p.Status.Conditions[0].Status = corev1.ConditionFalse
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}}
	storeTaskPod(t, c, p)
	if stop, err := c.ObserveTermination(ctx, r); err != nil || !stop.Stopped || !stop.NeverStarted {
		t.Fatal("failed init could not close an attempt whose task writer never started", err)
	}
	p.Status.ContainerStatuses = nil
	storeTaskPod(t, c, p)
	if stop, err := c.ObserveTermination(ctx, r); err != nil || !stop.Stopped || !stop.NeverStarted {
		t.Fatal("actual failed init did not prove its gated app never started", err)
	}
	p.Status.InitContainerStatuses[0].State.Terminated = &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "ContainerStatusUnknown"}
	storeTaskPod(t, c, p)
	if stop, _ := c.ObserveTermination(ctx, r); stop.Stopped {
		t.Fatal("missing init runtime evidence was accepted as never-started proof")
	}
}

func TestCancellationPreservesStartupEvidenceUntilWorkerExists(t *testing.T) {
	ctx := t.Context()
	c, r, p := terminatedTaskFixture(t)
	retainFinalizedPods(c)
	p.Status.Phase = corev1.PodPending
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}}
	storeTaskPod(t, c, p)
	if err := c.DeletePod(ctx, r); err != nil {
		t.Fatal(err)
	}
	retained, err := c.PodState(ctx, r)
	if err != nil || retained.DeletionTimestamp != nil {
		t.Fatal("cancellation destroyed the window for positive worker termination evidence", err)
	}
	// Once the worker exists, graceful deletion can obtain its real termination
	// record. The worker's pre-input barrier already holds the durable stop.
	p.Status.Phase = corev1.PodRunning
	p.Status.ContainerStatuses[0].ContainerID = "containerd://actual-worker"
	p.Status.ContainerStatuses[0].ImageID = r.RuntimeRef.Image
	p.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}
	storeTaskPod(t, c, p)
	if err := c.DeletePod(ctx, r); err != nil {
		t.Fatal(err)
	}
	retained, err = c.PodState(ctx, r)
	if err != nil || retained.DeletionTimestamp == nil {
		t.Fatal("cancellation failed to stop the now-existing worker", err)
	}
	if stop, err := c.ObserveTermination(ctx, r); err != nil || stop.Stopped {
		t.Fatal("graceful deletion was mistaken for completed writer termination", err)
	}
}

func TestDeleteUnscheduledPodPreventsFutureTaskWriters(t *testing.T) {
	ctx := context.Background()
	c, r, p := terminatedTaskFixture(t)
	p.Spec.NodeName = ""
	p.Status = corev1.PodStatus{Phase: corev1.PodPending}
	storeTaskPod(t, c, p)
	retainFinalizedPods(c)
	if stop, _ := c.ObserveTermination(ctx, r); stop.Stopped {
		t.Fatal("schedulable Pod released authority for a future writer")
	}
	if err := c.DeletePod(ctx, r); err != nil {
		t.Fatal(err)
	}
	if stop, err := c.ObserveTermination(ctx, r); err != nil || !stop.Stopped || !stop.NeverStarted {
		t.Fatal("binding-fenced Pod could not close its never-started task", err)
	}
	// If the scheduler won before deletion, a kubelet may still have writers.
	c, r, p = terminatedTaskFixture(t)
	p.Status = corev1.PodStatus{Phase: corev1.PodPending}
	storeTaskPod(t, c, p)
	retainFinalizedPods(c)
	if err := c.DeletePod(ctx, r); err != nil {
		t.Fatal(err)
	}
	if stop, _ := c.ObserveTermination(ctx, r); stop.Stopped {
		t.Fatal("bound Pod without runtime evidence released writer authority")
	}
}

func TestTerminatingWorkerCanReportStopButCannotExecute(t *testing.T) {
	ctx := context.Background()
	c, r, p := terminatedTaskFixture(t)
	p.DeletionTimestamp = &metav1.Time{Time: time.Unix(115, 0)}
	storeTaskPod(t, c, p)
	if err := c.API.CoreV1().Pods(c.Namespace).Delete(ctx, r.Owner.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AuthorizeStop(ctx, r); err != nil {
		t.Fatal("controller loss prevented authentic termination evidence", err)
	}
	if _, err := c.Authorize(ctx, r); err == nil {
		t.Fatal("stop-only identity acquired execution authority")
	}
	p.UID = types.UID("replacement")
	storeTaskPod(t, c, p)
	if _, err := c.AuthorizeStop(ctx, r); err == nil {
		t.Fatal("replacement pod obtained the previous worker's stop authority")
	}
}

// client-go's tracker deletes finalizers immediately. Model API retention and
// UID deletion preconditions here; its native JSON Patch owner handles atomic
// tests. This proves our cleanup decisions, not live kubelet process shutdown.
func retainFinalizedPods(c *Client) {
	api := c.API.(*fake.Clientset)
	tracker := api.Tracker()
	api.PrependReactor("delete", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		a := action.(clienttesting.DeleteAction)
		object, err := tracker.Get(action.GetResource(), action.GetNamespace(), a.GetName())
		if err != nil {
			return true, nil, err
		}
		p := object.(*corev1.Pod)
		if pre := a.GetDeleteOptions().Preconditions; pre != nil && pre.UID != nil && *pre.UID != p.UID {
			return true, nil, errors.New("Pod UID precondition failed")
		}
		if len(p.Finalizers) == 0 {
			return false, nil, nil
		}
		p.DeletionTimestamp = &metav1.Time{Time: time.Unix(115, 0)}
		p.ResourceVersion = "2"
		return true, nil, tracker.Update(action.GetResource(), p, action.GetNamespace())
	})
	api.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		handled, object, err := clienttesting.ObjectReaction(tracker)(action)
		if err == nil {
			p := object.(*corev1.Pod)
			if p.DeletionTimestamp != nil && len(p.Finalizers) == 0 {
				err = tracker.Delete(action.GetResource(), action.GetNamespace(), p.Name)
			}
		}
		return handled, object, err
	})
}

func TestTerminationEvidenceSurvivesDeleteAndFailedCleanup(t *testing.T) {
	ctx := context.Background()
	c, r, _ := terminatedTaskFixture(t)
	retainFinalizedPods(c)
	if err := c.DeletePod(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := c.DeletePod(ctx, r); err != nil {
		t.Fatal("pending normal deletion could not be retried", err)
	}
	if stop, err := c.ObserveTermination(ctx, r); err != nil || !stop.Stopped {
		t.Fatal("deletion destroyed uncommitted termination evidence", err)
	}
	api := c.API.(*fake.Clientset)
	failPatch := true
	api.PrependReactor("patch", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if failPatch {
			return true, nil, errors.New("temporary API failure")
		}
		return false, nil, nil
	})
	if err := c.Cleanup(ctx, r); err == nil {
		t.Fatal("failed cleanup was declared complete")
	}
	if stop, err := c.ObserveTermination(ctx, r); err != nil || !stop.Stopped {
		t.Fatal("cleanup error discarded termination evidence", err)
	}
	failPatch = false
	if err := c.Cleanup(ctx, r); err != nil {
		t.Fatal("cleanup did not recover after a transient error", err)
	}
	if err := c.Cleanup(ctx, r); err != nil {
		t.Fatal("completed cleanup was not idempotent", err)
	}
	if _, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, r.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("completed attempt still retains its worker", err)
	}
	if _, err := c.API.CoreV1().Secrets(c.Namespace).Get(ctx, r.SecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("completed attempt still retains its bootstrap authority", err)
	}
}

func TestCleanupCannotReleaseReplacementOrConcurrentFinalizer(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(map[bool]string{false: "concurrent finalizer", true: "replaced pod"}[replacement], func(t *testing.T) {
			ctx := context.Background()
			c, r, _ := terminatedTaskFixture(t)
			retainFinalizedPods(c)
			if err := c.DeletePod(ctx, r); err != nil {
				t.Fatal(err)
			}
			api := c.API.(*fake.Clientset)
			changed := false
			api.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if !changed {
					changed = true
					a := action.(clienttesting.PatchAction)
					object, err := api.Tracker().Get(action.GetResource(), action.GetNamespace(), a.GetName())
					if err != nil {
						return true, nil, err
					}
					p := object.(*corev1.Pod)
					p.ResourceVersion = "3"
					p.Finalizers = append(p.Finalizers, "other-controller.example/evidence")
					if replacement {
						p.UID = types.UID("replacement")
					}
					if err := api.Tracker().Update(action.GetResource(), p, action.GetNamespace()); err != nil {
						return true, nil, err
					}
				}
				return false, nil, nil
			})
			if err := c.Cleanup(ctx, r); err == nil {
				t.Fatal("stale cleanup authority discarded concurrent termination evidence")
			}
			if err := c.Cleanup(ctx, r); err == nil {
				t.Fatal("cleanup discarded another controller's or replacement Pod's evidence")
			}
			if _, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, r.PodName, metav1.GetOptions{}); err != nil {
				t.Fatal("protected Pod was destroyed", err)
			}
			if _, err := c.API.CoreV1().Secrets(c.Namespace).Get(ctx, r.SecretName, metav1.GetOptions{}); err != nil {
				t.Fatal("bootstrap authority was removed while a Pod still retained it", err)
			}
		})
	}
}
