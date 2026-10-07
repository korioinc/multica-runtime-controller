package kubernetes

import (
	"context"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	corev1 "k8s.io/api/core/v1"
)

type StopObservation struct {
	Stopped      bool
	NeverStarted bool
	ObservedAt   time.Time
}

// ObserveTermination relies on exact runtime termination records from the
// Kubernetes API, not on Pod deletion, terminal phase alone, or node readiness.
// It proves process termination, never client-side NFS flush. The admitted Pod
// spec permits one non-restarting init and worker, with no host PID namespace or
// ephemeral containers. Administrators and the kubelet remain trusted to report
// status truthfully; synthetic ContainerStatusUnknown records are not evidence.
func (c *Client) ObserveTermination(ctx context.Context, r Reference) (StopObservation, error) {
	if err := c.ValidatePVC(ctx, r); err != nil {
		return StopObservation{}, err
	}
	p, err := c.PodState(ctx, r)
	if err != nil {
		return StopObservation{}, err
	}
	// BindingREST rejects a Pod with deletionTimestamp inside its atomic
	// storage update. An admitted Pod that is still unbound after deletion was
	// accepted can never reach a kubelet; unlike missing statuses alone, this
	// is a durable scheduling fence. NodeName cannot be cleared by Pod update.
	if p.DeletionTimestamp != nil && p.Spec.NodeName == "" && p.Status.StartTime == nil &&
		len(p.Status.ContainerStatuses) == 0 && len(p.Status.InitContainerStatuses) == 0 &&
		len(p.Status.EphemeralContainerStatuses) == 0 && p.Status.HostIP == "" {
		return StopObservation{Stopped: true, NeverStarted: true, ObservedAt: time.Now().UTC()}, nil
	}
	if p.Spec.RestartPolicy != corev1.RestartPolicyNever || p.Spec.NodeName == "" ||
		p.Spec.InitContainers[0].RestartPolicy != nil || p.Spec.Containers[0].RestartPolicy != nil ||
		p.Status.Reason == "NodeLost" || p.Status.Phase != corev1.PodFailed && p.Status.Phase != corev1.PodSucceeded ||
		len(p.Status.InitContainerStatuses) != 1 || len(p.Status.ContainerStatuses) > 1 || len(p.Status.EphemeralContainerStatuses) != 0 {
		return StopObservation{}, nil
	}
	init := p.Status.InitContainerStatuses[0]
	if !exactTermination(init, p.Spec.InitContainers[0].Name, r.RuntimeRef.Image) {
		return StopObservation{}, nil
	}
	if len(p.Status.ContainerStatuses) == 1 && exactTermination(p.Status.ContainerStatuses[0], p.Spec.Containers[0].Name, r.RuntimeRef.Image) {
		return StopObservation{Stopped: true, ObservedAt: time.Now().UTC()}, nil
	}
	// A failed, never-restarted init gates all application starts. Unlike an
	// empty app status alone, its actual failed runtime termination establishes
	// why this worker never ran. The init itself must have no task volume mount.
	if p.Status.Phase != corev1.PodFailed || init.State.Terminated.ExitCode == 0 || !uninitialized(p) {
		return StopObservation{}, nil
	}
	for _, mount := range p.Spec.InitContainers[0].VolumeMounts {
		if mount.Name == "runtime-task" {
			return StopObservation{}, nil
		}
	}
	if len(p.Status.ContainerStatuses) == 1 && !neverStarted(p.Status.ContainerStatuses[0], p.Spec.Containers[0].Name) {
		return StopObservation{}, nil
	}
	return StopObservation{Stopped: true, NeverStarted: true, ObservedAt: time.Now().UTC()}, nil
}

func exactTermination(s corev1.ContainerStatus, name, image string) bool {
	t := s.State.Terminated
	if s.Name != name || s.RestartCount != 0 || s.State.Running != nil || s.State.Waiting != nil ||
		t == nil || t.Reason == "ContainerStatusUnknown" || t.Reason == "ContainerStatusNeverStarted" ||
		t.StartedAt.IsZero() || t.FinishedAt.IsZero() || t.FinishedAt.Before(&t.StartedAt) ||
		s.ContainerID == "" || t.ContainerID != s.ContainerID || !strings.Contains(s.ContainerID, "://") ||
		s.LastTerminationState.Running != nil || s.LastTerminationState.Waiting != nil || s.LastTerminationState.Terminated != nil {
		return false
	}
	normalized, err := runtimeimage.NormalizeImageID(s.ImageID)
	return err == nil && normalized == image
}

func neverStarted(s corev1.ContainerStatus, name string) bool {
	return s.Name == name && s.ContainerID == "" && s.ImageID == "" && s.RestartCount == 0 &&
		(s.Started == nil || !*s.Started) && s.State.Running == nil && s.State.Terminated == nil &&
		s.State.Waiting != nil && s.LastTerminationState.Running == nil &&
		s.LastTerminationState.Waiting == nil && s.LastTerminationState.Terminated == nil
}

func uninitialized(p *corev1.Pod) bool {
	for _, condition := range p.Status.Conditions {
		if condition.Type == corev1.PodInitialized {
			return condition.Status == corev1.ConditionFalse
		}
	}
	return false
}
