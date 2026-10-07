package kubernetes

import (
	"context"
	"errors"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var ErrControllerObservationPending = errors.New("running preparation controller container is unobserved")

// ControllerProcess binds preparation to the actual running container, not a
// reusable Pod name or an owner reference that later reconciliation can update.
func (c *Client) ControllerProcess(ctx context.Context, owner Owner) (workspace.PreparationProcess, error) {
	pod, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return workspace.PreparationProcess{}, ErrControllerObservationPending
	}
	if err != nil {
		return workspace.PreparationProcess{}, err
	}
	if string(pod.UID) != owner.UID || pod.DeletionTimestamp != nil {
		return workspace.PreparationProcess{}, errors.New("preparation controller identity changed")
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "controller" && status.State.Running != nil && status.ContainerID != "" {
			return workspace.PreparationProcess{PodName: pod.Name, PodUID: string(pod.UID), ContainerID: status.ContainerID}, nil
		}
	}
	return workspace.PreparationProcess{}, ErrControllerObservationPending
}

// PreparationStopped accepts kubelet evidence about the recorded container.
// Missing Pods, replacement Pod UIDs and free metadata locks prove nothing.
func (c *Client) PreparationStopped(ctx context.Context, process workspace.PreparationProcess) (bool, error) {
	pod, err := c.API.CoreV1().Pods(c.Namespace).Get(ctx, process.PodName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(pod.UID) != process.PodUID || process.ContainerID == "" {
		return false, nil
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != "controller" {
			continue
		}
		if terminated := status.State.Terminated; terminated != nil && terminated.ContainerID == process.ContainerID && !terminated.FinishedAt.IsZero() && terminated.Reason != "ContainerStatusUnknown" {
			return true, nil
		}
		if terminated := status.LastTerminationState.Terminated; terminated != nil && terminated.ContainerID == process.ContainerID && !terminated.FinishedAt.IsZero() && terminated.Reason != "ContainerStatusUnknown" {
			return true, nil
		}
		// Kubelet replaces a named container only after stopping its previous
		// instance. This also covers several restarts before reconciliation.
		if status.State.Running != nil && !status.State.Running.StartedAt.IsZero() && status.RestartCount > 0 && status.ContainerID != "" && status.ContainerID != process.ContainerID {
			return true, nil
		}
	}
	return false, nil
}
