package kubernetes

import (
	"context"
	"errors"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var ErrWriterUnknown = errors.New("writer termination is unproven; quarantine task storage")

// Authorize returns the same Pod observation used to validate execution access.
func (c *Client) Authorize(ctx context.Context, r Reference) (*corev1.Pod, error) {
	if err := c.validateReferenceAuthority(ctx, r); err != nil {
		return nil, err
	}
	if err := c.StorageAvailable(ctx, r); err != nil {
		return nil, err
	}
	secret, err := c.secret(ctx, r.SecretName)
	if err != nil {
		return nil, err
	}
	if r.SecretUID == "" || !secretMatches(secret, r) || secret.DeletionTimestamp != nil {
		return nil, errors.New("worker bootstrap Secret identity changed")
	}
	p, err := c.PodState(ctx, r)
	if err != nil {
		return nil, err
	}
	if p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
		return nil, errors.New("worker has stopped")
	}
	if err := admittedImages(p, r); err != nil {
		return nil, err
	}
	return p, nil
}

// AuthorizeStop authenticates an existing worker only for the stop protocol.
// Stop evidence must remain deliverable after cancellation or controller loss;
// it grants neither execution nor access to the task API or credentials.
func (c *Client) AuthorizeStop(ctx context.Context, r Reference) (*corev1.Pod, error) {
	if err := c.ValidatePVC(ctx, r); err != nil {
		return nil, err
	}
	secret, err := c.secret(ctx, r.SecretName)
	if err != nil {
		return nil, err
	}
	if r.SecretUID == "" || !secretMatches(secret, r) {
		return nil, errors.New("worker bootstrap Secret identity changed")
	}
	p, err := c.PodState(ctx, r)
	if err != nil {
		return nil, err
	}
	if err := admittedImages(p, r); err != nil {
		return nil, err
	}
	return p, nil
}
func admittedImages(p *corev1.Pod, r Reference) error {
	if len(p.Status.ContainerStatuses) != 1 || len(p.Status.InitContainerStatuses) != 1 {
		return errors.New("worker image identity pending")
	}
	for _, s := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
		image, err := runtimeimage.NormalizeImageID(s.ImageID)
		if err != nil || image != r.RuntimeRef.Image {
			return errors.New("worker executing image differs from admitted image")
		}
	}
	return nil
}
func (c *Client) PodState(ctx context.Context, r Reference) (*corev1.Pod, error) {
	if c.Namespace != r.Namespace || r.PodUID == "" {
		return nil, errors.New("journaled worker UID required")
	}
	p, err := c.pod(ctx, r.PodName)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, ErrWriterUnknown
	}
	if !podMatches(p, r) {
		return nil, errors.New("worker Pod identity changed")
	}
	return p, nil
}

// DeletePod requests normal termination; deletion/absence never proves fencing.
func (c *Client) DeletePod(ctx context.Context, r Reference) error {
	p, err := c.PodState(ctx, r)
	if err != nil {
		return err
	}
	if p.DeletionTimestamp != nil {
		return nil
	}
	// Before the app process exists, deletion can replace its runtime evidence
	// with ContainerStatusUnknown. Let the stop-aware init fail, or let the
	// worker reach its pre-input stop barrier, instead of creating that gap.
	if p.Spec.NodeName != "" {
		started := false
		for _, status := range p.Status.ContainerStatuses {
			if status.Name == "worker" && status.ContainerID != "" &&
				(status.State.Running != nil || status.State.Terminated != nil) {
				started = true
			}
		}
		if !started {
			return nil
		}
	}
	if p.ResourceVersion == "" {
		return errors.New("Pod resource version required for graceful shutdown")
	}
	uid := types.UID(r.PodUID)
	// Recheck scheduling and startup atomically with deletion. Binding or a
	// status update after the observation must trigger a fresh reconciliation.
	err = c.API.CoreV1().Pods(c.Namespace).Delete(ctx, r.PodName, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &p.ResourceVersion}})
	if apierrors.IsNotFound(err) {
		return ErrWriterUnknown
	}
	return err
}
