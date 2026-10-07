package controller

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
)

type resourceRecord struct {
	Reference             kubernetes.Reference `json:"reference"`
	SecretCreateRequested bool                 `json:"secretCreateRequested"`
	PodCreateRequested    bool                 `json:"podCreateRequested"`
	PodRequest            *corev1.Pod          `json:"podRequest,omitempty"`
	SecretRequest         *corev1.Secret       `json:"secretRequest,omitempty"`
}

var errPreparePending = errors.New("preparation lease temporarily unavailable")
var errStartupPodStopped = errors.New("worker Pod stopped before execution admission")

func leaseRefused(err error) bool {
	var response *daemonapi.ResponseError
	return errors.As(err, &response) && response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != 408 && response.StatusCode != 429
}

func (c *Controller) activate(ctx context.Context, g workspace.TaskGrant) error {
	if current, err := c.Store.Get(g.AttemptID); err != nil || current.Stop != nil {
		return err
	}
	deadline := g.CreatedAt.Add(time.Duration(c.Policy.Worker.PreparationTimeoutSeconds) * time.Second)
	if !time.Now().Before(deadline) {
		return c.recordFailure(g, nil)
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := c.API.PrepareLease(ctx, g.RuntimeID, g.TaskID); err != nil {
		if stopped, stopErr := c.observeBackendStop(ctx, g); stopped {
			return stopErr
		}
		if leaseRefused(err) {
			return c.recordFailure(g, nil)
		}
		status, statusErr := c.API.TaskStatus(ctx, g.TaskID)
		if statusErr == nil && (status == "cancelled" || status == "failed" || status == "completed") {
			return c.recordFailure(g, nil)
		}
		return nil // Keep durable waiting authority while a transient read fails.
	}
	if g.SessionProtocol {
		return c.activateTurn(ctx, g)
	}
	input := g
	residentLimit := c.MaxResidentPods
	if residentLimit == 0 {
		residentLimit = c.Capacity
	}
	g, err := c.Store.Create(input, residentLimit)
	if errors.Is(err, workspace.ErrAdmissionBudget) {
		return c.recordFailure(input, err)
	}
	if errors.Is(err, workspace.ErrStorageBusy) || errors.Is(err, workspace.ErrResidentCapacity) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := c.provision(ctx, g); err != nil {
		if errors.Is(err, errPreparePending) || transientObservation(err) {
			return nil
		}
		return c.recordFailure(g, nil)
	}
	return nil
}

func record(g workspace.TaskGrant) (resourceRecord, error) {
	var r resourceRecord
	if len(g.Resources) == 0 {
		return r, nil
	}
	if err := runtimeimage.Decode(g.Resources, &r); err != nil {
		return r, err
	}
	ref := r.Reference
	if g.WorkerSessionID != "" {
		if ref.WorkerSessionID != g.WorkerSessionID || ref.WorkspaceID != g.WorkspaceID || ref.WorkspaceAnchorTaskID != g.WorkspaceAnchorTaskID ||
			ref.StorageID != g.StorageID || ref.OwnerID != g.OwnerID || ref.PVCName != g.PVCName || ref.TaskRoot != g.TaskRoot || !ref.RuntimeRef.Equal(g.RuntimeRef) {
			return r, errors.New("journal session resource identity mismatch")
		}
		return r, nil
	}
	if ref.TaskID != g.TaskID || ref.AttemptID != g.AttemptID || ref.StorageID != g.StorageID || ref.OwnerID != g.OwnerID || ref.PVCName != g.PVCName || ref.TaskRoot != g.TaskRoot || !ref.RuntimeRef.Equal(g.RuntimeRef) {
		return r, errors.New("journal resource identity mismatch")
	}
	return r, nil
}
func (c *Controller) saveResources(id string, r resourceRecord) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return c.Store.SetResources(id, raw)
}

func (c *Controller) provision(ctx context.Context, g workspace.TaskGrant) (resultErr error) {
	defer func() {
		if !errors.Is(resultErr, workspace.ErrConflict) && !leaseRefused(resultErr) {
			return
		}
		// Admission and cancellation can commit while Kubernetes or a lease
		// request is in flight. Their new authority supersedes provisioning;
		// an obsolete metadata write or preparation lease refusal must not
		// fail the admitted worker after execution has advanced.
		current, err := c.Store.Get(g.AttemptID)
		if err == nil && (current.State != "intent" || current.Stop != nil) {
			resultErr = errPreparePending
		}
	}()
	if g.WorkerSessionID != "" {
		return c.provisionTurn(ctx, g)
	}
	current, err := c.Store.Get(g.AttemptID)
	if err != nil {
		return err
	}
	g = current
	if g.State != "intent" || g.Stop != nil {
		return nil
	}
	ctx, cancel := context.WithDeadline(ctx, g.CreatedAt.Add(time.Duration(c.Policy.Worker.PreparationTimeoutSeconds)*time.Second))
	defer cancel()
	r, err := record(g)
	if err != nil {
		return err
	}
	if len(g.Resources) == 0 {
		r.Reference = kubernetes.Reference{Namespace: c.Kube.Namespace, Owner: c.Owner, OwnerID: g.OwnerID, TaskID: g.TaskID, StorageID: g.StorageID, AttemptID: g.AttemptID, PodName: g.PodName, SecretName: "task-" + g.AttemptID, RuntimeRef: g.RuntimeRef, PVCName: g.PVCName, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, NFSServer: c.NFSServer}
		if err := c.saveResources(g.AttemptID, r); err != nil {
			return err
		}
	}
	r.Reference.Owner = c.Owner
	// BindPod is durable before the resource record is updated. A
	// crash in between retains acknowledged UIDs; reconcile those exact objects.
	if g.PVCUID != "" {
		if r.Reference.PVCUID != "" && r.Reference.PVCUID != g.PVCUID {
			return errors.New("recorded PVC UID conflict")
		}
		r.Reference.PVCUID = g.PVCUID
		if err := c.Kube.ValidatePVC(ctx, r.Reference); err != nil {
			return err
		}
	}
	if g.PodUID != "" {
		if r.Reference.PodUID != "" && r.Reference.PodUID != g.PodUID {
			return errors.New("recorded Pod UID conflict")
		}
		r.Reference.PodUID = g.PodUID
		pod, err := c.Kube.PodState(ctx, r.Reference)
		if err != nil {
			return err
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
			reason := "worker_startup_failed"
			for _, init := range pod.Status.InitContainerStatuses {
				if init.State.Terminated != nil && init.State.Terminated.ExitCode != 0 {
					reason = "worker_initialization_failed"
				}
			}
			return diagnostics.Wrap(reason, errStartupPodStopped)
		}
	}
	if err := c.saveResources(g.AttemptID, r); err != nil {
		return err
	}
	if err := c.API.PrepareLease(ctx, g.RuntimeID, g.TaskID); err != nil {
		if leaseRefused(err) {
			return err
		}
		return errPreparePending
	}
	current, err = c.Store.Get(g.AttemptID)
	if err != nil {
		return err
	}
	if current.State != "intent" || current.Stop != nil {
		return nil
	}
	g = current
	var b wire.Bootstrap
	if len(g.Bootstrap) == 0 {
		expiry := g.CreatedAt.Add(time.Duration(c.Policy.Worker.PreparationTimeoutSeconds+c.Policy.Worker.TaskDeadlineSeconds+c.Policy.Worker.TerminationGraceSeconds+300) * time.Second)
		tokens := map[string]string{}
		for _, audience := range []string{"daemon", "supervisor", "cache", "stop"} {
			authorityExpiry := expiry
			if audience == "stop" {
				// Pod creation can consume the preparation deadline. Its active
				// lifetime and subsequent kubelet shutdown grace start later.
				podDeadline := c.Policy.Worker.PreparationTimeoutSeconds + c.Policy.Worker.TaskDeadlineSeconds + c.Policy.Worker.TerminationGraceSeconds + 30
				authorityExpiry = g.CreatedAt.Add(time.Duration(c.Policy.Worker.PreparationTimeoutSeconds+podDeadline+wire.WorkerTerminationSeconds(c.Policy.Worker.TerminationGraceSeconds)+300) * time.Second)
			}
			token, err := c.Store.CapabilityToken(g.AttemptID, audience)
			if err != nil {
				token, err = c.Store.IssueCapability(g.AttemptID, audience, authorityExpiry)
			}
			if err != nil {
				return err
			}
			tokens[audience] = token
		}
		b = wire.Bootstrap{OwnerID: g.OwnerID, TaskID: g.TaskID, AttemptID: g.AttemptID, StorageID: g.StorageID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, Generation: g.Generation, PVCName: g.PVCName, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, NFSServer: c.NFSServer, RuntimeRef: g.RuntimeRef, GatewayURL: c.GatewayURL, APICapability: tokens["daemon"], SupervisorCapability: tokens["supervisor"], StopCapability: tokens["stop"], CacheCapability: tokens["cache"], ExpiresAt: expiry.UTC().Format(time.RFC3339), Configuration: c.Configuration, Environment: c.Environment, GitHubApp: c.App != nil, TerminationGraceSeconds: int(c.Policy.Worker.TerminationGraceSeconds)}
		if g.Prepared == nil {
			return c.startPreparation(ctx, g, b)
		}
		b.PreparedDigest, b.AllowedLinks, b.Provider = g.Prepared.Digest, g.Prepared.AllowedLinks, g.Prepared.Provider
		if g.Prepared.NativeMetadata != nil {
			b.NativeMetadataDigest = g.Prepared.NativeMetadata.Digest()
		}
		raw, err := json.Marshal(b)
		if err != nil {
			return err
		}
		if _, err := wire.DecodeBootstrap(raw); err != nil {
			return err
		}
		if err := c.Store.SetBootstrap(g.AttemptID, raw); err != nil {
			return err
		}
		g.Bootstrap = raw
		g.BootstrapDigest = wire.Digest(raw)
	} else {
		b, err = wire.DecodeBootstrap(g.Bootstrap)
		if err != nil {
			return err
		}
	}
	r.Reference.RequestDigest = g.BootstrapDigest
	// Once creation is requested, keep the issued payload's authority across
	// restarts and template changes, including a lost creation response.
	if !r.PodCreateRequested {
		r.Reference.PodDigest, err = kubernetes.PodFingerprint(c.Policy, r.Reference, b)
		if err != nil {
			return err
		}
	}
	if err := c.saveResources(g.AttemptID, r); err != nil {
		return err
	}
	if r.Reference.SecretUID == "" {
		if !r.SecretCreateRequested {
			r.SecretCreateRequested = true
			if err := c.saveResources(g.AttemptID, r); err != nil {
				return err
			}
		}
		// The journal records an issued request, not a successful creation.
		// Recover the same bootstrap after either an unsent request or a lost ACK.
		finish := diagnostics.StartPhase("task_secret_create", diagnostics.TaskAttributes(g.TaskID, g.AttemptID)...)
		r.Reference.SecretUID, err = c.Kube.EnsureSecret(ctx, r.Reference, b)
		finish(err, failureAttributes(err, "secret_creation_failed")...)
		if errors.Is(err, kubernetes.ErrSecretPending) {
			return errPreparePending
		}
		if err != nil {
			return err
		}
		if err := c.saveResources(g.AttemptID, r); err != nil {
			return err
		}
	}
	// Stop may commit while the Secret request is in flight. Keep its UID for
	// cleanup, but let the current grant decide whether a worker may be issued.
	current, err = c.Store.Get(g.AttemptID)
	if err != nil {
		return err
	}
	if current.State != "intent" || current.Stop != nil {
		return nil
	}
	if r.Reference.PodUID == "" {
		r.Reference.PodUID, err = c.provisionPod(ctx, g, &r, b)
		if err != nil {
			return err
		}
		if r.Reference.PodUID == "" {
			return errPreparePending
		}
		if err := c.Store.BindPod(g.AttemptID, g.PodName, r.Reference.PodUID, ""); err != nil {
			return err
		}
		if err := c.saveResources(g.AttemptID, r); err != nil {
			return err
		}
		slog.Info("worker pod bound", "task", g.TaskID, "attempt", g.AttemptID, "pod", g.PodName, "podUID", r.Reference.PodUID, "sinceClaim", time.Since(g.CreatedAt))
	}
	return nil
}

func (c *Controller) provisionPod(ctx context.Context, g workspace.TaskGrant, r *resourceRecord, b wire.Bootstrap) (string, error) {
	if r.PodRequest == nil {
		if r.PodCreateRequested {
			uid, err := c.Kube.ResolveCleanupPod(ctx, r.Reference)
			if err != nil || uid != "" {
				return uid, err
			}
			// Before schema 13 only the fingerprint was retained. A validated
			// stop-capable bootstrap was issued with the termination finalizer
			// (introduced together in schema 7); migrations never synthesize it.
			// Reconstruct only the exact original spec. A changed deployment
			// cannot supply new authority for an unresolved old request.
			digest, err := kubernetes.PodFingerprint(c.Policy, r.Reference, b)
			if err != nil || b.StopCapability == "" || digest != r.Reference.PodDigest {
				return "", errPreparePending
			}
		}
		var err error
		r.PodRequest, err = kubernetes.PodRequest(c.Policy, r.Reference, b)
		if err != nil {
			return "", err
		}
	}
	r.PodCreateRequested = true
	// Persist the immutable request and creation authority together, before
	// any POST. A timeout or crash after this point can replay the same request.
	if err := c.saveResources(g.AttemptID, *r); err != nil {
		return "", err
	}
	finish := diagnostics.StartPhase("task_pod_create", diagnostics.TaskAttributes(g.TaskID, g.AttemptID)...)
	uid, err := c.Kube.EnsurePod(ctx, r.Reference, r.PodRequest)
	finish(err, failureAttributes(err, "pod_creation_failed")...)
	if errors.Is(err, kubernetes.ErrPodPending) {
		return "", errPreparePending
	}
	return uid, err
}
