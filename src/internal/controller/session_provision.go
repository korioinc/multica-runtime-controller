package controller

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (c *Controller) turnBootstrap(g workspace.TaskGrant) (wire.Bootstrap, error) {
	gatewayURL, nfsServer := c.GatewayURL, c.NFSServer
	grace := int(c.Policy.Worker.TerminationGraceSeconds)
	session, err := c.Store.GetSession(g.WorkerSessionID)
	if err != nil {
		return wire.Bootstrap{}, err
	}
	if len(session.Bootstrap) != 0 {
		admitted, err := wire.DecodeSessionBootstrap(session.Bootstrap)
		if err != nil {
			return wire.Bootstrap{}, err
		}
		// These values describe the existing Pod and mount. A new turn cannot
		// alter that incarnation's control route or Kubernetes shutdown budget.
		gatewayURL, nfsServer, grace = admitted.GatewayURL, admitted.NFSServer, admitted.TerminationGraceSeconds
	}
	expiry := g.CreatedAt.Add(time.Duration(int(c.Policy.Worker.PreparationTimeoutSeconds+c.Policy.Worker.TaskDeadlineSeconds)+grace+300) * time.Second)
	tokens := map[string]string{}
	for _, audience := range []string{"daemon", "supervisor", "cache", "stop"} {
		token, err := c.Store.CapabilityToken(g.AttemptID, audience)
		if err != nil {
			token, err = c.Store.IssueCapability(g.AttemptID, audience, expiry)
		}
		if err != nil {
			return wire.Bootstrap{}, err
		}
		tokens[audience] = token
	}
	b := wire.Bootstrap{WorkerSessionID: g.WorkerSessionID, TurnSequence: g.TurnSequence, WorkspaceAnchorTaskID: g.WorkspaceAnchorTaskID,
		OwnerID: g.OwnerID, TaskID: g.TaskID, AttemptID: g.AttemptID, StorageID: g.StorageID, RuntimeID: g.RuntimeID,
		WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, Generation: g.Generation, PVCName: g.PVCName, PVCUID: g.PVCUID,
		TaskRoot: g.TaskRoot, NFSServer: nfsServer, RuntimeRef: g.RuntimeRef, GatewayURL: gatewayURL,
		APICapability: tokens["daemon"], SupervisorCapability: tokens["supervisor"], StopCapability: tokens["stop"], CacheCapability: tokens["cache"],
		ExpiresAt: expiry.UTC().Format(time.RFC3339Nano), Configuration: c.Configuration, Environment: c.Environment,
		GitHubApp: c.App != nil, TerminationGraceSeconds: grace}
	if g.Prepared != nil {
		b.Provider, b.PreparedDigest, b.AllowedLinks = g.Prepared.Provider, g.Prepared.Digest, g.Prepared.AllowedLinks
		if g.Prepared.NativeMetadata != nil {
			b.NativeMetadataDigest = g.Prepared.NativeMetadata.Digest()
		}
	}
	return b, nil
}

func (c *Controller) sessionTerminationGrace(session workspace.WorkerSession) (time.Duration, error) {
	if len(session.Bootstrap) == 0 {
		return time.Duration(c.Policy.Worker.TerminationGraceSeconds) * time.Second, nil
	}
	bootstrap, err := wire.DecodeSessionBootstrap(session.Bootstrap)
	return time.Duration(bootstrap.TerminationGraceSeconds) * time.Second, err
}

func (c *Controller) provisionTurn(ctx context.Context, grant workspace.TaskGrant) error {
	current, err := c.Store.Get(grant.AttemptID)
	if err != nil {
		return err
	}
	// Session reconciliation owns this same lock through resource cleanup.
	// Keep every local create call and UID publication ahead of that cleanup.
	lock := c.attemptLock("session:" + current.WorkerSessionID)
	if !lock.TryLock() {
		return errPreparePending
	}
	defer lock.Unlock()
	current, err = c.Store.Get(grant.AttemptID)
	if err != nil {
		return err
	}
	if current.State != "intent" || current.ExecutionRevoked {
		return nil
	}
	grant = current
	ctx, cancel := context.WithDeadline(ctx, grant.CreatedAt.Add(time.Duration(c.Policy.Worker.PreparationTimeoutSeconds)*time.Second))
	defer cancel()
	if err := c.API.PrepareLease(ctx, grant.RuntimeID, grant.TaskID); err != nil {
		if leaseRefused(err) {
			return err
		}
		return errPreparePending
	}
	grant, err = c.Store.Get(grant.AttemptID)
	if err != nil || grant.State != "intent" || grant.ExecutionRevoked {
		return err
	}
	session, err := c.Store.GetSession(grant.WorkerSessionID)
	if err != nil || session.ActiveAttempt != grant.AttemptID || session.Stop != nil {
		return err
	}
	if len(grant.Assignment) != 0 {
		return nil
	}
	b, err := c.turnBootstrap(grant)
	if err != nil {
		return err
	}
	if grant.Prepared == nil {
		return c.startPreparation(ctx, grant, b)
	}
	if len(session.Bootstrap) == 0 {
		bootstrap := wire.SessionBootstrap{Version: wire.SessionProtocolVersion, WorkerSessionID: session.ID, Conversation: session.Conversation,
			StorageID: session.StorageID, WorkspaceAnchorTaskID: session.WorkspaceAnchorTaskID, CompatibilityDigest: session.CompatibilityDigest,
			TaskRoot: session.TaskRoot, PVCName: session.PVCName, PVCUID: session.PVCUID, NFSServer: c.NFSServer, GatewayURL: c.GatewayURL,
			Provider: grant.Prepared.Provider, RuntimeID: session.RuntimeID, RuntimeRef: session.RuntimeRef, ControlCapability: session.ControlToken,
			TerminationGraceSeconds: int(c.Policy.Worker.TerminationGraceSeconds)}
		if err := bootstrap.Validate(); err != nil {
			return err
		}
		raw, err := json.Marshal(bootstrap)
		if err != nil {
			return err
		}
		if err := c.Store.SetSessionBootstrap(session.ID, raw); err != nil {
			return err
		}
	}
	if err := c.ensureSessionResources(ctx, session.ID); err != nil {
		return err
	}
	session, err = c.Store.GetSession(session.ID)
	if err != nil {
		return err
	}
	if session.Stop != nil || len(session.SupervisorKey) == 0 {
		return errPreparePending
	}
	grant, err = c.Store.Get(grant.AttemptID)
	if err != nil || grant.ExecutionRevoked {
		return err
	}
	bootstrap, err := wire.DecodeSessionBootstrap(session.Bootstrap)
	if err != nil {
		return err
	}
	b, err = c.turnBootstrap(grant)
	if err != nil {
		return err
	}
	var run wire.Run
	if json.Unmarshal(grant.Execution, &run) != nil {
		return errors.New("prepared turn input is unavailable")
	}
	assignment := wire.TurnAssignment{WorkerSessionID: session.ID, PodUID: session.PodUID, TurnSequence: grant.TurnSequence,
		Bootstrap: b, Run: run, Deadline: grant.CreatedAt.Add(time.Duration(c.Policy.Worker.PreparationTimeoutSeconds+c.Policy.Worker.TaskDeadlineSeconds) * time.Second)}
	assignment.InputDigest = assignment.Digest()
	if err := assignment.Validate(bootstrap, session.PodUID); err != nil {
		return err
	}
	record, err := sessionRecord(session)
	if err != nil {
		return err
	}
	if err := c.saveResources(grant.AttemptID, record); err != nil {
		return err
	}
	if err := c.Kube.AnnotateTurn(ctx, record.Reference, grant.TaskID, grant.AttemptID); err != nil {
		if apierrors.IsConflict(err) {
			return errPreparePending
		}
		return err
	}
	raw, err := json.Marshal(assignment)
	if err != nil || len(raw) > wire.MaxAssignmentBytes {
		return errors.New("turn assignment exceeds its transport budget")
	}
	return c.Store.PublishTurn(grant.AttemptID, raw)
}

// Both provisioning and stop reconciliation hold the session coordination lock
// through this call and their subsequent resource publication or cleanup.
func (c *Controller) ensureSessionResources(ctx context.Context, id string) error {
	session, err := c.Store.GetSession(id)
	if err != nil {
		return err
	}
	if session.ResourcesCleaned || session.State == workspace.SessionClosed {
		return nil
	}
	bootstrap, err := wire.DecodeSessionBootstrap(session.Bootstrap)
	if err != nil {
		return err
	}
	var record resourceRecord
	if len(session.Resources) == 0 {
		if session.Stop != nil {
			return nil
		}
		record.Reference = kubernetes.Reference{Namespace: c.Kube.Namespace, Owner: c.Owner, OwnerID: session.Conversation.OwnerID,
			WorkerSessionID: session.ID, WorkspaceID: session.Conversation.WorkspaceID, WorkspaceAnchorTaskID: session.WorkspaceAnchorTaskID,
			StorageID: session.StorageID, PodName: session.PodName, SecretName: session.PodName, RequestDigest: session.BootstrapDigest,
			RuntimeRef: session.RuntimeRef, PVCName: session.PVCName, PVCUID: session.PVCUID, TaskRoot: session.TaskRoot, NFSServer: bootstrap.NFSServer}
		record.Reference.PodDigest, err = kubernetes.SessionPodFingerprint(c.Policy, record.Reference, bootstrap)
		if err != nil {
			return err
		}
		record.PodRequest, err = kubernetes.SessionPodRequest(c.Policy, record.Reference, bootstrap)
		if err != nil {
			return err
		}
		record.SecretRequest, err = kubernetes.SessionSecretRequest(record.Reference, bootstrap)
		if err != nil {
			return err
		}
		if err := c.saveSessionResources(id, record); err != nil {
			return err
		}
	} else {
		record, err = sessionRecord(session)
		if err != nil {
			return err
		}
	}
	record.Reference.Owner = c.Owner
	if session.PodUID != "" {
		if record.Reference.PodUID != "" && record.Reference.PodUID != session.PodUID {
			return errors.New("session Pod UID differs from its create record")
		}
		record.Reference.PodUID = session.PodUID
		if err := c.saveSessionResources(id, record); err != nil {
			return err
		}
	}
	if err := c.Kube.ValidatePVC(ctx, record.Reference); err != nil {
		return err
	}
	if record.Reference.SecretUID == "" {
		if !record.SecretCreateRequested {
			if session.Stop != nil {
				return nil
			}
			record.SecretCreateRequested = true
			if err := c.saveSessionResources(id, record); err != nil {
				return err
			}
		}
		finish := diagnostics.StartPhase("session_secret_create", "worker_session_id", id)
		record.Reference.SecretUID, err = c.Kube.EnsureSessionSecret(ctx, record.Reference, record.SecretRequest)
		finish(err)
		if err != nil {
			return err
		}
		if err := c.saveSessionResources(id, record); err != nil {
			return err
		}
	}
	session, err = c.Store.GetSession(id)
	if err != nil {
		return err
	}
	if record.Reference.PodUID == "" {
		if !record.PodCreateRequested {
			if session.Stop != nil {
				return nil
			}
			record.PodCreateRequested = true
			if err := c.saveSessionResources(id, record); err != nil {
				return err
			}
		}
		// A lost create is resolved using these same bytes even after a stop.
		// Absence alone cannot prove that the previous request will not commit.
		finish := diagnostics.StartPhase("session_pod_create", "worker_session_id", id)
		record.Reference.PodUID, err = c.Kube.EnsurePod(ctx, record.Reference, record.PodRequest)
		finish(err)
		if err != nil {
			return err
		}
		if err := c.Store.BindSessionPod(id, session.PodName, record.Reference.PodUID, ""); err != nil {
			return err
		}
		if err := c.saveSessionResources(id, record); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) saveSessionResources(id string, record resourceRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return c.Store.SetSessionResources(id, raw)
}

func (c *Controller) sessionMetadata(g workspace.TaskGrant) (daemonapi.Bootstrap, error) {
	var metadata daemonapi.Bootstrap
	if json.Unmarshal(g.Metadata, &metadata) != nil || metadata.Runtime.ID != g.RuntimeID || metadata.Workspace.ID != g.WorkspaceID {
		return metadata, errors.New("turn runtime metadata is unavailable")
	}
	return metadata, nil
}
