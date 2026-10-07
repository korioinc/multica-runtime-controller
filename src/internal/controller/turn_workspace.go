package controller

import (
	"encoding/json"
	"errors"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

// selectWorkspace applies the same context-bound file policy to every turn.
// Native history selection remains separate and cannot widen this authority.
func (c *Controller) selectWorkspace(g workspace.TaskGrant, claim daemonapi.Claim, compatibility workspace.ConversationCompatibilityV1, backend workspace.Selection) (workspace.Selection, error) {
	eligible := claim.StartClaimSupported && compatibility.Authority.ValidForWorkspaceReuse()
	fresh := freshSelection(compatibility.Conversation, backend.ReuseEligible, backend.Reason)
	fresh.WorkspaceReuseEligible = eligible
	if !eligible {
		fresh.ReuseEligible = false
		return fresh, nil
	}
	digest, err := compatibility.WorkspaceDigest()
	if err != nil {
		return fresh, err
	}
	storageID := backend.StorageID
	if backend.Mode == workspace.SelectionFreshWorkspace {
		conversation, err := c.Store.GetConversation(compatibility.Conversation)
		if errors.Is(err, workspace.ErrUnauthorized) {
			return fresh, nil
		}
		if err != nil {
			return fresh, err
		}
		storageID = conversation.CurrentStorageID
	}
	if storageID == "" {
		return fresh, nil
	}
	storage, err := c.Store.GetStorage(storageID)
	if err != nil {
		return fresh, err
	}
	if storage.Conversation != compatibility.Conversation || storage.WorkspaceCompatibilityDigest != digest {
		fresh.Reason = "workspace_authority_changed"
		return fresh, nil
	}
	if claim.PriorWorkDir != "" && claim.PriorWorkDir != storage.TaskRoot+"/workdir" {
		fresh.Reason = "selected_workspace_unavailable"
		return fresh, nil
	}
	if err := c.workspaceWriterSettled(storage); err != nil {
		return fresh, err
	}
	var warm *workspace.WorkerSession
	workspaceSource := workspace.CheckpointSource{}
	if storage.WriterSessionID != "" {
		owner, err := c.Store.WarmSession(storage.ID)
		if err != nil {
			return fresh, err
		}
		warm = &owner
		latest, err := c.Store.Get(owner.LastCompleteAttempt)
		if err != nil {
			return fresh, err
		}
		workspaceSource = storage.LatestWriter
		if latest.CompletionWitness.WorkDir != storage.TaskRoot+"/workdir" {
			workspaceSource = latest.Selection.WorkspaceSource
		}
	} else if storage.Checkpoint != nil {
		workspaceSource = storage.Checkpoint.WorkspaceSource
	}
	if storage.Quarantined || storage.Prepared == nil || warm == nil && (storage.Dirty || storage.Checkpoint == nil || storage.Checkpoint.Source != storage.LatestWriter) {
		fresh.Reason = "selected_storage_unavailable"
		return fresh, nil
	}
	if backend.Mode != workspace.SelectionFreshWorkspace {
		// A queued native choice keeps its source while its writer fence advances.
		if backend.LatestWriter != storage.LatestWriter {
			backend.LatestWriter = storage.LatestWriter
			evidence, _ := json.Marshal([]any{"multica-selection-writer-fence-v1", backend.EvidenceDigest, storage.LatestWriter, storage.Checkpoint, warm})
			backend.EvidenceDigest = wire.Digest(evidence)
			if err := c.Store.RecordBackendSelection(g.AttemptID, backend); err != nil {
				return fresh, err
			}
		}
		backend.WorkspaceReuseEligible = true
		return backend, nil
	}
	allowed, err := c.workspaceIncludesNamedSource(claim, storage)
	if err != nil {
		return fresh, err
	}
	if !allowed {
		fresh.Reason = "named_source_unwitnessed"
		return fresh, nil
	}
	selection := workspace.Selection{Conversation: compatibility.Conversation, WorkspaceReuseEligible: true,
		Mode: workspace.SelectionFreshSession, StorageID: storage.ID, WorkspaceSource: workspaceSource,
		LatestWriter: storage.LatestWriter, WorkDir: storage.TaskRoot + "/workdir", Reason: "same_context_workspace"}
	evidence, _ := json.Marshal([]any{"multica-context-workspace-selection-v1", g.AttemptID, digest, storage.ID, storage.Checkpoint, warm})
	selection.EvidenceDigest = wire.Digest(evidence)
	return selection, nil
}

// Metadata-only probes require settled task authority in an exact live owner,
// or released cold storage. Resident apps may still write to a warm root.
func (c *Controller) workspaceWriterSettled(storage workspace.Storage) error {
	if storage.ActiveAttempt != "" {
		c.enqueueAttempt(storage.ActiveAttempt)
		c.enqueueDelivery(storage.ActiveAttempt)
		return workspace.ErrStorageBusy
	}
	if storage.WriterSessionID != "" {
		session, err := c.Store.WarmSession(storage.ID)
		if err != nil {
			c.enqueueSession(storage.WriterSessionID)
			return err
		}
		if session.ID != storage.WriterSessionID {
			return workspace.ErrStorageBusy
		}
	}
	return nil
}

func (c *Controller) workspaceIncludesNamedSource(claim daemonapi.Claim, storage workspace.Storage) (bool, error) {
	if err := c.workspaceWriterSettled(storage); err != nil {
		return false, err
	}
	if claim.Attribution == nil {
		return true, nil
	}
	named := claim.Attribution.RerunOfTaskID
	if named == "" {
		named = claim.Attribution.RetryOfTaskID
	}
	if named == "" {
		return true, nil
	}
	grants, err := c.Store.List()
	if err != nil {
		return false, err
	}
	for _, prior := range grants {
		if prior.TaskID == named && prior.Conversation == storage.Conversation && prior.StorageID == storage.ID &&
			prior.WorkspaceCompatibilityDigest == storage.WorkspaceCompatibilityDigest && prior.Selection != nil && prior.Selection.WorkspaceReuseEligible &&
			prior.TurnComplete && prior.CompletionWitness != nil && prior.CompletionWitness.WorkDir == storage.TaskRoot+"/workdir" {
			return true, nil
		}
	}
	return false, nil
}
