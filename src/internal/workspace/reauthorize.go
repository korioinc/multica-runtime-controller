package workspace

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

// ReauthorizeBinding validates an already bound task after acquiring its storage
// lease. Unlike AuthorizeAndBind, it cannot create, repair or move authority.
// The lock order is storage lease -> registry, also used by retirement.
func (s *Store) ReauthorizeBinding(taskID, token, workspaceID, agentID, root, piSession, storage string, ref runtimeimage.Ref) (authorized Claim, result Binding, err error) {
	finish := diagnostics.StartPhase("registry_reauthorize", diagnostics.TaskAttributes(taskID, "")...)
	var timing lockTiming
	defer func() { finish(err, "lock_wait", timing.wait, "lock_hold", timing.hold) }()
	if !s.preparationRoot(root) || !workerSubPath(storage) || !validRef(ref) {
		return authorized, result, errors.New("invalid bound task authority")
	}
	err = s.withLock(&timing, func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		claim, err := authorizeClaim(state, taskID, token, workspaceID, agentID)
		if err != nil {
			return err
		}
		if claim.BoundRoot != root || claim.WorkerSubPath != storage || !sameRef(*claim.RuntimeRef, ref) {
			return errors.New("task binding changed while awaiting storage")
		}
		if _, err := validateRootOwner(root, claim); err != nil {
			return err
		}
		binding, exists := state.Bindings[root]
		if !exists || binding.WorkerSubPath != storage || binding.Grant != claim.Grant || state.Retired[storage] != "" {
			return errors.New("bound storage authority is unavailable")
		}
		if err := s.liveBinding(binding); err != nil {
			return err
		}
		if piSession != "" {
			if !s.sessionPath(piSession) {
				return errors.New("Pi session must be in the daemon session directory")
			}
			session, known := binding.Sessions[piSession]
			if !known || session.State != "active" || session.RuntimeRef == nil || !sameRef(*session.RuntimeRef, ref) {
				return errors.New("Pi session authority changed while awaiting storage")
			}
			if _, err := s.sessionFile(piSession); err != nil {
				return err
			}
		}
		authorized, result = claim, binding
		return nil
	})
	return authorized, result, err
}

func validateRootOwner(root string, claim Claim) (string, error) {
	if err := realDirectory(root); err != nil {
		return "", err
	}
	if err := realDirectory(filepath.Join(root, "workdir")); err != nil {
		return "", err
	}
	var owner struct {
		WorkspaceID string `json:"workspace_id"`
		TaskID      string `json:"task_id"`
	}
	raw, err := os.ReadFile(filepath.Join(root, ".task_owner"))
	if err != nil || json.Unmarshal(raw, &owner) != nil || owner.WorkspaceID != claim.WorkspaceID || (owner.TaskID != claim.ID && filepath.Join(root, "workdir") != claim.PriorWorkDir) {
		return "", errors.New("official root is not owned or authorized by this claim")
	}
	return owner.TaskID, nil
}
