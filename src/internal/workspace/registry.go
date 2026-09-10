package workspace

import (
	"errors"
	"path/filepath"
	"strings"
)

// ValidateRegistry checks persistent authority without touching the filesystem.
// Migration uses this same schema-2 validator before publishing a candidate.
func ValidateRegistry(options Options, state Registry) error {
	if !absolute(options.Directory) || !absolute(options.WorkspaceRoot) || !absolute(options.SessionRoot) || !canonicalUUID(options.OwnerID) || options.Directory != filepath.Join(options.WorkspaceRoot, ".multica-runtime/state") {
		return errors.New("invalid workspace validation options")
	}
	if state.SchemaVersion != SchemaVersion || state.OwnerID != options.OwnerID || state.WorkspaceRoot != options.WorkspaceRoot || state.Claims == nil || state.Bindings == nil || state.Retired == nil {
		return errors.New("unsupported or foreign workspace owner store; schema 1 requires explicit migration")
	}
	s := &Store{options: options}
	if err := s.validateRecords(state); err != nil {
		return err
	}
	return validateAuthorityRelations(state)
}

func (s *Store) validateRecords(state Registry) error {
	for key, c := range state.Claims {
		if err := s.validateClaimAuthority(key, c); err != nil {
			return err
		}
	}
	for _, c := range state.Claims {
		if err := s.validateClaimContext(c); err != nil {
			return err
		}
	}
	for key, b := range state.Bindings {
		if err := s.validateBinding(key, b); err != nil {
			return err
		}
	}
	for storage, tombstone := range state.Retired {
		if !workerSubPath(storage) || !canonicalUUID(tombstone) {
			return errors.New("corrupt storage retirement")
		}
	}
	return nil
}

func (s *Store) validateClaimAuthority(key string, c Claim) error {
	if key != c.ID || !canonicalUUID(c.ID) || c.WorkspaceID == "" || c.AgentID == "" || !fingerprint(c.Grant) || c.ObservedAt.IsZero() || c.RepositoryURLs == nil || !validClaimRuntime(c) || (!c.Denied && !fingerprint(c.TokenHash)) || (c.Denied && c.TokenHash != "") {
		return errors.New("corrupt workspace claim authority")
	}
	if (c.BoundRoot == "") != (c.WorkerSubPath == "") || (c.BoundRoot != "" && (!s.preparationRoot(c.BoundRoot) || !workerSubPath(c.WorkerSubPath))) {
		return errors.New("corrupt workspace storage authority")
	}
	if c.Grant != scopeDigest(c.WorkspaceID, c.AgentID, c.IssueID, c.ChatID, c.ProjectID, c.RepositoryURLs) {
		return errors.New("claim scope fingerprint differs from persisted authority")
	}
	for i, u := range c.RepositoryURLs {
		if u == "" || strings.TrimSpace(u) != u || (i > 0 && c.RepositoryURLs[i-1] >= u) {
			return errors.New("corrupt repository scope")
		}
	}
	return nil
}

func (s *Store) validateClaimContext(c Claim) error {
	if (c.PriorWorkDir != "" && (!s.preparationRoot(filepath.Dir(c.PriorWorkDir)) || filepath.Base(c.PriorWorkDir) != "workdir")) || (c.PriorSession != "" && (c.PriorWorkDir == "" || !s.sessionPath(c.PriorSession))) {
		return errors.New("corrupt prior workspace or session path")
	}
	if c.TaskEnvKeys == nil {
		return errors.New("claim has no task environment provenance")
	}
	for i, key := range c.TaskEnvKeys {
		if !envName(key) || (i > 0 && c.TaskEnvKeys[i-1] >= key) {
			return errors.New("corrupt task environment provenance")
		}
	}
	return nil
}

func (s *Store) validateBinding(key string, b Binding) error {
	if key != b.Root || !s.preparationRoot(b.Root) || !canonicalUUID(b.Identity) || !fingerprint(b.Grant) || !workerSubPath(b.WorkerSubPath) || b.Sessions == nil {
		return errors.New("corrupt workspace binding")
	}
	for session, ref := range b.Sessions {
		if !s.sessionPath(session) || !validSessionRuntime(ref) {
			return errors.New("corrupt session authority")
		}
	}
	return nil
}

func validateAuthorityRelations(state Registry) error {
	storageScopes := map[string]string{}
	type sessionAuthority struct {
		storage   string
		reference SessionRecord
	}
	sessions := map[string]sessionAuthority{}
	for _, binding := range state.Bindings {
		if grant, known := storageScopes[binding.WorkerSubPath]; known && grant != binding.Grant {
			return errors.New("worker storage is bound to conflicting repository scopes")
		}
		storageScopes[binding.WorkerSubPath] = binding.Grant
		for session, ref := range binding.Sessions {
			if prior, known := sessions[session]; known && (prior.storage != binding.WorkerSubPath || !prior.reference.Equal(ref)) {
				return errors.New("session has conflicting storage or runtime authority")
			}
			sessions[session] = sessionAuthority{binding.WorkerSubPath, ref}
		}
	}
	claimedStorage := map[string]bool{}
	for _, claim := range state.Claims {
		if claim.BoundRoot == "" {
			continue
		}
		binding, ok := state.Bindings[claim.BoundRoot]
		if !ok || claim.Grant != binding.Grant || claim.WorkerSubPath != binding.WorkerSubPath {
			return errors.New("claim and storage binding authority disagree")
		}
		claimedStorage[claim.WorkerSubPath] = true
	}
	for storage := range storageScopes {
		if !claimedStorage[storage] {
			return errors.New("storage binding has no observed claim authority")
		}
	}
	for storage := range state.Retired {
		if !claimedStorage[storage] {
			return errors.New("retirement has no observed storage authority")
		}
	}
	return nil
}

func validClaimRuntime(c Claim) bool {
	switch c.ExecutionState {
	case "observed":
		return c.RuntimeRef != nil && c.RuntimeRef.Validate() == nil
	case "unobserved":
		return c.RuntimeRef == nil
	default:
		return false
	}
}
func validSessionRuntime(s SessionRecord) bool {
	switch s.State {
	case "active":
		return s.RuntimeRef != nil && s.RuntimeRef.Validate() == nil
	case "archived":
		return s.RuntimeRef == nil
	default:
		return false
	}
}
