package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

type Observation struct {
	ID, WorkspaceID, AgentID, IssueID, ChatID, ProjectID, AuthToken, PriorWorkDir, PriorSession string
	LocalDirectory, ExecutionMode                                                               string
	RepositoryURLs                                                                              []string
	TaskEnvKeys                                                                                 []string
	RuntimeRef                                                                                  runtimeimage.Ref
}
type ReuseDecision struct{ ResetWorkDir, ResetSession bool }

// ObserveBatch publishes a complete successful claim response atomically. An
// invalid task in the batch cannot authorize an earlier task in that response.
func (s *Store) ObserveBatch(tasks []Observation) ([]ReuseDecision, error) {
	claims := make([]Claim, len(tasks))
	decisions := make([]ReuseDecision, len(tasks))
	seen := map[string]bool{}
	for i, task := range tasks {
		if !canonicalUUID(task.ID) || task.WorkspaceID == "" || task.AgentID == "" || !strings.HasPrefix(task.AuthToken, "mat_") || !validRef(task.RuntimeRef) || task.LocalDirectory != "" || (task.ExecutionMode != "" && task.ExecutionMode != "isolated") || seen[task.ID] {
			return nil, errors.New("claim requires canonical managed task identity, credential and runtime")
		}
		seen[task.ID] = true
		urls := slices.Clone(task.RepositoryURLs)
		if urls == nil {
			urls = []string{}
		}
		for j, u := range urls {
			urls[j] = strings.TrimSpace(u)
			if urls[j] == "" {
				return nil, errors.New("empty repository URL in claim")
			}
		}
		slices.Sort(urls)
		urls = slices.Compact(urls)
		keys := slices.Clone(task.TaskEnvKeys)
		if keys == nil {
			keys = []string{}
		}
		for _, key := range keys {
			if !envName(key) {
				return nil, errors.New("invalid explicit task environment name")
			}
		}
		slices.Sort(keys)
		keys = slices.Compact(keys)
		claims[i] = Claim{ID: task.ID, WorkspaceID: task.WorkspaceID, AgentID: task.AgentID, IssueID: task.IssueID, ChatID: task.ChatID, ProjectID: task.ProjectID, Grant: scopeDigest(task.WorkspaceID, task.AgentID, task.IssueID, task.ChatID, task.ProjectID, urls), TokenHash: digest(task.AuthToken), RepositoryURLs: urls, TaskEnvKeys: keys, PriorWorkDir: task.PriorWorkDir, PriorSession: task.PriorSession, ExecutionState: "observed", RuntimeRef: new(task.RuntimeRef), ObservedAt: time.Now().UTC()}
	}
	err := s.locked(func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		// Empty polls still validate persisted authority, but publish no changes.
		if len(claims) == 0 {
			return nil
		}
		revoked := false
		for _, claim := range claims {
			if old, ok := state.Claims[claim.ID]; ok && (old.Denied || old.Grant != claim.Grant || state.Retired[old.WorkerSubPath] != "") {
				old.Denied = true
				old.TokenHash = ""
				state.Claims[claim.ID] = old
				revoked = true
			}
		}
		if revoked {
			if err := s.write(state); err != nil {
				return err
			}
			return errors.New("task repository scope changed; task authority revoked")
		}
		for i, claim := range claims {
			if previous, ok := state.Claims[claim.ID]; ok {
				claim.BoundRoot, claim.WorkerSubPath = previous.BoundRoot, previous.WorkerSubPath
			}
			if claim.PriorWorkDir != "" || claim.PriorSession != "" {
				binding, ok := state.Bindings[filepath.Dir(claim.PriorWorkDir)]
				reusable := ok && binding.Grant == claim.Grant && filepath.Join(binding.Root, "workdir") == claim.PriorWorkDir && s.liveBinding(binding) == nil && state.Retired[binding.WorkerSubPath] == ""
				if !reusable {
					claim.PriorWorkDir, claim.PriorSession = "", ""
					decisions[i] = ReuseDecision{true, true}
				} else {
					ref, known := binding.Sessions[claim.PriorSession]
					if claim.PriorSession != "" && (!known || ref.State != "active" || ref.RuntimeRef == nil || !sameRef(*ref.RuntimeRef, *claim.RuntimeRef)) {
						claim.PriorSession = ""
						decisions[i].ResetSession = true
					}
				}
			}
			state.Claims[claim.ID] = claim
		}
		return s.write(state)
	})
	if err != nil {
		return nil, err
	}
	return decisions, nil
}

func (s *Store) Lookup(taskID, token, workspaceID, agentID string) (Claim, error) {
	var claim Claim
	err := s.locked(func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		claim, err = authorizeClaim(state, taskID, token, workspaceID, agentID)
		return err
	})
	return claim, err
}

// authorizeClaim reads only the current locked registry snapshot.
func authorizeClaim(state Registry, taskID, token, workspaceID, agentID string) (Claim, error) {
	value, ok := state.Claims[taskID]
	if !ok || value.Denied || state.Retired[value.WorkerSubPath] != "" || value.ExecutionState != "observed" || value.RuntimeRef == nil || token == "" || value.TokenHash != digest(token) || value.WorkspaceID != workspaceID || value.AgentID != agentID {
		return Claim{}, errors.New("provider does not match an observed task claim")
	}
	return value, nil
}

// AuthorizeAndBind connects an authorized official root to independent worker
// data using one current authority snapshot. Root and session preparation remain
// owned by the official daemon; successful retries complete durable publication.
func (s *Store) AuthorizeAndBind(taskID, token, workspaceID, agentID, root, piSession string, ref runtimeimage.Ref) (authorized Claim, result Binding, err error) {
	finish := diagnostics.StartPhase("registry_authorize_bind", diagnostics.TaskAttributes(taskID, "")...)
	var timing lockTiming
	dirty := false
	defer func() { finish(err, "lock_wait", timing.wait, "lock_hold", timing.hold, "changed", dirty) }()
	if !s.preparationRoot(root) || !validRef(ref) {
		return authorized, result, errors.New("invalid official preparation root or runtime")
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
		if !sameRef(*claim.RuntimeRef, ref) {
			return errors.New("claim runtime changed before storage binding")
		}
		rootTask, err := validateRootOwner(root, claim)
		if err != nil {
			return err
		}
		binding, exists := state.Bindings[root]
		if exists {
			if binding.Grant != claim.Grant || state.Retired[binding.WorkerSubPath] != "" {
				return errors.New("root has conflicting or retired storage authority")
			}
			if err := s.liveBinding(binding); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
				exists = false
			}
		}
		if exists && claim.WorkerSubPath != "" && binding.WorkerSubPath != claim.WorkerSubPath {
			return errors.New("root conflicts with the task's bound storage")
		}
		if !exists {
			dirty = true
			// A prior root may only reuse its durable binding, never recreate it.
			if rootTask != claim.ID {
				return errors.New("prior root lost its durable storage binding")
			}
			binding = Binding{Root: root, Identity: uuid.NewString(), Grant: claim.Grant, WorkerSubPath: filepath.Join(StoragePrefix, uuid.NewString()), Sessions: map[string]SessionRecord{}}
			if claim.WorkerSubPath != "" {
				previous, ok := state.Bindings[claim.BoundRoot]
				if !ok || previous.Grant != claim.Grant || previous.WorkerSubPath != claim.WorkerSubPath || state.Retired[claim.WorkerSubPath] != "" {
					return errors.New("task lost its prior storage authority")
				}
				if err := realDirectory(filepath.Join(s.options.WorkspaceRoot, previous.WorkerSubPath)); err != nil {
					return err
				}
				binding.WorkerSubPath = previous.WorkerSubPath
				binding.Sessions = previous.Sessions
			}
		}
		if piSession != "" {
			if !s.sessionPath(piSession) {
				return errors.New("Pi session must be in the daemon session directory")
			}
			info, err := s.sessionFile(piSession)
			if err != nil {
				return err
			}

			knownRef, known := binding.Sessions[piSession]
			if known {
				if knownRef.State != "active" || knownRef.RuntimeRef == nil || !sameRef(*knownRef.RuntimeRef, ref) {
					return errors.New("Pi session is archived or belongs to another runtime")
				}
			} else {
				for _, other := range state.Bindings {
					if _, bound := other.Sessions[piSession]; bound && other.WorkerSubPath != binding.WorkerSubPath {
						return errors.New("Pi session belongs to another workspace")
					}
				}
				if info.Size() != 0 {
					return errors.New("unapproved nonempty Pi session")
				}
				dirty = true
				binding.Sessions[piSession] = SessionRecord{State: "active", RuntimeRef: new(ref)}
			}
		}
		workerPath := filepath.Join(s.options.WorkspaceRoot, binding.WorkerSubPath)
		if err := os.MkdirAll(workerPath, 0700); err != nil {
			return err
		}
		if err := realDirectory(workerPath); err != nil {
			return err
		}
		if !exists {
			if err := atomicWrite(filepath.Join(root, rootMarker), []byte(binding.Identity)); err != nil {
				return err
			}
		}
		if claim.BoundRoot != root || claim.WorkerSubPath != binding.WorkerSubPath {
			dirty = true
		}
		claim.BoundRoot, claim.WorkerSubPath = root, binding.WorkerSubPath
		if dirty {
			state.Bindings[root] = binding
			state.Claims[claim.ID] = claim
			if err := s.write(state); err != nil {
				return err
			}
		} else if err := syncDirectory(s.options.Directory); err != nil {
			// A previous rename may have succeeded before its directory sync
			// failed. Visible identical state alone does not prove durability.
			return err
		}
		authorized, result = claim, binding
		return nil
	})
	return authorized, result, err
}

func (s *Store) liveBinding(binding Binding) error {
	if err := realDirectory(binding.Root); err != nil {
		return err
	}
	marker, err := os.ReadFile(filepath.Join(binding.Root, rootMarker))
	if err != nil {
		return err
	}
	if string(marker) != binding.Identity {
		return os.ErrNotExist
	}
	return realDirectory(filepath.Join(s.options.WorkspaceRoot, binding.WorkerSubPath))
}

func envName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r != '_' && (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (i == 0 || r < '0' || r > '9') {
			return false
		}
	}
	return true
}
