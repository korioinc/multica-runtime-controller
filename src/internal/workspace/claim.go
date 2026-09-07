package workspace

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/environment"
)

type Observation struct {
	ID, WorkspaceID, AgentID, IssueID, ChatID, ProjectID, AuthToken, PriorWorkDir, PriorSession string
	LocalDirectory, ExecutionMode                                                               string
	RepositoryURLs                                                                              []string
	TaskEnvKeys                                                                                 []string
	Environment                                                                                 environment.Ref
}
type ReuseDecision struct{ ResetWorkDir, ResetSession bool }

func (s *Store) Observe(task Observation) (ReuseDecision, error) {
	decisions, err := s.ObserveBatch([]Observation{task})
	if err != nil {
		return ReuseDecision{}, err
	}
	return decisions[0], nil
}

// ObserveBatch publishes a complete successful claim response atomically. An
// invalid task in the batch cannot authorize an earlier task in that response.
func (s *Store) ObserveBatch(tasks []Observation) ([]ReuseDecision, error) {
	claims := make([]Claim, len(tasks))
	decisions := make([]ReuseDecision, len(tasks))
	seen := map[string]bool{}
	for i, task := range tasks {
		if !canonicalUUID(task.ID) || task.WorkspaceID == "" || task.AgentID == "" || !strings.HasPrefix(task.AuthToken, "mat_") || !validRef(task.Environment) || task.LocalDirectory != "" || (task.ExecutionMode != "" && task.ExecutionMode != "isolated") || seen[task.ID] {
			return nil, errors.New("claim requires canonical managed task identity, credential and environment")
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
		claims[i] = Claim{ID: task.ID, WorkspaceID: task.WorkspaceID, AgentID: task.AgentID, IssueID: task.IssueID, ChatID: task.ChatID, ProjectID: task.ProjectID, Grant: scopeDigest(task.WorkspaceID, task.AgentID, task.IssueID, task.ChatID, task.ProjectID, urls), TokenHash: digest(task.AuthToken), RepositoryURLs: urls, TaskEnvKeys: keys, PriorWorkDir: task.PriorWorkDir, PriorSession: task.PriorSession, Environment: task.Environment, ObservedAt: time.Now().UTC()}
	}
	err := s.locked(func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		changedScope := false
		for _, claim := range claims {
			if old, ok := state.Claims[claim.ID]; ok && (old.Denied || old.Grant != claim.Grant) {
				old.Denied = true
				old.TokenHash = ""
				state.Claims[claim.ID] = old
				changedScope = true
			}
		}
		if changedScope {
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
					if claim.PriorSession != "" && (!known || !sameRef(ref, claim.Environment)) {
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
		value, ok := state.Claims[taskID]
		if !ok || value.Denied || token == "" || value.TokenHash != digest(token) || value.WorkspaceID != workspaceID || value.AgentID != agentID {
			return errors.New("provider does not match an observed task claim")
		}
		claim = value
		return nil
	})
	return claim, err
}

// Bind connects a freshly validated official root to independent worker data.
// Root preparation and session files remain owned by the official daemon.
func (s *Store) Bind(claim Claim, root, piSession string, ref environment.Ref) (Binding, error) {
	var result Binding
	if !s.preparationRoot(root) || !validRef(ref) {
		return result, errors.New("invalid official preparation root or environment")
	}
	if err := realDirectory(root); err != nil {
		return result, err
	}
	if err := realDirectory(filepath.Join(root, "workdir")); err != nil {
		return result, err
	}
	err := s.locked(func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		current, ok := state.Claims[claim.ID]
		if !ok || current.Denied || current.Grant != claim.Grant || current.TokenHash == "" || current.TokenHash != claim.TokenHash || !sameRef(current.Environment, ref) {
			return errors.New("claim changed before storage binding")
		}
		claim = current
		var owner struct {
			WorkspaceID string `json:"workspace_id"`
			TaskID      string `json:"task_id"`
		}
		raw, err := os.ReadFile(filepath.Join(root, ".task_owner"))
		if err != nil || json.Unmarshal(raw, &owner) != nil || owner.WorkspaceID != claim.WorkspaceID || (owner.TaskID != claim.ID && filepath.Join(root, "workdir") != claim.PriorWorkDir) {
			return errors.New("official root is not owned or authorized by this claim")
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
			if owner.TaskID != claim.ID {
				return errors.New("prior root lost its durable storage binding")
			}
			binding = Binding{Root: root, Identity: uuid.NewString(), Grant: claim.Grant, WorkerSubPath: filepath.Join(StoragePrefix, uuid.NewString()), Sessions: map[string]environment.Ref{}}
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
			canonical, err := filepath.EvalSymlinks(piSession)
			if err != nil || canonical != piSession {
				return errors.New("Pi session must be a canonical regular file")
			}
			info, err := os.Lstat(piSession)
			if err != nil || !info.Mode().IsRegular() {
				return errors.New("Pi session is not a regular file")
			}
			knownRef, known := binding.Sessions[piSession]
			if known {
				if !sameRef(knownRef, ref) {
					return errors.New("Pi session belongs to another execution environment")
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
				binding.Sessions[piSession] = ref
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
		state.Bindings[root] = binding
		claim.BoundRoot, claim.WorkerSubPath = root, binding.WorkerSubPath
		state.Claims[claim.ID] = claim
		if err := s.write(state); err != nil {
			return err
		}
		result = binding
		return nil
	})
	return result, err
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
