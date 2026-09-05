// Package taskstate binds official claim responses to controller-owned storage.
// Its directory must never be mounted into a worker.
package taskstate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const DefaultDirectory = "/workspace/.runtime-controller/state-v2"
const rootMarker = ".runtime-controller-root-id"

type Claim struct {
	ID             string    `json:"id"`
	WorkspaceID    string    `json:"workspace_id"`
	AgentID        string    `json:"agent_id"`
	Grant          string    `json:"grant"`
	TokenHash      string    `json:"token_hash"`
	PriorWorkDir   string    `json:"prior_work_dir,omitempty"`
	PriorSession   string    `json:"prior_session_id,omitempty"`
	BoundRoot      string    `json:"bound_root,omitempty"`
	WorkerSubPath  string    `json:"worker_sub_path,omitempty"`
	RepositoryURLs []string  `json:"repository_urls"`
	Denied         bool      `json:"denied,omitempty"`
	ObservedAt     time.Time `json:"observed_at"`
}

type Binding struct {
	Root          string   `json:"root"`
	Identity      string   `json:"identity"`
	Grant         string   `json:"grant"`
	WorkerSubPath string   `json:"worker_sub_path"`
	Sessions      []string `json:"sessions,omitempty"`
}

type Store struct{ directory string }

func New(directory string) (*Store, error) {
	if !filepath.IsAbs(directory) {
		return nil, errors.New("task state directory must be absolute")
	}
	for _, part := range []string{"claims", "roots"} {
		if err := os.MkdirAll(filepath.Join(directory, part), 0o700); err != nil {
			return nil, err
		}
	}
	return &Store{directory: directory}, nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (s *Store) locked(fn func() error) error {
	file, err := os.OpenFile(filepath.Join(s.directory, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	defer unix.Flock(int(file.Fd()), unix.LOCK_UN)
	return fn()
}

// Observe preserves unknown API fields and commits the minimal claim before
// returning it to the official daemon. It never stores prompts or credentials.
func (s *Store) Observe(raw json.RawMessage) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	var task struct {
		ID           string `json:"id"`
		WorkspaceID  string `json:"workspace_id"`
		AgentID      string `json:"agent_id"`
		IssueID      string `json:"issue_id"`
		ChatID       string `json:"chat_session_id"`
		ProjectID    string `json:"project_id"`
		AuthToken    string `json:"auth_token"`
		PriorRoot    string `json:"prior_work_dir"`
		PriorSession string `json:"prior_session_id"`
		Repos        []struct {
			URL string `json:"url"`
		} `json:"repos"`
	}
	if json.Unmarshal(raw, &fields) != nil || fields == nil || json.Unmarshal(raw, &task) != nil {
		return nil, errors.New("invalid official task claim")
	}
	id, err := uuid.Parse(task.ID)
	if err != nil || id.String() != task.ID || task.WorkspaceID == "" || task.AgentID == "" || !strings.HasPrefix(task.AuthToken, "mat_") {
		return nil, errors.New("official claim has no canonical task identity or task credential")
	}
	urls := make([]string, 0, len(task.Repos))
	for _, repo := range task.Repos {
		if strings.TrimSpace(repo.URL) == "" {
			return nil, errors.New("official claim has an empty repository URL")
		}
		urls = append(urls, strings.TrimSpace(repo.URL))
	}
	slices.Sort(urls)
	urls = slices.Compact(urls)
	// Refs remain the official checkout API's responsibility. Repo URL changes
	// change access; selecting another ref in the same repo does not.
	scope, _ := json.Marshal([]any{task.WorkspaceID, task.AgentID, task.IssueID, task.ChatID, task.ProjectID, urls})
	claim := Claim{ID: task.ID, WorkspaceID: task.WorkspaceID, AgentID: task.AgentID, Grant: digest(string(scope)), TokenHash: digest(task.AuthToken), PriorWorkDir: task.PriorRoot, PriorSession: task.PriorSession}
	claim.RepositoryURLs = urls
	claim.ObservedAt = time.Now().UTC()
	err = s.locked(func() error {
		var previous Claim
		if err := s.read("claims", task.ID, &previous); err == nil && (previous.Denied || previous.Grant != claim.Grant) {
			previous.TokenHash = ""
			previous.Denied = true
			if err := s.write("claims", task.ID, previous); err != nil {
				return err
			}
			return errors.New("the repository scope of an existing task changed; create a new task")
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		claim.BoundRoot, claim.WorkerSubPath = previous.BoundRoot, previous.WorkerSubPath
		if claim.PriorWorkDir != "" || claim.PriorSession != "" {
			binding, err := s.binding(filepath.Dir(claim.PriorWorkDir))
			valid := err == nil && filepath.Join(binding.Root, "workdir") == claim.PriorWorkDir && binding.Grant == claim.Grant
			// Pi's opaque session ID is an absolute file path; never let a
			// different root's file enter this task even when its name exists.
			if valid && filepath.IsAbs(claim.PriorSession) {
				valid = slices.Contains(binding.Sessions, claim.PriorSession)
			}
			if !valid {
				claim.PriorWorkDir, claim.PriorSession = "", ""
				fields["prior_work_dir"] = json.RawMessage(`""`)
				fields["prior_session_id"] = json.RawMessage(`""`)
				fields["prior_session_resume_unavailable"] = json.RawMessage(`true`)
			}
		}
		return s.write("claims", task.ID, claim)
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

// Lookup is the final fail-closed check: unseen claim transports or stale
// claim credentials cannot start a worker even if a proxy route changes.
func (s *Store) Lookup(taskID, token, workspaceID, agentID string) (Claim, error) {
	var claim Claim
	err := s.locked(func() error {
		if err := s.read("claims", taskID, &claim); err != nil {
			return err
		}
		if claim.Denied || token == "" || claim.ID != taskID || claim.TokenHash != digest(token) || claim.WorkspaceID != workspaceID || claim.AgentID != agentID {
			return errors.New("provider request does not match an observed official claim")
		}
		return nil
	})
	return claim, err
}

// Bind runs in the shim, after the official daemon has locked/prepared its
// root and before creating the worker. The marker detects daemon GC/reset.
func (s *Store) Bind(claim Claim, root, piSession string) (Binding, error) {
	var result Binding
	err := s.locked(func() error {
		var current Claim
		if err := s.read("claims", claim.ID, &current); err != nil {
			return err
		}
		if current.Denied || current.Grant != claim.Grant || current.TokenHash != claim.TokenHash || current.TokenHash == "" {
			return errors.New("task claim changed before storage binding")
		}
		claim = current
		var owner struct {
			WorkspaceID string `json:"workspace_id"`
			TaskID      string `json:"task_id"`
		}
		raw, err := os.ReadFile(filepath.Join(root, ".task_owner"))
		if err != nil || json.Unmarshal(raw, &owner) != nil || owner.WorkspaceID != claim.WorkspaceID ||
			(owner.TaskID != claim.ID && filepath.Join(root, "workdir") != claim.PriorWorkDir) {
			return errors.New("daemon selected a root not owned or authorized by this claim")
		}
		binding, err := s.binding(root)
		if err == nil && binding.Grant != claim.Grant {
			return errors.New("prepared daemon root belongs to another repository scope")
		}
		if err == nil && claim.WorkerSubPath != "" && binding.WorkerSubPath != claim.WorkerSubPath {
			return errors.New("prepared root conflicts with this task's existing worker storage")
		}
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			binding = Binding{Root: root, Identity: uuid.NewString(), Grant: claim.Grant, WorkerSubPath: ".runtime-workers/" + uuid.NewString()}
			if owner.TaskID != claim.ID {
				return errors.New("authorized prior root lost its storage identity")
			}
			// Official Prepare resets a same-TaskID root on retry. Keep that
			// task's separately stored edits, which were never reset by daemon.
			if claim.WorkerSubPath != "" {
				binding.WorkerSubPath = claim.WorkerSubPath
				var previous Binding
				if s.read("roots", digest(claim.BoundRoot), &previous) == nil && previous.Grant == claim.Grant && previous.WorkerSubPath == binding.WorkerSubPath {
					binding.Sessions = previous.Sessions
				}
			}
			if err := atomicWrite(filepath.Join(root, rootMarker), []byte(binding.Identity)); err != nil {
				return err
			}
		}
		if piSession != "" && !slices.Contains(binding.Sessions, piSession) {
			info, err := os.Lstat(piSession)
			if err != nil || !info.Mode().IsRegular() || info.Size() != 0 {
				return errors.New("daemon selected an unbound nonempty Pi session")
			}
			binding.Sessions = append(binding.Sessions, piSession)
		}
		workerPath := filepath.Join(filepath.Dir(filepath.Dir(root)), binding.WorkerSubPath)
		if err := os.MkdirAll(workerPath, 0o700); err != nil {
			return err
		}
		if canonical, err := filepath.EvalSymlinks(workerPath); err != nil || canonical != workerPath {
			return errors.New("invalid worker storage path")
		}
		if err := s.write("roots", digest(root), binding); err != nil {
			return err
		}
		claim.BoundRoot, claim.WorkerSubPath = root, binding.WorkerSubPath
		if err := s.write("claims", claim.ID, claim); err != nil {
			return err
		}
		result = binding
		return nil
	})
	return result, err
}

func (s *Store) binding(root string) (Binding, error) {
	var result Binding
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return result, os.ErrNotExist
	}
	if err := s.read("roots", digest(root), &result); err != nil {
		return result, err
	}
	marker, err := os.ReadFile(filepath.Join(root, rootMarker))
	if errors.Is(err, os.ErrNotExist) || (err == nil && string(marker) != result.Identity) {
		return Binding{}, os.ErrNotExist
	}
	if err != nil || result.Root != root {
		return Binding{}, errors.New("invalid daemon root binding")
	}
	workerPath := filepath.Join(filepath.Dir(filepath.Dir(root)), result.WorkerSubPath)
	info, err := os.Lstat(workerPath)
	if err != nil || !info.IsDir() {
		return Binding{}, os.ErrNotExist
	}
	return result, nil
}

func (s *Store) read(kind, key string, value any) error {
	if !filepath.IsLocal(key) || strings.ContainsAny(key, "/\\") {
		return errors.New("invalid task state key")
	}
	raw, err := os.ReadFile(filepath.Join(s.directory, kind, key+".json"))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}

func (s *Store) write(kind, key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(s.directory, kind, key+".json"), raw)
}

func atomicWrite(destination string, raw []byte) error {
	file, err := os.CreateTemp(filepath.Dir(destination), ".state-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(raw); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), destination); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(destination))
	if err != nil {
		return fmt.Errorf("open state directory for sync: %w", err)
	}
	defer dir.Close()
	return dir.Sync()
}
