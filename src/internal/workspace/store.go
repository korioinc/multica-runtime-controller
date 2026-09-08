// Package workspace owns persistent claim, session and storage authority.
// The registry and its locks are controller-only and must never reach a worker.
package workspace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"golang.org/x/sys/unix"
)

const DefaultDirectory = "/workspace/.multica-runtime/state"
const DefaultSessionRoot = "/home/multica/agents/.multica/pi-sessions"
const SchemaVersion = 2
const StoragePrefix = ".multica-runtime/workers"
const rootMarker = ".multica-runtime-binding"

var ErrMigrationRequired = errors.New("workspace schema 1 requires explicit migration")

type Options struct{ Directory, WorkspaceRoot, SessionRoot, OwnerID string }
type Store struct{ options Options }

type Claim struct {
	ID             string            `json:"id"`
	WorkspaceID    string            `json:"workspaceID"`
	AgentID        string            `json:"agentID"`
	IssueID        string            `json:"issueID,omitempty"`
	ChatID         string            `json:"chatID,omitempty"`
	ProjectID      string            `json:"projectID,omitempty"`
	Grant          string            `json:"scope"`
	TokenHash      string            `json:"credentialFingerprint"`
	PriorWorkDir   string            `json:"priorWorkDir,omitempty"`
	PriorSession   string            `json:"priorSession,omitempty"`
	BoundRoot      string            `json:"boundRoot,omitempty"`
	WorkerSubPath  string            `json:"workerSubPath,omitempty"`
	RepositoryURLs []string          `json:"repositoryURLs"`
	TaskEnvKeys    []string          `json:"taskEnvKeys"`
	ExecutionState string            `json:"executionState"`
	RuntimeRef     *runtimeimage.Ref `json:"runtimeRef"`
	Denied         bool              `json:"denied,omitempty"`
	ObservedAt     time.Time         `json:"observedAt"`
}

type SessionRecord struct {
	State      string            `json:"state"`
	RuntimeRef *runtimeimage.Ref `json:"runtimeRef"`
}

func (s SessionRecord) Equal(other SessionRecord) bool {
	return s.State == other.State && ((s.RuntimeRef == nil && other.RuntimeRef == nil) || (s.RuntimeRef != nil && other.RuntimeRef != nil && s.RuntimeRef.Equal(*other.RuntimeRef)))
}

type Binding struct {
	Root          string                   `json:"root"`
	Identity      string                   `json:"identity"`
	Grant         string                   `json:"scope"`
	WorkerSubPath string                   `json:"workerSubPath"`
	Sessions      map[string]SessionRecord `json:"sessions"`
}

type Registry struct {
	SchemaVersion int                `json:"schemaVersion"`
	OwnerID       string             `json:"ownerID"`
	WorkspaceRoot string             `json:"workspaceRoot"`
	Claims        map[string]Claim   `json:"claims"`
	Bindings      map[string]Binding `json:"bindings"`
	Retired       map[string]string  `json:"retired"`
}

// Open creates a new store only on an empty installation. Current stores are
// fully validated before startup; absent metadata never adopts existing data.
func Open(options Options) (*Store, error) {
	if !absolute(options.Directory) || !absolute(options.WorkspaceRoot) || !absolute(options.SessionRoot) || !canonicalUUID(options.OwnerID) {
		return nil, errors.New("workspace requires canonical paths and installation owner UUID")
	}
	if options.Directory != filepath.Join(options.WorkspaceRoot, ".multica-runtime/state") {
		return nil, errors.New("workspace state must use the controller authority directory")
	}
	if err := realDirectory(options.WorkspaceRoot); err != nil {
		return nil, fmt.Errorf("workspace root: %w", err)
	}
	store := &Store{options: options}
	path := filepath.Join(options.Directory, "registry.json")
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if err := emptyInstallation(options.WorkspaceRoot); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(options.Directory, 0700); err != nil {
		return nil, err
	}
	if err := realDirectory(options.Directory); err != nil {
		return nil, err
	}
	err := store.locked(func() error {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			// A second initializer may have arrived while the first held this lock.
			if err := emptyInstallation(options.WorkspaceRoot); err != nil {
				return err
			}
			initial := Registry{SchemaVersion: SchemaVersion, OwnerID: options.OwnerID, WorkspaceRoot: options.WorkspaceRoot, Claims: map[string]Claim{}, Bindings: map[string]Binding{}, Retired: map[string]string{}}
			return store.write(initial)
		} else if err != nil {
			return err
		}
		_, err := store.read()
		return err
	})
	if err != nil {
		return nil, err
	}
	return store, nil
}

func emptyInstallation(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		switch rel {
		case "lost+found":
			if entry.IsDir() {
				return filepath.SkipDir
			}
		case ".multica-runtime", ".multica-runtime/state", ".multica-runtime/workers", ".multica-runtime/sessions":
			if entry.IsDir() {
				return nil
			}
		case ".multica-runtime/state/registry.lock":
			if entry.Type().IsRegular() {
				return nil
			}
		}
		return errors.New("workspace has data without a current owner store; reinstall with a fresh PVC")
	})
}

func (s *Store) locked(fn func() error) error {
	file, err := openLock(filepath.Join(s.options.Directory, "registry.lock"))
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

func (s *Store) read() (Registry, error) {
	var state Registry
	if err := readJSON(filepath.Join(s.options.Directory, "registry.json"), &state); err != nil {
		return state, err
	}
	return state, ValidateRegistry(s.options, state)
}

// ValidateRegistry checks persistent authority without touching the filesystem.
// Migration uses this same schema-2 validator before publishing a candidate.
func ValidateRegistry(options Options, state Registry) error {
	if !absolute(options.Directory) || !absolute(options.WorkspaceRoot) || !absolute(options.SessionRoot) || !canonicalUUID(options.OwnerID) || options.Directory != filepath.Join(options.WorkspaceRoot, ".multica-runtime/state") {
		return errors.New("invalid workspace validation options")
	}
	s := &Store{options: options}
	if state.SchemaVersion != SchemaVersion || state.OwnerID != options.OwnerID || state.WorkspaceRoot != options.WorkspaceRoot || state.Claims == nil || state.Bindings == nil || state.Retired == nil {
		return errors.New("unsupported or foreign workspace owner store; schema 1 requires explicit migration")
	}
	for key, c := range state.Claims {
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
	}
	for _, c := range state.Claims {
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
	}
	for key, b := range state.Bindings {
		if key != b.Root || !s.preparationRoot(b.Root) || !canonicalUUID(b.Identity) || !fingerprint(b.Grant) || !workerSubPath(b.WorkerSubPath) || b.Sessions == nil {
			return errors.New("corrupt workspace binding")
		}
		for session, ref := range b.Sessions {
			if !s.sessionPath(session) || !validSessionRuntime(ref) {
				return errors.New("corrupt session authority")
			}
		}
	}
	for storage, tombstone := range state.Retired {
		if !workerSubPath(storage) || !canonicalUUID(tombstone) {
			return errors.New("corrupt storage retirement")
		}
	}
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

func scopeDigest(workspaceID, agentID, issueID, chatID, projectID string, urls []string) string {
	raw, _ := json.Marshal([]any{workspaceID, agentID, issueID, chatID, projectID, urls})
	return digest(string(raw))
}

func (s *Store) write(state Registry) error {
	if err := ValidateRegistry(s.options, state); err != nil {
		return err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(s.options.Directory, "registry.json"), raw)
}

func readJSON(path string, value any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("authority file is not a regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var header struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return err
	}
	if header.SchemaVersion == 1 {
		return diagnostics.AtPath("workspace_migration_required", path, ErrMigrationRequired)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("invalid authority record suffix")
	}
	return nil
}

func atomicWrite(path string, raw []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func absolute(path string) bool { return filepath.IsAbs(path) && filepath.Clean(path) == path }
func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}
func fingerprint(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size && hex.EncodeToString(raw) == value
}
func workerSubPath(path string) bool {
	return filepath.Dir(path) == StoragePrefix && canonicalUUID(filepath.Base(path))
}
func realDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || resolved != path {
		return errors.New("directory must be canonical and contain no symlinks")
	}
	return nil
}
func (s *Store) preparationRoot(root string) bool {
	if !absolute(root) {
		return false
	}
	rel, err := filepath.Rel(s.options.WorkspaceRoot, root)
	parts := strings.Split(rel, string(filepath.Separator))
	return err == nil && len(parts) == 2 && parts[0] != ".." && !strings.HasPrefix(parts[0], ".") && parts[1] != ""
}
func (s *Store) sessionPath(path string) bool {
	return absolute(path) && filepath.Dir(path) == s.options.SessionRoot && strings.HasSuffix(filepath.Base(path), ".jsonl")
}
func validRef(ref runtimeimage.Ref) bool { return ref.Validate() == nil }
func sameRef(a, b runtimeimage.Ref) bool { return a.Equal(b) }
