// Package migration performs the explicit offline schema-1 workspace conversion.
// It never discovers a cluster, repairs attempts, or rolls back schema-2 writes.
// Callers must independently establish that old writers and task resources are
// quiescent; local locks cannot prove the absence of remote worker processes.
package migration

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type Options struct {
	Root                 string
	OwnerID              string
	ExpectedSourceSHA256 string
	Commit               bool
}
type Result struct {
	SourceSHA256       string   `json:"sourceSHA256"`
	AlreadyMigrated    bool     `json:"alreadyMigrated"`
	Committed          bool     `json:"committed"`
	MissingWorkerLocks []string `json:"missingWorkerLocks"`
}

func canonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value
}

// Migrate is read-only unless Commit is true and the source digest matches.
// No path or lock is created while validating or performing a dry-run.
func Migrate(options Options) (result Result, err error) {
	failureReason, failurePath := "migration_input_invalid", ""
	defer func() { err = diagnostics.AtPath(failureReason, failurePath, err) }()
	result.MissingWorkerLocks = []string{}
	if !filepath.IsAbs(options.Root) || filepath.Clean(options.Root) != options.Root || !canonicalUUID(options.OwnerID) || (options.Commit && !core.ValidSHA(options.ExpectedSourceSHA256)) || (!options.Commit && options.ExpectedSourceSHA256 != "") {
		return result, errors.New("migration requires a canonical workspace root, owner UUID and an expected source digest for commit")
	}
	stateDir := filepath.Join(options.Root, ".multica-runtime/state")
	failureReason = "migration_path_invalid"
	for _, path := range []string{options.Root, filepath.Join(options.Root, ".multica-runtime"), stateDir} {
		failurePath = path
		if err := realDirectory(path); err != nil {
			return result, fmt.Errorf("migration directory %s: %w", path, err)
		}
	}
	stateInfo, err := os.Stat(stateDir)
	if err != nil {
		return result, err
	}
	stateOwner, err := ownership(stateInfo)
	if err != nil {
		return result, err
	}
	held, err := acquire(filepath.Join(stateDir, "registry.lock"), stateOwner.uid)
	if err != nil {
		return result, err
	}
	locks := []*heldLock{held}
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].close()
		}
	}()
	registryPath := filepath.Join(stateDir, "registry.json")
	failureReason, failurePath = "migration_registry_invalid", registryPath
	source, sourceInfo, err := readExisting(registryPath)
	if err != nil {
		return result, err
	}
	owner, err := ownership(sourceInfo)
	if err != nil || owner.uid != stateOwner.uid {
		return result, errors.New("registry and state directory ownership differ")
	}
	result.SourceSHA256 = core.Digest(source)
	var header struct {
		SchemaVersion int `json:"schemaVersion"`
	}
	if err := json.Unmarshal(source, &header); err != nil {
		return result, err
	}
	validation := workspace.Options{Directory: stateDir, WorkspaceRoot: options.Root, SessionRoot: workspace.DefaultSessionRoot, OwnerID: options.OwnerID}
	if header.SchemaVersion == workspace.SchemaVersion {
		var current workspace.Registry
		if err := strictDecode(source, &current); err != nil {
			return result, err
		}
		if err := workspace.ValidateRegistry(validation, current); err != nil {
			return result, err
		}
		result.AlreadyMigrated = true
		return result, nil
	}
	if header.SchemaVersion != 1 {
		return result, errors.New("unsupported source workspace schema")
	}
	if options.Commit && result.SourceSHA256 != options.ExpectedSourceSHA256 {
		return result, diagnostics.AtPath("migration_source_changed", registryPath, errors.New("workspace source digest changed after dry-run"))
	}
	var previous legacyRegistry
	if err := strictDecode(source, &previous); err != nil {
		return result, err
	}
	candidate, err := convert(validation, previous)
	if err != nil {
		return result, err
	}
	existing, err := os.ReadDir(stateDir)
	if err != nil {
		return result, err
	}
	names := []string{}
	for _, entry := range existing {
		name := entry.Name()
		if !strings.HasPrefix(name, "worker-") {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, "worker-"), ".lock")
		if !strings.HasSuffix(name, ".lock") || !canonicalUUID(id) {
			return result, fmt.Errorf("unknown worker lock entry: %s", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	known := map[string]bool{}
	for _, name := range names {
		lock, err := acquire(filepath.Join(stateDir, name), stateOwner.uid)
		if err != nil {
			return result, err
		}
		locks = append(locks, lock)
		known[name] = true
	}
	required := map[string]bool{}
	for _, binding := range candidate.Bindings {
		required["worker-"+filepath.Base(binding.WorkerSubPath)+".lock"] = true
	}
	for storage := range candidate.Retired {
		required["worker-"+filepath.Base(storage)+".lock"] = true
	}
	for name := range required {
		if !known[name] {
			result.MissingWorkerLocks = append(result.MissingWorkerLocks, name)
		}
	}
	sort.Strings(result.MissingWorkerLocks)
	if err := noAttempts(filepath.Join(options.Root, ".multica-runtime/attempts")); err != nil {
		return result, err
	}
	if err := validateManagedPaths(options.Root, candidate, stateOwner.uid); err != nil {
		return result, err
	}
	if !options.Commit {
		return result, nil
	}
	backupDir := filepath.Join(stateDir, "migrations/v1-to-v2", result.SourceSHA256)
	failureReason, failurePath = "migration_backup_failed", backupDir
	if err := preserveBackup(stateDir, backupDir, source, owner); err != nil {
		return result, err
	}
	data, err := json.Marshal(candidate)
	if err != nil {
		return result, err
	}
	pending, err := writeCandidate(stateDir, data, owner)
	if err != nil {
		return result, err
	}
	defer os.Remove(pending)
	failureReason, failurePath = "migration_commit_failed", registryPath
	// Re-check the named authority and each held inode before the only replacement.
	for _, lock := range locks {
		if err := lock.unchanged(); err != nil {
			return result, err
		}
	}
	current, currentInfo, err := readExisting(registryPath)
	if err != nil || !os.SameFile(sourceInfo, currentInfo) || core.Digest(current) != result.SourceSHA256 {
		return result, errors.New("workspace source changed while migration held its locks")
	}
	if err := noAttempts(filepath.Join(options.Root, ".multica-runtime/attempts")); err != nil {
		return result, err
	}
	if err := os.Rename(pending, registryPath); err != nil {
		return result, err
	}
	result.Committed = true
	return result, syncDirectory(stateDir)
}

func validateManagedPaths(root string, state workspace.Registry, uid int) error {
	managed := filepath.Join(root, ".multica-runtime")
	for _, path := range []string{filepath.Join(managed, "workers"), filepath.Join(managed, "sessions"), filepath.Join(managed, "state/retired")} {
		if err := ownedExisting(path, true, uid); err != nil {
			return err
		}
	}
	for _, binding := range state.Bindings {
		if err := ownedExisting(filepath.Join(root, binding.WorkerSubPath), true, uid); err != nil {
			return err
		}
		for session := range binding.Sessions {
			if err := ownedExisting(filepath.Join(managed, "sessions", filepath.Base(session)), false, uid); err != nil {
				return err
			}
		}
	}
	for storage, tombstone := range state.Retired {
		for _, path := range []string{filepath.Join(root, storage), filepath.Join(managed, "state/retired", tombstone)} {
			if err := ownedExisting(path, true, uid); err != nil {
				return err
			}
		}
	}
	return nil
}

func noAttempts(path string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := realDirectory(path); err != nil {
		return fmt.Errorf("attempt directory: %w", err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return diagnostics.AtPath("migration_attempt_cleanup_required", filepath.Join(path, entries[0].Name()), errors.New("migration requires existing attempt cleanup"))
	}
	return nil
}
