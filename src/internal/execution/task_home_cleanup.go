package execution

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func openHomeStorage(workspaceRoot, storageID string) (*os.Root, error) {
	if !wire.UUID(storageID) {
		return nil, errors.New("task HOME cleanup requires a canonical storage identity")
	}
	path := filepath.Join(workspaceRoot, workspace.StoragePrefix, storageID)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil || !info.IsDir() {
		return nil, errors.New("task HOME cleanup storage is not a real directory")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return nil, errors.New("task HOME cleanup storage is not canonical")
	}
	return os.OpenRoot(path)
}

// The caller retains the lease until all possible Pod consumers are gone.
// Missing archives are normal when cleanup retries an earlier removal.
func removeTaskHomeArchive(workspaceRoot, storageID, attemptID string) error {
	if !wire.UUID(attemptID) {
		return errors.New("task HOME cleanup requires a canonical attempt identity")
	}
	worker, err := openHomeStorage(workspaceRoot, storageID)
	if err != nil || worker == nil {
		return err
	}
	defer worker.Close()
	info, err := worker.Lstat(taskHomeArtifacts)
	if errors.Is(err, os.ErrNotExist) {
		return syncHomeDirectory(worker, ".")
	}
	if err != nil || !info.IsDir() {
		return errors.New("task HOME cleanup archive directory is not a real directory")
	}
	artifacts, err := worker.OpenRoot(taskHomeArtifacts)
	if err != nil {
		return err
	}
	defer artifacts.Close()
	opened, err := artifacts.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		return errors.New("task HOME cleanup archive directory changed")
	}
	if err := artifacts.Remove(attemptID + ".tar"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncHomeDirectory(artifacts, ".")
}

// A process can die before it journals an attempt, leaving a staged HOME or a
// complete archive. A healthy collector reclaims those inputs only after every
// authority inventory succeeds, with no active producer or journaled consumer.
func (r *Runner) collectHomeArtifacts(active map[string]bool) error {
	ids, err := r.store.WorkerStorageIDs()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if active[id] {
			continue
		}
		release, err := r.store.AcquireLease(id)
		if errors.Is(err, workspace.ErrStorageBusy) {
			active[id] = true
			continue
		}
		if err != nil {
			return err
		}
		err = func() error {
			defer release()
			pending, err := r.journal.readAll()
			if err != nil {
				return err
			}
			for _, a := range pending {
				if a.Ref.StorageID == id {
					active[id] = true
					return nil
				}
			}
			worker, err := openHomeStorage(wire.WorkspaceRoot, id)
			if err != nil || worker == nil {
				return err
			}
			defer worker.Close()
			if _, err := worker.Lstat(taskHomeArtifacts); errors.Is(err, os.ErrNotExist) {
				// A prior sweep may have removed the directory but failed
				// its fsync. A retry still completes that durable boundary.
				return syncHomeDirectory(worker, ".")
			} else if err != nil {
				return err
			}
			// RemoveAll acts on this single reserved entry, never on a user
			// work/session path or an authority directory from another storage.
			if err := worker.RemoveAll(taskHomeArtifacts); err != nil {
				return err
			}
			return syncHomeDirectory(worker, ".")
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
