package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
)

type retirement struct{ storage, tombstone string }

// Collect accepts a cutoff computed as now - max(taskDeadline, 6h) - 24h.
// The minimum retention is also enforced here. active must include both live
// Pods and unresolved attempts; a failed resource inventory must skip Collect.
func (s *Store) Collect(root string, olderThan time.Time, active map[string]bool) (int, error) {
	if root != s.options.WorkspaceRoot || active == nil {
		return 0, errors.New("retirement requires the selected workspace and a complete active-storage inventory")
	}
	minimum := time.Now().Add(-30 * time.Hour)
	if olderThan.After(minimum) {
		olderThan = minimum
	}
	candidates, err := s.retirementCandidates(olderThan, active)
	if err != nil {
		return 0, err
	}
	retired := 0
	for _, storage := range candidates {
		if active[filepath.Base(storage)] {
			continue
		}
		completed, err := s.retireStorage(storage, olderThan)
		if err != nil {
			return retired, err
		}
		if completed {
			retired++
		}
	}
	return retired, nil
}

func (s *Store) retirementCandidates(olderThan time.Time, active map[string]bool) (result []string, err error) {
	finish := diagnostics.StartPhase("registry_gc_candidates")
	var timing lockTiming
	var claims, bindings, storages int
	defer func() {
		finish(err, "lock_wait", timing.wait, "lock_hold", timing.hold, "claims", claims, "bindings", bindings, "storages", storages, "candidates", len(result))
	}()
	err = s.withLock(&timing, func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		claims, bindings = len(state.Claims), len(state.Bindings)
		type candidate struct {
			known, recent, committed bool
			roots                    []string
		}
		candidates := make(map[string]*candidate)
		get := func(storage string) *candidate {
			value := candidates[storage]
			if value == nil {
				value = &candidate{}
				candidates[storage] = value
			}
			return value
		}
		for _, b := range state.Bindings {
			value := get(b.WorkerSubPath)
			value.roots = append(value.roots, b.Root)
		}
		for _, c := range state.Claims {
			if c.WorkerSubPath != "" {
				value := get(c.WorkerSubPath)
				value.known = true
				value.recent = value.recent || !c.ObservedAt.Before(olderThan)
			}
		}
		for storage := range state.Retired {
			get(storage).committed = true
		}
		storages = len(candidates)
		for storage, value := range candidates {
			if active[filepath.Base(storage)] {
				continue
			}
			if value.committed {
				result = append(result, storage)
				continue
			}
			if value.recent {
				continue
			}
			rootExists := false
			for _, root := range value.roots {
				if _, err := os.Lstat(root); err == nil {
					rootExists = true
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if !rootExists && value.known {
				result = append(result, storage)
			}
		}
		return nil
	})
	return result, err
}

func (s *Store) retireStorage(storage string, olderThan time.Time) (bool, error) {
	release, err := s.AcquireLease(filepath.Base(storage))
	if errors.Is(err, ErrStorageBusy) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer release()

	var pending retirement
	err = s.locked(func() error {
		// Candidate discovery grants no deletion authority. Another claim or
		// collector may have changed the storage before this lease was acquired.
		state, err := s.read()
		if err != nil {
			return err
		}
		eligible, err := retirementEligible(state, storage, olderThan)
		if err != nil || !eligible {
			return err
		}
		pending, err = s.markRetirement(state, storage)
		if err != nil {
			return err
		}
		return s.quarantineRetirement(pending)
	})
	if err != nil || pending.tombstone == "" {
		return false, err
	}
	// The tombstone and revocation are durable before the global lock is
	// released. The storage lease remains held through deletion and commit.
	if err := s.removeRetiredData(pending); err != nil {
		return false, err
	}
	if err := s.finalizeRetirement(pending); err != nil {
		return false, err
	}
	return true, nil
}

func retirementEligible(state Registry, storage string, olderThan time.Time) (bool, error) {
	if state.Retired[storage] != "" {
		// A committed retirement is resumed even if an old daemon preparation
		// root reappeared. It can no longer restore authority to this storage.
		return true, nil
	}
	known := false
	for _, c := range state.Claims {
		if c.WorkerSubPath == storage {
			known = true
			if !c.ObservedAt.Before(olderThan) {
				return false, nil
			}
		}
	}
	for _, b := range state.Bindings {
		if b.WorkerSubPath != storage {
			continue
		}
		if _, err := os.Lstat(b.Root); err == nil {
			return false, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return known, nil
}

// markRetirement runs under both the registry lock and the storage lease.
// Keeping claim bindings until finalization preserves the existing schema and
// recovery identity. Revocation prevents concurrent observation from reviving it.
func (s *Store) markRetirement(state Registry, storage string) (retirement, error) {
	pending := retirement{storage: storage, tombstone: state.Retired[storage]}
	if pending.tombstone == "" {
		pending.tombstone = uuid.NewString()
		state.Retired[storage] = pending.tombstone
	}
	for key, c := range state.Claims {
		if c.WorkerSubPath == storage {
			c.Denied, c.TokenHash = true, ""
			state.Claims[key] = c
		}
	}
	return pending, s.write(state)
}

// quarantineRetirement runs under the registry lock. Repeating both parent
// syncs also completes a rename whose previous caller failed during fsync.
func (s *Store) quarantineRetirement(pending retirement) error {
	trashRoot := filepath.Join(s.options.Directory, "retired")
	if err := os.MkdirAll(trashRoot, 0700); err != nil {
		return err
	}
	if err := realDirectory(trashRoot); err != nil {
		return err
	}
	if err := syncDirectory(s.options.Directory); err != nil {
		return err
	}
	from, to := filepath.Join(s.options.WorkspaceRoot, pending.storage), filepath.Join(trashRoot, pending.tombstone)
	fromExists, err := retirementDirectoryExists(from)
	if err != nil {
		return err
	}
	toExists, err := retirementDirectoryExists(to)
	if err != nil {
		return err
	}
	if fromExists && toExists {
		return errors.New("retirement source and destination both exist")
	}
	if fromExists {
		if err := os.Rename(from, to); err != nil {
			return err
		}
	}
	return errors.Join(syncDirectory(filepath.Dir(from)), syncDirectory(trashRoot))
}

func retirementDirectoryExists(path string) (bool, error) {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, realDirectory(path)
}

// removeRetiredData deliberately holds only the storage lease. Large trees
// must not block observations, bindings or lookups for other storage.
func (s *Store) removeRetiredData(pending retirement) error {
	trashRoot := filepath.Join(s.options.Directory, "retired")
	to := filepath.Join(trashRoot, pending.tombstone)
	exists, err := retirementDirectoryExists(to)
	if err != nil {
		return err
	}
	if exists {
		if err := os.RemoveAll(to); err != nil {
			return err
		}
	}
	return syncDirectory(trashRoot)
}

func (s *Store) finalizeRetirement(pending retirement) error {
	return s.locked(func() error {
		// Deletion released the registry lock. Never write its earlier snapshot
		// over claims committed by another process in the meantime.
		state, err := s.read()
		if err != nil {
			return err
		}
		if state.Retired[pending.storage] != pending.tombstone {
			return errors.New("retirement identity changed before finalization")
		}
		for _, path := range []string{filepath.Join(s.options.WorkspaceRoot, pending.storage), filepath.Join(s.options.Directory, "retired", pending.tombstone)} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				return errors.Join(errors.New("retirement data removal is unconfirmed"), err)
			}
		}
		for key, b := range state.Bindings {
			if b.WorkerSubPath == pending.storage {
				delete(state.Bindings, key)
			}
		}
		for key, c := range state.Claims {
			if c.WorkerSubPath == pending.storage {
				c.Denied, c.TokenHash = true, ""
				c.BoundRoot, c.WorkerSubPath, c.PriorWorkDir, c.PriorSession = "", "", "", ""
				state.Claims[key] = c
			}
		}
		delete(state.Retired, pending.storage)
		return s.write(state)
	})
}
