package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

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
	retired := 0
	err := s.locked(func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		candidates := map[string]bool{}
		for _, b := range state.Bindings {
			candidates[b.WorkerSubPath] = true
		}
		for _, c := range state.Claims {
			if c.WorkerSubPath != "" {
				candidates[c.WorkerSubPath] = true
			}
		}
		for storage := range state.Retired {
			candidates[storage] = true
		}
		for storage := range candidates {
			id := filepath.Base(storage)
			if active[id] {
				continue
			}
			safe := true
			for _, c := range state.Claims {
				if c.WorkerSubPath == storage && !c.ObservedAt.Before(olderThan) {
					safe = false
				}
			}
			for _, b := range state.Bindings {
				if b.WorkerSubPath != storage {
					continue
				}
				if _, err := os.Lstat(b.Root); err == nil {
					safe = false
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if !safe {
				continue
			}
			release, err := s.AcquireLease(id)
			if errors.Is(err, ErrStorageBusy) {
				continue
			}
			if err != nil {
				return err
			}
			err = func() error {
				defer release()
				tombstone := state.Retired[storage]
				if tombstone == "" {
					tombstone = uuid.NewString()
					state.Retired[storage] = tombstone
					if err := s.write(state); err != nil {
						return err
					}
				}
				trashRoot := filepath.Join(s.options.Directory, "retired")
				if err := os.MkdirAll(trashRoot, 0700); err != nil {
					return err
				}
				if err := realDirectory(trashRoot); err != nil {
					return err
				}
				from, to := filepath.Join(root, storage), filepath.Join(trashRoot, tombstone)
				if _, err := os.Lstat(from); err == nil {
					if err := realDirectory(from); err != nil {
						return err
					}
					if _, err := os.Lstat(to); !errors.Is(err, os.ErrNotExist) {
						return errors.New("retirement destination already exists")
					}
					if err := os.Rename(from, to); err != nil {
						return err
					}
					if err := syncDirectory(filepath.Dir(from)); err != nil {
						return err
					}
					if err := syncDirectory(trashRoot); err != nil {
						return err
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
				if _, err := os.Lstat(to); err == nil {
					if err := realDirectory(to); err != nil {
						return err
					}
					if err := os.RemoveAll(to); err != nil {
						return err
					}
					if err := syncDirectory(trashRoot); err != nil {
						return err
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
				for key, b := range state.Bindings {
					if b.WorkerSubPath == storage {
						delete(state.Bindings, key)
					}
				}
				for key, c := range state.Claims {
					if c.WorkerSubPath == storage {
						c.Denied = true
						c.TokenHash = ""
						c.BoundRoot, c.WorkerSubPath, c.PriorWorkDir, c.PriorSession = "", "", "", ""
						state.Claims[key] = c
					}
				}
				delete(state.Retired, storage)
				if err := s.write(state); err != nil {
					return err
				}
				retired++
				return nil
			}()
			if err != nil {
				return err
			}
		}
		return nil
	})
	return retired, err
}
