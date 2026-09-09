package workspace

import (
	"path/filepath"
	"slices"
)

// WorkerStorageIDs lists only storage referenced by this installation's
// validated authority. It grants no execution or deletion permission; callers
// must hold each storage lease and check its live consumers before mutation.
func (s *Store) WorkerStorageIDs() ([]string, error) {
	var ids []string
	err := s.locked(func() error {
		state, err := s.read()
		if err != nil {
			return err
		}
		known := map[string]bool{}
		for _, claim := range state.Claims {
			if claim.WorkerSubPath != "" {
				known[filepath.Base(claim.WorkerSubPath)] = true
			}
		}
		for _, binding := range state.Bindings {
			known[filepath.Base(binding.WorkerSubPath)] = true
		}
		for storage := range state.Retired {
			known[filepath.Base(storage)] = true
		}
		for id := range known {
			ids = append(ids, id)
		}
		return nil
	})
	slices.Sort(ids)
	return ids, err
}
