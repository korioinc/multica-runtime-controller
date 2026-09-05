package taskstate

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// Collect follows the official daemon's retirement decision: a worker is
// eligible only after every associated preparation directory has disappeared.
// Recent claims protect the gap before Prepare; Pod and lease checks protect
// live workers, including workers orphaned by a dead shim.
func (s *Store) Collect(workspace string, olderThan time.Time, active map[string]bool) (int, error) {
	retired := 0
	err := s.locked(func() error {
		type group struct {
			roots    map[string]bool
			bindings []string
			claims   map[string]Claim
		}
		groups := make(map[string]*group)
		get := func(subpath string) (*group, error) {
			if filepath.Dir(subpath) != ".runtime-workers" {
				return nil, errors.New("invalid worker binding during collection")
			}
			id, err := uuid.Parse(filepath.Base(subpath))
			if err != nil || id.String() != filepath.Base(subpath) {
				return nil, errors.New("invalid worker identity during collection")
			}
			if groups[subpath] == nil {
				groups[subpath] = &group{roots: make(map[string]bool), claims: make(map[string]Claim)}
			}
			return groups[subpath], nil
		}
		for _, kind := range []string{"roots", "claims"} {
			entries, err := os.ReadDir(filepath.Join(s.directory, kind))
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
					continue
				}
				key := strings.TrimSuffix(entry.Name(), ".json")
				if kind == "roots" {
					var binding Binding
					if err := s.read(kind, key, &binding); err != nil {
						return err
					}
					g, err := get(binding.WorkerSubPath)
					if err != nil {
						return err
					}
					g.roots[binding.Root] = true
					g.bindings = append(g.bindings, key)
				} else {
					var claim Claim
					if err := s.read(kind, key, &claim); err != nil {
						return err
					}
					if claim.WorkerSubPath == "" {
						if !claim.Denied && claim.ObservedAt.Before(olderThan) {
							if err := os.Remove(filepath.Join(s.directory, kind, entry.Name())); err != nil {
								return err
							}
						}
						continue
					}
					g, err := get(claim.WorkerSubPath)
					if err != nil {
						return err
					}
					g.roots[claim.BoundRoot] = true
					g.claims[key] = claim
				}
			}
		}
		trash := filepath.Join(s.directory, "trash")
		if err := os.MkdirAll(trash, 0o700); err != nil {
			return err
		}
		for subpath, g := range groups {
			id := filepath.Base(subpath)
			keep := active[id]
			for _, claim := range g.claims {
				if !claim.ObservedAt.Before(olderThan) {
					keep = true
				}
			}
			for root := range g.roots {
				if root == "" || !filepath.IsAbs(root) {
					return errors.New("invalid preparation root during collection")
				}
				if _, err := os.Lstat(root); err == nil {
					keep = true
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			if keep {
				continue
			}
			lease, err := os.OpenFile(filepath.Join(s.directory, "worker-"+id+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				return err
			}
			if err := unix.Flock(int(lease.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
				lease.Close()
				continue
			}
			err = func() error {
				defer lease.Close()
				defer unix.Flock(int(lease.Fd()), unix.LOCK_UN)
				// Rename is short and serialized with claim/binding mutations.
				// Slow recursive deletion happens after releasing the state lock.
				from := filepath.Join(workspace, subpath)
				if err := os.Rename(from, filepath.Join(trash, id+"-"+uuid.NewString())); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				for _, key := range g.bindings {
					if err := os.Remove(filepath.Join(s.directory, "roots", key+".json")); err != nil {
						return err
					}
				}
				for key, claim := range g.claims {
					if claim.Denied {
						claim.BoundRoot, claim.WorkerSubPath = "", ""
						if err := s.write("claims", key, claim); err != nil {
							return err
						}
					} else if err := os.Remove(filepath.Join(s.directory, "claims", key+".json")); err != nil {
						return err
					}
				}
				for _, name := range []string{"seed-" + id + ".json", "worker-" + id + ".lock"} {
					if err := os.Remove(filepath.Join(s.directory, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
						return err
					}
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
	if err != nil {
		return retired, err
	}
	entries, err := os.ReadDir(filepath.Join(s.directory, "trash"))
	if err != nil {
		return retired, err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(s.directory, "trash", entry.Name())); err != nil {
			return retired, err
		}
	}
	return retired, nil
}
