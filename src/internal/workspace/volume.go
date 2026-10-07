package workspace

import (
	"errors"
	"os"
	"path/filepath"
)

// OpenVolume establishes the controller journal inside an admitted metadata PVC.
// CSI may own the mount root and expose group/world write bits; the private
// journal directory is created by this process without changing mount ownership.
func OpenVolume(path string, options Options) (*Store, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || !canonicalUUID(options.OwnerID) {
		return nil, errors.New("invalid controller metadata volume identity")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return nil, errors.New("metadata volume must be a canonical real directory")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	journal, repositories := false, false
	for _, entry := range entries {
		// Filesystem recovery data belongs to the filesystem administrator. Never
		// traverse, adopt, or delete it while establishing controller authority.
		if (entry.Name() != "journal" && entry.Name() != "repositories" && entry.Name() != "lost+found") || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil, errors.New("metadata volume contains unrecognized data")
		}
		if entry.Name() == "journal" {
			journal = true
		}
		if entry.Name() == "repositories" {
			info, err := entry.Info()
			if err != nil || info.Mode().Perm()&0077 != 0 {
				return nil, errors.New("repository cache must be a private directory")
			}
			repositories = true
		}
	}
	if repositories {
		info, err := root.Lstat(filepath.Join("journal", "journal.json"))
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.New("repository cache without its controller journal requires recovery")
		}
	}
	if !journal {
		if err = root.Mkdir("journal", 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	info, err := root.Lstat("journal")
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0007 != 0 {
		return nil, errors.New("controller journal must be a real private directory")
	}
	if err = directory.Sync(); err != nil {
		return nil, err
	}
	options.Directory = filepath.Join(path, "journal")
	return Open(options)
}
