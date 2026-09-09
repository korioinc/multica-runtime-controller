package execution

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// stagedDirectories belongs to one fresh tree exclusively owned by its
// initializer. It must not be reused for an existing, mutable provider HOME.
type stagedDirectories struct {
	root  *os.Root
	paths map[string]bool
}

func newStagedDirectories(root *os.Root) *stagedDirectories {
	return &stagedDirectories{root: root, paths: map[string]bool{".": true}}
}

func (d *stagedDirectories) mkdirAll(path string) error {
	if d.paths[path] {
		return nil
	}
	if !fs.ValidPath(path) {
		return errors.New("staging directory path is not confined")
	}
	if err := d.mkdirAll(filepath.Dir(path)); err != nil {
		return err
	}
	if err := d.root.Mkdir(path, 0700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := d.root.Lstat(path)
		if err != nil || !info.IsDir() {
			return errors.New("staging directory has a link or non-directory parent")
		}
	}
	d.paths[path] = true
	return nil
}
