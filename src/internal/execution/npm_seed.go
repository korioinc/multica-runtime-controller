package execution

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

// copyNPMSeed initializes package-manager state as a complete tree. Existing
// state belongs to the provider/user, including files removed by npm updates.
func copyNPMSeed(home, source string) error {
	info, err := os.Lstat(home)
	if err != nil || !info.IsDir() {
		return errors.New("package HOME must be a real directory")
	}
	private, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer private.Close()
	parentPath := filepath.Dir(runtimeimage.PiNPMDirectory)
	if err := homeDirectories(private, parentPath); err != nil {
		return err
	}
	parent, err := private.OpenRoot(parentPath)
	if err != nil {
		return err
	}
	defer parent.Close()
	if info, err := parent.Lstat("npm"); err == nil {
		if !info.IsDir() {
			return errors.New("private npm destination must be a real directory")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	input, err := os.OpenRoot(source)
	if err != nil {
		return err
	}
	defer input.Close()
	stage := ".npm-seed-" + uuid.NewString()
	if err := parent.Mkdir(stage, 0700); err != nil {
		return err
	}
	defer parent.RemoveAll(stage)
	staged, err := parent.OpenRoot(stage)
	if err != nil {
		return err
	}
	defer staged.Close()
	if err := fs.WalkDir(input.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || path == "." {
			return err
		}
		info, err := input.Lstat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return homeDirectories(staged, path)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := input.Readlink(path)
			if err != nil {
				return err
			}
			return staged.Symlink(target, path)
		}
		if !info.Mode().IsRegular() {
			return errors.New("package seed contains a special file")
		}
		file, err := input.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		// These reproducible package assets become visible as one completed
		// tree. Keep writes in that private tree until directory publication.
		out, err := staged.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600|info.Mode().Perm()&0100)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, file)
		return errors.Join(copyErr, out.Close())
	}); err != nil {
		return err
	}
	if err := runtimeimage.ValidateNPMSeed(filepath.Join(home, parentPath, stage)); err != nil {
		return err
	}
	fd, err := parent.Open(".")
	if err != nil {
		return err
	}
	defer fd.Close()
	if err := checkout.PublishDirectory(int(fd.Fd()), stage, "npm"); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		// A concurrent initializer or provider owns the completed destination.
		if info, err := parent.Lstat("npm"); err != nil || !info.IsDir() {
			return errors.New("npm publication conflicted with a non-directory")
		}
	}
	return nil
}
