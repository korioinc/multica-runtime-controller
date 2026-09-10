package execution

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// copyHomeConfigExcept publishes plain defaults without replacing native files.
// Image npm state is excluded and published separately as a complete directory.
func copyHomeConfigExcept(home, source, target, excluded string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return errors.New("configuration input must be a regular file or directory")
	}
	if !configuration.HomePath(target, info.IsDir()) {
		return errors.New("configuration copy shadows native session or daemon authority")
	}
	homeInfo, err := os.Lstat(home)
	if err != nil {
		return err
	}
	if !homeInfo.IsDir() {
		return errors.New("configuration HOME must be a real directory")
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return err
	}
	defer root.Close()
	if !info.IsDir() {
		return copyHomeFile(root, source, strings.TrimPrefix(target, wire.Home+"/"), info.Mode())
	}
	// Operator projections are captured by configuration before reaching HOME.
	// Seeds are plain trees; preserve rejection of every reserved ..data entry.
	if _, statErr := os.Lstat(filepath.Join(source, "..data")); statErr == nil {
		return errors.New("HOME source must not contain a configuration projection")
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !configuration.HomePath(destination, info.IsDir()) {
			return errors.New("configuration tree shadows native session or daemon authority")
		}
		if destination == excluded {
			if !info.IsDir() {
				return errors.New("excluded package state must be a directory")
			}
			return fs.SkipDir
		}
		destination = strings.TrimPrefix(destination, wire.Home+"/")
		if info.IsDir() {
			return homeDirectories(root, destination)
		}
		if !info.Mode().IsRegular() {
			return errors.New("configuration tree contains a link or special file")
		}
		return copyHomeFile(root, path, destination, info.Mode())
	})
}

func homeDirectories(root *os.Root, relative string) error {
	current := ""
	for _, part := range strings.Split(relative, string(filepath.Separator)) {
		if part == "." {
			continue
		}
		current = filepath.Join(current, part)
		if err := root.Mkdir(current, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := root.Lstat(current)
		if err != nil || !info.IsDir() {
			return errors.New("native HOME destination has a link or non-directory parent")
		}
	}
	return nil
}

func copyHomeFile(root *os.Root, source, destination string, mode fs.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	return copyHomeContents(root, in, destination, mode)
}

func copyHomeContents(root *os.Root, in io.Reader, destination string, mode fs.FileMode) error {
	parent := filepath.Dir(destination)
	if err := homeDirectories(root, parent); err != nil {
		return err
	}
	if info, err := root.Lstat(destination); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("native HOME configuration destination is not a regular file")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	// A unique sibling plus a no-overwrite link avoids publishing partial files
	// if init is interrupted, and preserves files from an earlier successful copy.
	temporary := filepath.Join(parent, ".config-copy-"+uuid.NewString())
	out, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600|mode.Perm()&0100)
	if err != nil {
		return err
	}
	defer root.Remove(temporary)
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := root.Link(temporary, destination); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		info, statErr := root.Lstat(destination)
		if statErr != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("native HOME configuration publication conflict: %w", err)
		}
	}
	return nil
}
