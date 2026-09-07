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
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// copyHomeConfig publishes private, writable files without replacing native
// state. A retry preserves completed files; this is not a whole-tree transaction.
func copyHomeConfig(home, source, target string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.IsDir() && !info.Mode().IsRegular() {
		return errors.New("configuration input must be a regular file or directory")
	}
	if !wire.ConfigHomePath(target, info.IsDir()) {
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
	// Kubelet projects directories through ..data. Resolve that link once so
	// traversal sees the selected payload, rather than kubelet's internal links.
	snapshot := source
	if data, statErr := os.Lstat(filepath.Join(source, "..data")); statErr == nil {
		if data.Mode()&os.ModeSymlink == 0 {
			return errors.New("configuration projection has an invalid data link")
		}
		snapshot, err = filepath.EvalSymlinks(filepath.Join(source, "..data"))
		if err != nil {
			return err
		}
		base, err := filepath.EvalSymlinks(source)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(base, snapshot)
		if err != nil || relative == "." || !filepath.IsLocal(relative) {
			return errors.New("configuration projection escapes its input")
		}
		dataInfo, err := os.Stat(snapshot)
		if err != nil || !dataInfo.IsDir() {
			return errors.New("configuration projection payload is not a directory")
		}
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	return filepath.WalkDir(snapshot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(snapshot, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !wire.ConfigHomePath(destination, info.IsDir()) {
			return errors.New("configuration tree shadows native session or daemon authority")
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
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	// A unique sibling plus a no-overwrite link avoids publishing partial files
	// if init is interrupted, and preserves files from an earlier successful copy.
	temporary := filepath.Join(parent, ".config-copy-"+uuid.NewString())
	out, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600|mode.Perm()&0100)
	if err != nil {
		return err
	}
	defer root.Remove(temporary)
	_, copyErr := io.Copy(out, in)
	if copyErr == nil {
		copyErr = out.Sync()
	}
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
