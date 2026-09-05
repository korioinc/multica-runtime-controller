// Package snapshot copies directory contents across the controller/worker
// boundary as bytes. No filesystem object is shared across that boundary.
package snapshot

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
)

const (
	completionEntry = ".multica-snapshot-complete"
	maxEntries      = 1_000_000
	maxBytes        = int64(64 << 30)
)

// Write reads a controller-owned directory which workers cannot modify.
// Hardlinked source files are emitted separately as ordinary file contents.
func Write(w io.Writer, source string) error {
	root, err := os.OpenRoot(source)
	if err != nil {
		return err
	}
	defer root.Close()
	tw := tar.NewWriter(w)
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || name == "." {
			return walkErr
		}
		if name == completionEntry {
			return errors.New("reserved snapshot entry in source")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		var link string
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = root.Readlink(name)
			if err != nil {
				return err
			}
		} else if !info.Mode().IsRegular() && !info.IsDir() {
			return fmt.Errorf("unsupported snapshot file: %s", name)
		}
		header, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		header.Name = name
		header.Mode &= 0o777
		header.Uid, header.Gid = 0, 0
		header.Uname, header.Gname = "", ""
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			file, err := root.Open(name)
			if err != nil {
				return err
			}
			_, err = io.CopyN(tw, file, info.Size())
			closeErr := file.Close()
			return errors.Join(err, closeErr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: completionEntry, Typeflag: tar.TypeReg, Mode: 0o600}); err != nil {
		return err
	}
	return tw.Close()
}

// Extract writes only inside an empty, private staging directory. The caller
// publishes the completed directory and owns cleanup after failure.
func Extract(r io.Reader, destination string) error {
	root, err := os.OpenRoot(destination)
	if err != nil {
		return err
	}
	defer root.Close()
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil || len(entries) != 0 {
		return errors.New("snapshot destination must be an empty directory")
	}
	tr := tar.NewReader(r)
	seen := make(map[string]bool)
	var total int64
	complete := false
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			if !complete {
				return errors.New("incomplete snapshot")
			}
			return nil
		}
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(h.Name, "/")
		if complete || !fs.ValidPath(name) || name == "." || seen[name] {
			return errors.New("invalid or duplicate snapshot entry")
		}
		seen[name] = true
		total += h.Size
		if len(seen) > maxEntries || h.Size < 0 || total > maxBytes {
			return errors.New("snapshot exceeds extraction limit")
		}
		if name == completionEntry {
			if h.Typeflag != tar.TypeReg || h.Size != 0 {
				return errors.New("invalid snapshot completion entry")
			}
			complete = true
			continue
		}
		if err := makeParents(root, name); err != nil {
			return err
		}
		switch h.Typeflag {
		case tar.TypeDir:
			if err := root.Mkdir(name, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err := root.Lstat(name)
			if err != nil || !info.IsDir() {
				return errors.New("snapshot directory conflicts with existing file")
			}
		case tar.TypeReg, tar.TypeRegA:
			file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(h.Mode)&0o777)
			if err != nil {
				return err
			}
			_, err = io.CopyN(file, tr, h.Size)
			if err := errors.Join(err, file.Close()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			// Preserve Git symlink text. Parent checks never traverse a link
			// while extracting; runtime reads use the worker's mount namespace.
			if err := root.Symlink(h.Linkname, name); err != nil {
				return err
			}
		default:
			return errors.New("snapshot contains a hardlink or special file")
		}
	}
}

func makeParents(root *os.Root, name string) error {
	parent := path.Dir(name)
	if parent == "." {
		return nil
	}
	current := ""
	for _, part := range strings.Split(parent, "/") {
		current = path.Join(current, part)
		if err := root.Mkdir(current, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := root.Lstat(current)
		if err != nil || !info.IsDir() {
			return errors.New("snapshot entry has a non-directory parent")
		}
	}
	return nil
}
