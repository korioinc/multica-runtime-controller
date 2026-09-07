package checkout

import (
	"archive/tar"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
)

const archiveEnd = ".multica-archive-v1-complete"

// WriteArchive transports file bytes, never a shared object store or hardlink.
func WriteArchive(output io.Writer, directory string) error {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	tarball := tar.NewWriter(output)
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || name == "." {
			return err
		}
		if name == archiveEnd {
			return errors.New("reserved archive name")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&os.ModeSymlink != 0 {
			link, err = root.Readlink(name)
			if err != nil {
				return err
			}
		} else if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("unsupported archive object")
		}
		h, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		h.Name = name
		h.Mode &= 0777
		h.Uid = 0
		h.Gid = 0
		h.Uname = ""
		h.Gname = ""
		if err := tarball.WriteHeader(h); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := root.Open(name)
		if err != nil {
			return err
		}
		_, err = io.CopyN(tarball, f, info.Size())
		return errors.Join(err, f.Close())
	})
	if err != nil {
		return err
	}
	if err := tarball.WriteHeader(&tar.Header{Name: archiveEnd, Mode: 0600, Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	return tarball.Close()
}

func extractArchive(input io.Reader, root *os.Root) error {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("archive staging must be empty")
	}
	reader := tar.NewReader(input)
	seen := map[string]bool{}
	var size int64
	complete := false
	for {
		header, err := reader.Next()
		if err == io.EOF {
			if complete {
				return nil
			}
			return errors.New("archive transport ended before completion")
		}
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(header.Name, "/")
		if complete || !fs.ValidPath(name) || name == "." || seen[name] || len(seen) > 1_000_000 || header.Size < 0 || header.Size > 64<<30-size {
			return errors.New("invalid archive entry")
		}
		seen[name] = true
		size += header.Size
		if name == archiveEnd {
			if header.Typeflag != tar.TypeReg || header.Size != 0 {
				return errors.New("invalid archive trailer")
			}
			complete = true
			continue
		}
		if err := plainDirectories(root, path.Dir(name), 0755); err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := plainDirectories(root, name, 0755); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(header.Mode)&0777)
			if err != nil {
				return err
			}
			_, err = io.CopyN(f, reader, header.Size)
			if err := errors.Join(err, f.Close()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := root.Symlink(header.Linkname, name); err != nil {
				return err
			}
		default:
			return errors.New("archive hardlinks and device objects are unsupported")
		}
	}
}
func plainDirectories(root *os.Root, name string, mode os.FileMode) error {
	if name == "." {
		return nil
	}
	if !fs.ValidPath(name) {
		return errors.New("unconfined directory")
	}
	current := ""
	for _, part := range strings.Split(name, "/") {
		current = path.Join(current, part)
		if err := root.Mkdir(current, mode); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		st, err := root.Lstat(current)
		if err != nil || !st.IsDir() {
			return errors.New("directory contains symlink or other object")
		}
	}
	return nil
}

func Standalone(directory string) error {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	return standalone(root)
}
func standalone(root *os.Root) error {
	git, err := root.Lstat(".git")
	if err != nil || !git.IsDir() {
		return errors.New("official checkout must have its own Git directory")
	}
	for _, name := range []string{".git/commondir", ".git/objects/info/alternates", ".git/objects/info/http-alternates"} {
		if _, err := root.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			return errors.New("checkout depends on an external Git store")
		}
	}
	return nil
}
