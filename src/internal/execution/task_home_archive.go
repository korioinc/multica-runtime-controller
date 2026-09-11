package execution

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func publishTaskHomeArchive(artifacts *os.Root, stage string, identity taskHomeIdentity) (digest string, returnErr error) {
	finish := diagnostics.StartPhase("home_archive", diagnostics.TaskAttributes(identity.TaskID, identity.AttemptID)...)
	defer func() { finish(returnErr) }()
	home, err := artifacts.OpenRoot(stage)
	if err != nil {
		return "", err
	}
	defer home.Close()
	temporary := ".archive-" + uuid.NewString()
	file, err := artifacts.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer artifacts.Remove(temporary)
	hash := sha256.New()
	err = writeTaskHomeArchive(io.MultiWriter(file, hash), home, identity)
	if err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return "", err
	}
	if err := artifacts.Rename(temporary, identity.AttemptID+".tar"); err != nil {
		return "", err
	}
	if err := syncHomeDirectory(artifacts, "."); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func writeTaskHomeArchive(output io.Writer, home *os.Root, identity taskHomeIdentity) error {
	metadata, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	archive := tar.NewWriter(output)
	if err := archive.WriteHeader(&tar.Header{Name: taskHomeIdentityFile, Typeflag: tar.TypeReg, Mode: 0600, Size: int64(len(metadata))}); err != nil {
		return err
	}
	if _, err := archive.Write(metadata); err != nil {
		return err
	}
	err = fs.WalkDir(home.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		return writeTaskHomeEntry(archive, home, path)
	})
	return errors.Join(err, archive.Close())
}

func writeTaskHomeEntry(archive *tar.Writer, home *os.Root, path string) error {
	info, err := home.Lstat(path)
	if err != nil {
		return err
	}
	if path != "." && !validTaskHomePath(path, info.IsDir()) {
		return errors.New("task HOME changed to a reserved path while archiving")
	}
	header := &tar.Header{Name: filepath.ToSlash(filepath.Join("home", path)), Mode: int64(info.Mode().Perm())}
	switch {
	case info.IsDir():
		header.Typeflag = tar.TypeDir
	case info.Mode().IsRegular():
		header.Typeflag, header.Size = tar.TypeReg, info.Size()
	case info.Mode()&os.ModeSymlink != 0 && npmCommandPath(path):
		if err := runtimeimage.ValidateNPMCommandLink(filepath.Join(home.Name(), runtimeimage.PiNPMDirectory), filepath.Join(home.Name(), path)); err != nil {
			return err
		}
		header.Typeflag = tar.TypeSymlink
		header.Linkname, err = home.Readlink(path)
		if err != nil {
			return err
		}
	default:
		return errors.New("task HOME changed to an unapproved link or special file")
	}
	if err := archive.WriteHeader(header); err != nil || !info.Mode().IsRegular() {
		return err
	}
	input, err := home.Open(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(archive, input)
	return errors.Join(err, input.Close())
}

func syncHomeDirectory(root *os.Root, path string) error {
	dir, err := root.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
