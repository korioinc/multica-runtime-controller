package migration

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"golang.org/x/sys/unix"
)

type fileOwner struct {
	uid, gid int
	device   int64
}

func ownership(info os.FileInfo) (fileOwner, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileOwner{}, errors.New("unsupported authority filesystem metadata")
	}
	return fileOwner{int(stat.Uid), int(stat.Gid), int64(stat.Dev)}, nil
}
func realDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || canonical != path {
		return errors.New("authority directory must be canonical without symlinks")
	}
	return nil
}

// Existing managed authority may not be redirected or foreign-owned. Missing
// stale/retired data remains missing; migration neither repairs nor adopts it.
func ownedExisting(path string, directory bool, uid int) (resultErr error) {
	defer func() { resultErr = diagnostics.AtPath("migration_managed_path_invalid", path, resultErr) }()
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if directory && !info.IsDir() || !directory && !info.Mode().IsRegular() {
		return fmt.Errorf("managed migration path has an invalid type: %s", path)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return fmt.Errorf("managed migration path must be canonical: %s", path)
	}
	actual, err := ownership(info)
	if err != nil || actual.uid != uid {
		return fmt.Errorf("managed migration path has a foreign owner: %s", path)
	}
	return nil
}
func openExisting(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, errors.New("authority entry must be a regular file")
	}
	named, err := os.Lstat(path)
	if err != nil || !os.SameFile(info, named) {
		file.Close()
		return nil, errors.New("authority entry changed during open")
	}
	return file, nil
}
func readExisting(path string) ([]byte, os.FileInfo, error) {
	file, err := openExisting(path, unix.O_RDONLY)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	data, err := io.ReadAll(file)
	return data, info, err
}

type heldLock struct {
	file *os.File
	path string
}

func acquire(path string, uid int) (*heldLock, error) {
	file, err := openExisting(path, unix.O_RDWR)
	if err != nil {
		return nil, diagnostics.AtPath("migration_lock_invalid", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	owner, err := ownership(info)
	if err != nil || owner.uid != uid {
		file.Close()
		return nil, errors.New("migration lock has a foreign filesystem owner")
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		return nil, diagnostics.AtPath("migration_lock_busy", path, err)
	}
	lock := &heldLock{file, path}
	if err := lock.unchanged(); err != nil {
		lock.close()
		return nil, err
	}
	return lock, nil
}
func (l *heldLock) unchanged() error {
	actual, err := l.file.Stat()
	if err != nil {
		return err
	}
	named, err := os.Lstat(l.path)
	if err != nil || !os.SameFile(actual, named) {
		return errors.New("migration lock inode was replaced")
	}
	return nil
}
func (l *heldLock) close() { _ = unix.Flock(int(l.file.Fd()), unix.LOCK_UN); _ = l.file.Close() }
func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func privateDirectory(path string, owner fileOwner) error {
	err := os.Mkdir(path, 0700)
	created := err == nil
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := realDirectory(path); err != nil {
		return err
	}
	if created {
		if err := os.Chown(path, owner.uid, owner.gid); err != nil {
			return err
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	actual, err := ownership(info)
	if err != nil || actual.uid != owner.uid || info.Mode().Perm()&0077 != 0 {
		return errors.New("migration backup directory is not private and owned")
	}
	// An earlier invocation may have stopped between mkdir and parent fsync.
	// Existing names must cross the same durability boundary as new ones.
	if err := syncDirectory(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}
func writeCandidate(directory string, data []byte, owner fileOwner) (path string, err error) {
	file, err := os.CreateTemp(directory, ".migrate-v2-")
	if err != nil {
		return "", err
	}
	path = file.Name()
	defer func() {
		_ = file.Close()
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return path, err
	}
	actual, err := ownership(info)
	if err != nil {
		return path, err
	}
	if actual.device != owner.device {
		return path, errors.New("migration candidate is not on the source filesystem")
	}
	if err = file.Chown(owner.uid, owner.gid); err != nil {
		return path, err
	}
	if err = file.Chmod(0600); err != nil {
		return path, err
	}
	if _, err = file.Write(data); err != nil {
		return path, err
	}
	if err = file.Sync(); err != nil {
		return path, err
	}
	err = file.Close()
	return path, err
}
func preserveBackup(stateDir, backupDir string, source []byte, owner fileOwner) error {
	for _, path := range []string{filepath.Join(stateDir, "migrations"), filepath.Join(stateDir, "migrations/v1-to-v2"), backupDir} {
		if err := privateDirectory(path, owner); err != nil {
			return err
		}
	}
	backup := filepath.Join(backupDir, "registry.v1.json")
	if _, err := os.Lstat(backup); err == nil {
		if err := verifyBackup(backup, source, owner); err != nil {
			return err
		}
		return syncDirectory(backupDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Publish a complete fsynced backup without overwriting an existing name.
	pending, err := writeCandidate(backupDir, source, owner)
	if err != nil {
		return err
	}
	defer os.Remove(pending)
	if err := os.Link(pending, backup); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := verifyBackup(backup, source, owner); err != nil {
		return err
	}
	return syncDirectory(backupDir)
}
func verifyBackup(path string, source []byte, owner fileOwner) error {
	actual, info, err := readExisting(path)
	if err != nil {
		return err
	}
	metadata, err := ownership(info)
	if err != nil || metadata.uid != owner.uid || metadata.gid != owner.gid || info.Mode().Perm()&0077 != 0 || !bytes.Equal(actual, source) {
		return errors.New("existing migration backup does not match the private original bytes")
	}
	return nil
}
