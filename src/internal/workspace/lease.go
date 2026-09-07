package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

var ErrStorageBusy = errors.New("worker storage is held by another execution")

func openLock(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, errors.New("authority lock must be a regular file")
	}
	return file, nil
}

// AcquireLease serializes execution, seeding, cleanup, recovery and retirement.
// A persistent inode is shared across processes; lock files are never removed.
func (s *Store) AcquireLease(storageID string) (func(), error) {
	if !canonicalUUID(storageID) {
		return nil, errors.New("invalid worker storage identity")
	}
	file, err := openLock(filepath.Join(s.options.Directory, "worker-"+storageID+".lock"))
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrStorageBusy
		}
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { _ = unix.Flock(int(file.Fd()), unix.LOCK_UN); _ = file.Close() }) }, nil
}
