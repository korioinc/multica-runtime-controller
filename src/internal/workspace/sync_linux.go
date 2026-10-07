//go:build linux

package workspace

import "golang.org/x/sys/unix"

// SyncTaskFilesystem flushes the filesystem containing a validated task root.
// The caller must first prove its writers stopped. A controller-side flush
// cannot recover writes that an NFS client never sent to the server.
func SyncTaskFilesystem(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	return unix.Syncfs(fd)
}
