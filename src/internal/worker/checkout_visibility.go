package worker

import (
	"os"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

// A change made through NFS refreshes the server's cached directory metadata
// after the controller publishes through its local PVC mount. This is best
// effort; the caller retains its bounded polling fallback.
func refreshCheckoutDirectory(task *os.Root) {
	parent, err := task.OpenRoot("workdir")
	if err != nil {
		return
	}
	defer parent.Close()
	parentFD, err := parent.Open(".")
	if err != nil {
		return
	}
	defer parentFD.Close()
	name := ".checkout-observe-" + uuid.NewString()
	if err := parent.Mkdir(name, 0700); err != nil {
		return
	}
	directory, err := parent.OpenRoot(name)
	if err != nil {
		return
	}
	defer directory.Close()
	created, err := directory.Stat(".")
	if err != nil {
		return
	}
	current, err := parent.Lstat(name)
	if err != nil || !os.SameFile(created, current) {
		return
	}
	// Directory-only unlink rejects replacement files and nonempty directories.
	_ = unix.Unlinkat(int(parentFD.Fd()), name, unix.AT_REMOVEDIR)
}
