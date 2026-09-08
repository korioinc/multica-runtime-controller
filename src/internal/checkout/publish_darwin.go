package checkout

import "golang.org/x/sys/unix"

// PublishDirectory publishes a prepared sibling without replacing its target.
func PublishDirectory(directory int, source, target string) error {
	return unix.RenameatxNp(directory, source, directory, target, unix.RENAME_EXCL)
}
