package checkout

import "golang.org/x/sys/unix"

func publishDirectory(directory int, source, target string) error {
	return unix.RenameatxNp(directory, source, directory, target, unix.RENAME_EXCL)
}
