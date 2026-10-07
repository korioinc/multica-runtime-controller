//go:build darwin

package checkout

import "golang.org/x/sys/unix"

func renameCheckout(parent int, stage, name string) error {
	return unix.RenameatxNp(parent, stage, parent, name, unix.RENAME_EXCL)
}
