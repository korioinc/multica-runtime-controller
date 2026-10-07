//go:build linux

package checkout

import "golang.org/x/sys/unix"

func renameCheckout(parent int, stage, name string) error {
	return unix.Renameat2(parent, stage, parent, name, unix.RENAME_NOREPLACE)
}
