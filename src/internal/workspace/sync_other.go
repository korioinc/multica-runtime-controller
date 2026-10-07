//go:build !linux

package workspace

import "errors"

func SyncTaskFilesystem(path string) error {
	return errors.New("task filesystem flush requires Linux syncfs")
}
