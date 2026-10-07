//go:build !linux

package processgroup

import (
	"errors"
	"syscall"
)

func stopped(pgid int) (bool, error) {
	err := syscall.Kill(-pgid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, err
}
