// Package processgroup proves that a preparation command can no longer write.
package processgroup

import (
	"errors"
	"syscall"
	"time"
)

// Stop kills a command's dedicated process group and waits for its writers.
// Reaping belongs to the process supervisor, not this command's exec.Cmd.Wait.
func Stop(pgid int) error {
	if pgid <= 1 {
		return errors.New("invalid preparation process group")
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		stopped, err := stopped(pgid)
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("preparation process group still has unproven writers")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
