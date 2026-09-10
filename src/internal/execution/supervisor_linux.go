package execution

import (
	"context"
	"errors"
	"os"
	"strconv"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
)

// This supervisor is the sole wait owner in PID 1. The gateway runs as its
// child, so its Git subprocess waits cannot race the orphan reaper here.
func superviseWorker(ctx context.Context, grace time.Duration) error {
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return err
	}
	child, err := os.StartProcess(wire.ControllerRoot+"/runtime", []string{"runtime", "worker", "proxy"}, &os.ProcAttr{Env: os.Environ(), Files: []*os.File{nil, os.Stdout, os.Stderr}})
	if err != nil {
		return err
	}
	defer child.Release()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var stopping time.Time
	var result error
	for {
		for {
			var status unix.WaitStatus
			pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
			if err != nil && err != unix.ECHILD && err != unix.EINTR {
				return err
			}
			if pid <= 0 {
				break
			}
			if pid == child.Pid && stopping.IsZero() {
				stopping = time.Now()
				result = errors.New("worker gateway stopped")
				signalContainer(syscall.SIGTERM)
			}
		}
		if !stopping.IsZero() {
			if time.Since(stopping) >= grace {
				signalContainer(syscall.SIGKILL)
			}
			if !containerChildren() {
				return result
			}
		}
		select {
		case <-ctx.Done():
			if stopping.IsZero() {
				stopping = time.Now()
				signalContainer(syscall.SIGTERM)
			}
		case <-tick.C:
		}
	}
}
func containerPIDs() []int {
	entries, _ := os.ReadDir("/proc")
	var result []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err == nil && pid > 1 {
			result = append(result, pid)
		}
	}
	return result
}
func signalContainer(signal syscall.Signal) {
	for _, pid := range containerPIDs() {
		_ = syscall.Kill(pid, signal)
	}
}
func containerChildren() bool { return len(containerPIDs()) != 0 }
