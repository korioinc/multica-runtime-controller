package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"golang.org/x/sys/unix"
)

type processIdentity struct {
	pid, parent, threads int
	start                uint64
	state                byte
}

// A live root is a kernel identity, never a name, environment marker or PID list.
type processRoot struct {
	identity processIdentity
	fd       int
	close    sync.Once
}

func readProcess(pid int) (processIdentity, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return processIdentity{}, err
	}
	end := strings.LastIndexByte(string(raw), ')')
	if end < 0 {
		return processIdentity{}, errors.New("process identity unavailable")
	}
	fields := strings.Fields(string(raw[end+1:]))
	if len(fields) < 20 || len(fields[0]) != 1 {
		return processIdentity{}, errors.New("process identity incomplete")
	}
	parent, parentErr := strconv.Atoi(fields[1])
	threads, threadErr := strconv.Atoi(fields[17])
	start, startErr := strconv.ParseUint(fields[19], 10, 64)
	if err := errors.Join(parentErr, threadErr, startErr); err != nil {
		return processIdentity{}, err
	}
	return processIdentity{pid: pid, parent: parent, threads: threads, start: start, state: fields[0][0]}, nil
}

func pinProcess(pid int) (*processRoot, error) {
	identity, err := readProcess(pid)
	if err != nil {
		return nil, err
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil && !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) {
		return nil, err
	}
	if err != nil {
		fd = -1
	} else {
		unix.CloseOnExec(fd)
	}
	root := &processRoot{identity: identity, fd: fd}
	if !root.alive() {
		root.Close()
		return nil, errors.New("process identity changed during admission")
	}
	return root, nil
}

func (r *processRoot) Close() {
	if r != nil {
		r.close.Do(func() {
			if r.fd >= 0 {
				_ = unix.Close(r.fd)
			}
		})
	}
}

func (r *processRoot) alive() bool {
	if r == nil {
		return false
	}
	if r.fd >= 0 {
		fds := []unix.PollFd{{Fd: int32(r.fd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, 0)
		for retry := 0; retry < 3 && errors.Is(err, unix.EINTR); retry++ {
			n, err = unix.Poll(fds, 0)
		}
		if err != nil || n != 0 {
			return false
		}
	}
	current, err := readProcess(r.identity.pid)
	return err == nil && current.start == r.identity.start && (current.state != 'Z' || current.threads > 1)
}

var errAncestryChanged = errors.New("process ancestry changed during observation")

// An opened proc entry can return ESRCH when its process exits before reading.
func processGone(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, unix.ESRCH)
}

func beneath(pid int, root *processRoot) (bool, error) {
	if !root.alive() {
		return false, diagnostics.Wrap("worker_desktop_owner_lost", errors.New("process owner unavailable"))
	}
	// Reparenting is atomic in the kernel but separate proc reads are not a snapshot.
	for retry := 0; retry < 3; retry++ {
		member, err := traceAncestry(pid, root.identity)
		if !errors.Is(err, errAncestryChanged) {
			return member, err
		}
	}
	return false, errAncestryChanged
}

func traceAncestry(pid int, root processIdentity) (bool, error) {
	for hops := 0; pid > 1 && hops < 4096; hops++ {
		child, err := readProcess(pid)
		if processGone(err) {
			return false, errAncestryChanged
		}
		if err != nil {
			return false, err
		}
		if child.pid == root.pid {
			return child.start == root.start, nil
		}
		if child.parent <= 1 {
			return false, nil
		}
		parent, err := readProcess(child.parent)
		if processGone(err) {
			return false, errAncestryChanged
		}
		if err != nil {
			return false, err
		}
		if parent.start > child.start {
			return false, errAncestryChanged
		}
		fresh, err := readProcess(child.pid)
		if err != nil || fresh.start != child.start || fresh.parent != child.parent {
			return false, errAncestryChanged
		}
		pid = parent.pid
	}
	if pid > 1 {
		return false, errors.New("process ancestry exceeds observation limit")
	}
	return false, nil
}

func processList() ([]processIdentity, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	return observeProcessEntries(entries)
}

// A vanished parent can leave a child born after enumeration. Such a batch
// cannot certify task settlement; the caller must collect a fresh one.
func observeProcessEntries(entries []os.DirEntry) ([]processIdentity, error) {
	var processes []processIdentity
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}
		identity, err := readProcess(pid)
		if processGone(err) {
			return nil, errAncestryChanged
		}
		if err != nil {
			return nil, err
		}
		// A zombie leader can still own living threads. Observe the thread group
		// before treating it as reapable or declaring task-process termination.
		threads, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
		if processGone(err) {
			return nil, errAncestryChanged
		}
		if err != nil {
			return nil, err
		}
		identity.threads = len(threads)
		processes = append(processes, identity)
	}
	return processes, nil
}

func signalProcess(identity processIdentity, signal syscall.Signal) error {
	current, err := readProcess(identity.pid)
	if processGone(err) || err == nil && current.start != identity.start {
		return nil
	}
	if err != nil {
		return err
	}
	fd, err := unix.PidfdOpen(identity.pid, 0)
	if err == nil {
		defer unix.Close(fd)
		current, err = readProcess(identity.pid)
		if processGone(err) || err == nil && current.start != identity.start {
			return nil
		}
		if err == nil {
			err = unix.PidfdSendSignal(fd, unix.Signal(signal), nil, 0)
		}
	} else if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
		err = syscall.Kill(identity.pid, signal)
	}
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	return err
}

func reapUnmanaged(processes []processIdentity, runner *providerRunner, desktop *residentDesktop) error {
	for _, process := range processes {
		if process.parent != 1 || process.state != 'Z' || process.threads != 1 {
			continue
		}
		if runner != nil && runner.command.Process.Pid == process.pid || desktop != nil && desktop.command.Process.Pid == process.pid {
			// Even after exit, Cmd.Wait owns this child until its barrier closes.
			continue
		}
		var status unix.WaitStatus
		if _, err := unix.Wait4(process.pid, &status, unix.WNOHANG, nil); err != nil && !errors.Is(err, unix.ECHILD) && !errors.Is(err, unix.EINTR) {
			return err
		}
	}
	return nil
}

func stopTaskProcesses(ctx context.Context, grace time.Duration, runner *providerRunner, desktop *residentDesktop) error {
	if os.Getpid() != 1 {
		return errors.New("task fencing requires PID 1")
	}
	if runner != nil {
		runner.Cancel()
	}
	deadline := time.Now().Add(grace)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !desktop.live() {
			return diagnostics.Wrap("worker_desktop_owner_lost", errors.New("resident process owner unavailable"))
		}
		processes, err := processList()
		uncertain := errors.Is(err, errAncestryChanged)
		if err != nil && !uncertain {
			return err
		}
		remaining := 0
		for _, process := range processes {
			resident, err := beneath(process.pid, desktop.root)
			if errors.Is(err, errAncestryChanged) {
				uncertain = true
				continue
			}
			if err != nil {
				return err
			}
			if resident {
				continue
			}
			remaining++
			signal := syscall.SIGTERM
			if !time.Now().Before(deadline) {
				// Freeze only outsiders; accepted application activity keeps running.
				if err := signalProcess(process, syscall.SIGSTOP); err != nil {
					return err
				}
				signal = syscall.SIGKILL
			}
			if err := signalProcess(process, signal); err != nil {
				return err
			}
		}
		if err := reapUnmanaged(processes, runner, desktop); err != nil {
			return err
		}
		if remaining == 0 && !uncertain {
			if runner != nil && !runner.Reaped() {
				select {
				case <-runner.Done:
					if !runner.Reaped() {
						return diagnostics.Wrap("worker_runner_reaping_unproven", errors.New("runner wait ownership unavailable"))
					}
					return nil
				default:
					remaining++
				}
			} else {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
