//go:build linux

package initprocess

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	reapWindow       = time.Second
	fallbackInterval = time.Second
)

var controlSignals = []os.Signal{
	syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT,
	syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGPIPE, syscall.SIGALRM,
	syscall.SIGCONT, syscall.SIGTSTP, syscall.SIGWINCH, syscall.SIGIO,
	syscall.SIGPWR, syscall.SIGXCPU, syscall.SIGXFSZ, syscall.SIGVTALRM,
	syscall.SIGTTIN, syscall.SIGTTOU,
}

// Run starts only the corresponding role in this executable and returns its status.
func Run(args []string) int {
	return run(args, os.StartProcess)
}

func run(args []string, start func(string, []string, *os.ProcAttr) (*os.Process, error)) int {
	selected := role(args)
	if selected == "" {
		return failure(reasonInvalidArguments, "", 0, exitFailure)
	}
	executable, err := os.Executable()
	if err != nil {
		return failure(reasonExecutable, selected, 0, exitFailure)
	}
	// Separate channels prevent coalescing child-exit bursts from dropping TERM.
	childExits := make(chan os.Signal, 1)
	controls := make(chan os.Signal, len(controlSignals)*2)
	signal.Notify(childExits, syscall.SIGCHLD)
	signal.Notify(controls, controlSignals...)
	defer signal.Stop(childExits)
	defer signal.Stop(controls)
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return failure(reasonSubreaper, selected, 0, exitFailure)
	}
	process, err := start(executable, append([]string{executable}, args...), &os.ProcAttr{
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Sys:   &syscall.SysProcAttr{Setpgid: true},
	})
	if err != nil {
		code := exitFailure
		if errors.Is(err, syscall.ENOENT) {
			code = exitNotFound
		} else if errors.Is(err, syscall.EACCES) {
			code = exitCannotExecute
		}
		return failure(reasonSpawn, selected, 0, code)
	}
	// StartProcess has returned the primary PID before this sole waiter runs.
	return waitPrimary(selected, process, childExits, controls, unix.Wait4, process.Signal)
}

type waitCall func(int, *unix.WaitStatus, int, *unix.Rusage) (int, error)

type primary struct {
	pid      int
	status   unix.WaitStatus
	observed time.Time
}

// waitPrimary is the consuming-wait owner. Its two syscall boundaries also let
// process fixtures exercise lost ownership without changing production policy.
func waitPrimary(role string, process *os.Process, childExits, controls <-chan os.Signal, wait waitCall, send func(os.Signal) error) int {
	defer process.Release()
	current := primary{pid: process.Pid}
	tick := time.NewTicker(fallbackInterval)
	defer tick.Stop()
	drain := true
	for {
		if drain {
			var err error
			drain, err = current.drain(wait)
			if !current.observed.IsZero() {
				if err != nil {
					failure(reasonFinalDrain, role, current.pid, exitFailure)
				}
				return current.exit(role)
			}
			if err != nil {
				if errors.Is(err, unix.ECHILD) {
					return failure(reasonLostOwnership, role, current.pid, exitFailure)
				}
				cleanupPrimary(role, current.pid, childExits, wait, send)
				return failure(reasonWait, role, current.pid, exitFailure)
			}
		}
		// Give controls a turn before every subsequent drain, including a pass
		// that exhausted its budget during a continuous orphan-exit stream.
		select {
		case control := <-controls:
			if control == syscall.SIGTTIN || control == syscall.SIGTTOU {
				continue
			}
			if err := send(control); err != nil && !errors.Is(err, unix.ESRCH) && !errors.Is(err, os.ErrProcessDone) {
				cleanupPrimary(role, current.pid, childExits, wait, send)
				return failure(reasonSignal, role, current.pid, exitFailure)
			}
			drain = true
			continue
		default:
		}
		if drain {
			continue
		}
		select {
		case <-childExits:
			drain = true
		case <-tick.C:
			drain = true
		case control := <-controls:
			if control == syscall.SIGTTIN || control == syscall.SIGTTOU {
				continue
			}
			if err := send(control); err != nil && !errors.Is(err, unix.ESRCH) && !errors.Is(err, os.ErrProcessDone) {
				cleanupPrimary(role, current.pid, childExits, wait, send)
				return failure(reasonSignal, role, current.pid, exitFailure)
			}
			drain = true
		}
	}
}

func (p *primary) drain(wait waitCall) (bool, error) {
	deadline := time.Now().Add(reapWindow)
	for {
		if !p.observed.IsZero() {
			deadline = p.observed.Add(reapWindow)
		}
		if !time.Now().Before(deadline) {
			return p.observed.IsZero(), nil
		}
		var status unix.WaitStatus
		pid, err := wait(-1, &status, unix.WNOHANG, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.ECHILD) && !p.observed.IsZero() {
			return false, nil
		}
		if err != nil || pid == 0 {
			return false, err
		}
		if pid == p.pid && p.observed.IsZero() {
			p.status = status
			p.observed = time.Now()
		}
	}
}

func (p *primary) exit(role string) int {
	code := p.status.ExitStatus()
	signal := 0
	if p.status.Signaled() {
		signal = int(p.status.Signal())
		code = signalExitOffset + signal
	}
	slog.Info("runtime init child exited", "reason", reasonPrimaryExit, "role", role, "pid", p.pid, "exit_code", code, "signal", signal)
	return code
}

func cleanupPrimary(role string, pid int, childExits <-chan os.Signal, wait waitCall, send func(os.Signal) error) {
	// The caller still owns an unreaped primary. Never signal after ECHILD.
	deadline := time.Now().Add(reapWindow)
	if err := send(syscall.SIGKILL); err != nil && !errors.Is(err, unix.ESRCH) && !errors.Is(err, os.ErrProcessDone) {
		failure(reasonCleanup, role, pid, exitFailure)
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for time.Now().Before(deadline) {
		var status unix.WaitStatus
		waited, err := wait(pid, &status, unix.WNOHANG, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if waited == pid || errors.Is(err, unix.ECHILD) {
			return
		}
		if err != nil {
			failure(reasonCleanup, role, pid, exitFailure)
			return
		}
		select {
		case <-childExits:
		case <-timer.C:
			return
		}
	}
}
