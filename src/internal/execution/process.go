package execution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type ProcessStreams struct {
	Stdin          *os.File
	Stdout, Stderr io.Writer
}
type ProcessResult struct {
	Code   int
	Exited bool
	Err    error
}

var ErrTransport = errors.New("provider input transport failed")
var ErrProviderStart = errors.New("provider process could not start")

// RunProcess owns its input descriptors and process group. A provider that
// finishes first cannot be held open by an upstream writer that never sends EOF.
func RunProcess(ctx context.Context, executable string, args, env []string, directory string, grace time.Duration, streams ProcessStreams) (result ProcessResult) {
	result.Code = 1
	var input, childInput, writer *os.File
	var pumped chan error
	defer func() {
		for _, f := range []*os.File{input, childInput, writer} {
			if f != nil {
				_ = f.Close()
			}
		}
		if pumped != nil {
			if inputErr := <-pumped; inputErr != nil {
				result.Err = errors.Join(result.Err, fmt.Errorf("%w: %w", ErrTransport, inputErr))
			}
		}
	}()
	if streams.Stdin != nil {
		var err error
		input, err = interruptibleInput(streams.Stdin)
		if err != nil {
			result.Err = err
			return
		}
		childInput, writer, err = os.Pipe()
		if err != nil {
			result.Err = err
			return
		}
	}
	command := exec.Command(executable, args...)
	command.Env = env
	command.Dir = directory
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Stdout = streams.Stdout
	command.Stderr = streams.Stderr
	command.WaitDelay = grace
	if childInput != nil {
		command.Stdin = childInput
	}
	if ctx.Err() != nil {
		result.Err = ctx.Err()
		return
	}
	if err := command.Start(); err != nil {
		result.Err = fmt.Errorf("%w: %w", ErrProviderStart, err)
		return
	}
	pid := command.Process.Pid
	var stopBy time.Time
	terminationSent := false
	defer func() {
		if stopBy.IsZero() {
			stopBy = time.Now().Add(grace)
		}
		if !terminationSent {
			_ = syscall.Kill(-pid, syscall.SIGTERM)
		}
		for syscall.Kill(-pid, 0) == nil && time.Now().Before(stopBy) {
			time.Sleep(10 * time.Millisecond)
		}
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}()
	if childInput != nil {
		_ = childInput.Close()
		pumped = make(chan error, 1)
		go func() {
			observed := &inputReader{file: input}
			_, _ = io.Copy(writer, observed)
			_ = writer.Close()
			pumped <- observed.failure
		}()
	}
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	select {
	case result.Err = <-finished:
	case <-ctx.Done():
		stopBy = time.Now().Add(grace)
		terminationSent = true
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		timer := time.NewTimer(grace)
		select {
		case result.Err = <-finished:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			result.Err = <-finished
		}
		if result.Err == nil {
			result.Err = ctx.Err()
		}
	}
	if command.ProcessState != nil {
		result.Exited = true
		result.Code = command.ProcessState.ExitCode()
		if status, ok := command.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			result.Code = 128 + int(status.Signal())
		}
		if result.Code < 0 {
			result.Code = 1
		}
	}
	if ctx.Err() != nil && result.Code == 0 {
		result.Code = 1
	}
	return
}

type inputReader struct {
	file    *os.File
	failure error
}

func (r *inputReader) Read(buffer []byte) (int, error) {
	n, err := r.file.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) {
		r.failure = err
	}
	return n, err
}

func interruptibleInput(source *os.File) (*os.File, error) {
	defer source.Close()
	info, err := source.Stat()
	if err != nil {
		return nil, err
	}
	connection, err := source.SyscallConn()
	if err != nil {
		return nil, err
	}
	fd := -1
	var duplicateErr error
	if err := connection.Control(func(raw uintptr) { fd, duplicateErr = unix.FcntlInt(raw, unix.F_DUPFD_CLOEXEC, 0) }); err != nil {
		return nil, err
	}
	if duplicateErr != nil {
		return nil, duplicateErr
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "provider-input")
	null, _ := os.Stat(os.DevNull)
	if err := file.SetReadDeadline(time.Time{}); err != nil && !info.Mode().IsRegular() && (null == nil || !os.SameFile(info, null)) {
		_ = file.Close()
		return nil, fmt.Errorf("input cannot be interrupted: %w", err)
	}
	return file, nil
}

type ExitError struct{ Code int }

func (e *ExitError) Error() string { return fmt.Sprintf("provider exited with code %d", e.Code) }
func ResultError(result ProcessResult) error {
	if result.Exited && result.Code != 0 {
		return &ExitError{Code: result.Code}
	}
	return result.Err
}
func ErrorCode(err error) int {
	if err == nil {
		return 0
	}
	var exit *ExitError
	if errors.As(err, &exit) {
		return exit.Code
	}
	var process *exec.ExitError
	if errors.As(err, &process) && process.ExitCode() > 0 {
		return process.ExitCode()
	}
	return 1
}
