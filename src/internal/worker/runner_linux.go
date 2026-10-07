package worker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/multica-ai/multica/server/pkg/agent"
	"golang.org/x/sys/unix"
)

type providerRunner struct {
	Session  *agent.Session
	Done     <-chan struct{}
	command  *exec.Cmd
	conn     *net.UnixConn
	closed   chan struct{}
	waitErr  error // Published with command.ProcessState before Done closes.
	root     *processRoot
	received chan struct{}
	cancel   sync.Once
	close    sync.Once
}

// Cancel lets internal init forward SIGTERM to the SDK runner. PID 1 retains the final
// namespace-wide kill and waits for Done before using its generic child reaper.
func (r *providerRunner) Cancel() {
	r.cancel.Do(func() { _ = r.command.Process.Signal(syscall.SIGTERM) })
}

func (r *providerRunner) Close() {
	r.close.Do(func() {
		close(r.closed)
		if r.conn != nil {
			_ = r.conn.Close()
		}
	})
	if r.received != nil {
		<-r.received
	}
	r.root.Close()
}

func (r *providerRunner) Reaped() bool {
	select {
	case <-r.Done:
		return r.command.ProcessState != nil
	default:
		return false
	}
}

func startRunner(ctx context.Context, request runnerRequest, desktop *residentDesktop) (_ *providerRunner, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp(wire.ControlRoot, "runner-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	socketPath := filepath.Join(directory, "provider.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	command := exec.Command(executable, "init", "worker", "run", socketPath)
	// Provider-controlled LD_*, GODEBUG, and other launch variables must
	// never configure these trusted processes. The provider environment is IPC input.
	command.Env = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=" + wire.Home, "LANG=C.UTF-8"}
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	runner := &providerRunner{command: command, Done: done, closed: make(chan struct{})}
	go func() {
		runner.waitErr = command.Wait()
		close(done)
	}()
	runner.root, err = pinProcess(command.Process.Pid)
	if err != nil {
		runner.Cancel()
		return runner, err
	}
	if desktop != nil {
		if err := desktop.beginTurn(runner); err != nil {
			runner.Cancel()
			return runner, err
		}
	}
	stopCancellation := context.AfterFunc(ctx, runner.Cancel)
	go func() {
		<-done
		stopCancellation()
		_ = listener.Close()
		reason, code, terminatedBy := "worker_runner_exited", -1, 0
		if runner.waitErr != nil {
			reason = "worker_runner_exit_failed"
		}
		if state := command.ProcessState; state != nil {
			code = state.ExitCode()
			if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				terminatedBy = int(status.Signal())
			}
		} else {
			reason = "worker_runner_reaping_unproven"
		}
		slog.Info("worker runner exited", "reason", reason, "role", "worker run",
			"pid", command.Process.Pid, "exit_code", code, "signal", terminatedBy)
	}()
	defer func() {
		if resultErr != nil {
			runner.Cancel()
			runner.Close()
		}
	}()

	startupCtx, cancelStartup := context.WithTimeout(ctx, time.Minute)
	defer cancelStartup()
	stopAccept := context.AfterFunc(startupCtx, func() { _ = listener.Close() })
	defer stopAccept()
	deadline, _ := startupCtx.Deadline()
	_ = listener.SetDeadline(deadline)
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return runner, diagnostics.Wrap("worker_runner_handshake_failed", err)
		}
		if err := authenticateRunner(conn, runner); err != nil {
			_ = conn.Close()
			continue
		}
		runner.conn = conn
		break
	}
	// No provider has started yet. Remove the rendezvous before releasing its
	// input; internal init never owns the connected result socket.
	_ = listener.Close()
	stopConnection := context.AfterFunc(startupCtx, func() { _ = runner.conn.Close() })
	defer stopConnection()
	if err := startupCtx.Err(); err != nil {
		return runner, err
	}
	_ = runner.conn.SetDeadline(deadline)
	if err := writeRunnerPacket(runner.conn, request, runnerInputLimit); err != nil {
		return runner, diagnostics.Wrap("worker_runner_input_failed", err)
	}
	var ready runnerFrame
	if err := readRunnerPacket(runner.conn, &ready, runnerFrameLimit); err != nil {
		return runner, diagnostics.Wrap("worker_runner_handshake_failed", err)
	}
	if ready.Kind != "ready" || ready.Message != nil || ready.Result != nil {
		return runner, errors.New("provider runner did not become ready")
	}
	if !stopConnection() {
		return runner, startupCtx.Err()
	}
	if err := startupCtx.Err(); err != nil {
		return runner, err
	}
	if err := runner.conn.SetDeadline(time.Time{}); err != nil {
		return runner, err
	}
	messages := make(chan agent.Message)
	results := make(chan agent.Result, 1)
	runner.Session = &agent.Session{Messages: messages, Result: results}
	runner.received = make(chan struct{})
	go runner.receive(messages, results)
	return runner, nil
}

func authenticateRunner(conn *net.UnixConn, runner *providerRunner) error {
	return authenticateChild(conn, runner.command.Process.Pid, runner.live)
}

func (r *providerRunner) live() bool {
	select {
	case <-r.Done:
		return false
	default:
		return r.command.Process.Signal(syscall.Signal(0)) == nil
	}
}

func (r *providerRunner) receive(messages chan agent.Message, results chan agent.Result) {
	defer close(r.received)
	defer close(results)
	defer close(messages)
	for {
		var frame runnerFrame
		if err := readRunnerPacket(r.conn, &frame, runnerFrameLimit); err != nil {
			return
		}
		switch {
		case frame.Kind == "message" && frame.Message != nil && frame.Result == nil:
			select {
			case messages <- *frame.Message:
			case <-r.closed:
				return
			}
		case frame.Kind == "result" && frame.Result != nil && frame.Message == nil:
			results <- *frame.Result
			return
		default:
			return
		}
	}
}

// RunProvider runs only the unchanged official SDK. It must not be a subreaper:
// exited SDK descendants belong to its parent init, not to this Go process.
func RunProvider(ctx context.Context, socketPath string) error {
	if os.Getpid() == 1 {
		return errors.New("provider runner requires a supervisor")
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0); err != nil {
		return err
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return err
	}
	defer connection.Close()
	conn := connection.(*net.UnixConn)
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	if err := raw.Control(func(fd uintptr) { unix.CloseOnExec(int(fd)) }); err != nil {
		return err
	}
	_ = conn.SetDeadline(time.Now().Add(time.Minute))
	stopInput := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopInput()
	var request runnerRequest
	if err := readRunnerPacket(conn, &request, runnerInputLimit); err != nil {
		return diagnostics.Wrap("worker_runner_input_failed", err)
	}
	if !stopInput() {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return err
	}
	providerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	backend, err := agent.New(request.Provider, agent.Config{ExecutablePath: request.ExecutablePath,
		CLIVersion: request.CLIVersion, Env: request.Environment,
		Logger: providerSDKLogger(request.TaskID, request.RuntimeID), TaskID: request.TaskID,
		RuntimeID: request.RuntimeID, DaemonVersion: request.DaemonVersion, CodexVersion: request.CodexVersion, BuiltinRuntime: true})
	var session *agent.Session
	if err == nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		session, err = backend.Execute(providerCtx, request.Prompt, request.Options)
	}
	if err != nil {
		// Preserve Serve's existing SDK startup-failure outcome. IPC failures
		// never manufacture a result.
		if writeErr := writeRunnerPacket(conn, runnerFrame{Kind: "ready"}, runnerFrameLimit); writeErr != nil {
			return writeErr
		}
		return writeRunnerPacket(conn, runnerFrame{Kind: "result", Result: &agent.Result{Status: "failed", Error: "provider startup failed"}}, runnerFrameLimit)
	}
	if session == nil || session.Result == nil {
		return errors.New("provider returned no execution session")
	}
	if session.TerminalObserved != nil || session.ToolActivity != nil || session.InterruptBackgroundTools != nil {
		return diagnostics.Wrap("worker_runner_callbacks_unsupported", errors.New("provider lifecycle callbacks require transport support"))
	}
	if err := writeRunnerPacket(conn, runnerFrame{Kind: "ready"}, runnerFrameLimit); err != nil {
		return err
	}
	messages, results := session.Messages, session.Result
	var final *agent.Result
	for messages != nil || final == nil {
		select {
		case message, ok := <-messages:
			if !ok {
				messages = nil
				continue
			}
			if err := writeRunnerPacket(conn, runnerFrame{Kind: "message", Message: &message}, runnerFrameLimit); err != nil {
				return err
			}
		case result, ok := <-results:
			if !ok {
				return errors.New("provider ended without a result")
			}
			final = &result
			results = nil
		}
	}
	return writeRunnerPacket(conn, runnerFrame{Kind: "result", Result: final}, runnerFrameLimit)
}
