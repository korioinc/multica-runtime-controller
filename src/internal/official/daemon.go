package official

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os/exec"
	"syscall"
	"time"
)

type DaemonProcess struct {
	Path           string
	Args, Env      []string
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}

// RunDaemon couples the fresh official process and its observation bridge.
// Neither survives failure of the other. Diagnostics never enter its streams.
func RunDaemon(ctx context.Context, listener net.Listener, handler http.Handler, process DaemonProcess) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	defer server.Close()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	command := exec.CommandContext(ctx, process.Path, process.Args...)
	command.Env = process.Env
	command.Stdin = process.Stdin
	command.Stdout = process.Stdout
	command.Stderr = process.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return syscall.Kill(-command.Process.Pid, syscall.SIGTERM) }
	command.WaitDelay = 10 * time.Second
	if err := command.Start(); err != nil {
		return err
	}
	defer syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	select {
	case err := <-finished:
		if err == nil && ctx.Err() == nil {
			return errors.New("official daemon exited while controller was active")
		}
		return err
	case err := <-serving:
		cancel()
		<-finished
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
