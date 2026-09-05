package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/claimproxy"
	"github.com/korioinc/multica-runtime-controller/internal/intercept"
	"github.com/korioinc/multica-runtime-controller/internal/taskstate"
)

// The release executable remains untouched. The wrapper owns only its lifecycle
// and the public claim boundary, alongside the existing provider shims.
func runOfficialDaemon() error {
	if err := intercept.PersistLauncherConfig(); err != nil {
		return err
	}
	launcherConfig, _, err := intercept.LoadConfig()
	if err != nil {
		return err
	}
	store, err := taskstate.New(taskstate.DefaultDirectory)
	if err != nil {
		return err
	}
	handler, err := claimproxy.New(strings.TrimRight(os.Getenv("MULTICA_BASE_URL"), "/"), store)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	getenv := func(key string) string {
		if key == "MULTICA_BASE_URL" {
			return "http://" + listener.Addr().String()
		}
		return os.Getenv(key)
	}
	if err := writeOfficialCLIConfig(getenv); err != nil {
		return err
	}
	path, args, err := commandFor([]string{"daemon"}, getenv)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	go collectRetiredStorage(ctx, launcherConfig)
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	defer server.Close()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	command := exec.CommandContext(ctx, path, args...)
	command.Env = daemonEnvironment(os.Environ(), getenv)
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	command.Cancel = func() error { return command.Process.Signal(syscall.SIGTERM) }
	command.WaitDelay = 30 * time.Second
	if err := command.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		return err
	case err := <-serveErr:
		cancel()
		<-done
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
