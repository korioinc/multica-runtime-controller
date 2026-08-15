package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/intercept"
	utilexec "k8s.io/client-go/util/exec"
)

func main() {
	provider := filepath.Base(os.Args[0])
	providerPath, err := intercept.ProviderExecutable(provider)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	taskID := strings.TrimSpace(os.Getenv("MULTICA_TASK_ID"))
	if taskID == "" {
		if err := syscall.Exec(providerPath, append([]string{providerPath}, os.Args[1:]...), os.Environ()); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	workDir, err := os.Getwd()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	request, err := intercept.PrepareRequest(provider, os.Args[1:], os.Environ(), workDir)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cfg, controllerPodName, err := intercept.LoadConfig()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	launcher, err := intercept.NewLauncher(ctx, cfg, controllerPodName)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := launcher.Run(ctx, taskID, request, intercept.Streams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(providerExitCode(err))
	}
}

func providerExitCode(err error) int {
	var exitErr utilexec.ExitError
	if errors.As(err, &exitErr) && exitErr.Exited() {
		return exitErr.ExitStatus()
	}
	return 1
}
