package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/go-logr/logr"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/initprocess"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/worker"
	"k8s.io/klog/v2"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "init" {
		os.Exit(initprocess.Run(os.Args[2:]))
	}
	klog.SetLogger(logr.Discard())
	controllerRole := len(os.Args) > 1 && os.Args[1] == "controller"
	workerRole := len(os.Args) > 2 && os.Args[1] == "worker" && (os.Args[2] == "serve" || os.Args[2] == "layout" || os.Args[2] == "run" || os.Args[2] == "desktop")
	if controllerRole || workerRole {
		component := "controller"
		if workerRole {
			component = "worker"
		}
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("component", component))
	}
	if controllerRole {
		slog.Info("runtime controller starting")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := dispatch(ctx, os.Args[1:]); err != nil && !errors.Is(err, flag.ErrHelp) {
		if controllerRole && errors.Is(ctx.Err(), context.Canceled) && errors.Is(err, context.Canceled) {
			slog.Info("runtime controller stopped", "reason", "shutdown_requested")
			return
		}
		var exit *exec.ExitError
		exitCode := 1
		if errors.As(err, &exit) {
			exitCode = exit.ExitCode()
			if !controllerRole {
				os.Exit(exitCode)
			}
		}
		if len(os.Args) > 1 && os.Args[1] == "github" {
			fmt.Fprintln(os.Stderr, err)
		} else {
			reason := "operation_failed"
			var diagnostic *diagnostics.Error
			if errors.As(err, &diagnostic) {
				reason = diagnostic.Reason
			}
			slog.Error("runtime operation failed", "reason", reason)
		}
		os.Exit(exitCode)
	}
	if controllerRole {
		slog.Info("runtime controller stopped", "reason", "completed")
	}
}

func dispatch(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("runtime role required")
	}
	switch args[0] {
	case "version":
		if len(args) != 1 {
			return errors.New("usage: runtime version")
		}
		contract, err := core.Check(core.Root, core.HostPlatform())
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(contract)
	case "image":
		if len(args) != 2 || args[1] != "verify" {
			return errors.New("usage: runtime image verify")
		}
		_, _, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
		return err
	case "home":
		if len(args) < 2 || args[1] != "layout" {
			return errors.New("usage: runtime home layout")
		}
		return layoutHome(ctx, args[2:])
	case "controller":
		if len(args) != 1 {
			return errors.New("usage: runtime controller")
		}
		return controller(ctx)
	case "worker":
		if len(args) < 2 {
			return errors.New("worker operation required")
		}
		switch args[1] {
		case "serve":
			if len(args) != 2 {
				return errors.New("usage: runtime worker serve")
			}
			return worker.Serve(ctx)
		case "run":
			if len(args) != 3 {
				return errors.New("usage: runtime worker run <supervisor-socket>")
			}
			return worker.RunProvider(ctx, args[2])
		case "desktop":
			if len(args) != 3 || args[2] == "" {
				return errors.New("usage: runtime worker desktop <supervisor-socket>")
			}
			return worker.RunDesktop(ctx, args[2])
		case "chrome":
			return worker.Chrome(ctx, args[2:])
		case "ready":
			_, err := os.Stat(wire.ControlRoot + "/ready")
			return err
		case "layout":
			flags := flag.NewFlagSet("worker layout", flag.ContinueOnError)
			privateRoot := flags.String("private-root", "", "")
			request := flags.String("request", wire.RequestPath, "")
			if err := flags.Parse(args[2:]); err != nil {
				return err
			}
			if flags.NArg() != 0 {
				return errors.New("invalid layout arguments")
			}
			return worker.Layout(ctx, *privateRoot, *request)
		}
	case "github":
		return githubCommand(ctx, args[1:])
	}
	return errors.New("unsupported runtime operation")
}

func githubCommand(ctx context.Context, args []string) error {
	if len(args) == 2 && args[0] == "credential" {
		return githubauth.Credential(ctx, args[1], os.Stdin, os.Stdout)
	}
	if len(args) < 2 || args[0] != "gh" {
		return errors.New("usage: runtime github credential <get|store|erase> | github gh <executable> [arguments]")
	}
	executable := args[1]
	if !runtimeimage.ImmutablePath(executable) || filepath.Base(executable) != "gh" {
		return errors.New("installed GitHub executable required")
	}
	directory, err := os.Getwd()
	if err != nil {
		return errors.New("GitHub working directory unavailable")
	}
	env, err := githubauth.PrepareGH(ctx, args[2:], githubauth.WithoutAppCredentials(os.Environ()), directory)
	if err != nil {
		return err
	}
	command := exec.CommandContext(ctx, executable, args[2:]...)
	command.Env = env
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	return command.Run()
}
