package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/go-logr/logr"
	"io"
	"k8s.io/klog/v2"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/migration"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func main() {
	phase, closeDiagnostics := configureDiagnostics(os.Args)
	defer closeDiagnostics()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	err := dispatch(ctx, os.Args)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		var providerExit *execution.ExitError
		if errors.As(err, &providerExit) {
			os.Exit(providerExit.Code)
		}
		if phase == "github" {
			// Authentication helpers only construct redacted errors. Git reserves
			// stdout for credentials; actionable diagnostics belong on stderr.
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		logger := slog.Default()
		id := os.Getenv("MULTICA_RUNTIME_IMAGE_BUILD_ID")
		task, attempt := os.Getenv("MULTICA_TASK_ID"), os.Getenv("MULTICA_ATTEMPT_ID")
		var attemptFailure *execution.AttemptError
		if errors.As(err, &attemptFailure) {
			id, task, attempt = attemptFailure.ImageBuildID, attemptFailure.TaskID, attemptFailure.AttemptID
		}
		if !wire.UUID(id) {
			id = ""
		}
		reason := "operation_failed"
		if errors.Is(err, os.ErrPermission) {
			reason = "permission_denied"
		} else if errors.Is(err, os.ErrNotExist) {
			reason = "required_file_missing"
		} else if errors.Is(err, context.DeadlineExceeded) {
			reason = "deadline_exceeded"
		}
		class := errorClass(phase)
		switch {
		case errors.Is(err, core.ErrCompatibility):
			class = "core_compatibility"
		case errors.Is(err, runtimeimage.ErrCompatibility):
			class = "runtime_image_compatibility"
		case errors.Is(err, execution.ErrTransport) || errors.Is(err, kubernetes.ErrTransport):
			class = "transport"
		case errors.Is(err, execution.ErrProviderStart):
			class = "provider_start"
		}
		var diagnostic *diagnostics.Error
		if errors.As(err, &diagnostic) {
			reason = diagnostic.Reason
			if diagnostic.AttemptID != "" {
				attempt = diagnostic.AttemptID
			}
		}
		attributes := []any{"phase", phase, "error_class", class, "reason", reason, "imageBuildID", id}
		if diagnostic != nil {
			if diagnostic.Path != "" {
				attributes = append(attributes, "path", diagnostic.Path)
			}
			if diagnostic.SourceGroup != "" {
				attributes = append(attributes, "sourceGroup", diagnostic.SourceGroup)
			}
			if diagnostic.StorageID != "" {
				attributes = append(attributes, "storage", diagnostic.StorageID)
			}
			if diagnostic.EntrySHA256 != "" {
				attributes = append(attributes, "entrySHA256", diagnostic.EntrySHA256)
			}
		}
		if wire.UUID(task) {
			attributes = append(attributes, "task", task)
		}
		if wire.UUID(attempt) {
			attributes = append(attributes, "attempt", attempt)
		}
		logger.Error("runtime operation failed", attributes...)
		os.Exit(execution.ErrorCode(err))
	}
}

// Configure every diagnostic writer before the shim can start an execution.
// Cleanup warnings and nested library logs must not enter provider protocol pipes.
func configureDiagnostics(args []string) (string, func()) {
	// Kubernetes warning headers and verbose request logs can contain caller
	// data. Runtime failures are reported through our separate, structured sink.
	klog.SetLogger(logr.Discard())
	phase := "configuration"
	providerStream := wire.Provider(filepath.Base(args[0])) != ""
	if providerStream {
		phase = "provider"
	} else if len(args) > 1 {
		switch args[1] {
		case "version", "image", "workspace", "controller", "worker", "home", "github":
			phase = args[1]
		}
	}
	providerStream = providerStream || phase == "worker" && len(args) > 2 && args[2] == "execute"
	if !providerStream {
		if phase == "controller" || phase == "worker" {
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
		}
		return phase, func() {}
	}
	var output io.Writer = io.Discard
	cleanup := func() {}
	if runtime.GOOS == "linux" {
		if file, err := os.OpenFile("/proc/1/fd/2", os.O_WRONLY, 0); err == nil {
			output = file
			cleanup = func() { _ = file.Close() }
		}
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(output, nil)))
	return phase, cleanup
}

func dispatch(ctx context.Context, args []string) error {
	if provider := wire.Provider(filepath.Base(args[0])); provider != "" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		return execution.Launch(ctx, provider, args[1:], os.Environ(), cwd, execution.ProcessStreams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
	}
	if len(args) < 2 {
		return errors.New("runtime role required")
	}
	switch args[1] {
	case "github":
		return githubCommand(ctx, args[2:])
	case "version":
		if len(args) != 2 {
			return errors.New("usage: runtime version")
		}
		contract, err := core.Check(core.Root, core.HostPlatform())
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(contract)
	case "image":
		if len(args) != 3 || args[2] != "verify" {
			return errors.New("usage: runtime image verify")
		}
		_, _, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
		return err
	case "workspace":
		if len(args) >= 3 && args[2] == "migrate" {
			return migrateWorkspace(args[3:])
		}
		return errors.New("usage: runtime workspace migrate --root PATH --owner-id UUID (--dry-run | --expected-source-sha256 DIGEST --commit)")
	case "home":
		if len(args) >= 3 && args[2] == "layout" {
			return layoutHome(ctx, args[3:])
		}
		return errors.New("usage: runtime home layout --private-root PATH [--config-copy JSON ...]")
	case "controller":
		return controller(ctx)
	case "worker":
		if len(args) < 3 {
			return errors.New("worker operation required")
		}
		switch args[2] {
		case "serve":
			return execution.WorkerServe(ctx)
		case "proxy":
			return execution.WorkerProxy(ctx)
		case "ready":
			return execution.WorkerReady()
		case "execute":
			parser := flag.NewFlagSet("worker execute", flag.ContinueOnError)
			digest := parser.String("request-digest", "", "")
			uid := parser.String("pod-uid", "", "")
			stdin := parser.Bool("stdin", true, "")
			stdout := parser.Bool("stdout", true, "")
			stderr := parser.Bool("stderr", true, "")
			if err := parser.Parse(args[3:]); err != nil {
				return err
			}
			var inputFile *os.File
			if *stdin {
				inputFile = os.Stdin
			}
			var outputWriter, errorWriter io.Writer
			if *stdout {
				outputWriter = os.Stdout
			}
			if *stderr {
				errorWriter = os.Stderr
			}
			return execution.WorkerExecute(ctx, *digest, *uid, execution.ProcessStreams{Stdin: inputFile, Stdout: outputWriter, Stderr: errorWriter})
		}
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
		return errors.New("GitHub CLI wrapper requires its installed immutable executable")
	}
	directory, err := os.Getwd()
	if err != nil {
		return errors.New("GitHub CLI working directory unavailable")
	}
	env := githubauth.WithoutAppCredentials(os.Environ())
	env, err = githubauth.PrepareGH(ctx, args[2:], env, directory)
	if err != nil {
		return err
	}
	result := execution.RunProcess(ctx, executable, args[2:], env, directory, 5*time.Second, execution.ProcessStreams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
	return execution.ResultError(result)
}

func errorClass(phase string) string {
	switch phase {
	case "version":
		return "core_compatibility"
	case "image":
		return "runtime_image_compatibility"
	case "controller", "workspace", "home", "configuration":
		return "configuration"
	case "worker":
		return "provider_start"
	default:
		return "task_authorization"
	}
}

func migrateWorkspace(args []string) error {
	parser := flag.NewFlagSet("workspace migrate", flag.ContinueOnError)
	root := parser.String("root", "", "existing workspace root")
	owner := parser.String("owner-id", "", "existing installation UUID")
	dryRun := parser.Bool("dry-run", false, "validate without writing")
	commit := parser.Bool("commit", false, "commit the validated conversion")
	expected := parser.String("expected-source-sha256", "", "source digest from dry-run")
	if err := parser.Parse(args); err != nil {
		return err
	}
	if parser.NArg() != 0 || *dryRun == *commit {
		parser.Usage()
		return diagnostics.Wrap("migration_mode_invalid", errors.New("workspace migrate requires exactly one of --dry-run or --commit"))
	}
	result, err := migration.Migrate(migration.Options{Root: *root, OwnerID: *owner, Commit: *commit, ExpectedSourceSHA256: *expected})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
