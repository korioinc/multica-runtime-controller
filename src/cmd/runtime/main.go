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
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/environment"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func main() {
	phase, closeDiagnostics := configureDiagnostics(os.Args)
	defer closeDiagnostics()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	err := dispatch(ctx, os.Args)
	if err != nil {
		var providerExit *execution.ExitError
		if errors.As(err, &providerExit) {
			os.Exit(providerExit.Code)
		}
		logger := slog.Default()
		id := os.Getenv("MULTICA_ENVIRONMENT_ID")
		task, attempt := os.Getenv("MULTICA_TASK_ID"), os.Getenv("MULTICA_ATTEMPT_ID")
		var attemptFailure *execution.AttemptError
		if errors.As(err, &attemptFailure) {
			id, task, attempt = attemptFailure.EnvironmentID, attemptFailure.TaskID, attemptFailure.AttemptID
		}
		if !core.ValidSHA(id) {
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
		case errors.Is(err, environment.ErrIntegrity):
			class = "environment_integrity"
		case errors.Is(err, execution.ErrTransport) || errors.Is(err, kubernetes.ErrTransport):
			class = "transport"
		case errors.Is(err, execution.ErrProviderStart):
			class = "provider_start"
		}
		attributes := []any{"phase", phase, "error_class", class, "reason", reason, "environmentID", id}
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
		case "materialize", "environment", "workspace", "controller", "worker", "home":
			phase = args[1]
		}
	}
	providerStream = providerStream || phase == "worker" && len(args) > 2 && args[2] == "execute"
	if !providerStream {
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

func value(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func input() (environment.Input, error) {
	return environment.ReadInput(value("MULTICA_ENVIRONMENT_INPUT_FILE", wire.InputPath))
}
func checkedInput() (environment.Input, error) {
	in, err := input()
	if err != nil {
		return in, err
	}
	if in.Platform != core.HostPlatform() {
		return in, fmt.Errorf("%w: selected platform differs from running process", core.ErrCompatibility)
	}
	id, err := environment.Identity(in)
	if err != nil {
		return in, err
	}
	if configured := os.Getenv("MULTICA_ENVIRONMENT_ID"); configured != "" && configured != id {
		return in, errors.New("environment_integrity: configured identity mismatch")
	}
	return in, nil
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
	case "materialize":
		platform := value("MULTICA_PLATFORM", core.HostPlatform())
		if platform != core.HostPlatform() {
			return errors.New("core platform mismatch")
		}
		if _, err := core.Materialize(value("MULTICA_CORE_SOURCE", "/artifact"), value("MULTICA_CORE_ROOT", wire.CoreRoot), platform); err != nil {
			return err
		}
		if os.Getenv("MULTICA_TOOLS_STORE") != "" {
			in, err := checkedInput()
			if err != nil {
				return err
			}
			id, err := environment.Identity(in)
			if err != nil {
				return err
			}
			return environment.Layout(os.Getenv("MULTICA_TOOLS_STORE"), value("MULTICA_OWNER_ID", os.Getenv("MULTICA_DAEMON_ID")), id)
		}
		return nil
	case "environment":
		if len(args) < 3 {
			return errors.New("environment operation required")
		}
		if args[2] == "capture" {
			raw, err := json.Marshal(os.Environ())
			if err != nil {
				return err
			}
			if err := os.WriteFile(value("MULTICA_ENVIRONMENT_BASE_ENV_FILE", wire.ControlRoot+"/image-env.json"), raw, 0600); err != nil {
				return err
			}
			// The following init mounts the external lock over this file while
			// the containing control volume is read-only.
			file, err := os.OpenFile(wire.ControlRoot+"/prepare.lock", os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			return file.Close()
		}
		if args[2] == "identity" {
			in, err := input()
			if err != nil {
				return err
			}
			id, err := environment.Identity(in)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(os.Stdout, id)
			return err
		}
		in, err := checkedInput()
		if err != nil {
			return err
		}
		id, err := environment.Identity(in)
		if err != nil {
			return err
		}
		switch args[2] {
		case "layout":
			return environment.Layout(value("MULTICA_TOOLS_STORE", "/tools-store"), value("MULTICA_OWNER_ID", os.Getenv("MULTICA_DAEMON_ID")), id)
		case "prepare":
			timeout, err := strconv.Atoi(value("MULTICA_ENVIRONMENT_TIMEOUT_SECONDS", "1200"))
			if err != nil || timeout < 1 {
				return errors.New("invalid prepare timeout")
			}
			var baseEnv []string
			raw, err := os.ReadFile(value("MULTICA_ENVIRONMENT_BASE_ENV_FILE", wire.ControlRoot+"/image-env.json"))
			if err != nil {
				return err
			}
			if err := json.Unmarshal(raw, &baseEnv); err != nil {
				return err
			}
			_, err = environment.Prepare(ctx, environment.Options{Root: value("MULTICA_ENVIRONMENT_ROOT", wire.EnvironmentRoot), CoreRoot: value("MULTICA_CORE_ROOT", wire.CoreRoot), Input: in, ExpectedID: id, ScriptPath: value("MULTICA_ENVIRONMENT_SCRIPT_FILE", "/etc/multica/bootstrap/script.sh"), LockPath: os.Getenv("MULTICA_ENVIRONMENT_LOCK_FILE"), InstallEnv: os.Environ(), BaseEnv: baseEnv, Timeout: time.Duration(timeout) * time.Second, Output: os.Stdout})
			return err
		case "check":
			_, _, err := environment.Check(value("MULTICA_ENVIRONMENT_ROOT", wire.EnvironmentRoot), value("MULTICA_CORE_ROOT", wire.CoreRoot), in, nil)
			return err
		}
	case "workspace":
		if len(args) >= 3 && args[2] == "layout" {
			if err := execution.LayoutWorkspace(value("MULTICA_OWNER_ID", os.Getenv("MULTICA_DAEMON_ID"))); err != nil {
				return err
			}
			return execution.LayoutHome(args[3:])
		}
	case "home":
		if len(args) >= 3 && args[2] == "layout" {
			return execution.LayoutHome(args[3:])
		}
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

func controller(ctx context.Context) error {
	in, err := checkedInput()
	if err != nil {
		return err
	}
	ref, manifest, err := environment.Check(wire.EnvironmentRoot, wire.CoreRoot, in, nil)
	if err != nil {
		return err
	}
	cfg, err := kubernetes.LoadConfig(value("MULTICA_WORKER_CONFIG_FILE", wire.WorkerConfigPath))
	if err != nil {
		return err
	}
	ownerID := value("MULTICA_OWNER_ID", os.Getenv("MULTICA_DAEMON_ID"))
	if ownerID != os.Getenv("MULTICA_DAEMON_ID") {
		return errors.New("installation owner differs from daemon identity")
	}
	if cfg.SingleNodeName != "" && os.Getenv("POD_NODE_NAME") != cfg.SingleNodeName {
		return errors.New("controller fixed Node mismatch")
	}
	if err := execution.LayoutWorkspace(ownerID); err != nil {
		return err
	}
	store, err := execution.OpenWorkspace(ownerID)
	if err != nil {
		return err
	}
	if err := environment.CopySeed(wire.EnvironmentRoot, manifest, wire.Home); err != nil {
		return err
	}
	if err := environment.CheckWritable(environment.Locations{Home: wire.Home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot}); err != nil {
		return err
	}
	resources, err := kubernetes.InCluster(os.Getenv("POD_NAMESPACE"))
	if err != nil {
		return err
	}
	owner, err := resources.Controller(ctx, os.Getenv("POD_NAME"), os.Getenv("POD_UID"), cfg.SingleNodeName)
	if err != nil {
		return err
	}
	vars, err := execution.SelectedEnvironment(manifest, os.Environ(), environment.Locations{Root: wire.EnvironmentRoot, Home: wire.Home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot})
	if err != nil {
		return err
	}
	backend, err := official.NormalizeBackendURL(os.Getenv("MULTICA_BASE_URL"))
	if err != nil {
		return err
	}
	selection := execution.Selection{SchemaVersion: 1, OwnerID: ownerID, Controller: owner, Namespace: resources.Namespace, Gateway: os.Getenv("MULTICA_DAEMON_PROXY_URL"), Backend: backend, Input: in, Environment: ref, Worker: cfg, OperatorKeys: execution.OperatorNames(os.Environ())}
	if err := execution.SaveSelection(selection); err != nil {
		return err
	}
	runner, err := execution.NewRunner(selection, resources, store, manifest)
	if err != nil {
		return err
	}
	bridge, err := official.NewBridge(official.BridgeOptions{BackendURL: backend, Store: store, Environment: ref, Providers: in.Providers})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	capacity, err := strconv.Atoi(value("MULTICA_RUNTIME_CAPACITY", "20"))
	if err != nil || capacity < 1 {
		return errors.New("invalid runtime capacity")
	}
	poll, err := time.ParseDuration(value("MULTICA_POLL_INTERVAL", "10s"))
	if err != nil || poll <= 0 {
		return errors.New("invalid poll interval")
	}
	heartbeat, err := time.ParseDuration(value("MULTICA_HEARTBEAT_INTERVAL", "15s"))
	if err != nil || heartbeat <= 0 {
		return errors.New("invalid heartbeat interval")
	}
	process, err := official.Setup(official.DaemonOptions{CoreRoot: wire.CoreRoot, Home: wire.Home, TokenFile: os.Getenv("MULTICA_CONTROLLER_TOKEN_FILE"), DaemonID: ownerID, Capacity: capacity, PollInterval: poll, HeartbeatInterval: heartbeat, Name: value("MULTICA_RUNTIME_NAME", "runtime-controller"), BackendURL: backend, ProxyURL: "http://" + listener.Addr().String(), Providers: in.Providers, Environment: ref, Env: vars, Stdout: os.Stdout, Stderr: os.Stderr})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gateway := &http.Server{Addr: ":8080", Handler: execution.ControllerGateway(resources), ReadHeaderTimeout: 10 * time.Second}
	defer gateway.Close()
	gatewayError := make(chan error, 1)
	go func() { gatewayError <- gateway.ListenAndServe(); cancel() }()
	go runner.RunCollector(ctx)
	err = official.RunDaemon(ctx, listener, bridge, process)
	select {
	case gatewayErr := <-gatewayError:
		if gatewayErr != nil && !errors.Is(gatewayErr, http.ErrServerClosed) {
			return gatewayErr
		}
	default:
	}
	return err
}
func errorClass(phase string) string {
	switch phase {
	case "materialize":
		return "core_compatibility"
	case "environment":
		return "environment_prepare"
	case "controller", "workspace", "home", "configuration":
		return "configuration"
	case "worker":
		return "provider_start"
	default:
		return "task_authorization"
	}
}
