package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"github.com/go-logr/logr"
	"io"
	"k8s.io/klog/v2"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/migration"
	"github.com/korioinc/multica-runtime-controller/internal/official"
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
		}
		attributes := []any{"phase", phase, "error_class", class, "reason", reason, "imageBuildID", id}
		if diagnostic != nil {
			if diagnostic.Path != "" {
				attributes = append(attributes, "path", diagnostic.Path)
			}
			if diagnostic.SourceGroup != "" {
				attributes = append(attributes, "sourceGroup", diagnostic.SourceGroup)
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
		case "version", "image", "workspace", "controller", "worker", "home":
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
			return execution.LayoutHome(ctx, args[3:])
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

func controller(ctx context.Context) error {
	descriptor, descriptorDigest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		return err
	}
	// Receipt validation precedes the API read: a restarted B rootfs must never
	// accept the old A status left briefly visible by the kubelet.
	if err = runtimeimage.CheckReceipt(wire.ControlRoot, descriptor, descriptorDigest); err != nil {
		return err
	}
	bundle, err := configuration.Read(wire.ControlRoot)
	if err != nil {
		return err
	}
	if err = execution.CheckPrivate(); err != nil {
		return err
	}
	cfg, err := kubernetes.LoadConfig(value("MULTICA_WORKER_CONFIG_FILE", wire.WorkerConfigPath))
	if err != nil {
		return err
	}
	if cfg.Platform != descriptor.Platform {
		return errors.New("worker policy differs from installed runtime platform")
	}
	ownerID := value("MULTICA_OWNER_ID", os.Getenv("MULTICA_DAEMON_ID"))
	if ownerID != os.Getenv("MULTICA_DAEMON_ID") || !wire.UUID(ownerID) {
		return errors.New("installation owner differs from daemon identity")
	}
	if cfg.SingleNodeName != "" && os.Getenv("POD_NODE_NAME") != cfg.SingleNodeName {
		return errors.New("controller fixed Node mismatch")
	}
	startupTimeout, err := time.ParseDuration(value("MULTICA_STARTUP_TIMEOUT", "120s"))
	if err != nil || startupTimeout <= 0 {
		return errors.New("startup timeout must be a positive duration")
	}
	resources, err := kubernetes.InCluster(os.Getenv("POD_NAMESPACE"))
	if err != nil {
		return err
	}
	startup, stopStartup := context.WithTimeout(ctx, startupTimeout)
	defer stopStartup()
	binding, err := resources.BindImage(startup, os.Getenv("POD_NAME"), os.Getenv("POD_UID"), value("POD_CONTAINER_NAME", "controller"), os.Getenv("POD_NODE_NAME"), descriptor.Platform)
	if err != nil {
		return err
	}
	ref, err := descriptor.Reference(binding.Image, descriptorDigest, bundle.Digest)
	if err != nil {
		return err
	}
	var snapshots []configuration.SnapshotRef
	if _, err = os.Lstat(wire.SelectionPath); err == nil {
		previous, err := execution.LoadSelection()
		if err != nil {
			return err
		}
		if previous.Controller != binding.Owner || previous.Namespace != resources.Namespace || previous.OwnerID != ownerID || !previous.RuntimeRef.Equal(ref) {
			return errors.New("restarted controller differs from its persisted execution selection")
		}
		if err = resources.ValidateSnapshots(startup, binding.Owner, previous.Snapshots, ref.ConfigurationDigest); err != nil {
			return err
		}
		snapshots = previous.Snapshots
	} else if os.IsNotExist(err) {
		snapshots, err = resources.EnsureSnapshots(startup, binding.Owner, bundle)
		if err != nil {
			return err
		}
	} else {
		return err
	}
	vars, err := execution.SelectedEnvironment(descriptor, os.Environ(), runtimeimage.Locations{Home: wire.Home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot})
	if err != nil {
		return err
	}
	backend, err := official.NormalizeBackendURL(os.Getenv("MULTICA_BASE_URL"))
	if err != nil {
		return err
	}
	selection := execution.Selection{SchemaVersion: 2, OwnerID: ownerID, Controller: binding.Owner, Namespace: resources.Namespace, Gateway: os.Getenv("MULTICA_DAEMON_PROXY_URL"), Backend: backend, RuntimeRef: ref, Snapshots: snapshots, Worker: cfg, OperatorKeys: execution.OperatorNames(os.Environ())}
	if err := execution.SaveSelection(selection); err != nil {
		return err
	}
	slog.Info("runtime image bound", "phase", "controller", "image", ref.Image, "platform", ref.Platform, "controllerBuild", ref.Controller.BuildID, "imageBuildID", ref.ImageBuildID)
	// Application workspace mutations are admitted only after image and config
	// identity have been tied to this controller Pod.
	if err := execution.LayoutWorkspace(ownerID); err != nil {
		return err
	}
	if err := execution.BindControllerSessions(); err != nil {
		return err
	}
	store, err := execution.OpenWorkspace(ownerID)
	if err != nil {
		return err
	}
	runner, err := execution.NewRunner(selection, resources, store, descriptor)
	if err != nil {
		return err
	}
	providers := slices.Sorted(maps.Keys(descriptor.Providers))
	bridge, err := official.NewBridge(official.BridgeOptions{BackendURL: backend, Store: store, RuntimeRef: ref, Providers: providers})
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
	process, err := official.Setup(official.DaemonOptions{CoreRoot: wire.ControllerRoot, Home: wire.Home, TokenFile: os.Getenv("MULTICA_CONTROLLER_TOKEN_FILE"), DaemonID: ownerID, Capacity: capacity, PollInterval: poll, HeartbeatInterval: heartbeat, Name: value("MULTICA_RUNTIME_NAME", "runtime-controller"), BackendURL: backend, ProxyURL: "http://" + listener.Addr().String(), Providers: providers, RuntimeRef: ref, Env: vars, Stdout: os.Stdout, Stderr: os.Stderr})
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
