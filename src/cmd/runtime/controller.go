package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	control "github.com/korioinc/multica-runtime-controller/internal/controller"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/repocache"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func controller(ctx context.Context) error {
	o, err := loadControllerOptions()
	if err != nil {
		return diagnostics.Wrap("controller_options_invalid", err)
	}
	slog.Info("controller options validated", "capacity", o.capacity)
	slog.Info("controller startup stage", "stage", "runtime_image_verification")
	d, digest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		return diagnostics.Wrap("controller_runtime_image_invalid", err)
	}
	if err := runtimeimage.CheckReceipt(wire.ControlRoot, d, digest); err != nil {
		return diagnostics.Wrap("controller_runtime_receipt_invalid", err)
	}
	slog.Info("controller runtime image verified", "providers", len(d.Providers))
	slog.Info("controller startup stage", "stage", "configuration")
	bundle, err := configuration.Read(wire.ControlRoot)
	if err != nil {
		return diagnostics.Wrap("controller_configuration_unavailable", err)
	}
	if err := configuration.ApplyHomeConfiguration(wire.Home, bundle); err != nil {
		return diagnostics.Wrap("controller_home_configuration_failed", err)
	}
	cfg, err := kubernetes.LoadConfig(o.workerConfigFile)
	if err != nil {
		return diagnostics.Wrap("controller_worker_config_invalid", err)
	}
	slog.Info("controller startup stage", "stage", "kubernetes_client")
	resources, err := kubernetes.InCluster(o.namespace, o.capacity)
	if err != nil {
		return diagnostics.Wrap("controller_kubernetes_client_unavailable", err)
	}
	startup, cancel := context.WithTimeout(ctx, o.startupTimeout)
	defer cancel()
	slog.Info("controller startup stage", "stage", "image_admission")
	binding, err := resources.BindImage(startup, o.podName, o.podUID, o.containerName, o.nodeName, d.Platform, d, cfg)
	if err != nil {
		return diagnostics.Wrap("controller_image_admission_failed", err)
	}
	slog.Info("controller startup stage", "stage", "storage_admission")
	if err := resources.AdmitControllerStorage(startup, binding.Owner, cfg); err != nil {
		return diagnostics.Wrap("controller_storage_admission_failed", err)
	}
	environment := []string{}
	appEnv := map[string]string{}
	for _, entry := range o.environment {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		key, operator := strings.CutPrefix(key, "MULTICA_OPERATOR_")
		if !operator {
			continue
		}
		if githubapp.ControllerEnvironmentKey(key) {
			appEnv[key] = value
			continue
		}
		if runtimeimage.Reserved(key) && !runtimeimage.ExecutionSetting(key) {
			return diagnostics.Wrap("controller_operator_environment_invalid", errors.New("operator environment overrides runtime authority"))
		}
		environment = append(environment, key+"="+value)
	}
	ref, err := d.Reference(binding.Image, digest, configuration.ExecutionDigest(bundle, environment))
	if err != nil {
		return diagnostics.Wrap("controller_runtime_reference_invalid", err)
	}
	slog.Info("controller startup stage", "stage", "backend_client")
	token, err := os.ReadFile(o.tokenFile)
	if err != nil {
		return diagnostics.Wrap("controller_token_unavailable", err)
	}
	upstream, err := daemonapi.NewClient(o.backendURL, strings.TrimSpace(string(token)), d.Daemon.Version, nil)
	if err != nil {
		return diagnostics.Wrap("controller_backend_client_invalid", err)
	}
	slog.Info("controller startup stage", "stage", "metadata")
	stateRoot := value("MULTICA_STATE_ROOT", kubernetes.ControllerStateRoot)
	if stateRoot != kubernetes.ControllerStateRoot {
		return diagnostics.Wrap("controller_metadata_path_invalid", errors.New("metadata path differs from admitted volume"))
	}
	store, err := workspace.OpenVolume(stateRoot, workspace.Options{OwnerID: o.ownerID, MaxTasks: cfg.Storage.MaxTasks})
	if err != nil {
		return diagnostics.Wrap("controller_metadata_unavailable", err)
	}
	defer store.Close()
	slog.Info("controller startup stage", "stage", "workspace_binding")
	workspaceUID, nfsServer, err := resources.ResolveWorkspace(startup, cfg)
	if err != nil {
		return diagnostics.Wrap("controller_workspace_unavailable", err)
	}
	if err := store.BindWorkspace(cfg.Storage.ClaimName, workspaceUID, nfsServer); err != nil {
		return diagnostics.Wrap("controller_workspace_binding_failed", err)
	}
	slog.Info("controller startup stage", "stage", "repository_cache")
	cache, err := repocache.New(ctx, filepath.Join(stateRoot, "repositories"))
	if err != nil {
		return diagnostics.Wrap("controller_repository_cache_unavailable", err)
	}
	defer cache.Close()
	var app *githubapp.Manager
	if appEnv["GITHUB_APP_ID"] != "" || appEnv["GITHUB_APP_PRIVATE_KEY"] != "" {
		slog.Info("controller startup stage", "stage", "github_app")
		app, err = githubapp.New(appEnv["GITHUB_APP_ID"], appEnv["GITHUB_APP_PRIVATE_KEY"])
		if err != nil {
			return diagnostics.Wrap("controller_github_app_invalid", err)
		}
	}
	c := &control.Controller{Store: store, API: upstream, Kube: resources, Policy: cfg, Owner: binding.Owner, OwnerID: o.ownerID, Descriptor: d, RuntimeRef: ref, Configuration: bundle, Environment: environment, GatewayURL: o.gatewayURL, NFSServer: nfsServer, Name: o.name, Capacity: o.capacity, PollInterval: o.pollInterval, HeartbeatInterval: o.heartbeatInterval, App: app, Cache: cache}
	c.ConversationIdleTimeout, c.MaxResidentPods = o.conversationIdleTimeout, o.maxResidentPods
	server := &http.Server{Addr: ":8080", Handler: c.Handler(), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10}
	defer server.Close()
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	serverErrors := make(chan error, 1)
	slog.Info("controller services starting", "port", 8080)
	go func() {
		serverErrors <- server.ListenAndServe()
		stop()
	}()
	err = c.Run(ctx)
	select {
	case serverErr := <-serverErrors:
		if !errors.Is(serverErr, http.ErrServerClosed) {
			return diagnostics.Wrap("controller_http_service_failed", serverErr)
		}
	default:
	}
	return diagnostics.Wrap("controller_service_stopped", err)
}
