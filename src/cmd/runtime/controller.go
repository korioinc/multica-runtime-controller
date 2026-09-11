package main

import (
	"context"
	"errors"
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func controller(ctx context.Context) error {
	options, err := loadControllerOptions()
	if err != nil {
		return err
	}
	admitted, err := admitController(ctx, options)
	if err != nil {
		return err
	}
	return serveController(ctx, options, admitted)
}

func serveController(ctx context.Context, options controllerOptions, admitted admittedController) error {
	selection, resources := admitted.selection, admitted.resources
	// Application workspace mutations are admitted only after image and config
	// identity have been tied to this controller Pod.
	if err := execution.LayoutWorkspace(selection.OwnerID); err != nil {
		return err
	}
	if err := execution.BindControllerSessions(); err != nil {
		return err
	}
	store, err := execution.OpenWorkspace(selection.OwnerID)
	if err != nil {
		return err
	}
	runner, err := execution.NewRunner(selection, resources, store, admitted.descriptor)
	if err != nil {
		return err
	}
	providers := slices.Sorted(maps.Keys(admitted.descriptor.Providers))
	bridge, err := official.NewBridge(official.BridgeOptions{BackendURL: selection.Backend, Store: store, RuntimeRef: selection.RuntimeRef, Providers: providers})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	// App signing authority stays in this process. The official daemon and its
	// per-task shims use the private credential broker instead of inheriting keys.
	daemonEnvironment := githubauth.WithoutAppCredentials(admitted.environment)
	var authServer *http.Server
	var authListener net.Listener
	appID, privateKey := wire.Value(admitted.environment, "GITHUB_APP_ID"), wire.Value(admitted.environment, "GITHUB_APP_PRIVATE_KEY")
	if appID != "" || privateKey != "" {
		manager, err := githubapp.New(appID, privateKey)
		if err != nil {
			return err
		}
		authListener, err = githubauth.ListenPrivate()
		if err != nil {
			return err
		}
		defer authListener.Close()
		authServer = &http.Server{Handler: githubauth.PrivateHandler(manager), ReadHeaderTimeout: 10 * time.Second}
		defer authServer.Close()
		daemonEnvironment, err = githubauth.GitEnvironment(daemonEnvironment)
		if err != nil {
			return err
		}
	}
	process, err := official.Setup(admitted.descriptor, official.DaemonOptions{
		CoreRoot: wire.ControllerRoot, Home: wire.Home, TokenFile: options.tokenFile,
		DaemonID: selection.OwnerID, Capacity: options.capacity, PollInterval: options.pollInterval,
		HeartbeatInterval: options.heartbeatInterval, Name: options.name,
		BackendURL: selection.Backend, ProxyURL: "http://" + listener.Addr().String(),
		Providers: providers, RuntimeRef: selection.RuntimeRef, Env: daemonEnvironment,
		Stdout: os.Stdout, Stderr: os.Stderr,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	monitor, err := execution.ListenAttemptMonitor(execution.AttemptMonitorPath)
	if err != nil {
		return err
	}
	defer monitor.Close()
	gateway := &http.Server{Addr: ":8080", Handler: execution.ControllerGateway(resources), ReadHeaderTimeout: 10 * time.Second}
	defer gateway.Close()
	serviceError := make(chan error, 3)
	if authServer != nil {
		go func() { serviceError <- authServer.Serve(authListener); cancel() }()
	}
	go func() { serviceError <- gateway.ListenAndServe(); cancel() }()
	go func() { serviceError <- runner.ServeAttemptMonitor(ctx, monitor); cancel() }()
	go runner.RunCollector(ctx)
	err = official.RunDaemon(ctx, listener, bridge, process)
	select {
	case serviceErr := <-serviceError:
		if serviceErr != nil && !errors.Is(serviceErr, http.ErrServerClosed) {
			return serviceErr
		}
	default:
	}
	return err
}
