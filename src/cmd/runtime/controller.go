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
	process, err := official.Setup(official.DaemonOptions{
		CoreRoot: wire.ControllerRoot, Home: wire.Home, TokenFile: options.tokenFile,
		DaemonID: selection.OwnerID, Capacity: options.capacity, PollInterval: options.pollInterval,
		HeartbeatInterval: options.heartbeatInterval, Name: options.name,
		BackendURL: selection.Backend, ProxyURL: "http://" + listener.Addr().String(),
		Providers: providers, RuntimeRef: selection.RuntimeRef, Env: admitted.environment,
		Stdout: os.Stdout, Stderr: os.Stderr,
	})
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
