package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
)

func readWorkerRequest(digest, uid string) (wire.Request, runtimeimage.Descriptor, error) {
	var empty runtimeimage.Descriptor
	if !core.ValidSHA(digest) || uid == "" || uid != os.Getenv("POD_UID") {
		return wire.Request{}, empty, errors.New("worker Pod UID or request digest mismatch")
	}
	raw, err := os.ReadFile(wire.RequestPath)
	if err != nil {
		return wire.Request{}, empty, err
	}
	if wire.Digest(raw) != digest {
		return wire.Request{}, empty, errors.New("mounted request differs from the execution attempt")
	}
	request, err := wire.Decode(raw)
	if err != nil {
		return request, empty, err
	}
	if request.TaskID != os.Getenv("MULTICA_TASK_ID") || request.RuntimeRef.Platform != core.HostPlatform() {
		return request, empty, errors.New("worker task or platform mismatch")
	}
	manifest, imageDigest, err := runtimeimage.Check(context.Background(), runtimeimage.Root, wire.ControllerRoot, core.HostPlatform())
	if err != nil {
		return request, empty, err
	}
	if err := runtimeimage.Match(manifest, imageDigest, request.RuntimeRef); err != nil {
		return request, empty, err
	}
	if err := runtimeimage.CheckReceipt(wire.ControlRoot, manifest, imageDigest); err != nil {
		return request, empty, err
	}
	bundle, err := configuration.Read(wire.ControlRoot)
	if err != nil {
		return request, empty, err
	}
	if bundle.Digest != request.RuntimeRef.ConfigurationDigest {
		return request, empty, errors.New("worker committed configuration differs from request")
	}
	if err := CheckPrivate(); err != nil {
		return request, empty, err
	}
	return request, manifest, nil
}

func WorkerServe(ctx context.Context) error {
	if os.Getpid() != 1 {
		return errors.New("worker serve must own container PID 1")
	}
	request, manifest, err := readWorkerRequest(os.Getenv("MULTICA_REQUEST_DIGEST"), os.Getenv("POD_UID"))
	if err != nil {
		return err
	}
	if request.Provider == "codex" {
		bundle, err := configuration.Read(wire.ControlRoot)
		if err != nil {
			return err
		}
		if bundle.Digest != request.RuntimeRef.ConfigurationDigest {
			return errors.New("worker committed configuration differs from request")
		}
		if err := hydrateSkills(wire.ControlRoot+"/assigned-skills", wire.Home, manifest.HomeSeed, bundle); err != nil {
			return err
		}
	}
	for _, dir := range []string{wire.ControlRoot, wire.Home + "/.multica/pi-sessions", wire.Home + "/.pi/agent", wire.Home + "/.codex"} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	return superviseWorker(ctx, time.Duration(request.TerminationGraceSeconds)*time.Second)
}

func WorkerProxy(ctx context.Context) error {
	request, _, err := readWorkerRequest(os.Getenv("MULTICA_REQUEST_DIGEST"), os.Getenv("POD_UID"))
	if err != nil {
		return err
	}
	handler, err := WorkerGateway(request, os.Getenv("MULTICA_DAEMON_PROXY_URL"), os.Getenv("MULTICA_REQUEST_SECRET_NAME"))
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(wire.Value(request.Env, "MULTICA_DAEMON_PORT"))
	if err != nil || port < 1 || port > 65535 {
		return errors.New("invalid worker daemon port")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	defer server.Close()
	if err := os.WriteFile(wire.ControlRoot+"/ready", []byte(strconv.Itoa(port)), 0600); err != nil {
		return err
	}
	defer os.Remove(wire.ControlRoot + "/ready")
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	logger := slog.Default().With("phase", "worker_gateway", "task", request.TaskID, "attempt", request.AttemptID, "provider", request.Provider)
	logger.Info("worker gateway started")
	defer logger.Info("worker gateway stopped")
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), time.Duration(request.TerminationGraceSeconds)*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	case err := <-stopped:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
func WorkerReady() error {
	raw, err := os.ReadFile(wire.ControlRoot + "/ready")
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(string(raw))
	if err != nil || port < 1 || port > 65535 {
		return errors.New("worker gateway not ready")
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/health")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return errors.New("worker gateway unhealthy")
	}
	return nil
}

func WorkerExecute(ctx context.Context, digest, uid string, streams ProcessStreams) error {
	request, manifest, err := readWorkerRequest(digest, uid)
	if err != nil {
		return err
	}
	if err := WorkerReady(); err != nil {
		return err
	}
	lock, err := os.OpenFile(wire.ControlRoot+"/execution.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.New("worker already has an execution")
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	marker := wire.ControlRoot + "/executed"
	f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("worker attempt has already executed")
	}
	if err := f.Close(); err != nil {
		return err
	}
	vars, err := SelectedEnvironment(manifest, os.Environ(), runtimeimage.Locations{Home: wire.Home, TmpDir: "/tmp", Workspace: request.WorkDir})
	if err != nil {
		return err
	}
	values := map[string]string{}
	for _, entry := range append(vars, request.Env...) {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	// Actual provider aliases are links in this task's control volume. No worker
	// path contains the controller's core hardlink shims.
	bin := wire.ControlRoot + "/providers"
	if err := os.MkdirAll(bin, 0700); err != nil {
		return err
	}
	for id := range manifest.Providers {
		path, err := runtimeimage.ProviderPath(manifest, id)
		if err != nil {
			return err
		}
		target := filepath.Join(bin, wire.Alias(id))
		if err := os.Symlink(path, target); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	basePath := wire.Value(vars, "PATH")
	parts := []string{bin, wire.ControllerRoot}
	for _, part := range strings.Split(basePath, ":") {
		if part != "" && !strings.HasPrefix(part, wire.ControllerRoot) {
			parts = append(parts, part)
		}
	}
	values["PATH"] = strings.Join(parts, ":")
	values["HOME"], values["TMPDIR"] = wire.Home, "/tmp"
	for key := range values {
		if strings.HasPrefix(key, "MULTICA_") && strings.HasSuffix(key, "_PATH") {
			delete(values, key)
		}
	}
	values["MULTICA_REPO_CHECKOUT_MODE"] = "isolated"
	if request.Provider == "codex" {
		values["CODEX_HOME"] = wire.Home + "/.codex"
	}
	path, err := runtimeimage.ProviderPath(manifest, request.Provider)
	if err != nil {
		return err
	}
	result := RunProcess(ctx, path, request.Args, wire.Environment(values), request.WorkDir, time.Duration(request.TerminationGraceSeconds)*time.Second, streams)
	if result.Err != nil && !result.Exited {
		return fmt.Errorf("provider_start: %w", result.Err)
	}
	return ResultError(result)
}
