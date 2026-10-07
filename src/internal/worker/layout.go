// Package worker owns provider execution in a prepared task Pod.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func readPodBootstrap(path string) (wire.Bootstrap, *wire.SessionBootstrap, []byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return wire.Bootstrap{}, nil, nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, wire.MaxRequestBytes+1))
	if err != nil || len(raw) > wire.MaxRequestBytes {
		return wire.Bootstrap{}, nil, nil, errors.New("worker bootstrap could not be read")
	}
	var identity struct {
		Version *int `json:"version"`
	}
	if json.Unmarshal(raw, &identity) != nil {
		return wire.Bootstrap{}, nil, nil, errors.New("invalid worker bootstrap")
	}
	if identity.Version != nil {
		session, err := wire.DecodeSessionBootstrap(raw)
		return wire.Bootstrap{}, &session, raw, err
	}
	b, err := wire.DecodeBootstrap(raw)
	if err == nil && b.WorkerSessionID != "" {
		err = errors.New("turn authority cannot be mounted as a session bootstrap")
	}
	return b, nil, raw, err
}

// Layout initializes only private scratch in the init container.
func Layout(ctx context.Context, privateRoot, requestPath string) (resultErr error) {
	b, session, _, err := readPodBootstrap(requestPath)
	if err != nil {
		return err
	}
	finish := diagnostics.StartPhase("worker_layout", diagnostics.TaskAttributes(b.TaskID, b.AttemptID)...)
	defer func() { finish(resultErr) }()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	client := gatewayClient()
	defer client.CloseIdleConnections()
	go func() {
		for ctx.Err() == nil {
			stopped := false
			if session != nil {
				var command wire.SessionStopCommand
				stopped = sessionControl(ctx, client, *session, http.MethodGet, "stop-control", nil, &command) == nil && command.Revision != 0
			} else {
				var command wire.StopCommand
				stopped = control(ctx, client, b, http.MethodGet, "stop-control", nil, &command) == nil && command.Cancel
			}
			if stopped {
				cancel(errStopRequested)
				return
			}
			if waitControl(ctx) != nil {
				return
			}
		}
	}()
	// Image verification only reads installed files and starts no processes.
	// Returning on cancellation lets PID 1 exit even during a large file hash;
	// the init never receives a task volume or provider execution authority.
	type verification struct {
		descriptor runtimeimage.Descriptor
		digest     string
		err        error
	}
	verified := make(chan verification, 1)
	go func() {
		d, digest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
		verified <- verification{d, digest, err}
	}()
	var checked verification
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case checked = <-verified:
	}
	if checked.err != nil {
		return checked.err
	}
	d, digest := checked.descriptor, checked.digest
	ref := b.RuntimeRef
	if session != nil {
		ref = session.RuntimeRef
	}
	if err := runtimeimage.Match(d, digest, ref); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	for _, directory := range []string{privateRoot} {
		resolved, err := filepath.EvalSymlinks(directory)
		if err != nil || resolved != directory || !filepath.IsAbs(directory) || directory == "/" {
			return errors.New("layout requires real volume roots")
		}
	}
	for _, name := range []string{"run", "tmp"} {
		path := filepath.Join(privateRoot, name)
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() {
			return errors.New("private directory is not a real directory")
		}
		if err := os.Chmod(path, 0700); err != nil {
			return err
		}
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return runtimeimage.PublishReceipt(filepath.Join(privateRoot, "run"), d, digest)
}
