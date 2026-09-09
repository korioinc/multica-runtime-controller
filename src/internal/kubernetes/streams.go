package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"

	"github.com/go-logr/logr"
	streamprotocol "k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	utilexec "k8s.io/client-go/util/exec"
	"k8s.io/klog/v2"
)

var ErrTransport = errors.New("task transport failed")

type Streams struct {
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}

func streamExec(ctx context.Context, config *rest.Config, endpoint *url.URL, streams Streams) error {
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return fmt.Errorf("%w: configure exec upgrade: %w", ErrTransport, err)
	}
	executor, err := remotecommand.NewSPDYExecutorForProtocols(transport, statusUpgrader{delegate: upgrader}, "POST", endpoint, streamprotocol.StreamProtocolV4Name)
	if err != nil {
		return fmt.Errorf("%w: create exec stream: %w", ErrTransport, err)
	}
	return transferStreams(ctx, executor, streams)
}

func transferStreams(ctx context.Context, executor remotecommand.Executor, streams Streams) error {
	streamContext, cancel := context.WithCancel(ctx)
	defer cancel()
	fault := &streamFault{cancel: cancel}
	options := remotecommand.StreamOptions{}
	if streams.Stdin != nil {
		options.Stdin = trackedReader{source: streams.Stdin, fault: fault}
	}
	if streams.Stdout != nil {
		options.Stdout = trackedWriter{target: streams.Stdout, fault: fault}
	}
	if streams.Stderr != nil {
		options.Stderr = trackedWriter{target: streams.Stderr, fault: fault}
	}
	err := executor.StreamWithContext(klog.NewContext(streamContext, logr.Discard()), options)
	if localErr := fault.failure(); localErr != nil {
		return fmt.Errorf("%w: task stream endpoint failed: %w", ErrTransport, localErr)
	}
	if _, exited := ExitCode(err); err != nil && !exited {
		return fmt.Errorf("%w: %w", ErrTransport, err)
	}
	return err
}

func ExitCode(err error) (int, bool) {
	if err == nil {
		return 0, true
	}
	var code utilexec.ExitError
	if errors.As(err, &code) {
		return code.ExitStatus(), true
	}
	return 1, false
}

// client-go reports remote exit status separately from local io.Copy failures.
// Capture broken local endpoints and cancel the transport instead of returning
// a successful remote status after silently dropping input or output bytes.
type streamFault struct {
	mu     sync.Mutex
	err    error
	cancel context.CancelFunc
}

func (f *streamFault) record(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
	f.cancel()
}
func (f *streamFault) failure() error { f.mu.Lock(); defer f.mu.Unlock(); return f.err }

type trackedWriter struct {
	target io.Writer
	fault  *streamFault
}

func (w trackedWriter) Write(data []byte) (int, error) {
	n, err := w.target.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	w.fault.record(err)
	return n, err
}

type trackedReader struct {
	source io.Reader
	fault  *streamFault
}

func (r trackedReader) Read(data []byte) (int, error) {
	n, err := r.source.Read(data)
	if !errors.Is(err, io.EOF) {
		r.fault.record(err)
	}
	return n, err
}
