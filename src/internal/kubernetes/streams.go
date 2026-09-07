package kubernetes

import (
	"context"
	"errors"
	"io"
	"sync"
)

var ErrTransport = errors.New("task transport failed")

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
