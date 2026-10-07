package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

var (
	errGatewayUnavailable = errors.New("supervisor gateway unavailable")
	errActionRefused      = errors.New("supervisor action refused")
	errStopRequested      = errors.New("worker stop requested")
)

// Kubelet may publish the running image after the process starts. Only the
// read-only input and same-key admission can wait for that observation.
func startupControl(ctx context.Context, client *http.Client, b wire.Bootstrap, action string, body, result any) (resultErr error) {
	method := http.MethodGet
	switch action {
	case "input":
	case "admit", "stop-admit":
		method = http.MethodPost
	default:
		return errors.New("action cannot be retried during startup")
	}
	finish := diagnostics.StartPhase("worker_"+action, diagnostics.TaskAttributes(b.TaskID, b.AttemptID)...)
	defer func() { finish(resultErr) }()
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		err := control(ctx, client, b, method, action, body, result)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errGatewayUnavailable) {
			return diagnostics.Wrap("worker_"+action+"_invalid", err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return diagnostics.Wrap("worker_"+action+"_timeout", ctx.Err())
		case <-timer.C:
		}
	}
}

func gatewayClient() *http.Client {
	return &http.Client{
		Transport:     &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext, ResponseHeaderTimeout: 30 * time.Second},
		Timeout:       60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

type taskRelay struct {
	proxy          http.Handler
	proxyTransport http.RoundTripper
	bootstrap      wire.Bootstrap
	checkoutHTTP   *http.Client
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.Mutex
	stopped        bool
	active         sync.WaitGroup
	checkoutSlot   chan struct{}
}

func relay(b wire.Bootstrap, client *http.Client) *taskRelay {
	ctx, cancel := context.WithCancel(context.Background())
	relay := &taskRelay{bootstrap: b, checkoutHTTP: checkoutClient(), ctx: ctx, cancel: cancel, checkoutSlot: make(chan struct{}, 1)}
	transport := client.Transport
	if base, ok := transport.(*http.Transport); ok {
		// Native skill import/refresh can fetch for 45 seconds before responding.
		copy := base.Clone()
		copy.ResponseHeaderTimeout = time.Minute
		transport = copy
	}
	relay.proxyTransport = transport
	relay.proxy = &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.Out.URL.Scheme = "http"
			p.Out.URL.Host = strings.TrimPrefix(b.GatewayURL, "http://")
			p.Out.Host = p.Out.URL.Host
			p.Out.Header.Del("X-Multica-Attempt-Capability")
			if strings.HasPrefix(p.In.Header.Get("Authorization"), "Bearer mat_") {
				p.Out.Header.Set("X-Multica-Attempt-Capability", b.APICapability)
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "task relay unavailable", http.StatusBadGateway)
		},
	}
	return relay
}

func (s *taskRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		http.Error(w, "task execution has ended", http.StatusForbidden)
		return
	}
	s.active.Add(1)
	s.mu.Unlock()
	defer s.active.Done()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	if r.URL.Path != "/repo/checkout" {
		s.proxy.ServeHTTP(w, r.WithContext(ctx))
		return
	}
	ctx, cancelCheckout := context.WithTimeout(ctx, wire.CheckoutTimeout)
	defer cancelCheckout()
	select {
	case s.checkoutSlot <- struct{}{}:
		defer func() { <-s.checkoutSlot }()
	case <-ctx.Done():
		http.Error(w, "repository checkout cancelled", http.StatusRequestTimeout)
		return
	}
	s.checkout(w, r.WithContext(ctx))
}

// No PID 1 checkout handler may write after the worker seals its filesystem.
func (s *taskRelay) stopCheckouts(ctx context.Context) error {
	if err := s.stopRequests(ctx); err != nil {
		return err
	}
	return retryControl(ctx, s.checkoutHTTP, s.bootstrap, "checkout-stop", struct{}{}, nil)
}

func (s *taskRelay) stopRequests(ctx context.Context) error {
	s.mu.Lock()
	s.stopped = true
	s.cancel()
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.active.Wait(); close(done) }()
	select {
	case <-done:
		s.checkoutHTTP.CloseIdleConnections()
		if transport, ok := s.proxyTransport.(interface{ CloseIdleConnections() }); ok {
			transport.CloseIdleConnections()
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *taskRelay) checkout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.RawPath != "" || r.URL.RawQuery != "" {
		http.Error(w, "invalid repository checkout request", http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		http.Error(w, "repository checkout request is too large", http.StatusBadRequest)
		return
	}
	var input wire.CheckoutRequest
	if json.Unmarshal(raw, &input) != nil {
		http.Error(w, "invalid repository checkout request", http.StatusBadRequest)
		return
	}
	request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, s.bootstrap.GatewayURL+"/internal/attempts/"+s.bootstrap.AttemptID+"/checkout", bytes.NewReader(raw))
	if err != nil {
		http.Error(w, "repository checkout unavailable", http.StatusBadGateway)
		return
	}
	request.Header.Set("Authorization", "Bearer "+s.bootstrap.SupervisorCapability)
	request.Header.Set(wire.CheckoutTaskAuthorizationHeader, r.Header.Get("Authorization"))
	request.Header.Set("Content-Type", "application/json")
	response, err := s.checkoutHTTP.Do(request)
	if err != nil {
		http.Error(w, "repository checkout unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, io.LimitReader(response.Body, 4096))
		return
	}
	var result wire.CheckoutResult
	if json.NewDecoder(io.LimitReader(response.Body, 128<<10)).Decode(&result) != nil {
		http.Error(w, "invalid repository checkout result", http.StatusBadGateway)
		return
	}
	name, err := checkout.DirectoryName(input.URL)
	if err != nil || result.Path != filepath.Join(s.bootstrap.TaskRoot, "workdir", name) {
		http.Error(w, "repository checkout path differs from task", http.StatusBadGateway)
		return
	}
	if err := observeCheckout(r.Context(), s.bootstrap.TaskRoot, name); err != nil {
		http.Error(w, "repository checkout is not visible on the task mount", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// Cache refresh and local copying share the outer checkout deadline.
func checkoutClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:            (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
			MaxResponseHeaderBytes: 128 << 10,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// A controller-local rename does not immediately invalidate a node's cached
// negative NFS lookup. Observe the published repository before the CLI uses it.
func observeCheckout(ctx context.Context, taskRoot, name string) error {
	root, err := os.OpenRoot(taskRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	refreshed := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := root.Stat(filepath.Join("workdir", name, ".git", "config"))
		if err == nil {
			if !info.Mode().IsRegular() {
				return errors.New("repository config is not a regular file")
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ESTALE) {
			return err
		}
		if !refreshed {
			refreshed = true
			refreshCheckoutDirectory(root)
			continue
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func control(ctx context.Context, client *http.Client, b wire.Bootstrap, method, action string, body, result any) error {
	token := b.SupervisorCapability
	switch action {
	case "stop-admit", "stop-control", "stop-request", "stop-receipt", "checkout-stop":
		token = b.StopCapability
	}
	limit := int64(wire.MaxRequestBytes)
	if action == "input" {
		limit = wire.MaxAssignmentBytes
	}
	return gatewayControl(ctx, client, b.GatewayURL+"/internal/attempts/"+b.AttemptID+"/"+action, token, method, body, result, limit, diagnostics.TaskAttributes(b.TaskID, b.AttemptID)...)
}

func gatewayControl(ctx context.Context, client *http.Client, endpoint, token, method string, body, result any, limit int64, attributes ...any) error {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	if int64(len(raw)) > wire.MaxAssignmentBytes {
		return errors.New("supervisor request exceeds limit")
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return diagnostics.Wrap("worker_control_transport_failed", errGatewayUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		reason, cause := "worker_control_refused", errActionRefused
		if response.StatusCode == http.StatusGone {
			reason, cause = "worker_control_stopped", errStopRequested
		} else if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests {
			reason, cause = "worker_control_unavailable", errGatewayUnavailable
		}
		slog.Warn("worker control request failed", append(attributes, "action", filepath.Base(endpoint), "status", response.StatusCode, "reason", reason)...)
		return diagnostics.Wrap(reason, cause)
	}
	if result != nil {
		raw, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
		if int64(len(raw)) > limit {
			return errors.New("supervisor response exceeds limit")
		}
		if err != nil {
			return errGatewayUnavailable
		}
		return json.Unmarshal(raw, result)
	}
	// Consume the small acknowledgement so the next startup or event request
	// can reuse this connection. Do not retain an unbounded response body.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<10))
	return nil
}

// Monotonic closure and immutable result/evidence submissions may be replayed.
// Start never enters this path; permanent rejection returns immediately.
func retryControl(ctx context.Context, client *http.Client, b wire.Bootstrap, action string, body, result any) error {
	for {
		err := control(ctx, client, b, http.MethodPost, action, body, result)
		if err == nil || !errors.Is(err, errGatewayUnavailable) {
			return err
		}
		if err := waitControl(ctx); err != nil {
			return err
		}
	}
}

func waitControl(ctx context.Context) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
