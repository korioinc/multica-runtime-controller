package execution

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func taskRoute(r *http.Request) bool {
	return r.URL.RawPath == "" && r.URL.Opaque == "" && r.URL.RawQuery == "" && (r.Method == http.MethodPost && r.URL.Path == "/repo/checkout" || r.Method == http.MethodGet && r.URL.Path == "/health")
}
func startBroker(request wire.Request) (int, string, func(), error) {
	port, err := strconv.Atoi(wire.Value(request.Env, "MULTICA_DAEMON_PORT"))
	if err != nil || port < 1 || port > 65535 {
		return 0, "", nil, errors.New("invalid daemon port")
	}
	client, err := official.NewCheckoutClient("http://127.0.0.1:"+strconv.Itoa(port), nil)
	if err != nil {
		return 0, "", nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, "", nil, err
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		listener.Close()
		return 0, "", nil, err
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !taskRoute(r) || r.Method != http.MethodPost || subtle.ConstantTimeCompare([]byte(r.Header.Get(wire.CapabilityHeader)), []byte(token)) != 1 {
			http.Error(w, "task checkout unauthorized", http.StatusForbidden)
			return
		}
		var plan wire.Plan
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&plan) != nil || plan.TaskID != request.TaskID || !slices.Contains(request.RepositoryURLs, strings.TrimSpace(plan.URL)) {
			http.Error(w, "repository is outside the observed task scope", http.StatusForbidden)
			return
		}
		stage, err := os.MkdirTemp(request.WorkDir, ".checkout-")
		if err != nil {
			http.Error(w, "checkout staging unavailable", 500)
			return
		}
		defer os.RemoveAll(stage)
		canonical, err := filepath.EvalSymlinks(stage)
		if err != nil || canonical != stage {
			http.Error(w, "checkout staging changed", 500)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
		defer cancel()
		result, err := client.Checkout(ctx, official.NativeCheckoutRequest{URL: plan.URL, Ref: plan.Ref, TaskID: request.TaskID, WorkspaceID: wire.Value(request.Env, "MULTICA_WORKSPACE_ID"), WorkDir: stage, Token: wire.Value(request.Env, "MULTICA_TOKEN"), RetryBusy: true})
		if err != nil {
			var refusal *official.CheckoutError
			if errors.As(err, &refusal) {
				for k, v := range refusal.Header {
					w.Header()[k] = v
				}
				w.WriteHeader(refusal.StatusCode)
				_, _ = w.Write(refusal.Body)
			} else {
				http.Error(w, "official checkout failed", 502)
			}
			return
		}
		if err := checkout.Standalone(result.Path); err != nil {
			http.Error(w, "checkout is not standalone", 502)
			return
		}
		w.Header().Set("Content-Type", wire.ArchiveType)
		w.Header().Set(wire.BranchHeader, base64.RawURLEncoding.EncodeToString([]byte(result.BranchName)))
		if err := checkout.WriteArchive(w, result.Path); err != nil {
			panic(http.ErrAbortHandler)
		}
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().(*net.TCPAddr).Port, token, func() { _ = server.Close(); client.CloseIdleConnections() }, nil
}

func ControllerGateway(resources *kubernetes.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			return
		}
		if !taskRoute(r) {
			http.Error(w, "operation unavailable", 403)
			return
		}
		task, token, secret := r.Header.Get(wire.TaskHeader), r.Header.Get(wire.TokenHeader), r.Header.Get(wire.SecretHeader)
		if !wire.UUID(task) || token == "" || secret == "" {
			http.Error(w, "task authorization required", 401)
			return
		}
		request, err := resources.Request(r.Context(), secret, task)
		if err != nil || subtle.ConstantTimeCompare([]byte(token), []byte(wire.Value(request.Env, "MULTICA_TOKEN"))) != 1 {
			http.Error(w, "task authorization failed", 401)
			return
		}
		if r.URL.Path == "/health" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"ok"}`)
			return
		}
		target := &url.URL{Scheme: "http", Host: "127.0.0.1:" + strconv.Itoa(request.BrokerPort)}
		proxy := &httputil.ReverseProxy{Rewrite: func(p *httputil.ProxyRequest) {
			p.SetURL(target)
			p.Out.Header.Del(wire.SecretHeader)
			p.Out.Header.Del(wire.TaskHeader)
			p.Out.Header.Del(wire.TokenHeader)
			p.Out.Header.Set(wire.CapabilityHeader, request.BrokerToken)
		}, ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "checkout transport unavailable", 502)
		}}
		proxy.ServeHTTP(w, r)
	})
}

func WorkerGateway(request wire.Request, origin, secret string) (http.Handler, error) {
	target, err := url.Parse(origin)
	if err != nil || target.Scheme != "http" || target.Host == "" || target.Path != "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" {
		return nil, errors.New("invalid controller gateway origin")
	}
	publisher, err := checkout.New(request.WorkDir, request.TaskID, request.Env)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: &http.Transport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !taskRoute(r) {
			http.Error(w, "operation unavailable", 403)
			return
		}
		if r.URL.Path == "/health" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"ok"}`)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		var plan wire.Plan
		if err != nil || json.Unmarshal(raw, &plan) != nil || plan.TaskID != request.TaskID {
			http.Error(w, "invalid checkout plan", 400)
			return
		}
		upstream, err := http.NewRequestWithContext(r.Context(), http.MethodPost, origin+"/repo/checkout", bytes.NewReader(raw))
		if err != nil {
			http.Error(w, "checkout unavailable", 500)
			return
		}
		upstream.Header.Set(wire.SecretHeader, secret)
		upstream.Header.Set(wire.TaskHeader, request.TaskID)
		upstream.Header.Set(wire.TokenHeader, wire.Value(request.Env, "MULTICA_TOKEN"))
		upstream.Header.Set("Content-Type", "application/json")
		response, err := client.Do(upstream)
		if err != nil {
			http.Error(w, "checkout transport failed", 502)
			return
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			for k, v := range response.Header {
				w.Header()[k] = v
			}
			w.WriteHeader(response.StatusCode)
			_, _ = io.Copy(w, io.LimitReader(response.Body, 1<<20))
			return
		}
		branch, err := base64.RawURLEncoding.DecodeString(response.Header.Get(wire.BranchHeader))
		if err != nil || response.Header.Get("Content-Type") != wire.ArchiveType {
			http.Error(w, "invalid checkout archive", 502)
			return
		}
		result, err := publisher.Publish(r.Context(), plan, string(branch), response.Body)
		if err != nil {
			http.Error(w, "checkout publication refused", 409)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}), nil
}
