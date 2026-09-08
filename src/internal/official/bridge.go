// Package official adapts the immutable Multica release daemon. It observes
// successful claims without initiating scheduling, claims or retry requests.
package official

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

const maxProtocolBytes = 16 << 20
const claimRoute = "/api/daemon/tasks/claim"
const registerRoute = "/api/daemon/register"

type BridgeOptions struct {
	BackendURL string
	Store      *workspace.Store
	RuntimeRef runtimeimage.Ref
	Providers  []string
}
type bridge struct {
	target     *url.URL
	store      *workspace.Store
	runtimeRef runtimeimage.Ref
	providers  map[string]bool
	proxy      *httputil.ReverseProxy
}

func NewBridge(options BridgeOptions) (http.Handler, error) {
	origin, err := NormalizeBackendURL(options.BackendURL)
	if err != nil {
		return nil, err
	}
	target, _ := url.Parse(origin)
	if options.Store == nil || options.RuntimeRef.Validate() != nil {
		return nil, errors.New("official bridge requires a verified environment and workspace store")
	}
	enabled, err := providerSet(options.Providers)
	if err != nil {
		return nil, err
	}
	if len(enabled) != len(options.RuntimeRef.Providers) {
		return nil, errors.New("bridge provider set differs from verified environment")
	}
	for id := range options.RuntimeRef.Providers {
		if !enabled[id] {
			return nil, errors.New("bridge provider set differs from verified environment")
		}
	}
	b := &bridge{target: target, store: options.Store, runtimeRef: options.RuntimeRef, providers: enabled}
	slog.Info("backend custom runtime profiles are unsupported", "phase", "official", "imageBuildID", options.RuntimeRef.ImageBuildID)
	b.proxy = &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			slog.Info("backend request started", "phase", "controller")
			request.SetURL(target)
			if intercepted(request.In.Method, request.In.URL.Path) {
				request.Out.Header.Set("Accept-Encoding", "identity")
			}
		},
		ModifyResponse: b.response,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			slog.Warn("backend request failed", "phase", "controller", "error_class", "backend_transport_or_protocol")
			http.Error(w, "official backend protocol validation failed", http.StatusBadGateway)
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/daemon/workspaces/{workspace}/runtime-profiles", func(w http.ResponseWriter, r *http.Request) {
		// This boundary runs before every discovery/refresh probe. Backend custom
		// profiles are intentionally unavailable in this runtime implementation.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(struct {
			WorkspaceID string `json:"workspace_id"`
			Profiles    []any  `json:"runtime_profiles"`
		}{r.PathValue("workspace"), []any{}})
	})
	mux.HandleFunc("POST "+registerRoute, b.registration)
	mux.HandleFunc("GET /api/daemon/ws", b.websocket)
	mux.Handle("/api/daemon/", b.proxy)
	mux.Handle("POST /api/tokens/current/renew", b.proxy)
	return mux, nil
}

func NormalizeBackendURL(raw string) (string, error) {
	target, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	switch target.Scheme {
	case "ws":
		target.Scheme = "http"
	case "wss":
		target.Scheme = "https"
	case "http", "https":
	default:
		return "", errors.New("backend URL requires HTTP(S) or WS(S)")
	}
	if target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" {
		return "", errors.New("backend URL requires an origin without credentials, query or fragment")
	}
	if target.Path == "/ws" {
		target.Path = ""
	}
	target.RawPath = ""
	return strings.TrimRight(target.String(), "/"), nil
}

func intercepted(method, path string) bool {
	return method == http.MethodPost && (path == claimRoute || path == registerRoute)
}

func readProtocol(body io.ReadCloser) ([]byte, error) {
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(body, maxProtocolBytes+1))
	if err != nil || len(raw) > maxProtocolBytes {
		return nil, errors.New("invalid or oversized official protocol body")
	}
	return raw, nil
}

func (b *bridge) registration(w http.ResponseWriter, r *http.Request) {
	if encoding := r.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		http.Error(w, "encoded registration is unsupported", http.StatusBadRequest)
		return
	}
	raw, err := readProtocol(r.Body)
	if err == nil {
		err = b.validateRegistration(raw, "type")
	}
	if err != nil {
		http.Error(w, "registration requires enabled builtins without profiles", http.StatusForbidden)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	r.ContentLength = int64(len(raw))
	b.proxy.ServeHTTP(w, r)
}

func (b *bridge) validateRegistration(raw []byte, providerField string) error {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil || envelope == nil {
		return errors.New("invalid registration")
	}
	values, present := envelope["runtimes"]
	if !present || string(values) == "null" {
		return errors.New("registration requires runtimes")
	}
	var runtimes []map[string]json.RawMessage
	if json.Unmarshal(values, &runtimes) != nil {
		return errors.New("invalid runtime registration")
	}
	for _, runtime := range runtimes {
		var id, profile string
		if json.Unmarshal(runtime[providerField], &id) != nil || !b.providers[id] {
			return errors.New("runtime registration contains an unsupported provider")
		}
		if rawProfile, ok := runtime["profile_id"]; ok {
			if json.Unmarshal(rawProfile, &profile) != nil || profile != "" {
				return errors.New("custom runtime profiles are unsupported")
			}
		}
	}
	return nil
}

func (b *bridge) response(response *http.Response) error {
	slog.Info("backend response received", "phase", "controller", "status", response.StatusCode)
	path := strings.TrimPrefix(response.Request.URL.Path, strings.TrimRight(b.target.Path, "/"))
	if intercepted(response.Request.Method, path) && response.StatusCode >= 300 && response.StatusCode < 400 {
		return errors.New("redirect would bypass the official registration or claim boundary")
	}
	if response.StatusCode != http.StatusOK || !intercepted(response.Request.Method, path) {
		return nil
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return errors.New("encoded intercepted response is unsupported")
	}
	raw, err := readProtocol(response.Body)
	if err != nil {
		return err
	}
	if path == registerRoute {
		err = b.validateRegistration(raw, "provider")
	} else {
		raw, err = b.claim(raw)
	}
	if err != nil {
		return err
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	response.Header.Set("Content-Length", strconv.Itoa(len(raw)))
	response.Header.Del("ETag")
	return nil
}
