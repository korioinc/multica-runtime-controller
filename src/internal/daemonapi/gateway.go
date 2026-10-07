package daemonapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type Bootstrap struct {
	Workspace    Workspace
	Runtime      Runtime
	ReposVersion string
	Settings     json.RawMessage
}

// Gateway exposes task-scoped CLI and contribution routes. Admission compares the
// recorded identity against actual Pod, PVC, image, and mounted resources.
type Gateway struct {
	mcpCalls  map[string]int
	Store     *workspace.Store
	Client    *Client
	Admission func(context.Context, workspace.TaskGrant) error
	mu        sync.Mutex
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A stalled reader must not retain the task coordination lock indefinitely.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(60 * time.Second))
	if r.URL.RawPath != "" || r.URL.ForceQuery || r.URL.Fragment != "" || path.Clean(r.URL.Path) != r.URL.Path || strings.ContainsAny(r.URL.Path, "\\\x00\r\n") || r.URL.IsAbs() {
		reject(w, 403)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	for _, part := range parts {
		if !segment(part) {
			reject(w, 403)
			return
		}
	}
	// Local policy rejections never consume a request body or invoke services.
	// Every permitted request still passes the task authorization checks below.
	if reason := blockedEndpointReason(r.Method, r.URL.Path); reason != "" {
		rejectBlockedEndpoint(w, r, reason)
		return
	}
	if g.Store == nil || g.Client == nil || g.Admission == nil {
		reject(w, 503)
		return
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == r.Header.Get("Authorization") || token == "" {
		reject(w, 401)
		return
	}
	if !strings.HasPrefix(token, "mat_") {
		reject(w, 403)
		return
	}
	capability := r.Header.Get("X-Multica-Attempt-Capability")
	grant, err := g.Store.Authorize(capability, "daemon")
	if err != nil {
		reject(w, 403)
		return
	}
	gate := g.Store.ExecutionGate(grant.AttemptID)
	gate.RLock()
	defer gate.RUnlock()
	grant, err = g.Store.Authorize(capability, "daemon")
	if err != nil {
		reject(w, 403)
		return
	}
	w = &authorizedResponse{ResponseWriter: w, check: func() error { _, err := g.Store.Authorize(capability, "daemon"); return err }}
	if err = g.Admission(r.Context(), grant); err != nil {
		reject(w, 403)
		return
	}
	claim, err := ParseClaim(grant.Envelope)
	if err != nil || claim.ID != grant.TaskID || claim.RuntimeID != grant.RuntimeID || claim.WorkspaceID != grant.WorkspaceID || claim.AgentID != grant.AgentID {
		reject(w, 503)
		return
	}
	if token != claim.AuthToken {
		reject(w, 403)
		return
	}
	if endpointNamespace(r.URL.Path, "/api/task-mcp") {
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxPayload+1))
		if err != nil || len(body) > MaxPayload || (len(body) > 0 && !json.Valid(body)) {
			reject(w, 400)
			return
		}
		g.taskMCP(w, r, grant, claim, body)
		return
	}
	if !endpointNamespace(r.URL.Path, "/api") || !proxyMethod(r.Method) {
		reject(w, 403)
		return
	}
	g.proxy(w, r, grant, token)
}

func reject(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte("{\"error\":\"task API request rejected\"}"))
}
func writeJSON(w http.ResponseWriter, body any) {
	raw, err := json.Marshal(body)
	if err != nil {
		reject(w, 503)
		return
	}
	writeBody(w, raw)
}
func writeBody(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// The response check is a disclosure authorization point. Already-authorized
// in-flight requests may finish, but a revoked task cannot begin a new response.
type authorizedResponse struct {
	http.ResponseWriter
	check           func() error
	started, denied bool
}

func (w *authorizedResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *authorizedResponse) WriteHeader(status int) {
	// ReverseProxy can emit informational responses before the final status.
	// They must not consume the disclosure check or suppress a later denial.
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		return
	}
	if w.started {
		return
	}
	w.started = true
	if w.check() != nil {
		w.denied = true
		clear(w.Header())
		http.Error(w.ResponseWriter, "task authority expired", 403)
		return
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *authorizedResponse) Write(raw []byte) (int, error) {
	if !w.started {
		w.WriteHeader(http.StatusOK)
	}
	if w.denied {
		return 0, workspace.ErrUnauthorized
	}
	return w.ResponseWriter.Write(raw)
}
func (w *authorizedResponse) FlushError() error {
	if !w.started {
		w.WriteHeader(http.StatusOK)
	}
	if w.denied {
		return workspace.ErrUnauthorized
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
