// Package daemonapi implements the official daemon HTTP contract.
package daemonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

const MaxPayload = 8 << 20

// ResponseError deliberately excludes URL, credentials, and backend bodies.
type ResponseError struct{ StatusCode int }

func (e *ResponseError) Error() string { return fmt.Sprintf("daemon API HTTP %d", e.StatusCode) }

type requestNotSent struct{}

func (*requestNotSent) Error() string { return "daemon API request not sent" }

// RequestNotSent identifies failures known to precede acquiring any connection.
// All other transport failures remain ambiguous and must not replay a terminal.
func RequestNotSent(err error) bool {
	var notSent *requestNotSent
	return errors.As(err, &notSent)
}

type Client struct {
	origin, token, version string
	http                   *http.Client
}

func NewClient(origin, token, version string, client *http.Client) (*Client, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || token == "" || strings.ContainsAny(token, "\r\n") || strings.TrimSpace(version) != version || version == "" || strings.ContainsAny(version, "\x00\r\n") {
		return nil, errors.New("invalid daemon API configuration")
	}
	// Backend URLs must support both HTTP and HTTPS.
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("daemon API requires HTTP or HTTPS")
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if copyClient.Timeout == 0 {
		copyClient.Timeout = 30 * time.Second
	}
	return &Client{origin: strings.TrimSuffix(origin, "/"), token: token, version: version, http: &copyClient}, nil
}

func segment(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 512 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == ':') {
			return false
		}
	}
	return true
}

func (c *Client) request(ctx context.Context, method, path, token string, body []byte) ([]byte, error) {
	return c.send(ctx, method, path, token, "", body)
}
func (c *Client) send(ctx context.Context, method, path, token, workspaceID string, body []byte) ([]byte, error) {
	data, _, err := c.sendResponse(ctx, method, path, token, workspaceID, body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// sendResponse retains headers for paginated observations and bounded error
// bodies for explicit capability responses. Neither is included in errors.
func (c *Client) sendResponse(ctx context.Context, method, path, token, workspaceID string, body []byte) ([]byte, http.Header, error) {
	req, err := c.newRequest(ctx, method, path, token, workspaceID, body)
	if err != nil {
		return nil, nil, err
	}
	var connected atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }}))
	resp, err := c.http.Do(req)
	if err != nil {
		var op *net.OpError
		if !connected.Load() && errors.As(err, &op) && op.Op == "dial" {
			return nil, nil, &requestNotSent{}
		}
		return nil, nil, errors.New("daemon API transport outcome uncertain")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxPayload+1))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if err != nil || len(data) > MaxPayload {
			data = nil
		}
		return data, resp.Header, &ResponseError{StatusCode: resp.StatusCode}
	}
	if err != nil || len(data) > MaxPayload {
		return nil, nil, errors.New("daemon API response outcome uncertain")
	}
	return data, resp.Header, nil
}

func (c *Client) newRequest(ctx context.Context, method, path, token, workspaceID string, body []byte) (*http.Request, error) {
	if len(body) > MaxPayload {
		return nil, errors.New("daemon API payload exceeds limit")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.origin+path, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("invalid daemon API request")
	}
	if ctx.Err() != nil {
		return nil, &requestNotSent{}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	if workspaceID != "" {
		req.Header.Set("X-Workspace-ID", workspaceID)
	}
	req.Header.Set("X-Client-Platform", "daemon")
	req.Header.Set("X-Client-Version", c.version)
	req.Header.Set("X-Client-OS", runtime.GOOS)
	req.Header.Set("X-Client-Capabilities", "skill-bundles-v1,coalesced-comments-v1,execution-manifest-v1,agent-skill-v1,remote-mcp-v1,local-worktree-v1,source_context_quick_create_v1,rpc-v1,platform-skill-v1,checkout-keeps-work-v1")
	return req, nil
}

// Skill bundles use the native daemon's size-scaled context, not the control
// API's timeout or buffered response limit. Only the scoped gateway calls this.
func (c *Client) skillBundle(ctx context.Context, path string, body []byte) (*http.Response, error) {
	req, err := c.newRequest(ctx, http.MethodPost, path, c.token, "", body)
	if err != nil {
		return nil, err
	}
	client := *c.http
	client.Timeout = 0
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("daemon API transport outcome uncertain")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, &ResponseError{StatusCode: resp.StatusCode}
	}
	return resp, nil
}

func (c *Client) json(ctx context.Context, method, path string, body, output any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return errors.New("invalid daemon API body")
		}
	}
	data, err = c.request(ctx, method, path, c.token, data)
	if err != nil {
		return err
	}
	if output != nil && json.Unmarshal(data, output) != nil {
		return errors.New("invalid daemon API response")
	}
	return nil
}

type Workspace struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type Runtime struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Provider string `json:"provider"`
	Status   string `json:"status"`
}
type Inventory struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Version string `json:"version"`
	Status  string `json:"status"`
}
type Registration struct {
	WorkspaceID string      `json:"workspace_id"`
	DaemonID    string      `json:"daemon_id"`
	DeviceName  string      `json:"device_name"`
	CLIVersion  string      `json:"cli_version"`
	Runtimes    []Inventory `json:"runtimes"`
}
type Repository struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
	Ref         string `json:"ref,omitempty"`
}
type RegisterResponse struct {
	Runtimes     []Runtime       `json:"runtimes"`
	Repos        []Repository    `json:"repos"`
	ReposVersion string          `json:"repos_version"`
	Settings     json.RawMessage `json:"settings,omitempty"`
}

func (c *Client) Workspaces(ctx context.Context) ([]Workspace, error) {
	var result []Workspace
	err := c.json(ctx, http.MethodGet, "/api/daemon/workspaces", nil, &result)
	return result, err
}
func (c *Client) Register(ctx context.Context, request Registration) (RegisterResponse, error) {
	var result RegisterResponse
	if !segment(request.WorkspaceID) || !segment(request.DaemonID) || len(request.Runtimes) == 0 {
		return result, errors.New("invalid registration")
	}
	request.CLIVersion = c.version
	err := c.json(ctx, http.MethodPost, "/api/daemon/register", request, &result)
	return result, err
}
func (c *Client) Claim(ctx context.Context, daemonID string, runtimeIDs []string, maxTasks int) ([]json.RawMessage, error) {
	if !segment(daemonID) || len(runtimeIDs) == 0 || maxTasks < 1 {
		return nil, errors.New("invalid claim")
	}
	for _, id := range runtimeIDs {
		if !segment(id) {
			return nil, errors.New("invalid runtime")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var result struct {
		Tasks []json.RawMessage `json:"tasks"`
	}
	err := c.json(ctx, http.MethodPost, "/api/daemon/tasks/claim", map[string]any{"daemon_id": daemonID, "runtime_ids": runtimeIDs, "max_tasks": maxTasks}, &result)
	if err == nil && len(result.Tasks) > maxTasks {
		return nil, errors.New("claim exceeded capacity")
	}
	return result.Tasks, err
}
func (c *Client) Heartbeat(ctx context.Context, runtimeID string) error {
	if !segment(runtimeID) {
		return errors.New("invalid runtime")
	}
	return c.json(ctx, http.MethodPost, "/api/daemon/heartbeat", map[string]any{"runtime_id": runtimeID, "supports_batch_import": false}, nil)
}
func (c *Client) PrepareLease(ctx context.Context, runtimeID, taskID string) error {
	if !segment(runtimeID) || !segment(taskID) {
		return errors.New("invalid lease scope")
	}
	return c.json(ctx, http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/"+taskID+"/prepare-lease", struct{}{}, nil)
}
func (c *Client) TaskStatus(ctx context.Context, taskID string) (string, error) {
	if !segment(taskID) {
		return "", errors.New("invalid task")
	}
	var result struct {
		Status string `json:"status"`
	}
	err := c.json(ctx, http.MethodGet, "/api/daemon/tasks/"+taskID+"/status", nil, &result)
	if err == nil && result.Status == "" {
		err = errors.New("missing task status")
	}
	return result.Status, err
}

// Terminal performs exactly one request. The durable outbox owns any reconciliation.
func (c *Client) Terminal(ctx context.Context, taskID, kind string, body []byte) error {
	if !segment(taskID) || !terminalKind(kind) || !json.Valid(body) {
		return errors.New("invalid terminal callback")
	}
	_, err := c.request(ctx, http.MethodPost, "/api/daemon/tasks/"+taskID+"/"+kind, c.token, body)
	return err
}
func terminalKind(kind string) bool {
	return kind == "complete" || kind == "fail" || kind == "cancel-ack"
}

func (c *Client) Start(ctx context.Context, taskID string) error {
	if !segment(taskID) {
		return errors.New("invalid task")
	}
	return c.json(ctx, http.MethodPost, "/api/daemon/tasks/"+taskID+"/start", struct{}{}, nil)
}

func (c *Client) Report(ctx context.Context, taskID, kind string, body any) error {
	if !segment(taskID) || (kind != "messages" && kind != "usage" && kind != "progress") {
		return errors.New("invalid task report")
	}
	return c.json(ctx, http.MethodPost, "/api/daemon/tasks/"+taskID+"/"+kind, body, nil)
}
