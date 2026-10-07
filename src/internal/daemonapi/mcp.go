package daemonapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/remotemcp"
)

type pluginTool struct {
	InstallationID string          `json:"installation_id"`
	HookKey        string          `json:"hook_key"`
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	InputSchema    json.RawMessage `json:"input_schema"`
}

type taskMCPInput struct {
	Connections []remotemcp.Connection `json:"remote_mcp_connections"`
	Hooks       []pluginTool           `json:"plugin_hook_tools"`
}

func remoteServerName(connection remotemcp.Connection) string {
	name := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, strings.ToLower(connection.ContributionKey))
	suffix := strings.ReplaceAll(connection.ContributionID, "-", "")
	if len(suffix) > 8 {
		suffix = suffix[:8]
	}
	return "plugin-" + name + "-" + suffix
}

// PrepareMCP verifies discovery before Prepared is committed. Worker URLs reach
// this controller through the existing authenticated loopback task relay.
func (c *Client) PrepareMCP(ctx context.Context, claim Claim) (json.RawMessage, error) {
	checked, err := ParseClaim(claim.Envelope)
	if err != nil || checked.ID != claim.ID || checked.RuntimeID != claim.RuntimeID || checked.WorkspaceID != claim.WorkspaceID {
		return nil, errors.New("MCP task identity differs from claim")
	}
	claim = checked
	var input taskMCPInput
	if json.Unmarshal(claim.Envelope, &input) != nil {
		return nil, errors.New("invalid task MCP input")
	}
	servers := map[string]any{}
	add := func(name string) error {
		if !segment(name) || servers[name] != nil {
			return errors.New("task MCP server identity collision")
		}
		servers[name] = map[string]any{"type": "http", "url": wire.RelayURL + "/api/task-mcp/" + claim.ID + "/" + name, "headers": map[string]string{"Authorization": "Bearer " + claim.AuthToken}}
		return nil
	}
	if len(input.Hooks) > 0 {
		if claim.RemoteMCPDaemonToken == "" {
			return nil, errors.New("plugin hooks have no controller credential")
		}
		seen := map[string]bool{}
		for _, hook := range input.Hooks {
			if hook.Name == "" || seen[hook.Name] || !segment(hook.InstallationID) || !segment(hook.HookKey) || len(hook.InputSchema) > 0 && !json.Valid(hook.InputSchema) {
				return nil, errors.New("invalid task plugin hook")
			}
			seen[hook.Name] = true
		}
		if err := add("multica-plugins"); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ready := make([]bool, len(input.Connections))
	jobs := make(chan int)
	var workers sync.WaitGroup
	var failed sync.Once
	var discoveryErr error
	for range min(4, len(input.Connections)) {
		workers.Go(func() {
			for i := range jobs {
				if ctx.Err() != nil {
					return
				}
				connection := input.Connections[i]
				headers, err := c.remoteHeaders(ctx, claim, connection)
				if err == nil {
					var discovered []remotemcp.Tool
					discovered, _, err = remotemcp.Discover(ctx, connection.Endpoint, connection.EndpointAllowedHosts, connection.ProtocolVersions, headers)
					if err == nil {
						err = matchApprovedTools(connection.ApprovedTools, discovered)
					}
				}
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					if connection.FailurePolicy == "optional" {
						slog.Warn("optional task MCP unavailable", "task_id", claim.ID, "contribution_id", connection.ContributionID)
						continue
					}
					failed.Do(func() {
						discoveryErr = errors.New("required task MCP discovery or credential validation failed")
						cancel()
					})
					return
				}
				ready[i] = true
			}
		})
	}
dispatch:
	for i := range input.Connections {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break dispatch
		}
	}
	close(jobs)
	workers.Wait()
	if discoveryErr != nil {
		return nil, discoveryErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i, connection := range input.Connections {
		if ready[i] {
			if err := add(remoteServerName(connection)); err != nil {
				return nil, err
			}
		}
	}
	return json.Marshal(map[string]any{"mcpServers": servers})
}

func (c *Client) remoteHeaders(ctx context.Context, claim Claim, connection remotemcp.Connection) (http.Header, error) {
	headers := make(http.Header)
	if connection.CredentialHeader == "" {
		return headers, nil
	}
	if claim.RemoteMCPDaemonToken == "" || !segment(connection.ContributionID) {
		return nil, errors.New("task MCP credential is unavailable")
	}
	route := "remote-mcp"
	if strings.HasPrefix(connection.ContributionID, remotemcp.PluginContributionPrefix) {
		route = "plugin-mcp"
	}
	raw, err := c.request(ctx, http.MethodGet, "/api/daemon/tasks/"+claim.ID+"/"+route+"/"+connection.ContributionID+"/credential", claim.RemoteMCPDaemonToken, nil)
	if err != nil {
		return nil, err
	}
	var credential struct {
		Header string `json:"credential_header"`
		Value  string `json:"credential"`
	}
	if json.Unmarshal(raw, &credential) != nil || !strings.EqualFold(credential.Header, connection.CredentialHeader) || !segment(credential.Header) || strings.ContainsAny(credential.Value, "\x00\r\n") || credential.Value == "" {
		return nil, errors.New("invalid task MCP credential")
	}
	switch strings.ToLower(credential.Header) {
	case "host", "connection", "content-length", "content-type", "transfer-encoding", "proxy-authorization", "accept", "mcp-session-id", "mcp-protocol-version", "last-event-id":
		return nil, errors.New("invalid task MCP credential header")
	}
	headers.Set(credential.Header, credential.Value)
	return headers, nil
}

func matchApprovedTools(approved, actual []remotemcp.Tool) error {
	current := map[string]string{}
	for _, tool := range actual {
		current[tool.Name] = tool.SchemaDigest
	}
	seen := map[string]bool{}
	for _, tool := range approved {
		if tool.Name == "" || seen[tool.Name] || tool.SchemaDigest == "" || current[tool.Name] != tool.SchemaDigest {
			return errors.New("approved MCP tool schema drift")
		}
		var schema any
		if json.Unmarshal(tool.InputSchema, &schema) != nil {
			return errors.New("invalid approved MCP schema")
		}
		canonical, _ := json.Marshal(schema)
		if remotemcp.DigestBytes(canonical) != tool.SchemaDigest {
			return errors.New("approved MCP schema digest mismatch")
		}
		seen[tool.Name] = true
	}
	return nil
}

// taskMCP runs under Gateway's attempt gate after live admission and mat_ token
// checks. Neither the provider's headers nor its body select upstream authority.
func (g *Gateway) taskMCP(w http.ResponseWriter, r *http.Request, grant workspace.TaskGrant, claim Claim, body []byte) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	r = r.WithContext(ctx)
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || len(parts) != 4 || parts[2] != claim.ID || len(body) > 1<<20 {
		reject(w, 403)
		return
	}
	var run wire.Run
	var config struct {
		Servers map[string]json.RawMessage `json:"mcpServers"`
	}
	if json.Unmarshal(grant.Execution, &run) != nil || json.Unmarshal(run.Options.McpConfig, &config) != nil || config.Servers[parts[3]] == nil {
		reject(w, 403)
		return
	}
	g.mu.Lock()
	if g.mcpCalls == nil {
		g.mcpCalls = map[string]int{}
	}
	g.mcpCalls[grant.AttemptID]++
	exhausted := g.mcpCalls[grant.AttemptID] > 256
	g.mu.Unlock()
	if exhausted {
		mcpError(w, nil, "task MCP call budget exhausted")
		return
	}
	var request struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if json.Unmarshal(body, &request) != nil || request.JSONRPC != "2.0" {
		mcpError(w, nil, "invalid task MCP request")
		return
	}
	switch request.Method {
	case "initialize", "notifications/initialized", "notifications/cancelled", "ping", "tools/list", "tools/call":
	default:
		mcpError(w, request.ID, "only approved task MCP tools are available")
		return
	}
	var input taskMCPInput
	if json.Unmarshal(claim.Envelope, &input) != nil {
		mcpError(w, request.ID, "invalid task MCP input")
		return
	}
	if parts[3] == "multica-plugins" {
		g.pluginMCP(w, r, claim, input.Hooks, request.ID, request.Method, request.Params)
		return
	}
	var connection *remotemcp.Connection
	for i := range input.Connections {
		if remoteServerName(input.Connections[i]) == parts[3] {
			connection = &input.Connections[i]
			break
		}
	}
	if connection == nil {
		mcpError(w, request.ID, "task MCP server is not authorized")
		return
	}
	if request.Method == "tools/call" {
		var call struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(request.Params, &call) != nil {
			mcpError(w, request.ID, "invalid tool call")
			return
		}
		approved := false
		for _, tool := range connection.ApprovedTools {
			if tool.Name == call.Name {
				approved = true
				break
			}
		}
		if !approved {
			mcpError(w, request.ID, "task MCP tool is not approved")
			return
		}
	}
	endpoint, err := remotemcp.ValidatePublicHTTPSEndpoint(r.Context(), connection.Endpoint, connection.EndpointAllowedHosts, nil)
	if err != nil {
		mcpError(w, request.ID, "task MCP endpoint is unavailable")
		return
	}
	headers, err := g.Client.remoteHeaders(r.Context(), claim, *connection)
	if err != nil {
		mcpError(w, request.ID, "task MCP credential is unavailable")
		return
	}
	upstream, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		mcpError(w, request.ID, "task MCP request failed")
		return
	}
	upstream.Header = headers
	upstream.Header.Set("Content-Type", "application/json")
	upstream.Header.Set("Accept", "application/json, text/event-stream")
	for _, name := range []string{"Mcp-Session-Id", "Mcp-Protocol-Version", "Last-Event-ID"} {
		if value := r.Header.Get(name); value != "" {
			upstream.Header.Set(name, value)
		}
	}
	client := remotemcp.NewSecureHTTPClient(endpoint)
	defer client.CloseIdleConnections()
	response, err := client.Do(upstream)
	if err != nil {
		mcpError(w, request.ID, "task MCP service is unavailable")
		return
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		mcpError(w, request.ID, "task MCP service returned an error")
		return
	}
	if len(request.ID) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	raw, err := readMCPResponse(response, request.ID)
	if err != nil {
		mcpError(w, request.ID, "task MCP response failed or exceeded budget")
		return
	}
	if request.Method == "tools/list" {
		raw, err = approvedToolResponse(raw, connection.ApprovedTools)
		if err != nil {
			mcpError(w, request.ID, "task MCP tool schema changed")
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	for _, name := range []string{"Mcp-Session-Id", "Mcp-Protocol-Version"} {
		if value := response.Header.Get(name); value != "" {
			w.Header().Set(name, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(raw)
}

// The secure HTTP client's timeout covers the body read. Limit the complete
// stream, including ignored frames, and close it as soon as this call resolves.
func readMCPResponse(response *http.Response, id json.RawMessage) ([]byte, error) {
	stream := &io.LimitedReader{R: response.Body, N: remotemcp.MaxResponseBytes + 1}
	if !strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		raw, err := io.ReadAll(stream)
		if err != nil || stream.N == 0 || !mcpResponseMatches(raw, id) {
			return nil, errors.New("invalid or oversized MCP response")
		}
		return raw, nil
	}
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 4096), remotemcp.MaxResponseBytes+1)
	// SSE permits CR, LF and CRLF. Consume CR immediately so a server need not
	// send another byte after a CR-delimited result to release the response.
	skipLF := false
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		offset := 0
		if skipLF && len(data) > 0 && data[0] == '\n' {
			offset = 1
		}
		line := data[offset:]
		if i := bytes.IndexAny(line, "\r\n"); i >= 0 {
			skipLF = line[i] == '\r'
			return offset + i + 1, line[:i], nil
		}
		if atEOF && len(line) > 0 {
			return len(data), line, nil
		}
		return 0, nil, nil
	})
	var frame bytes.Buffer
	first := true
	for scanner.Scan() {
		line := scanner.Bytes()
		if first {
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
			first = false
		}
		if len(line) == 0 {
			if mcpResponseMatches(frame.Bytes(), id) {
				if stream.N == 0 {
					return nil, errors.New("MCP stream exceeds response budget")
				}
				return bytes.TrimSpace(frame.Bytes()), nil
			}
			frame.Reset()
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		if bytes.Equal(field, []byte("data")) {
			frame.Write(bytes.TrimPrefix(value, []byte(" ")))
			frame.WriteByte('\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("MCP stream ended or exceeded budget without this call's response")
}

func mcpResponseMatches(raw []byte, id json.RawMessage) bool {
	var response struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if json.Unmarshal(raw, &response) != nil || response.JSONRPC != "2.0" || len(response.ID) == 0 || len(response.Result) == 0 && len(response.Error) == 0 {
		return false
	}
	id, actual := bytes.TrimSpace(id), bytes.TrimSpace(response.ID)
	if len(id) == 0 {
		return false
	}
	if id[0] == '"' {
		var expected, received string
		return json.Unmarshal(id, &expected) == nil && json.Unmarshal(actual, &received) == nil && expected == received
	}
	// Numeric IDs stay opaque; float64 conversion could alias large integers.
	return bytes.Equal(id, actual)
}

func approvedToolResponse(raw []byte, approved []remotemcp.Tool) ([]byte, error) {
	var response map[string]json.RawMessage
	var result struct {
		Tools []struct {
			Name   string          `json:"name"`
			Schema json.RawMessage `json:"inputSchema"`
		} `json:"tools"`
	}
	if json.Unmarshal(raw, &response) != nil || json.Unmarshal(response["result"], &result) != nil {
		return nil, errors.New("invalid MCP tools")
	}
	actual := make([]remotemcp.Tool, 0, len(result.Tools))
	seen := map[string]bool{}
	for _, tool := range result.Tools {
		if seen[tool.Name] {
			return nil, errors.New("duplicate MCP tool")
		}
		seen[tool.Name] = true
		var schema any
		if json.Unmarshal(tool.Schema, &schema) != nil {
			return nil, errors.New("invalid MCP schema")
		}
		canonical, _ := json.Marshal(schema)
		actual = append(actual, remotemcp.Tool{Name: tool.Name, SchemaDigest: remotemcp.DigestBytes(canonical)})
	}
	if err := matchApprovedTools(approved, actual); err != nil {
		return nil, err
	}
	tools := make([]map[string]any, 0, len(approved))
	for _, tool := range approved {
		tools = append(tools, map[string]any{"name": tool.Name, "description": tool.Description, "inputSchema": tool.InputSchema})
	}
	response["result"], _ = json.Marshal(map[string]any{"tools": tools})
	return json.Marshal(response)
}

func (g *Gateway) pluginMCP(w http.ResponseWriter, r *http.Request, claim Claim, hooks []pluginTool, id json.RawMessage, method string, params json.RawMessage) {
	switch method {
	case "initialize":
		mcpResult(w, id, map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "multica-plugins", "version": "1"}})
	case "notifications/initialized", "notifications/cancelled":
		w.WriteHeader(http.StatusAccepted)
	case "ping":
		mcpResult(w, id, map[string]any{})
	case "tools/list":
		tools := make([]map[string]any, 0, len(hooks))
		for _, hook := range hooks {
			schema := hook.InputSchema
			if len(schema) == 0 {
				schema = json.RawMessage(`{"type":"object","properties":{}}`)
			}
			tools = append(tools, map[string]any{"name": hook.Name, "description": hook.Description, "inputSchema": schema})
		}
		mcpResult(w, id, map[string]any{"tools": tools})
	case "tools/call":
		var call struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if json.Unmarshal(params, &call) != nil {
			mcpError(w, id, "invalid plugin tool input")
			return
		}
		for _, hook := range hooks {
			if hook.Name != call.Name {
				continue
			}
			body, _ := json.Marshal(map[string]any{"installation_id": hook.InstallationID, "hook_key": hook.HookKey, "input": call.Arguments})
			raw, err := g.Client.request(r.Context(), http.MethodPost, "/api/daemon/tasks/"+claim.ID+"/plugin-hooks", claim.RemoteMCPDaemonToken, body)
			var response struct {
				Status string          `json:"status"`
				Output json.RawMessage `json:"output"`
			}
			if err != nil || json.Unmarshal(raw, &response) != nil || response.Status != "ok" {
				mcpResult(w, id, map[string]any{"isError": true, "content": []map[string]string{{"type": "text", "text": "plugin hook failed"}}})
				return
			}
			mcpResult(w, id, map[string]any{"content": []map[string]string{{"type": "text", "text": string(response.Output)}}})
			return
		}
		mcpError(w, id, "plugin tool is not authorized")
	}
}

func mcpResult(w http.ResponseWriter, id json.RawMessage, result any) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}
func mcpError(w http.ResponseWriter, id json.RawMessage, message string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32603, "message": message}})
}
