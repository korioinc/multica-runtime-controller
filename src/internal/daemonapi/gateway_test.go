package daemonapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/textproto"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/multica-ai/multica/server/pkg/remotemcp"
	"github.com/multica-ai/multica/server/pkg/remotemcp/remotemcptest"
)

func fixtureRef() runtimeimage.Ref {
	hash := strings.Repeat("a", 64)
	return runtimeimage.Ref{Image: "registry.invalid/runtime@sha256:" + hash, Platform: "linux/amd64", ImageBuildID: uuid.NewString(), DescriptorDigest: hash, ConfigurationDigest: hash, Controller: core.Contract{BuildID: hash, Platform: "linux/amd64", RuntimePath: "/opt/multica/controller/runtime", RuntimeSHA256: hash, GoVersion: "go1.26.6"}, Daemon: runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/usr/bin/multica", Version: "fixture", SHA256: hash}, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"codex": {Path: "/usr/bin/native-engine", Version: "1", SHA256: hash}}}
}

func fixtureGateway(t *testing.T, backend http.Handler, overrides ...map[string]any) (*Gateway, workspace.TaskGrant, string) {
	t.Helper()
	upstream := httptest.NewServer(backend)
	t.Cleanup(upstream.Close)
	client, err := NewClient(upstream.URL, "owner-secret", fixtureRef().Daemon.Version, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store, err := workspace.Open(workspace.Options{Directory: filepath.Join(root, "metadata"), OwnerID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.BindWorkspace("workspace-fixture", "pvc-a", "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	taskID, agentID, runtimeID, workspaceID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	input := map[string]any{"id": taskID, "agent_id": agentID, "runtime_id": runtimeID, "workspace_id": workspaceID, "issue_id": "issue-a", "auth_token": "mat_task-a", "remote_mcp_daemon_token": "broad-daemon-secret", "repos": []Repository{{URL: "https://git.invalid/a"}}, "agent": map[string]any{"id": agentID, "name": "native task", "skill_refs": []any{map[string]any{"id": "skill-a", "source": "workspace", "name": "skill", "hash": "sha256:a", "size_bytes": 1, "file_count": 1}}}, "plugin_hook_tools": []any{map[string]any{"installation_id": "install-a", "hook_key": "hook-a", "name": "allowed-tool", "input_schema": map[string]any{"type": "object"}}}, "native_option": map[string]any{"opaque": true}}
	for _, values := range overrides {
		for key, value := range values {
			input[key] = value
		}
	}
	envelope, _ := json.Marshal(input)
	grant, err := store.Create(workspace.TaskGrant{TaskID: taskID, RuntimeID: runtimeID, WorkspaceID: workspaceID, AgentID: agentID, RuntimeRef: fixtureRef(), Envelope: envelope, Repositories: []workspace.Repository{{URL: "https://git.invalid/a"}}})
	if err != nil {
		t.Fatal(err)
	}
	api, err := store.IssueCapability(grant.AttemptID, "daemon", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BeginPreparation(grant.AttemptID, workspace.PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://fixture"}); err != nil {
		t.Fatal(err)
	}
	prepared := workspace.Prepared{OwnerID: grant.OwnerID, WorkspaceID: grant.WorkspaceID, TaskID: grant.TaskID, AgentID: grant.AgentID, AttemptID: grant.AttemptID, Generation: grant.Generation, PVCUID: grant.PVCUID, TaskRoot: grant.TaskRoot, Provider: "codex", Executable: grant.RuntimeRef.Providers["codex"].Path, RuntimeDigest: grant.RuntimeRef.DescriptorDigest, ConfigurationDigest: grant.RuntimeRef.ConfigurationDigest, CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`), Environment: workspace.NativeEnvironment{RootDir: grant.TaskRoot, WorkDir: grant.TaskRoot + "/workdir", MulticaConfigRoot: grant.TaskRoot + "/multica-config", CodexHome: grant.TaskRoot + "/codex-home"}}
	encoded, _ := json.Marshal(prepared)
	prepared.Digest = core.Digest(encoded)
	parsed, err := ParseClaim(envelope)
	if err != nil {
		t.Fatal(err)
	}
	mcpConfig, err := client.PrepareMCP(context.Background(), parsed)
	if err != nil {
		t.Fatal(err)
	}
	execution, _ := json.Marshal(wire.Run{Options: agent.ExecOptions{McpConfig: mcpConfig}})
	if err = store.CompletePreparation(grant.AttemptID, &prepared, execution); err != nil {
		t.Fatal(err)
	}
	if err = store.BindPod(grant.AttemptID, grant.PodName, "pod-a", "node-a"); err != nil {
		t.Fatal(err)
	}
	key, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Admit(grant.AttemptID, key); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := store.Offer(grant.AttemptID); err != nil || !ready {
		t.Fatal("fixture execution could not be offered", err)
	}
	if err := store.BeginStart(grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkStarted(grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	grant, err = store.Get(grant.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	gateway := &Gateway{Store: store, Client: client, Admission: func(context.Context, workspace.TaskGrant) error { return nil }}
	return gateway, grant, api
}

func request(t *testing.T, gateway *Gateway, token, method, path string, body any, capability ...string) *httptest.ResponseRecorder {
	t.Helper()
	var raw string
	if value, ok := body.(string); ok {
		raw = value
	} else if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		raw = string(encoded)
	}
	r := httptest.NewRequest(method, path, strings.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+token)
	if len(capability) > 0 {
		r.Header.Set("X-Multica-Attempt-Capability", capability[0])
	}
	w := httptest.NewRecorder()
	gateway.ServeHTTP(w, r)
	return w
}
func TestGatewayPreservesTaskAndPluginAuthority(t *testing.T) {
	var mu sync.Mutex
	applied := ""
	g, grant, api := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/issues/issue-a" {
			_, _ = io.WriteString(w, `{"description":"authorized-issue-content"}`)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/issues/") {
			_, _ = io.WriteString(w, `{"description":"authorized-workspace-issue"}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/plugin-hooks") {
			var input struct {
				InstallationID string `json:"installation_id"`
				HookKey        string `json:"hook_key"`
			}
			_ = json.NewDecoder(r.Body).Decode(&input)
			mu.Lock()
			applied = input.InstallationID + "/" + input.HookKey
			mu.Unlock()
			_, _ = io.WriteString(w, `{"status":"ok","output":"authorized-hook-result"}`)
		}
	}))
	own := request(t, g, "mat_task-a", http.MethodGet, "/api/issues/issue-a", nil, api)
	if !strings.Contains(own.Body.String(), "authorized-issue-content") {
		t.Fatal("admitted task could not read its issue")
	}
	other := request(t, g, "mat_task-a", http.MethodGet, "/api/issues/sibling", nil, api)
	if !strings.Contains(other.Body.String(), "authorized-workspace-issue") {
		t.Fatal("admitted task could not read another issue authorized by the backend")
	}
	for _, capability := range []string{"owner-secret", "broad-daemon-secret"} {
		forbidden := request(t, g, "mat_task-a", http.MethodGet, "/api/issues/sibling", nil, capability)
		if strings.Contains(forbidden.Body.String(), "authorized-workspace-issue") {
			t.Fatal("unadmitted caller read issue content")
		}
	}
	endpoint := "/api/task-mcp/" + grant.TaskID + "/multica-plugins"
	request(t, g, "mat_task-a", http.MethodPost, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"foreign-hook","arguments":{}}}`, api)
	mu.Lock()
	unauthorized := applied
	mu.Unlock()
	if unauthorized != "" {
		t.Fatal("unapproved plugin tool caused a mutation")
	}
	allowed := request(t, g, "mat_task-a", http.MethodPost, endpoint, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"allowed-tool","arguments":{}}}`, api)
	mu.Lock()
	authorized := applied
	mu.Unlock()
	if authorized != "install-a/hook-a" || !strings.Contains(allowed.Body.String(), "authorized-hook-result") {
		t.Fatal("admitted plugin invocation did not apply the authorized operation")
	}
	for _, value := range []string{"owner-secret", "broad-daemon-secret"} {
		if strings.Contains(allowed.Body.String(), value) {
			t.Fatal("plugin invocation disclosed controller credentials")
		}
	}
}

func TestClientCannotDiscloseCredentialThroughRedirect(t *testing.T) {
	var mu sync.Mutex
	exposed := ""
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		exposed = r.Header.Get("Authorization")
	}))
	defer destination.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", destination.URL)
		w.WriteHeader(307)
		_, _ = io.WriteString(w, "owner-secret")
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "owner-secret", fixtureRef().Daemon.Version, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Workspaces(context.Background())
	if err != nil && strings.Contains(err.Error(), "owner-secret") {
		t.Fatal("backend response leaked credential in diagnostics")
	}
	mu.Lock()
	defer mu.Unlock()
	if exposed != "" {
		t.Fatal("redirect disclosed upstream credential")
	}
}

func TestGatewayUploadsUseBackendPermissions(t *testing.T) {
	for _, task := range []struct {
		name, issueID, chatSessionID, quickCreatePrompt string
	}{
		{name: "issue", issueID: "assigned-issue"},
		{name: "chat", chatSessionID: "chat-a"},
		{name: "quick-create", quickCreatePrompt: "Create the requested issue"},
	} {
		t.Run(task.name, func(t *testing.T) {
			var mu sync.Mutex
			backendAllows := false
			files := map[string]string{}
			g, grant, api := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if !backendAllows {
					http.Error(w, "backend permission denied", http.StatusForbidden)
					return
				}
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Error(err)
					return
				}
				defer r.MultipartForm.RemoveAll()
				file, _, err := r.FormFile("file")
				if err != nil {
					t.Error(err)
					return
				}
				defer file.Close()
				content, err := io.ReadAll(file)
				if err != nil {
					t.Error(err)
					return
				}
				files[r.FormValue("issue_id")+"|"+r.FormValue("comment_id")] = string(content)
				_, _ = io.WriteString(w, `{}`)
			}), map[string]any{"issue_id": task.issueID, "chat_session_id": task.chatSessionID, "quick_create_prompt": task.quickCreatePrompt})
			upload := func(field, target, payload string) {
				var body bytes.Buffer
				form := multipart.NewWriter(&body)
				file, err := form.CreateFormFile("file", "result.txt")
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.WriteString(file, payload)
				if field != "" {
					if err := form.WriteField(field, target); err != nil {
						t.Fatal(err)
					}
				}
				if err := form.Close(); err != nil {
					t.Fatal(err)
				}
				r := httptest.NewRequest(http.MethodPost, "/api/upload-file", &body)
				r.Header.Set("Authorization", "Bearer mat_task-a")
				r.Header.Set("X-Multica-Attempt-Capability", api)
				r.Header.Set("Content-Type", form.FormDataContentType())
				g.ServeHTTP(httptest.NewRecorder(), r)
			}
			for _, target := range []struct{ field, id, key string }{
				{"issue_id", "backend-approved-issue", "backend-approved-issue|"},
				{"comment_id", "backend-approved-comment", "|backend-approved-comment"},
				{"", "", "|"},
			} {
				mu.Lock()
				backendAllows = false
				mu.Unlock()
				upload(target.field, target.id, "denied output")
				mu.Lock()
				deniedWrite := files[target.key]
				backendAllows = true
				mu.Unlock()
				if deniedWrite != "" {
					t.Fatal("backend-denied upload attached a file")
				}
				upload(target.field, target.id, "authorized output")
				mu.Lock()
				stored := files[target.key]
				mu.Unlock()
				if stored != "authorized output" {
					t.Fatal("backend-authorized target did not receive the file")
				}
			}
			if _, err := g.Store.RequestStop(grant.AttemptID, "cancelled"); err != nil {
				t.Fatal(err)
			}
			upload("issue_id", "backend-approved-issue", "revoked output")
			mu.Lock()
			defer mu.Unlock()
			if files["backend-approved-issue|"] != "authorized output" {
				t.Fatal("cancelled task changed an attachment")
			}
		})
	}
}

func TestGatewayRevocationBeforeResponsePreventsDisclosure(t *testing.T) {
	const secret = "protected-attachment-content"
	g, grant, capability := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("ETag", secret)
		writer := gzip.NewWriter(w)
		_, _ = io.WriteString(writer, secret)
		_ = writer.Close()
	}))
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		Got1xxResponse: func(int, textproto.MIMEHeader) error {
			_, err := g.Store.RequestStop(grant.AttemptID, "cancelled")
			return err
		},
	})
	r := httptest.NewRequest(http.MethodGet, "/api/attachments/attachment/content", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer mat_task-a")
	r.Header.Set("X-Multica-Attempt-Capability", capability)
	r.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if _, err := g.Store.Authorize(capability, "daemon"); err == nil {
		t.Fatal("cancelled task retained API authority")
	}
	var response bytes.Buffer
	if err := w.Result().Write(&response); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(response.String(), secret) {
		t.Fatal("cancelled task received protected attachment data")
	}
}

func TestGatewayCannotExchangeTaskAuthorityForCredentials(t *testing.T) {
	const secret = "credential-outside-task-authority"
	g, _, capability := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, secret)
	}))
	for _, endpoint := range []string{"/api/cli-token", "/api/tokens"} {
		t.Run(endpoint, func(t *testing.T) {
			response := request(t, g, "mat_task-a", http.MethodPost, endpoint, map[string]string{}, capability)
			if strings.Contains(response.Body.String(), secret) {
				t.Fatal("task authority was exchanged for a user credential")
			}
		})
	}
}

func TestGatewayRemoteMCPRejectsUnapprovedWritesAndRevokedCredential(t *testing.T) {
	remote := remotemcptest.NewServer()
	defer remote.Close()
	releaseStream := make(chan struct{})
	streaming := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var input struct {
			Method string `json:"method"`
		}
		if json.Unmarshal(raw, &input) != nil {
			t.Error("invalid fixture request")
			return
		}
		if input.Method != "tools/call" {
			remote.Config.Handler.ServeHTTP(w, r)
			return
		}
		result := httptest.NewRecorder()
		remote.Config.Handler.ServeHTTP(result, r)
		if result.Code != http.StatusOK {
			w.WriteHeader(result.Code)
			_, _ = w.Write(result.Body.Bytes())
			return
		}
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, result.Body.Bytes(), "", "  "); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": keepalive\n\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/message\"}\n\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"other-call\",\"result\":{}}\n\n")
		for _, line := range bytes.Split(pretty.Bytes(), []byte("\n")) {
			_, _ = fmt.Fprintf(w, "data: %s\n", line)
		}
		_, _ = io.WriteString(w, "\n")
		w.(http.Flusher).Flush()
		// Keep the upstream handler open until the authorized read and later
		// revocation check finish; the gateway must consume the result itself.
		<-releaseStream
	}))
	defer streaming.Close()
	defer close(releaseStream)
	t.Setenv(remotemcp.DevOriginsEnv, streaming.URL)
	headers := http.Header{"Authorization": []string{"Bearer " + remotemcptest.Credential}}
	tools, _, err := remotemcp.Discover(context.Background(), streaming.URL, nil, nil, headers)
	if err != nil {
		t.Fatal(err)
	}
	var approved []remotemcp.Tool
	for _, tool := range tools {
		if tool.Name == "fixture.read" {
			approved = append(approved, tool)
		}
	}
	connection := remotemcp.Connection{InstallationID: "fixture-install", ContributionID: "fixture-contribution", ContributionKey: "fixture", Endpoint: streaming.URL, CredentialHeader: "Authorization", ApprovedTools: approved, FailurePolicy: "required"}
	var mu sync.Mutex
	revoked := false
	g, grant, api := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		deny := revoked
		mu.Unlock()
		if deny {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"credential_header": "Authorization", "credential": "Bearer " + remotemcptest.Credential})
	}), map[string]any{"remote_mcp_connections": []remotemcp.Connection{connection}, "plugin_hook_tools": nil})
	endpoint := "/api/task-mcp/" + grant.TaskID + "/" + remoteServerName(connection)
	request(t, g, "mat_task-a", http.MethodPost, endpoint, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fixture.write","arguments":{"value":"unauthorized-change"}}}`, api)
	if len(remote.Writes()) != 0 {
		t.Fatal("task changed a remote resource through an unapproved tool")
	}
	read := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"fixture.read","arguments":{}}}`
	allowed := request(t, g, "mat_task-a", http.MethodPost, endpoint, read, api)
	if !strings.Contains(allowed.Body.String(), "fixture-value") {
		t.Fatal("approved tool could not read its permitted resource")
	}
	mu.Lock()
	revoked = true
	mu.Unlock()
	denied := request(t, g, "mat_task-a", http.MethodPost, endpoint, read, api)
	if strings.Contains(denied.Body.String(), "fixture-value") {
		t.Fatal("revoked credential retained remote resource access")
	}
}
