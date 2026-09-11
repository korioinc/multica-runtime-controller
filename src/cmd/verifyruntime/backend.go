package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const fixtureWorkspace = "52000000-0000-4000-8000-000000000001"
const fixtureAgent = "52000000-0000-4000-8000-000000000002"
const fixtureCodexRuntime = "52000000-0000-4000-8000-000000000010"
const fixtureRuntime = "52000000-0000-4000-8000-000000000003"
const fixtureChatA = "52000000-0000-4000-8000-000000000004"
const fixtureChatB = "52000000-0000-4000-8000-000000000005"
const fixtureIssue = "52000000-0000-4000-8000-000000000006"
const cleanupChat = "52000000-0000-4000-8000-000000000007"
const interruptedChat = "52000000-0000-4000-8000-000000000008"
const journalChat = "52000000-0000-4000-8000-000000000009"

type runtimeBackend struct {
	mutex                  sync.Mutex
	origin, evidence, mode string
	state                  backendState
	connections            map[*websocket.Conn]bool
}

func runBackend(ctx context.Context, listen, origin, evidence string) error {
	if err := fixtureOnly(); err != nil {
		return err
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.Path != "" {
		return errors.New("fixture origin must be a local HTTP origin")
	}
	if err := os.MkdirAll(evidence, 0700); err != nil {
		return err
	}
	if err := createRepository(ctx); err != nil {
		return err
	}
	backend := &runtimeBackend{origin: origin, evidence: evidence, mode: "http", connections: map[*websocket.Conn]bool{}, state: backendState{Tasks: map[string]*taskRecord{}}}
	if raw, err := os.ReadFile(filepath.Join(evidence, "runtime-state.json")); err == nil {
		if err := json.Unmarshal(raw, &backend.state); err != nil {
			return err
		}
		if backend.state.Tasks == nil {
			return errors.New("invalid fixture state")
		}
	}
	mux := http.NewServeMux()
	mux.Handle("/git/", http.StripPrefix("/git/", http.FileServer(http.Dir("/git"))))
	mux.HandleFunc("/fixture/", backend.control)
	mux.HandleFunc("/", backend.official)
	server := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { <-ctx.Done(); _ = server.Close() }()
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func createRepository(ctx context.Context) error {
	path := "/git/repository.git"
	if _, err := os.Stat(filepath.Join(path, "HEAD")); err == nil {
		return nil
	}
	if err := os.MkdirAll("/git", 0700); err != nil {
		return err
	}
	commands := [][]string{{"init", "--bare", path}, {"-C", path, "symbolic-ref", "HEAD", "refs/heads/main"}}
	for _, args := range commands {
		command := exec.CommandContext(ctx, "git", args...)
		if raw, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("fixture Git initialization: %w: %s", err, raw)
		}
	}
	content := "fixture original\n"
	stream := fmt.Sprintf("blob\nmark :1\ndata %d\n%s\ncommit refs/heads/main\ncommitter Local Fixture <fixture@invalid> 1 +0000\ndata 7\nfixture\nM 100644 :1 tracked.txt\n\ndone\n", len(content), content)
	command := exec.CommandContext(ctx, "git", "-C", path, "fast-import")
	command.Stdin = strings.NewReader(stream)
	if raw, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("fixture Git seed: %w: %s", err, raw)
	}
	command = exec.CommandContext(ctx, "git", "-C", path, "update-server-info")
	if raw, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("fixture Git publication: %w: %s", err, raw)
	}
	return nil
}

func (f *runtimeBackend) persist() error {
	raw, err := json.MarshalIndent(f.state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(f.evidence, "runtime-state.json"), append(raw, '\n'), 0600)
}
func (f *runtimeBackend) fail(err error) { f.state.Error = err.Error(); _ = f.persist() }

func (f *runtimeBackend) control(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/fixture/home" && r.Method == http.MethodPost {
		f.captureHome(w, r)
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.codexControl(w, r) {
		return
	}
	switch {
	case r.URL.Path == "/fixture/health":
		writeJSON(w, map[string]any{"ready": true})
		return
	case r.URL.Path == "/fixture/state":
		writeJSON(w, f.state)
		return
	case r.URL.Path == "/fixture/run" && r.Method == http.MethodPost:
		var input runRequest
		if decodeRequest(w, r, &input) != nil {
			http.Error(w, "invalid run", 400)
			return
		}
		record, err := f.schedule(input)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := f.persist(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, record)
		return
	case strings.HasPrefix(r.URL.Path, "/fixture/task/"):
		record := f.state.Tasks[strings.TrimPrefix(r.URL.Path, "/fixture/task/")]
		if record == nil {
			http.Error(w, "unknown task", 404)
			return
		}
		writeJSON(w, record)
		return
	case r.URL.Path == "/fixture/provider" && r.Method == http.MethodPost:
		var report providerResult
		if err := decodeRequest(w, r, &report); err != nil {
			http.Error(w, "invalid provider report", 400)
			return
		}
		record := f.state.Tasks[report.TaskID]
		if record == nil || record.Input.Case != report.Case {
			http.Error(w, "unassigned provider report", 403)
			return
		}
		record.Provider = &report
		if report.Stage == "held" {
			checkpoint := report
			record.Checkpoint = &checkpoint
		}
		f.state.LastEnvironment = report.RuntimeRef.ImageBuildID
		if err := f.persist(); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]any{"recorded": true})
		return
	case strings.HasPrefix(r.URL.Path, "/fixture/release/"):
		record := f.state.Tasks[strings.TrimPrefix(r.URL.Path, "/fixture/release/")]
		if record == nil {
			http.Error(w, "unknown task", 404)
			return
		}
		if r.Method == http.MethodPost {
			record.Released = true
			_ = f.persist()
		}
		writeJSON(w, map[string]bool{"released": record.Released})
		return
	case r.URL.Path == "/fixture/baseline" && r.Method == http.MethodPost:
		var value struct {
			TaskID string `json:"taskID"`
		}
		if decodeRequest(w, r, &value) != nil || f.state.Tasks[value.TaskID] == nil {
			http.Error(w, "invalid baseline", 400)
			return
		}
		f.state.Baseline = value.TaskID
		_ = f.persist()
		writeJSON(w, map[string]any{"saved": true})
		return
	}
	http.NotFound(w, r)
}

func (f *runtimeBackend) schedule(input runRequest) (*taskRecord, error) {
	if f.state.Pending != "" {
		return nil, errors.New("another fixture task is awaiting its official claim")
	}
	if input.Scope != "a" && input.Scope != "b" && input.Scope != "cleanup" && input.Scope != "interrupted" && input.Scope != "journal" && !strings.HasPrefix(input.Scope, "codex-") {
		return nil, errors.New("unsupported fixture scope")
	}
	switch input.Transport {
	case "http", "ws", "failure", "uncertain":
	default:
		return nil, errors.New("invalid fixture transport")
	}
	if input.TaskID == "" {
		input.TaskID = uuid.NewString()
	} else if id, err := uuid.Parse(input.TaskID); err != nil || id.String() != input.TaskID {
		return nil, errors.New("invalid fixture task ID")
	}
	chat := fixtureChatA
	if input.Scope == "b" {
		chat = fixtureChatB
	}
	if input.Scope == "cleanup" {
		chat = cleanupChat
	}
	if input.Scope == "interrupted" {
		chat = interruptedChat
	}
	if input.Scope == "journal" {
		chat = journalChat
	}
	if strings.HasPrefix(input.Scope, "codex-") {
		chat = uuid.NewSHA1(uuid.NameSpaceOID, []byte(input.Scope)).String()
	}
	if input.Provider != "" && input.Provider != "pi" && input.Provider != "codex" {
		return nil, errors.New("unsupported fixture provider")
	}
	customEnv := map[string]string{"VERIFYRUNTIME_BACKEND": f.origin, "VERIFYRUNTIME_CASE": input.Case, "LOCALVERIFY_DISPOSABLE_CLUSTER": "true"}
	if input.CacheOverride != "" {
		customEnv["FIXTURE_CACHE"] = input.CacheOverride
		customEnv["VERIFYRUNTIME_EXPECT_CACHE"] = input.CacheOverride
	}
	if input.Hold {
		customEnv["VERIFYRUNTIME_HOLD"] = "true"
		f.state.Held = input.TaskID
	}
	if input.HoldAfterWork {
		customEnv["VERIFYRUNTIME_HOLD_AFTER_WORK"] = "true"
		f.state.InterruptedTask = input.TaskID
	}
	if input.Case == "cleanup-failure" {
		f.state.CleanupTask = input.TaskID
	}
	if input.Case == "journal-failure" {
		f.state.JournalTask = input.TaskID
	}
	task := map[string]any{"id": input.TaskID, "agent_id": fixtureAgent, "runtime_id": fixtureRuntime, "workspace_id": fixtureWorkspace, "workspace_slug": "runtime-fixture", "issue_id": fixtureIssue, "issue_identifier": "VERIFY-1", "auth_token": "mat_fixture_" + input.TaskID, "chat_session_id": chat, "chat_message": "Verify the local runtime repository while preserving unfinished work.", "agent": map[string]any{"id": fixtureAgent, "name": "Runtime fixture", "instructions": "Verify the local runtime repository.", "custom_env": customEnv}, "repos": []any{map[string]any{"url": f.origin + "/git/repository.git"}}}
	if input.Provider == "codex" {
		task["runtime_id"] = fixtureCodexRuntime
		task["chat_message"] = input.Prompt
		task["issue_id"] = nil
		task["issue_identifier"] = ""
		customEnv["VERIFYRUNTIME_CODEX_MODE"] = input.CodexMode
	}
	if input.PriorTaskID != "" {
		prior := f.state.Tasks[input.PriorTaskID]
		if prior == nil || prior.Provider == nil || (prior.Completion == nil && prior.Status != "cancelled") {
			return nil, errors.New("prior task has no completed or cancelled official result")
		}
		if prior.Completion != nil {
			task["prior_work_dir"] = prior.Completion["work_dir"]
			task["prior_session_id"] = prior.Completion["session_id"]
		} else {
			// The real server can claim a cancelled task's recorded directory
			// before cancel-ack. Use the directory observed from its live worker.
			task["prior_work_dir"] = prior.Provider.WorkDir
			task["prior_session_id"] = prior.Provider.Session
		}
		if input.Scope != prior.Input.Scope {
			customEnv["VERIFYRUNTIME_FORBIDDEN_ROOT"] = filepath.Dir(prior.Provider.WorkDir)
		}
	}
	record := &taskRecord{Status: "queued", Epoch: uuid.NewString(), Input: input, Claim: task}
	if previous := f.state.Tasks[input.TaskID]; previous != nil {
		record.Checkpoint = previous.Checkpoint
	}
	f.state.Tasks[input.TaskID] = record
	f.state.Pending = input.TaskID
	f.mode = input.Transport
	if input.Transport == "http" {
		for connection := range f.connections {
			_ = connection.Close()
		}
	}
	return record, nil
}

func (f *runtimeBackend) official(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/daemon/ws" {
		f.websocket(w, r)
		return
	}
	var body map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 16<<20)).Decode(&body)
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	output := any(map[string]any{})
	if f.codexOfficial(w, r, body) {
		return
	}
	switch {
	case r.URL.Path == "/api/daemon/workspaces":
		output = []any{map[string]any{"id": fixtureWorkspace, "slug": "runtime-fixture", "name": "Runtime fixture"}}
	case r.URL.Path == "/api/daemon/register":
		entries, ok := body["runtimes"].([]any)
		if !ok {
			f.fail(errors.New("official registration omitted its actual runtimes"))
			http.Error(w, "invalid registration", 400)
			return
		}
		runtimes := []any{}
		for _, raw := range entries {
			entry, ok := raw.(map[string]any)
			if !ok || (entry["type"] != "pi" && entry["type"] != "codex") || entry["profile_id"] != nil && entry["profile_id"] != "" {
				f.fail(errors.New("registration attempted unsupported runtime"))
				http.Error(w, "unsupported runtime", 403)
				return
			}
			runtimeID := fixtureRuntime
			if entry["type"] == "codex" {
				runtimeID = fixtureCodexRuntime
			}
			runtimes = append(runtimes, map[string]any{"id": runtimeID, "provider": entry["type"], "name": "Runtime fixture", "status": "online"})
		}
		output = map[string]any{"runtimes": runtimes, "repos": []any{map[string]any{"url": f.origin + "/git/repository.git"}}}
		raw, _ := json.MarshalIndent(body, "", "  ")
		_ = os.WriteFile(filepath.Join(f.evidence, "registration.json"), raw, 0600)
	case strings.HasSuffix(r.URL.Path, "/runtime-profiles"):
		f.fail(errors.New("runtime profiles escaped production suppression"))
		output = map[string]any{"workspace_id": fixtureWorkspace, "runtime_profiles": []any{}}
	case strings.HasSuffix(r.URL.Path, "/repos"):
		output = map[string]any{"workspace_id": fixtureWorkspace, "repos": []any{map[string]any{"url": f.origin + "/git/repository.git"}}}
	case r.URL.Path == "/api/daemon/tasks/claim":
		output = f.claim("http")
	case strings.HasSuffix(r.URL.Path, "/complete") || strings.HasSuffix(r.URL.Path, "/fail"):
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) > 4 {
			if task := f.state.Tasks[parts[4]]; task != nil {
				if strings.HasSuffix(r.URL.Path, "/complete") {
					task.Completion = body
				} else {
					task.Failure = body
				}
				_ = f.persist()
			}
		}
	case strings.HasSuffix(r.URL.Path, "/status"):
		output = map[string]any{"status": "running"}
	case r.URL.Path == "/api/daemon/heartbeat":
		output = map[string]any{"runtime_id": fixtureRuntime}
	case r.URL.Path == "/api/tokens/current/renew":
		output = map[string]any{"token": "mul_disposable_runtime_fixture"}
	case strings.Contains(r.URL.Path, "/issues/"):
		output = map[string]any{"id": fixtureIssue, "title": "Verify the local runtime repository", "description": "Preserve unfinished work.", "status": "in_progress"}
	}
	writeJSON(w, output)
}

func (f *runtimeBackend) claim(transport string) any {
	empty := map[string]any{"tasks": []any{}}
	record := f.state.Tasks[f.state.Pending]
	if record == nil {
		return empty
	}
	if f.mode == "http" && transport != "http" || f.mode == "ws" && transport != "ws" {
		return empty
	}
	if (f.mode == "failure" || f.mode == "uncertain") && !record.Injected {
		return empty
	}
	f.state.Pending = ""
	record.Transport = transport
	record.Status = "dispatched"
	record.Events = append(record.Events, taskEvent{Phase: "claimed", At: time.Now().UTC(), Transport: transport})
	_ = f.persist()
	return map[string]any{"tasks": []any{record.Claim}}
}

func (f *runtimeBackend) websocket(w http.ResponseWriter, r *http.Request) {
	f.mutex.Lock()
	httpOnly := f.mode == "http"
	f.mutex.Unlock()
	if httpOnly {
		http.Error(w, "HTTP-only phase", 503)
		return
	}
	connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()
	f.mutex.Lock()
	f.connections[connection] = true
	f.mutex.Unlock()
	defer func() { f.mutex.Lock(); delete(f.connections, connection); f.mutex.Unlock() }()
	for {
		var message struct {
			Type    string `json:"type"`
			Payload struct {
				ID     string `json:"request_id"`
				Method string `json:"method"`
			} `json:"payload"`
		}
		if connection.ReadJSON(&message) != nil {
			return
		}
		switch message.Type {
		case "daemon:heartbeat":
			if connection.WriteJSON(map[string]any{"type": "daemon:heartbeat_ack", "payload": map[string]any{"runtime_id": fixtureRuntime, "server_capabilities": []string{"rpc-v1"}}}) != nil {
				return
			}
			if connection.WriteJSON(map[string]any{"type": "daemon:task_available", "payload": map[string]any{"runtime_id": fixtureRuntime}}) != nil {
				return
			}
		case "daemon:rpc_request":
			if message.Payload.Method != "tasks.claim" {
				continue
			}
			f.mutex.Lock()
			record := f.state.Tasks[f.state.Pending]
			inject := record != nil && !record.Injected && (f.mode == "failure" || f.mode == "uncertain")
			uncertain := f.mode == "uncertain"
			if inject {
				record.Injected = true
				_ = f.persist()
				f.mutex.Unlock()
				if uncertain {
					return
				}
				if connection.WriteJSON(map[string]any{"type": "daemon:rpc_response", "payload": map[string]any{"request_id": message.Payload.ID, "status": 503, "error": "disposable transport refusal"}}) != nil {
					return
				}
				continue
			}
			body := f.claim("ws")
			f.mutex.Unlock()
			if connection.WriteJSON(map[string]any{"type": "daemon:rpc_response", "payload": map[string]any{"request_id": message.Payload.ID, "status": 200, "body": body}}) != nil {
				return
			}
		}
	}
}
