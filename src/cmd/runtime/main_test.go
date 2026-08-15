package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/intercept"
)

func TestCommandForModes(t *testing.T) {
	getenv := func(key string) string {
		return map[string]string{
			"MULTICA_BINARY":             "/multica",
			"MULTICA_DAEMON_ID":          "11111111-2222-3333-4444-555555555555",
			"MULTICA_RUNTIME_NAME":       "runtime-controller",
			"MULTICA_RUNTIME_CAPACITY":   "2",
			"MULTICA_POLL_INTERVAL":      "10s",
			"MULTICA_HEARTBEAT_INTERVAL": "15s",
		}[key]
	}
	path, args, err := commandFor([]string{"daemon"}, getenv)
	if err != nil || path != "/multica" {
		t.Fatalf("daemon: %s %v %v", path, args, err)
	}
	want := []string{
		"daemon", "start", "--foreground", "--no-auto-update", "--no-auto-reload",
		"--daemon-id", "11111111-2222-3333-4444-555555555555", "--runtime-name", "runtime-controller",
		"--max-concurrent-tasks", "2", "--poll-interval", "10s", "--heartbeat-interval", "15s",
	}
	if len(args) != len(want) {
		t.Fatalf("daemon args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("daemon args = %v, want %v", args, want)
		}
	}
}

func TestCommandForDefaultsToOfficialDaemonCapacity(t *testing.T) {
	getenv := func(key string) string {
		if key == "MULTICA_DAEMON_ID" {
			return "11111111-2222-3333-4444-555555555555"
		}
		return ""
	}
	_, args, err := commandFor([]string{"daemon"}, getenv)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(strings.Join(args, " "), "--max-concurrent-tasks 20") {
		t.Fatalf("daemon args = %v, want official default capacity 20", args)
	}
}

func TestCommandForRequiresDaemonID(t *testing.T) {
	_, _, err := commandFor([]string{"daemon"}, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "MULTICA_DAEMON_ID is required") {
		t.Fatalf("commandFor() error = %v, want missing daemon ID", err)
	}
}

func TestWriteOfficialCLIConfig(t *testing.T) {
	home := t.TempDir()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("mul_official\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	getenv := func(key string) string {
		return map[string]string{
			"HOME":                          home,
			"MULTICA_BASE_URL":              "https://multica.example",
			"MULTICA_RUNTIME_NAME":          "runtime-controller",
			"MULTICA_POLL_INTERVAL":         "10s",
			"MULTICA_HEARTBEAT_INTERVAL":    "15s",
			"MULTICA_CONTROLLER_TOKEN_FILE": tokenPath,
		}[key]
	}
	if err := writeOfficialCLIConfig(getenv); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(home, ".multica", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	want := `"server_url":"https://multica.example"`
	if string(data) == "" || !contains(string(data), want) || !contains(string(data), `"token":"mul_official"`) || !contains(string(data), `"max_concurrent_tasks":20`) {
		t.Fatalf("unexpected CLI config: %s", data)
	}
	if contains(string(data), `"workspace_id"`) {
		t.Fatalf("CLI config must not pin a workspace: %s", data)
	}
	info, err := os.Stat(filepath.Join(home, ".multica", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o, want 600", info.Mode().Perm())
	}
}

func TestDaemonEnvironmentConfiguresGitHubAuthentication(t *testing.T) {
	environ := []string{
		"GITHUB_PAT_TOKEN=github_pat_test",
		"GITHUB_USER_NAME=Multica Runtime",
		"GITHUB_USER_EMAIL=runtime@example.com",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=core.autocrlf",
		"GIT_CONFIG_VALUE_0=false",
	}
	got := environmentMap(daemonEnvironment(environ, func(string) string { return "" }))

	if got["GH_TOKEN"] != "github_pat_test" {
		t.Fatalf("GH_TOKEN = %q, want PAT value", got["GH_TOKEN"])
	}
	if got["GIT_TERMINAL_PROMPT"] != "0" {
		t.Fatalf("GIT_TERMINAL_PROMPT = %q, want 0", got["GIT_TERMINAL_PROMPT"])
	}
	if got["GIT_CONFIG_COUNT"] != "6" {
		t.Fatalf("GIT_CONFIG_COUNT = %q, want 6", got["GIT_CONFIG_COUNT"])
	}

	configs := map[string][]string{}
	for index := range 6 {
		key := got["GIT_CONFIG_KEY_"+strconv.Itoa(index)]
		value := got["GIT_CONFIG_VALUE_"+strconv.Itoa(index)]
		configs[key] = append(configs[key], value)
	}
	for key, want := range map[string][]string{
		"core.autocrlf":                        {"false"},
		"user.name":                            {"Multica Runtime"},
		"user.email":                           {"runtime@example.com"},
		"url.https://github.com/.insteadOf":    {"git@github.com:", "ssh://git@github.com/"},
		"credential.https://github.com.helper": {"!gh auth git-credential"},
	} {
		if !slices.Equal(configs[key], want) {
			t.Fatalf("Git config %q = %q, want %q", key, configs[key], want)
		}
	}
}

func TestDaemonEnvironmentDoesNotDuplicatePreconfiguredGitHubAuthentication(t *testing.T) {
	environ := []string{
		"GITHUB_PAT_TOKEN=github_pat_test",
		"GH_TOKEN=github_pat_test",
		"GITHUB_USER_NAME=Multica Runtime",
		"GITHUB_USER_EMAIL=runtime@example.com",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_COUNT=5",
		"GIT_CONFIG_KEY_0=user.name",
		"GIT_CONFIG_VALUE_0=Multica Runtime",
		"GIT_CONFIG_KEY_1=user.email",
		"GIT_CONFIG_VALUE_1=runtime@example.com",
		"GIT_CONFIG_KEY_2=url.https://github.com/.insteadOf",
		"GIT_CONFIG_VALUE_2=git@github.com:",
		"GIT_CONFIG_KEY_3=url.https://github.com/.insteadOf",
		"GIT_CONFIG_VALUE_3=ssh://git@github.com/",
		"GIT_CONFIG_KEY_4=credential.https://github.com.helper",
		"GIT_CONFIG_VALUE_4=!gh auth git-credential",
	}
	got := environmentMap(daemonEnvironment(environ, func(string) string { return "" }))

	if got["GIT_CONFIG_COUNT"] != "5" {
		t.Fatalf("GIT_CONFIG_COUNT = %q, want existing five entries without duplicates", got["GIT_CONFIG_COUNT"])
	}
}

func TestRunProviderProcessMirrorsProtocolActivityWithoutLoggingPayloads(t *testing.T) {
	const (
		taskID = "11111111-2222-3333-4444-555555555555"
		input  = `{"jsonrpc":"2.0","id":7,"method":"initialize","params":{"token":"secret-value"}}` + "\n"
	)
	request := intercept.Request{
		Provider: "codex",
		Args:     []string{"-c", `IFS= read -r line; printf '%s\n' "$line"; exit 7`},
		Env:      append(os.Environ(), "MULTICA_TASK_ID="+taskID),
		WorkDir:  t.TempDir(),
	}
	var stdout bytes.Buffer
	var podLog bytes.Buffer
	err := runProviderProcess(context.Background(), "/bin/sh", request, intercept.Streams{
		Stdin: strings.NewReader(input), Stdout: &stdout, Stderr: &bytes.Buffer{},
	}, &podLog)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 {
		t.Fatalf("runProviderProcess() error = %v, want exit code 7", err)
	}
	if stdout.String() != input {
		t.Fatalf("provider stdout = %q, want exact protocol payload %q", stdout.String(), input)
	}
	logs := podLog.String()
	for _, want := range []string{
		`"msg":"provider execution started"`,
		`"direction":"controller_to_provider"`,
		`"direction":"provider_to_controller"`,
		`"method":"initialize"`,
		`"id":7`,
		`"exit_code":7`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("task Pod log does not contain %q: %s", want, logs)
		}
	}
	if strings.Contains(logs, "secret-value") {
		t.Fatalf("task Pod log leaked protocol payload: %s", logs)
	}
}

func TestTaskDaemonProxyAuthenticatesAndForwardsEntireRequest(t *testing.T) {
	const (
		requestSecretName = "task-1111111-selected"
		taskID            = "11111111-2222-4333-8444-555555555555"
		token             = "mat_task_scoped_token"
		body              = `{"arbitrary":"daemon request"}`
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/daemon/arbitrary" || r.URL.RawQuery != "mode=full" {
			t.Fatalf("forwarded request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Basic original-authorization" {
			t.Fatalf("original authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get(intercept.DaemonProxyRequestSecretHeader) != requestSecretName ||
			r.Header.Get(intercept.DaemonProxyTaskIDHeader) != taskID ||
			r.Header.Get(intercept.DaemonProxyTaskTokenHeader) != token {
			t.Fatalf("task proxy headers = %q, %q, %q", r.Header.Get(intercept.DaemonProxyRequestSecretHeader), r.Header.Get(intercept.DaemonProxyTaskIDHeader), r.Header.Get(intercept.DaemonProxyTaskTokenHeader))
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != body {
			t.Fatalf("forwarded body = %q, want %q", raw, body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	handler, err := newTaskDaemonProxy(upstream.URL, requestSecretName, taskID, token)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPatch, "/daemon/arbitrary?mode=full", strings.NewReader(body))
	req.Header.Set("Authorization", "Basic original-authorization")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("proxy response status=%d body=%s", rec.Code, rec.Body.String())
	}

	for _, selector := range []string{"", "INVALID SECRET NAME"} {
		if _, err := newTaskDaemonProxy(upstream.URL, selector, taskID, token); err == nil || !strings.Contains(err.Error(), "MULTICA_REQUEST_SECRET_NAME") {
			t.Fatalf("newTaskDaemonProxy() selector %q error = %v, want selector validation", selector, err)
		}
	}
}

func contains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}

func environmentMap(environ []string) map[string]string {
	values := make(map[string]string, len(environ))
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	return values
}
