//go:build githubintegration

package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type nativeTaskTokenSource struct{ authority *taskGitHubAuthority }

func (s nativeTaskTokenSource) Token(ctx context.Context, repositories []githubapp.Repository) (githubapp.Token, error) {
	return s.authority.issue(ctx, repositories)
}

// Run only in a disposable Linux container with a fresh /workspace tmpfs:
// GITHUB_AUTH_NATIVE_TEST=1 go test -tags githubintegration ./internal/execution -run NativeGitHub
// The filesystem is the production authority boundary, rather than a mocked
// path resolver. Never run against an existing installation's workspace.
func TestNativeGitHubAuthorizationSurvivesOfficialEnvironmentFiltering(t *testing.T) {
	if os.Getenv("GITHUB_AUTH_NATIVE_TEST") != "1" {
		t.Skip("requires explicit disposable-container native integration opt-in")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("native integration requires a disposable Linux container")
	}
	entries, err := os.ReadDir(wire.WorkspaceRoot)
	if err != nil || len(entries) != 0 {
		t.Fatal("native integration requires a fresh empty /workspace tmpfs")
	}
	_, attempt := journalAttempt(t)
	ref := attempt.Ref.RuntimeRef
	provider := ref.Providers["pi"]
	provider.Path = "/opt/tools/codex"
	ref.Providers = map[string]runtimeimage.Executable{"codex": provider}
	ownerID := uuid.NewString()
	store, err := workspace.Open(workspace.Options{
		Directory:     filepath.Join(wire.WorkspaceRoot, ".multica-runtime/state"),
		WorkspaceRoot: wire.WorkspaceRoot,
		SessionRoot:   filepath.Join(t.TempDir(), "sessions"),
		OwnerID:       ownerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name       string
		appEnabled bool
		forgedMode bool
	}{
		{name: "admitted App mode survives missing inherited marker", appEnabled: true},
		{name: "untrusted marker cannot enable App mode", forgedMode: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			taskID, workspaceID, agentID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			token := "mat_" + uuid.NewString()
			privateKey := "private-App-credential-" + uuid.NewString()
			observation := workspace.Observation{
				ID: taskID, WorkspaceID: workspaceID, AgentID: agentID, AuthToken: token,
				RuntimeRef: ref, RepositoryURLs: []string{"https://github.com/acme/alpha.git"},
				TaskEnvKeys: []string{"GITHUB_APP_PRIVATE_KEY"},
			}
			if _, err := store.ObserveBatch([]workspace.Observation{observation}); err != nil {
				t.Fatal(err)
			}
			parent := filepath.Join(wire.WorkspaceRoot, "native-github-"+uuid.NewString())
			root := filepath.Join(parent, taskID)
			if err := os.MkdirAll(filepath.Join(root, "workdir"), 0700); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(parent) })
			owner, err := json.Marshal(map[string]string{"workspace_id": workspaceID, "task_id": taskID})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, ".task_owner"), owner, 0600); err != nil {
				t.Fatal(err)
			}
			request := wire.Request{
				TaskID: taskID, Provider: "codex", WorkDir: filepath.Join(root, "workdir"), RuntimeRef: ref,
				// A shim cannot expand the authenticated claim's repository scope.
				RepositoryURLs: []string{"https://github.com/acme/secret.git"},
				Env: []string{
					"MULTICA_TOKEN=" + token,
					"MULTICA_WORKSPACE_ID=" + workspaceID,
					"MULTICA_AGENT_ID=" + agentID,
					"MULTICA_TASK_ID=" + taskID,
					"MULTICA_TASK_CONFIG_ROOT=" + filepath.Join(root, "multica-config"),
					"GITHUB_APP_PRIVATE_KEY=" + privateKey,
				},
			}
			if scenario.forgedMode {
				request.Env = append(request.Env, githubauth.EnabledEnv+"=true")
			}
			runner := &Runner{
				selection: Selection{OwnerID: ownerID, RuntimeRef: ref, GitHubApp: scenario.appEnabled},
				store:     store,
			}
			prepared, err := runner.authorizeTask(request)
			if err != nil {
				t.Fatal(err)
			}
			// Only remove worker storage that this successful Bind just created.
			t.Cleanup(func() { _ = os.RemoveAll(filepath.Join(wire.WorkspaceRoot, prepared.request.WorkerSubPath)) })
			authority := &taskGitHubAuthority{tokens: make(map[string]map[githubapp.Repository]bool)}
			capability := uuid.NewString()
			broker := httptest.NewServer(taskBrokerHandler(prepared.request, capability, nil, authority.issue))
			t.Cleanup(broker.Close)
			credential := requestTaskGitHubCredential(t, broker, "https://github.com/acme/alpha.git", map[string]string{wire.CapabilityHeader: capability})
			canRead := authority.canRead(credential, "acme", "alpha")
			if scenario.appEnabled && !canRead {
				t.Fatal("official environment filtering disabled the admitted task's GitHub authorization")
			}
			if !scenario.appEnabled && canRead {
				t.Fatal("a forged task marker enabled unadmitted GitHub App authorization")
			}
			if authority.canRead(credential, "acme", "secret") {
				t.Fatal("shim-supplied repository scope replaced the observed task grant")
			}
			// A real child process must not be able to disclose the controller's
			// private App credential, even when a claim named it as a task env key.
			child := exec.CommandContext(t.Context(), "/usr/bin/env")
			child.Env = prepared.request.Env
			output, err := child.Output()
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(output, []byte(privateKey)) {
				t.Fatal("task process disclosed the controller's private App credential")
			}
		})
	}
}

// This process-and-transport proof uses an App issuer fixture and Kubernetes API
// fake. Git, the runtime CLI, mounted task request, worker and controller
// gateways, task capability broker and private Unix socket are real.
func TestNativeGitHubCredentialsReachControllerAndWorkerAuthorization(t *testing.T) {
	if os.Getenv("GITHUB_AUTH_NATIVE_TEST") != "1" {
		t.Skip("requires explicit disposable-container native integration opt-in")
	}
	binary := os.Getenv("GITHUB_AUTH_NATIVE_RUNTIME")
	if binary == "" {
		t.Skip("requires a Linux runtime binary mounted at its immutable controller path")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("native integration requires a disposable Linux container")
	}
	for _, directory := range []string{filepath.Dir(githubauth.SocketPath), filepath.Dir(wire.RequestPath)} {
		entries, err := os.ReadDir(directory)
		if err != nil || len(entries) != 0 {
			t.Fatal("native credential proof requires fresh private socket and task-request tmpfs mounts")
		}
	}
	fixture := newWorkerGitHubFixtureWithSource(t, githubauth.PrivateToken)
	listener, err := githubauth.ListenPrivate()
	if err != nil {
		t.Fatal(err)
	}
	private := &http.Server{Handler: githubauth.PrivateHandler(nativeTaskTokenSource{authority: fixture.authority})}
	go func() { _ = private.Serve(listener) }()
	t.Cleanup(func() { _ = private.Close() })
	request := fixture.request
	request.WorkDir = fixture.workDir
	workerHandler, err := WorkerGateway(request, fixture.controller.URL, fixture.monitor.attempt.Ref.SecretName)
	if err != nil {
		t.Fatal(err)
	}
	// The native helper reads the same immutable request's official daemon port.
	workerListener, err := net.Listen("tcp", "127.0.0.1:"+wire.Value(request.Env, "MULTICA_DAEMON_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	worker := &http.Server{Handler: workerHandler}
	go func() { _ = worker.Serve(workerListener) }()
	t.Cleanup(func() { _ = worker.Close() })
	baseEnv := []string{"PATH=/usr/local/go/bin:/usr/bin:/bin", "HOME=" + t.TempDir(), "GIT_CONFIG_NOSYSTEM=1"}
	gitEnv, err := githubauth.GitEnvironment(baseEnv)
	if err != nil {
		t.Fatal(err)
	}
	commands := []struct {
		name string
		args []string
		env  []string
	}{
		{name: "runtime credential", args: []string{binary, "github", "credential", "get"}, env: baseEnv},
		{name: "native Git credential fill", args: []string{"git", "credential", "fill"}, env: gitEnv},
	}
	credential := func(command []string, env []string, repository string) (githubapp.Token, error) {
		child := exec.CommandContext(t.Context(), command[0], command[1:]...)
		child.Dir, child.Env = fixture.workDir, env
		child.Stdin = strings.NewReader("protocol=https\nhost=github.com\npath=acme/" + repository + ".git\n\n")
		output, err := child.Output()
		for _, line := range strings.Split(string(output), "\n") {
			if key, value, ok := strings.Cut(line, "="); ok && key == "password" {
				return githubapp.Token{Value: value}, err
			}
		}
		return githubapp.Token{}, err
	}
	for _, command := range commands {
		token, err := credential(command.args, command.env, "alpha")
		if err != nil || !fixture.authority.canRead(token, "acme", "alpha") || fixture.authority.canRead(token, "acme", "beta") {
			t.Fatalf("%s failed controller repository authorization: %v", command.name, err)
		}
	}
	// Creating the real fixed request path changes RequestToken to worker mode.
	// The file is the exact request serialized into the immutable task Secret.
	raw, err := json.Marshal(fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(wire.RequestPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0400)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(wire.RequestPath) })
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		token, err := credential(command.args, command.env, "alpha")
		if err != nil || !fixture.authority.canRead(token, "acme", "alpha") || fixture.authority.canRead(token, "acme", "beta") {
			t.Fatalf("%s failed worker repository authorization: %v", command.name, err)
		}
		outside, _ := credential(command.args, command.env, "secret")
		if fixture.authority.canRead(outside, "acme", "secret") || fixture.authority.canRead(outside, "acme", "alpha") {
			t.Fatal("worker Git request acquired a credential outside its observed task grant")
		}
	}
	t.Run("native GitHub CLI obtains current task credentials", func(t *testing.T) {
		gh := os.Getenv("GITHUB_AUTH_NATIVE_GH")
		if gh == "" {
			t.Skip("requires the native GitHub CLI at an immutable mounted path")
		}
		invoke := func(env []string) (githubapp.Token, error) {
			// gh auth token resolves its environment credential locally; it never
			// calls a live GitHub endpoint. Capture its sensitive stdout in memory.
			child := exec.CommandContext(t.Context(), binary, "github", "gh", gh, "auth", "token")
			child.Dir, child.Env = fixture.workDir, env
			output, err := child.Output()
			return githubapp.Token{Value: strings.TrimSpace(string(output))}, err
		}
		ghEnv := append(append([]string{}, baseEnv...), "GH_REPO=acme/alpha")
		first, err := invoke(ghEnv)
		if err != nil || !fixture.authority.canRead(first, "acme", "alpha") || fixture.authority.canRead(first, "acme", "beta") {
			t.Fatalf("native GitHub CLI could not authenticate inside the task repository grant: %v", err)
		}
		// The prior credential stops authorizing reads. A later invocation must
		// obtain current authority even if the shell retained the old value.
		fixture.authority.mu.Lock()
		delete(fixture.authority.tokens, first.Value)
		fixture.authority.mu.Unlock()
		staleEnv := append(append([]string{}, ghEnv...), "GH_TOKEN="+first.Value, "GITHUB_TOKEN="+first.Value)
		current, err := invoke(staleEnv)
		if err != nil || !fixture.authority.canRead(current, "acme", "alpha") {
			t.Fatalf("next native GitHub CLI invocation reused revoked shell credentials: %v", err)
		}
		outside, _ := invoke(append(append([]string{}, baseEnv...), "GH_REPO=acme/secret"))
		if fixture.authority.canRead(outside, "acme", "secret") || fixture.authority.canRead(outside, "acme", "alpha") {
			t.Fatal("native GitHub CLI obtained authority outside its task's observed repositories")
		}
	})
}
