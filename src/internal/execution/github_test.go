package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The external token authority preserves the scope passed by the real task
// broker. A credential proves authorization only when it permits a protected
// repository read; proxy status codes and response representations are not the
// oracle. App signing and expiry are covered at the githubapp owner.
type taskGitHubAuthority struct {
	mu     sync.Mutex
	tokens map[string]map[githubapp.Repository]bool
}

func (a *taskGitHubAuthority) issue(_ context.Context, scope []githubapp.Repository) (githubapp.Token, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	value := uuid.NewString()
	repositories := make(map[githubapp.Repository]bool, len(scope))
	for _, repository := range scope {
		repositories[repository] = true
	}
	a.tokens[value] = repositories
	return githubapp.Token{Value: value, ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (a *taskGitHubAuthority) canRead(token githubapp.Token, owner, name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tokens[token.Value][githubapp.Repository{Owner: owner, Name: name}]
}

type workerGitHubFixture struct {
	monitor    monitorFixture
	request    wire.Request
	authority  *taskGitHubAuthority
	broker     *httptest.Server
	controller *httptest.Server
	worker     *httptest.Server
	workDir    string
}

func newWorkerGitHubFixture(t *testing.T) workerGitHubFixture {
	return newWorkerGitHubFixtureWithSource(t, nil)
}

func newWorkerGitHubFixtureWithSource(t *testing.T, source taskTokenSource) workerGitHubFixture {
	t.Helper()
	fixture := workerGitHubFixture{
		monitor:   newMonitorFixture(t),
		authority: &taskGitHubAuthority{tokens: make(map[string]map[githubapp.Repository]bool)},
	}
	fixture.request = fixture.monitor.request
	fixture.request.Env = append(slices.Clone(fixture.request.Env), githubauth.EnabledEnv+"=true", "MULTICA_TOKEN="+uuid.NewString())
	fixture.request.RepositoryURLs = []string{"https://github.com/acme/alpha.git", "https://github.com/acme/beta.git"}
	fixture.request.BrokerToken = uuid.NewString()
	// Exercise exactly the production broker's capability and scope enforcement.
	// Checkout is outside this proof, so its client is not instantiated.
	if source == nil {
		source = fixture.authority.issue
	}
	fixture.broker = httptest.NewServer(taskBrokerHandler(fixture.request, fixture.request.BrokerToken, nil, source))
	t.Cleanup(fixture.broker.Close)
	fixture.request.BrokerPort = fixture.broker.Listener.Addr().(*net.TCPAddr).Port
	raw, err := json.Marshal(fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	fixture.monitor.attempt.Ref.RequestDigest = wire.Digest(raw)
	if _, err := fixture.monitor.runner.resources.CreateSecret(t.Context(), fixture.monitor.attempt.Ref, fixture.request); err != nil {
		t.Fatal(err)
	}
	fixture.controller = httptest.NewServer(ControllerGateway(fixture.monitor.runner.resources))
	t.Cleanup(fixture.controller.Close)
	// The mounted task directory is represented by a canonical temporary path.
	// Its authenticated immutable request still uses the real /workspace layout.
	fixture.workDir, err = filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixture.worker = fixture.newWorker(t, fixture.request, fixture.monitor.attempt.Ref.SecretName)
	return fixture
}

func (f workerGitHubFixture) newWorker(t *testing.T, request wire.Request, secret string) *httptest.Server {
	t.Helper()
	request.WorkDir = f.workDir
	handler, err := WorkerGateway(request, f.controller.URL, secret)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func requestTaskGitHubCredential(t *testing.T, server *httptest.Server, selected string, headers map[string]string) githubapp.Token {
	t.Helper()
	body, err := json.Marshal(githubauth.Request{Repository: selected})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+githubauth.Route, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var token githubapp.Token
	// Refused authorization deliberately returns no usable credential. A
	// successful request must subsequently complete the protected read below.
	_ = json.NewDecoder(response.Body).Decode(&token)
	return token
}

func TestWorkerGitHubAuthorizationHonorsObservedRepositories(t *testing.T) {
	fixture := newWorkerGitHubFixture(t)
	group := requestTaskGitHubCredential(t, fixture.worker, "", nil)
	if !fixture.authority.canRead(group, "acme", "alpha") || !fixture.authority.canRead(group, "acme", "beta") || fixture.authority.canRead(group, "acme", "secret") {
		t.Fatal("worker could not authenticate within its complete observed task scope")
	}
	selected := requestTaskGitHubCredential(t, fixture.worker, "https://GITHUB.COM/ACME/ALPHA.GIT", nil)
	if !fixture.authority.canRead(selected, "acme", "alpha") || fixture.authority.canRead(selected, "acme", "beta") {
		t.Fatal("selected repository authorization escaped or lost its task grant")
	}
	for _, target := range []string{
		"https://github.com/acme/secret.git",
		"https://github.com/other/alpha.git",
		"https://github.com/acme/alpha/../secret.git",
		"https://github.com/acme/%61lpha.git",
		"https://attacker@github.com/acme/alpha.git",
	} {
		token := requestTaskGitHubCredential(t, fixture.worker, target, nil)
		if fixture.authority.canRead(token, "acme", "alpha") || fixture.authority.canRead(token, "acme", "secret") || fixture.authority.canRead(token, "other", "alpha") {
			t.Fatal("unassigned or ambiguous repository request obtained a protected read")
		}
	}
}

func TestWorkerGitHubCredentialIsBoundToTaskIdentityAndActiveRequest(t *testing.T) {
	fixture := newWorkerGitHubFixture(t)
	target := "https://github.com/acme/alpha.git"
	valid := requestTaskGitHubCredential(t, fixture.worker, target, nil)
	if !fixture.authority.canRead(valid, "acme", "alpha") {
		t.Fatal("valid task could not authenticate before authority changes")
	}
	wrongToken := fixture.request
	wrongToken.Env = append(slices.Clone(wrongToken.Env), "MULTICA_TOKEN="+uuid.NewString())
	wrongTask := fixture.request
	wrongTask.TaskID = uuid.NewString()
	for _, unauthorized := range []*httptest.Server{
		fixture.newWorker(t, wrongToken, fixture.monitor.attempt.Ref.SecretName),
		fixture.newWorker(t, wrongTask, fixture.monitor.attempt.Ref.SecretName),
		fixture.newWorker(t, fixture.request, "task-request-"+uuid.NewString()),
	} {
		token := requestTaskGitHubCredential(t, unauthorized, target, nil)
		if fixture.authority.canRead(token, "acme", "alpha") {
			t.Fatal("mismatched task identity obtained the observed task's repository credential")
		}
	}
	ref := fixture.monitor.attempt.Ref
	if err := fixture.monitor.runner.resources.API.CoreV1().Secrets(ref.Namespace).Delete(t.Context(), ref.SecretName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	afterRemoval := requestTaskGitHubCredential(t, fixture.worker, target, nil)
	if fixture.authority.canRead(afterRemoval, "acme", "alpha") {
		t.Fatal("worker obtained a new credential after task request authority was removed")
	}
}

func TestTaskGitHubBrokerRequiresItsOwnAttemptCapability(t *testing.T) {
	fixture := newWorkerGitHubFixture(t)
	target := "https://github.com/acme/alpha.git"
	valid := requestTaskGitHubCredential(t, fixture.broker, target, map[string]string{wire.CapabilityHeader: fixture.request.BrokerToken})
	if !fixture.authority.canRead(valid, "acme", "alpha") {
		t.Fatal("active attempt capability could not authorize its assigned repository")
	}
	for _, capability := range []string{"", uuid.NewString()} {
		token := requestTaskGitHubCredential(t, fixture.broker, target, map[string]string{wire.CapabilityHeader: capability})
		if fixture.authority.canRead(token, "acme", "alpha") {
			t.Fatal("missing or foreign attempt capability obtained a task credential")
		}
	}
}
