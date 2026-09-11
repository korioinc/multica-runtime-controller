package githubapp

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The fixture enforces App signatures, repository membership, permissions and
// credential expiry. Tests observe protected operations, not request shapes.
type githubFixture struct {
	mu           sync.Mutex
	clock        time.Time
	publicKey    *rsa.PublicKey
	tokens       map[string]fixtureCredential
	denyExchange bool
	owner        string
}

type fixtureCredential struct {
	repositories map[string]bool
	permissions  map[string]string
	expiresAt    time.Time
}

func newGitHubFixture(t *testing.T) (*githubFixture, *Manager) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	// PKCS#8 is used here; PKCS#1 parsing is verified with the same authentication
	// fixture in the redirect test below.
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := New("42", string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})))
	if err != nil {
		t.Fatal(err)
	}
	fixture := &githubFixture{
		clock:     time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		publicKey: &key.PublicKey,
		tokens:    make(map[string]fixtureCredential),
		owner:     "acme",
	}
	server := httptest.NewServer(fixture)
	t.Cleanup(server.Close)
	manager.baseURL = server.URL
	manager.now = fixture.now
	return fixture, manager
}

func (f *githubFixture) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clock
}

func (f *githubFixture) authenticateJWT(value string) bool {
	parts := strings.Split(strings.TrimPrefix(value, "Bearer "), ".")
	if len(parts) != 3 {
		return false
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(f.publicKey, crypto.SHA256, digest[:], signature) != nil {
		return false
	}
	claims, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var parsed struct {
		Issuer    string `json:"iss"`
		IssuedAt  int64  `json:"iat"`
		ExpiresAt int64  `json:"exp"`
	}
	if json.Unmarshal(claims, &parsed) != nil {
		return false
	}
	return parsed.Issuer == "42" && parsed.IssuedAt <= f.clock.Unix() && parsed.ExpiresAt > f.clock.Unix()
}

func (f *githubFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.authenticateJWT(r.Header.Get("Authorization")) {
		http.Error(w, "unauthenticated App", http.StatusUnauthorized)
		return
	}
	grants := map[string]string{"contents": "write", "pull_requests": "write", "members": "write", "administration": "write"}
	repositories := map[string]bool{"alpha": true, "beta": true, "secret": true}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/repos/acme/") && strings.HasSuffix(r.URL.Path, "/installation") {
		name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/repos/acme/"), "/installation")
		if !repositories[name] {
			http.Error(w, "repository unavailable", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 7, "account": map[string]string{"login": f.owner}, "permissions": grants})
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/app/installations/7/access_tokens" {
		http.Error(w, "unknown operation", http.StatusNotFound)
		return
	}
	if f.denyExchange {
		// A server error body may contain sensitive details; it is never suitable
		// for propagation to task logs.
		http.Error(w, "private-provider-diagnostic", http.StatusForbidden)
		return
	}
	var request struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}
	if json.NewDecoder(r.Body).Decode(&request) != nil {
		http.Error(w, "invalid scope", http.StatusBadRequest)
		return
	}
	selected := repositories
	if len(request.Repositories) != 0 {
		selected = make(map[string]bool)
		for _, name := range request.Repositories {
			if !repositories[name] {
				http.Error(w, "repository unavailable", http.StatusForbidden)
				return
			}
			selected[name] = true
		}
	}
	permissions := grants
	if request.Permissions != nil {
		permissions = request.Permissions
		for name, level := range permissions {
			if grants[name] == "" || (grants[name] == "read" && level != "read") {
				http.Error(w, "permission unavailable", http.StatusForbidden)
				return
			}
		}
	}
	value := fmt.Sprintf("fixture-credential-%d", len(f.tokens)+1)
	expiresAt := f.clock.Add(time.Hour)
	f.tokens[value] = fixtureCredential{repositories: selected, permissions: permissions, expiresAt: expiresAt}
	_ = json.NewEncoder(w).Encode(map[string]any{"token": value, "expires_at": expiresAt})
}

func (f *githubFixture) canRead(token Token, repository string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	credential, ok := f.tokens[token.Value]
	return ok && credential.expiresAt.After(f.clock) && credential.repositories[repository] && credential.permissions["contents"] != ""
}

func (f *githubFixture) canAdminister(token Token) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	credential, ok := f.tokens[token.Value]
	return ok && credential.expiresAt.After(f.clock) &&
		(credential.permissions["members"] == "write" || credential.permissions["administration"] == "write")
}

func TestTaskScopeDoesNotInheritOtherRepositoriesOrAdministration(t *testing.T) {
	fixture, manager := newGitHubFixture(t)
	group, err := manager.Token(context.Background(), []Repository{{Owner: "ACME", Name: "Alpha"}, {Owner: "acme", Name: "beta"}})
	if err != nil {
		t.Fatal(err)
	}
	if !fixture.canRead(group, "alpha") || !fixture.canRead(group, "beta") {
		t.Fatal("task cannot read its assigned repositories")
	}
	if fixture.canRead(group, "secret") || fixture.canAdminister(group) {
		t.Fatal("task inherited access outside its repository coding scope")
	}
	// A narrower task must not receive a broader credential from the same App's
	// cache, even when all of its repositories overlap an existing scope.
	narrow, err := manager.Token(context.Background(), []Repository{{Owner: "acme", Name: "alpha"}})
	if err != nil {
		t.Fatal(err)
	}
	if !fixture.canRead(narrow, "alpha") || fixture.canRead(narrow, "beta") {
		t.Fatal("cached App authorization crossed task repository scope")
	}
}

func TestConcurrentRequestsRemainAuthorizedAcrossExpiry(t *testing.T) {
	fixture, manager := newGitHubFixture(t)
	scope := []Repository{{Owner: "acme", Name: "alpha"}}
	original, err := manager.Token(context.Background(), scope)
	if err != nil || !fixture.canRead(original, "alpha") {
		t.Fatalf("initial authorization failed: %v", err)
	}
	fixture.mu.Lock()
	fixture.clock = original.ExpiresAt.Add(time.Second)
	fixture.mu.Unlock()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			<-start
			token, err := manager.Token(context.Background(), scope)
			if err != nil || !fixture.canRead(token, "alpha") || fixture.canRead(token, "secret") {
				t.Errorf("concurrent renewal lost scoped authorization: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
}

// observedWaitContext exposes only a scheduling barrier: its caller has entered
// a cancellable wait. It lets the test overlap two real credential requests.
type observedWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *observedWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestCancellingOneTaskDoesNotCancelAnotherTasksAuthorization(t *testing.T) {
	fixture, manager := newGitHubFixture(t)
	exchangeStarted := make(chan struct{})
	allowExchange := make(chan struct{})
	var started sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			started.Do(func() { close(exchangeStarted) })
			select {
			case <-allowExchange:
			case <-r.Context().Done():
				return
			}
		}
		fixture.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	manager.baseURL = server.URL
	ctx, cleanup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanup()
	firstContext, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	secondContext := &observedWaitContext{Context: ctx, waiting: make(chan struct{})}
	scope := []Repository{{Owner: "acme", Name: "alpha"}}
	type result struct {
		token Token
		err   error
	}
	firstResult := make(chan result, 1)
	secondResult := make(chan result, 1)
	go func() {
		token, err := manager.Token(firstContext, scope)
		firstResult <- result{token: token, err: err}
	}()
	<-exchangeStarted
	go func() {
		token, err := manager.Token(secondContext, scope)
		secondResult <- result{token: token, err: err}
	}()
	<-secondContext.waiting
	cancelFirst()
	first := <-firstResult
	close(allowExchange)
	second := <-secondResult
	if first.err == nil || fixture.canRead(first.token, "alpha") {
		t.Fatal("cancelled task received a credential after leaving authorization")
	}
	if second.err != nil || !fixture.canRead(second.token, "alpha") || fixture.canRead(second.token, "secret") {
		t.Fatalf("one task cancellation interrupted another task's scoped authorization: %v", second.err)
	}
}

func TestRequiredRenewalFailureDoesNotReleaseOldCredential(t *testing.T) {
	fixture, manager := newGitHubFixture(t)
	scope := []Repository{{Owner: "acme", Name: "alpha"}}
	original, err := manager.Token(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mu.Lock()
	fixture.clock = original.ExpiresAt.Add(-time.Nanosecond)
	fixture.denyExchange = true
	fixture.mu.Unlock()
	if !fixture.canRead(original, "alpha") {
		t.Fatal("fixture must keep the old credential valid during required renewal")
	}
	token, err := manager.Token(context.Background(), scope)
	if err == nil || fixture.canRead(token, "alpha") {
		t.Fatal("failed required renewal released a usable old credential")
	}
	if strings.Contains(err.Error(), "private-provider-diagnostic") {
		t.Fatal("private provider error body escaped the token boundary")
	}
}

func TestInstallationOwnerMismatchCannotMintTaskCredential(t *testing.T) {
	fixture, manager := newGitHubFixture(t)
	fixture.owner = "another-owner"
	token, err := manager.Token(context.Background(), []Repository{{Owner: "acme", Name: "alpha"}})
	if err == nil || fixture.canRead(token, "alpha") {
		t.Fatal("an installation belonging to another owner authorized this task")
	}
}

func TestAppCredentialsCannotFollowRedirect(t *testing.T) {
	fixture, existing := newGitHubFixture(t)
	manager, err := New("42", string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(existing.privateKey)})))
	if err != nil {
		t.Fatal(err)
	}
	manager.baseURL = existing.baseURL
	manager.now = fixture.now
	initial, err := manager.Token(context.Background(), []Repository{{Owner: "acme", Name: "alpha"}})
	if err != nil || !fixture.canRead(initial, "alpha") {
		t.Fatalf("PKCS#1 App authentication failed: %v", err)
	}
	var leaked atomic.Bool
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			leaked.Store(true)
		}
	}))
	t.Cleanup(untrusted.Close)
	trusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fixture.mu.Lock()
		valid := fixture.authenticateJWT(r.Header.Get("Authorization"))
		fixture.mu.Unlock()
		if !valid {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, untrusted.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(trusted.Close)
	manager.baseURL = trusted.URL
	token, err := manager.Token(context.Background(), []Repository{{Owner: "acme", Name: "beta"}})
	if err == nil || fixture.canRead(token, "beta") || leaked.Load() {
		t.Fatal("redirect escaped the trusted App authentication boundary")
	}
}
