package githubapp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type accessFixture struct {
	*githubFixture
	repositoryID     atomic.Int64
	installationID   atomic.Int64
	denyRepository   atomic.Bool
	denyInstallation atomic.Bool
}

func newAccessFixture(t *testing.T) (*accessFixture, *Manager) {
	t.Helper()
	base, manager := newGitHubFixture(t)
	fixture := &accessFixture{githubFixture: base}
	fixture.repositoryID.Store(123)
	fixture.installationID.Store(7)
	server := httptest.NewServer(fixture)
	t.Cleanup(server.Close)
	manager.baseURL = server.URL
	return fixture, manager
}

func (f *accessFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/repos/acme/alpha" {
		token := Token{Value: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")}
		if f.denyRepository.Load() || !f.canRead(token, "alpha") {
			http.Error(w, "repository unavailable", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": f.repositoryID.Load(), "full_name": "acme/alpha"})
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/repos/acme/alpha/installation" {
		f.mu.Lock()
		valid := f.authenticateJWT(r.Header.Get("Authorization"))
		f.mu.Unlock()
		if !valid || f.denyInstallation.Load() {
			http.Error(w, "installation unavailable", http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": f.installationID.Load(), "account": map[string]string{"login": "acme"}, "permissions": map[string]string{"contents": "write"}})
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/app/installations/"+strconv.FormatInt(f.installationID.Load(), 10)+"/access_tokens" {
		// Reuse the scoped-credential fixture for this incarnation's exchange.
		r.URL.Path = "/app/installations/7/access_tokens"
	}
	f.githubFixture.ServeHTTP(w, r)
}

func TestRepositoryAccessIdentitySurvivesRenewalAndSeparatesIncarnations(t *testing.T) {
	fixture, manager := newAccessFixture(t)
	repository := Repository{Owner: "acme", Name: "alpha"}
	first, err := manager.RepositoryAccess(t.Context(), repository)
	if err != nil {
		t.Fatal(err)
	}
	if !fixture.canRead(first.Token, "alpha") || strings.Contains(first.Identity, first.Token.Value) {
		t.Fatal("repository authority is unusable or exposes its credential in the persistent identity")
	}
	fixture.mu.Lock()
	fixture.clock = first.Token.ExpiresAt.Add(time.Second)
	fixture.mu.Unlock()
	renewed, err := manager.RepositoryAccess(t.Context(), repository)
	if err != nil || renewed.Identity != first.Identity || renewed.Token.Value == first.Token.Value {
		t.Fatalf("token renewal changed cache identity or reused an expired token: %v", err)
	}
	fixture.repositoryID.Store(124)
	recreated, err := manager.RepositoryAccess(t.Context(), repository)
	if err != nil || recreated.Identity == renewed.Identity {
		t.Fatalf("recreated repository inherited old cache identity: %v", err)
	}
	fixture.installationID.Store(8)
	reinstalled, err := manager.RepositoryAccess(t.Context(), repository)
	if err != nil || reinstalled.Identity == recreated.Identity || reinstalled.Token.Value == recreated.Token.Value {
		t.Fatalf("replacement installation inherited old cache authority: %v", err)
	}
}

func TestRepositoryAccessRechecksCachedAuthority(t *testing.T) {
	for _, revoked := range []string{"installation", "repository", "identity"} {
		t.Run(revoked, func(t *testing.T) {
			fixture, manager := newAccessFixture(t)
			repository := Repository{Owner: "acme", Name: "alpha"}
			if _, err := manager.RepositoryAccess(t.Context(), repository); err != nil {
				t.Fatal(err)
			}
			switch revoked {
			case "installation":
				fixture.denyInstallation.Store(true)
			case "repository":
				fixture.denyRepository.Store(true)
			case "identity":
				fixture.repositoryID.Store(0)
			}
			access, err := manager.RepositoryAccess(t.Context(), repository)
			if err == nil || access.Token.Value != "" || access.Identity != "" {
				t.Fatal("cached token bypassed current repository authority")
			}
		})
	}
}

func TestAuthorizeStillAcceptsRepositoryWithoutImmutableID(t *testing.T) {
	fixture, manager := newAccessFixture(t)
	fixture.repositoryID.Store(0)
	repository := Repository{Owner: "acme", Name: "alpha"}
	token, err := manager.Token(t.Context(), []Repository{repository})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Authorize(t.Context(), repository, token); err != nil {
		t.Fatalf("legacy authorization now requires an immutable ID: %v", err)
	}
}

func TestRepositoryAccessRejectsInstallationReplacementDuringAuthorization(t *testing.T) {
	fixture, manager := newAccessFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchanged := r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/app/installations/")
		fixture.ServeHTTP(w, r)
		if exchanged {
			// The old installation token was issued, then the App was reinstalled.
			fixture.installationID.Store(8)
		}
	}))
	t.Cleanup(server.Close)
	manager.baseURL = server.URL
	access, err := manager.RepositoryAccess(t.Context(), Repository{Owner: "acme", Name: "alpha"})
	if err == nil || access.Token.Value != "" || access.Identity != "" {
		t.Fatal("credential from a previous installation was paired with the new identity")
	}
}
