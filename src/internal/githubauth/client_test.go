package githubauth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
)

func TestTokenClientDoesNotAcceptRedirectedAuthority(t *testing.T) {
	grant := githubapp.Token{Value: "authorized-installation-credential", ExpiresAt: time.Now().Add(time.Hour)}
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(grant)
	}))
	defer issuer.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, issuer.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := tokenClient(transport)
	ctx := context.Background()

	// A grant obtained directly from the selected authority remains usable.
	accepted, err := exchangeToken(ctx, client, issuer.URL, Request{})
	if err != nil || accepted.Value != grant.Value {
		t.Fatal("the selected token authority could not grant access")
	}
	refused, err := exchangeToken(ctx, client, redirect.URL, Request{})
	if err == nil || refused.Value != "" {
		t.Fatal("a redirect changed which authority could grant a credential")
	}
}

func TestTokenClientDoesNotReleaseExpiredOrInjectedCredentials(t *testing.T) {
	for _, token := range []githubapp.Token{
		{Value: "expired-installation-credential", ExpiresAt: time.Unix(1, 0)},
		{Value: "credential\npassword=another-identity", ExpiresAt: time.Now().Add(time.Hour)},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(token)
		}))
		transport := &http.Transport{}
		result, err := exchangeToken(context.Background(), tokenClient(transport), server.URL, Request{})
		transport.CloseIdleConnections()
		server.Close()
		if err == nil || result.Value != "" {
			t.Fatal("an unusable credential escaped to the caller")
		}
		if strings.Contains(err.Error(), token.Value) {
			t.Fatal("a refused credential escaped through an error")
		}
	}
}

func TestTokenClientDoesNotExposeProviderFailureSecrets(t *testing.T) {
	secret := "private-provider-error-credential"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, secret)
	}))
	defer server.Close()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	result, err := exchangeToken(context.Background(), tokenClient(transport), server.URL, Request{})
	if err == nil || result.Value != "" || strings.Contains(err.Error(), secret) {
		t.Fatal("an upstream failure exposed its private credential")
	}
}
