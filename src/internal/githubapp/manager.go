// Package githubapp obtains repository-scoped GitHub App installation tokens.
package githubapp

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const refreshWindow = 5 * time.Minute

// Token is a short-lived installation credential. Value must never be logged.
type Token struct {
	Value     string
	ExpiresAt time.Time
}

// Manager keeps credentials only in memory and shares renewals for equal scopes.
// It must not be copied after use.
type Manager struct {
	appID      string
	privateKey *rsa.PrivateKey
	client     *http.Client
	baseURL    string
	now        func() time.Time

	mu       sync.Mutex
	cache    map[string]Token
	inflight map[string]*renewal
}

type renewal struct {
	done  chan struct{}
	token Token
	err   error
}

// New validates an App ID and an unencrypted PKCS#1 or PKCS#8 RSA private key.
// The API host is fixed; tasks cannot choose where App credentials are sent.
func New(appID, privateKey string) (*Manager, error) {
	appID = strings.TrimSpace(appID)
	id, err := strconv.ParseInt(appID, 10, 64)
	if err != nil || id <= 0 {
		return nil, errors.New("GitHub App ID must be a positive integer")
	}
	block, rest := pem.Decode([]byte(strings.ReplaceAll(privateKey, `\n`, "\n")))
	if block == nil || strings.TrimSpace(string(rest)) != "" || len(block.Headers) != 0 {
		return nil, errors.New("GitHub App private key must contain one unencrypted RSA PEM key")
	}
	var key *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		var parsed any
		parsed, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err == nil {
			key, _ = parsed.(*rsa.PrivateKey)
		}
	default:
		return nil, errors.New("GitHub App private key must be an RSA PEM key")
	}
	if err != nil || key == nil || key.Validate() != nil {
		return nil, errors.New("GitHub App private key is not a valid RSA key")
	}
	return &Manager{
		appID:      strconv.FormatInt(id, 10),
		privateKey: key,
		client: &http.Client{
			Timeout: 15 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		baseURL:  "https://api.github.com",
		now:      time.Now,
		cache:    make(map[string]Token),
		inflight: make(map[string]*renewal),
	}, nil
}

// Token obtains a credential limited to the requested repositories. A cached
// credential is reused only while more than five minutes remain. A failed
// required renewal never falls back to the old credential.
func (m *Manager) Token(ctx context.Context, repositories []Repository) (Token, error) {
	scope, key, err := canonicalScope(repositories)
	if err != nil {
		return Token{}, err
	}
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}
	m.mu.Lock()
	now := m.now()
	if token, ok := m.cache[key]; ok && token.ExpiresAt.After(now.Add(refreshWindow)) {
		m.mu.Unlock()
		return token, nil
	}
	pending, exists := m.inflight[key]
	if !exists {
		// Remove expired scopes opportunistically to avoid retaining old credentials.
		for cachedKey, token := range m.cache {
			if !token.ExpiresAt.After(now) {
				delete(m.cache, cachedKey)
			}
		}
		pending = &renewal{done: make(chan struct{})}
		m.inflight[key] = pending
		// A task cancelling its own request must not cancel the shared renewal
		// needed by other workers. The independent exchange remains time-bounded.
		renewCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		go func() {
			defer cancel()
			m.renew(renewCtx, scope, key, pending)
		}()
	}
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return Token{}, ctx.Err()
	case <-pending.done:
		if err := ctx.Err(); err != nil {
			return Token{}, err
		}
		return pending.token, pending.err
	}
}

func (m *Manager) renew(ctx context.Context, scope []Repository, key string, pending *renewal) {
	token, err := m.issue(ctx, scope)

	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		m.cache[key] = token
	} else {
		delete(m.cache, key)
	}
	pending.token, pending.err = token, err
	delete(m.inflight, key)
	close(pending.done)
}
