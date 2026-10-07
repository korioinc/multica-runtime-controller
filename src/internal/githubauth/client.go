package githubauth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

const TaskAuthorizationName = "github-authorization.json"

// TaskAuthorization is the only per-turn broker authority exposed to Git/gh.
// Supervisor, session-control, and upstream backend credentials stay in PID 1.
type TaskAuthorization struct {
	GatewayURL      string `json:"gatewayURL"`
	CacheCapability string `json:"cacheCapability"`
}

func (a TaskAuthorization) Validate() error {
	u, err := url.Parse(a.GatewayURL)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || len(a.CacheCapability) < 32 || strings.ContainsAny(a.CacheCapability, "\x00\r\n \t") {
		return errors.New("invalid GitHub task authorization")
	}
	return nil
}

// RequestToken uses the mounted task request rather than shell environment
// credentials. An empty repository means the complete observed task scope.
func RequestToken(ctx context.Context, repository string) (githubapp.Token, error) {
	var parsed githubapp.Repository
	if repository != "" {
		var err error
		parsed, err = githubapp.ParseRepositoryURL(repository)
		if err != nil {
			return githubapp.Token{}, errors.New("GitHub token requires an HTTPS github.com repository")
		}
		repository = repositoryURL(parsed)
	}
	file, err := os.Open(wire.ControlRoot + "/" + TaskAuthorizationName)
	legacy := errors.Is(err, os.ErrNotExist)
	if legacy {
		file, err = os.Open(wire.RequestPath)
	}
	if err != nil {
		return githubapp.Token{}, errors.New("GitHub task authorization is unavailable")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, wire.MaxRequestBytes+1))
	if err != nil || len(raw) > wire.MaxRequestBytes {
		return githubapp.Token{}, errors.New("GitHub task authorization could not be read")
	}
	var authorization TaskAuthorization
	if legacy {
		request, err := wire.DecodeBootstrap(raw)
		if err != nil || request.WorkerSessionID != "" {
			return githubapp.Token{}, errors.New("GitHub task authorization is invalid")
		}
		authorization = TaskAuthorization{GatewayURL: request.GatewayURL, CacheCapability: request.CacheCapability}
	} else if json.Unmarshal(raw, &authorization) != nil {
		return githubapp.Token{}, errors.New("GitHub task authorization is invalid")
	}
	if authorization.Validate() != nil {
		return githubapp.Token{}, errors.New("GitHub task authorization is invalid")
	}
	// Task capabilities stay on the internal gateway connection, never an
	// environment-configured HTTP proxy.
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext}
	defer transport.CloseIdleConnections()
	client := tokenClient(attemptTransport{base: transport, capability: authorization.CacheCapability})
	return exchangeToken(ctx, client, authorization.GatewayURL+Route, Request{Repository: repository})
}

type attemptTransport struct {
	base       http.RoundTripper
	capability string
}

func (t attemptTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+t.capability)
	return t.base.RoundTrip(r)
}

func tokenClient(transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: transport,
		Timeout:   35 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func exchangeToken(ctx context.Context, client *http.Client, endpoint string, scope any) (githubapp.Token, error) {
	body, err := json.Marshal(scope)
	if err != nil {
		return githubapp.Token{}, errors.New("GitHub token scope could not be encoded")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return githubapp.Token{}, errors.New("GitHub token request could not be prepared")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return githubapp.Token{}, ctx.Err()
		}
		return githubapp.Token{}, errors.New("GitHub token service is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return githubapp.Token{}, fmt.Errorf("GitHub token request was refused (HTTP %d)", response.StatusCode)
	}
	const maxResponse = 128 << 10
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
	if err != nil || len(raw) > maxResponse {
		return githubapp.Token{}, errors.New("GitHub token response exceeds its size limit or could not be read")
	}
	var token githubapp.Token
	if json.Unmarshal(raw, &token) != nil || !usableToken(token) {
		return githubapp.Token{}, errors.New("GitHub token service returned an unusable credential")
	}
	return token, nil
}

func usableToken(token githubapp.Token) bool {
	return token.Value != "" && len(token.Value) <= 64<<10 && token.ExpiresAt.After(time.Now()) &&
		strings.IndexFunc(token.Value, func(r rune) bool { return r <= 0x20 || r >= 0x7f }) < 0
}

func repositoryURL(repository githubapp.Repository) string {
	return "https://github.com/" + repository.Owner + "/" + repository.Name + ".git"
}
