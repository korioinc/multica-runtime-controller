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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// PrivateToken asks the controller's private broker for a repository grant.
// The socket is intentionally fixed and is not mounted into task workers.
func PrivateToken(ctx context.Context, repositories []githubapp.Repository) (githubapp.Token, error) {
	if len(repositories) == 0 || len(repositories) > 500 {
		return githubapp.Token{}, errors.New("GitHub token requires an explicit repository scope")
	}
	scope := make([]githubapp.Repository, len(repositories))
	for i, repository := range repositories {
		parsed, err := githubapp.ParseRepository(repository.Owner + "/" + repository.Name)
		if err != nil {
			return githubapp.Token{}, errors.New("invalid GitHub token repository scope")
		}
		scope[i] = parsed
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "unix", SocketPath)
	}}
	defer transport.CloseIdleConnections()
	return exchangeToken(ctx, tokenClient(transport), "http://localhost"+Route, PrivateRequest{Repositories: scope})
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
	file, err := os.Open(wire.RequestPath)
	if errors.Is(err, os.ErrNotExist) {
		if repository == "" {
			return githubapp.Token{}, errors.New("select a GitHub repository outside a task worker")
		}
		return PrivateToken(ctx, []githubapp.Repository{parsed})
	}
	if err != nil {
		return githubapp.Token{}, errors.New("GitHub task authorization is unavailable")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, wire.MaxRequestBytes+1))
	if err != nil || len(raw) > wire.MaxRequestBytes {
		return githubapp.Token{}, errors.New("GitHub task authorization could not be read")
	}
	request, err := wire.Decode(raw)
	if err != nil {
		return githubapp.Token{}, errors.New("GitHub task authorization is invalid")
	}
	port, err := strconv.Atoi(wire.Value(request.Env, "MULTICA_DAEMON_PORT"))
	if err != nil || port < 1 || port > 65535 {
		return githubapp.Token{}, errors.New("GitHub task gateway is unavailable")
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext}
	defer transport.CloseIdleConnections()
	return exchangeToken(ctx, tokenClient(transport), "http://127.0.0.1:"+strconv.Itoa(port)+Route, Request{Repository: repository})
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
