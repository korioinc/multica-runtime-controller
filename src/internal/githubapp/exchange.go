package githubapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (m *Manager) issue(ctx context.Context, repositories []Repository) (Token, error) {
	jwt, err := m.jwt()
	if err != nil {
		return Token{}, err
	}
	// One App has one installation per owning account. The exchange below asks
	// GitHub to enforce membership of every named repository in that installation.
	repository := repositories[0]
	var installation struct {
		ID      int64 `json:"id"`
		Account struct {
			Login string `json:"login"`
		} `json:"account"`
		SuspendedAt *time.Time        `json:"suspended_at"`
		Permissions map[string]string `json:"permissions"`
	}
	if err := m.request(ctx, http.MethodGet, "/repos/"+repository.Owner+"/"+repository.Name+"/installation", jwt, nil, &installation); err != nil {
		return Token{}, fmt.Errorf("discover GitHub App installation: %w", err)
	}
	if installation.ID <= 0 || !strings.EqualFold(installation.Account.Login, repository.Owner) || installation.SuspendedAt != nil {
		return Token{}, errors.New("GitHub App installation does not match an active repository owner")
	}
	if level := installation.Permissions["contents"]; level != "read" && level != "write" {
		return Token{}, errors.New("GitHub App installation requires repository contents read or write permission")
	}
	// Explicitly request only coding-related repository permissions. Omitting
	// this map would inherit organization and administration grants from the App.
	permissions := make(map[string]string)
	for _, name := range []string{"contents", "metadata", "pull_requests", "issues", "actions", "checks", "statuses", "workflows", "deployments", "discussions"} {
		if level := installation.Permissions[name]; level == "read" || level == "write" {
			permissions[name] = level
		}
	}
	names := make([]string, len(repositories))
	for i, repo := range repositories {
		names[i] = repo.Name
	}
	body, err := json.Marshal(struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}{Repositories: names, Permissions: permissions})
	if err != nil {
		return Token{}, errors.New("encode GitHub repository token scope")
	}
	var response struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := m.request(ctx, http.MethodPost, "/app/installations/"+strconv.FormatInt(installation.ID, 10)+"/access_tokens", jwt, body, &response); err != nil {
		return Token{}, fmt.Errorf("issue GitHub App installation token: %w", err)
	}
	if response.Token == "" || len(response.Token) > 64*1024 || strings.IndexFunc(response.Token, func(r rune) bool { return r <= 0x20 || r >= 0x7f }) >= 0 ||
		!response.ExpiresAt.After(m.now().Add(refreshWindow)) {
		return Token{}, errors.New("GitHub returned an unusable installation token")
	}
	return Token{Value: response.Token, ExpiresAt: response.ExpiresAt}, nil
}

func (m *Manager) jwt() (string, error) {
	now := m.now()
	claims, err := json.Marshal(struct {
		Issuer    string `json:"iss"`
		IssuedAt  int64  `json:"iat"`
		ExpiresAt int64  `json:"exp"`
	}{Issuer: m.appID, IssuedAt: now.Add(-time.Minute).Unix(), ExpiresAt: now.Add(9 * time.Minute).Unix()})
	if err != nil {
		return "", errors.New("encode GitHub App authentication claims")
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	unsigned := header + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, m.privateKey, crypto.SHA256, digest[:])
	if err != nil {
		return "", errors.New("sign GitHub App authentication token")
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (m *Manager) request(ctx context.Context, method, path, jwt string, body []byte, result any) error {
	req, err := http.NewRequestWithContext(ctx, method, m.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return errors.New("build GitHub App request")
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := m.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Transport errors and response bodies may contain sensitive provider data.
		return errors.New("GitHub App request could not be completed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("GitHub App request rejected (HTTP %d)", response.StatusCode)
	}
	const maxBody = 1024 * 1024
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxBody+1))
	if err != nil || len(payload) > maxBody {
		return errors.New("GitHub App response could not be read within its size limit")
	}
	if err := json.Unmarshal(payload, result); err != nil {
		return errors.New("GitHub App response could not be decoded")
	}
	return nil
}
