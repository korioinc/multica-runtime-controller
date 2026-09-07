package official

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
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// NativeCheckoutRequest carries only controller-validated identity and staging.
// The daemon remains responsible for Git credentials, refs and checkout logic.
type NativeCheckoutRequest struct {
	URL, Ref, TaskID, WorkspaceID, WorkDir, Token string
	RetryBusy                                     bool
}

type CheckoutClient struct {
	origin string
	client *http.Client
}
type CheckoutError struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (e *CheckoutError) Error() string {
	return fmt.Sprintf("official checkout refusal: HTTP %d", e.StatusCode)
}

func NewCheckoutClient(origin string, client *http.Client) (*CheckoutClient, error) {
	target, err := url.Parse(origin)
	if err != nil || target.Scheme != "http" || target.Host == "" || target.User != nil || target.Path != "" || target.RawQuery != "" || target.Fragment != "" {
		return nil, errors.New("checkout requires the local official daemon origin")
	}
	address := net.ParseIP(target.Hostname())
	if address == nil || !address.IsLoopback() {
		return nil, errors.New("checkout daemon must listen on loopback")
	}
	if client == nil {
		client = &http.Client{Transport: &http.Transport{}}
	}
	// Never carry the task credential to a redirect destination.
	localClient := *client
	localClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return &CheckoutClient{origin: origin, client: &localClient}, nil
}

func (c *CheckoutClient) CloseIdleConnections() { c.client.CloseIdleConnections() }

func (c *CheckoutClient) Checkout(ctx context.Context, input NativeCheckoutRequest) (wire.Result, error) {
	var result wire.Result
	if !filepath.IsAbs(input.WorkDir) || filepath.Clean(input.WorkDir) != input.WorkDir || !wire.UUID(input.TaskID) || input.WorkspaceID == "" || !strings.HasPrefix(input.Token, "mat_") || strings.TrimSpace(input.URL) == "" {
		return result, errors.New("checkout requires authorized task identity and canonical staging")
	}
	body := struct {
		URL         string `json:"url"`
		Ref         string `json:"ref"`
		TaskID      string `json:"task_id"`
		WorkspaceID string `json:"workspace_id"`
		WorkDir     string `json:"workdir"`
		Mode        string `json:"checkout_mode"`
		RetryBusy   bool   `json:"retry_busy"`
	}{input.URL, input.Ref, input.TaskID, input.WorkspaceID, input.WorkDir, "isolated", input.RetryBusy}
	raw, err := json.Marshal(body)
	if err != nil {
		return result, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+"/repo/checkout", bytes.NewReader(raw))
	if err != nil {
		return result, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+input.Token)
	response, err := c.client.Do(request)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	raw, err = io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return result, errors.New("invalid or oversized official checkout result")
	}
	if response.StatusCode != http.StatusOK {
		return result, &CheckoutError{StatusCode: response.StatusCode, Header: response.Header.Clone(), Body: raw}
	}
	if json.Unmarshal(raw, &result) != nil || !filepath.IsAbs(result.Path) || filepath.Clean(result.Path) != result.Path || filepath.Dir(result.Path) != input.WorkDir {
		return wire.Result{}, errors.New("official checkout returned a directory outside its staging area")
	}
	return result, nil
}
