package intercept

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/snapshot"
)

const BrokerTokenHeader = "X-Multica-Broker-Token"
const CheckoutBranchHeader = "X-Multica-Checkout-Branch"
const CheckoutArchiveType = "application/vnd.multica.checkout+tar"

// The shim owns preparation files. A port is not an identity: this instance's
// random capability must match on every request, including after port reuse.
func startCheckoutBroker(request Request) (int, string, func(), error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, "", nil, err
	}
	capability := make([]byte, 32)
	if _, err := rand.Read(capability); err != nil {
		listener.Close()
		return 0, "", nil, err
	}
	token := base64.RawURLEncoding.EncodeToString(capability)
	port, err := strconv.Atoi(requestEnvironmentValue(request.Env, "MULTICA_DAEMON_PORT"))
	if err != nil || port < 1 || port > 65535 {
		listener.Close()
		return 0, "", nil, errors.New("invalid official daemon port")
	}
	broker := &checkoutBroker{request: request, token: token, upstream: "http://127.0.0.1:" + strconv.Itoa(port), client: &http.Client{Transport: &http.Transport{}}}
	server := &http.Server{Handler: broker, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	close := func() { _ = server.Close(); broker.client.CloseIdleConnections() }
	return listener.Addr().(*net.TCPAddr).Port, token, close, nil
}

type checkoutBroker struct {
	request  Request
	token    string
	upstream string
	client   *http.Client
}

func (b *checkoutBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if b.token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get(BrokerTokenHeader)), []byte(b.token)) != 1 {
		http.Error(w, "invalid checkout broker capability", http.StatusUnauthorized)
		return
	}
	if r.Method != http.MethodPost || r.URL.Path != "/repo/checkout" {
		http.NotFound(w, r)
		return
	}
	var input struct {
		URL         string `json:"url"`
		Ref         string `json:"ref"`
		TaskID      string `json:"task_id"`
		WorkspaceID string `json:"workspace_id"`
		RetryBusy   bool   `json:"retry_busy"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, maxDaemonCheckoutBody)).Decode(&input) != nil ||
		input.TaskID != requestEnvironmentValue(b.request.Env, "MULTICA_TASK_ID") ||
		input.WorkspaceID != requestEnvironmentValue(b.request.Env, "MULTICA_WORKSPACE_ID") {
		http.Error(w, "checkout does not belong to this broker's task", http.StatusForbidden)
		return
	}
	input.URL = strings.TrimSpace(input.URL)
	if !slices.Contains(b.request.RepositoryURLs, input.URL) {
		http.Error(w, "checkout URL is not assigned to this task; use the repository URL supplied in the task", http.StatusForbidden)
		return
	}
	stage, err := os.MkdirTemp(b.request.WorkDir, ".runtime-checkout-")
	if err != nil {
		http.Error(w, "create controller checkout staging", http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(stage)
	stage, err = filepath.EvalSymlinks(stage)
	if err != nil {
		http.Error(w, "resolve controller checkout staging", http.StatusInternalServerError)
		return
	}
	body, _ := json.Marshal(map[string]any{"url": input.URL, "ref": input.Ref, "task_id": input.TaskID, "workspace_id": input.WorkspaceID, "workdir": stage, "checkout_mode": "isolated", "retry_busy": input.RetryBusy})
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, b.upstream+"/repo/checkout", bytes.NewReader(body))
	if err != nil {
		http.Error(w, "create official checkout request", http.StatusInternalServerError)
		return
	}
	upstream.Header.Set("Content-Type", "application/json")
	upstream.Header.Set("Authorization", "Bearer "+requestEnvironmentValue(b.request.Env, "MULTICA_TOKEN"))
	response, err := b.client.Do(upstream)
	if err != nil {
		http.Error(w, "official repository checkout failed", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		for key, values := range response.Header {
			w.Header()[key] = values
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, io.LimitReader(response.Body, maxDaemonCheckoutBody))
		return
	}
	var result checkout.Result
	if json.NewDecoder(io.LimitReader(response.Body, maxDaemonCheckoutBody)).Decode(&result) != nil || filepath.Dir(result.Path) != stage {
		http.Error(w, "official checkout returned an unexpected directory", http.StatusBadGateway)
		return
	}
	if err := standaloneCheckout(result.Path); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", CheckoutArchiveType)
	w.Header().Set(CheckoutBranchHeader, base64.RawURLEncoding.EncodeToString([]byte(result.BranchName)))
	if err := snapshot.Write(w, result.Path); err != nil {
		// Extraction also requires the completion record, so a broken stream
		// can never publish a partly prepared repository.
		panic(http.ErrAbortHandler)
	}
}

func standaloneCheckout(directory string) error {
	for _, name := range []string{directory, filepath.Join(directory, ".git")} {
		info, err := os.Lstat(name)
		if err != nil || !info.IsDir() {
			return errors.New("official checkout is not a standalone Git directory")
		}
	}
	for _, name := range []string{"objects/info/alternates", "objects/info/http-alternates", "commondir"} {
		if _, err := os.Lstat(filepath.Join(directory, ".git", name)); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("official checkout has an external Git dependency: %s", name)
		}
	}
	return nil
}
