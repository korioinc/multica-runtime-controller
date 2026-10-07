package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func TestStopCheckoutsWaitsForControllerWriter(t *testing.T) {
	task, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workdir := filepath.Join(task, "workdir")
	if err := os.Mkdir(workdir, 0700); err != nil {
		t.Fatal(err)
	}
	written := filepath.Join(workdir, "controller-copy")
	started := make(chan struct{})
	allowWrite := make(chan struct{})
	writerDone := make(chan error, 1)
	barrierSeen := make(chan struct{})
	var release sync.Once
	finishWrite := func() { release.Do(func() { close(allowWrite) }) }
	defer finishWrite()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch filepath.Base(r.URL.Path) {
		case "checkout":
			_, _ = io.Copy(io.Discard, r.Body)
			go func() { <-allowWrite; writerDone <- os.WriteFile(written, []byte("completed controller copy"), 0600) }()
			close(started)
			<-r.Context().Done()
		case "checkout-stop":
			if r.Header.Get("Authorization") != "Bearer stop-authority" {
				http.Error(w, "denied", http.StatusForbidden)
				return
			}
			close(barrierSeen)
			if err := <-writerDone; err != nil {
				http.Error(w, "copy failed", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(struct{}{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer func() { finishWrite(); server.Close() }()
	taskAPI := relay(wire.Bootstrap{GatewayURL: server.URL, TaskRoot: task, StopCapability: "stop-authority"}, gatewayClient())
	defer taskAPI.cancel()
	body, err := json.Marshal(wire.CheckoutRequest{URL: "https://example.invalid/repo.git"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/repo/checkout", bytes.NewReader(body))
	handlerDone := make(chan struct{})
	go func() { taskAPI.ServeHTTP(httptest.NewRecorder(), request); close(handlerDone) }()
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- taskAPI.stopCheckouts(t.Context()) }()
	<-barrierSeen
	select {
	case err := <-stopped:
		t.Fatal("worker accepted sealing while controller copy remained active", err)
	default:
	}
	finishWrite()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	<-handlerDone
	content, err := os.ReadFile(written)
	if err != nil || string(content) != "completed controller copy" {
		t.Fatal("worker closed before controller writes completed", err)
	}
}

func TestStopCheckoutsRefusesUnprovenControllerWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "writer termination unknown", http.StatusServiceUnavailable)
		cancel()
	}))
	defer server.Close()
	taskAPI := relay(wire.Bootstrap{GatewayURL: server.URL}, gatewayClient())
	defer taskAPI.cancel()
	if err := taskAPI.stopCheckouts(ctx); err == nil {
		t.Fatal("worker accepted filesystem sealing without controller writer proof")
	}
}
