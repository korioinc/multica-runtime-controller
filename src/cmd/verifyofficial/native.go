package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// nativeCatalog runs the installed daemon against the installed providers.
// Its HTTP backend can request catalog discovery only; no task or model
// generation endpoint exists. Adverse transport cases use separate fake
// providers so a fixture can fail a probe without modifying installed bytes.
func (v *verifier) nativeCatalog(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	home, err := os.MkdirTemp("/tmp", "official-native-home-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(home)
	for _, directory := range []string{".pi/agent", ".codex"} {
		if err := os.MkdirAll(filepath.Join(home, directory), 0700); err != nil {
			return err
		}
	}
	// These synthetic credentials have no external authority. The Pi model
	// definition gives offline catalog discovery a configured native provider.
	models := []byte(`{"providers":{"fixture":{"baseUrl":"http://127.0.0.1:1/v1","apiKey":"disposable-fixture","api":"openai-completions","models":[{"id":"fixture-model","name":"Local fixture","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":8192,"maxTokens":1024}]}}}`)
	if err := os.WriteFile(filepath.Join(home, ".pi/agent/models.json"), models, 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(home, ".codex/auth.json"), []byte(`{"OPENAI_API_KEY":"sk-disposable-local-fixture"}`), 0600); err != nil {
		return err
	}
	var mu sync.Mutex
	byID := map[string]string{}
	requested := map[string]bool{}
	completed := map[string]bool{}
	results := make(chan error, len(v.descriptor.Providers)+4)
	problem := func(err error) {
		select {
		case results <- err:
		default:
		}
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RuntimeID string `json:"runtime_id"`
			Runtimes  []struct {
				Type    string `json:"type"`
				Profile string `json:"profile_id"`
			} `json:"runtimes"`
		}
		if r.Method == http.MethodPost && !strings.Contains(r.URL.Path, "/models/") {
			_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body)
		}
		output := any(map[string]any{})
		switch {
		case r.URL.Path == "/api/daemon/ws":
			http.Error(w, "local HTTP catalog fixture", http.StatusServiceUnavailable)
			return
		case r.URL.Path == "/api/daemon/workspaces":
			output = []any{map[string]any{"id": workspaceID, "slug": "native-catalog", "name": "Native catalog"}}
		case r.URL.Path == "/api/daemon/register":
			runtimes := []any{}
			mu.Lock()
			for _, entry := range body.Runtimes {
				if _, ok := v.descriptor.Providers[entry.Type]; !ok || entry.Profile != "" {
					problem(errors.New("native daemon registered an unsupported provider"))
					continue
				}
				id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("native-catalog/"+entry.Type)).String()
				byID[id] = entry.Type
				runtimes = append(runtimes, map[string]any{"id": id, "provider": entry.Type, "name": "Native catalog", "status": "online"})
			}
			mu.Unlock()
			output = map[string]any{"runtimes": runtimes, "repos": []any{}}
		case r.URL.Path == "/api/daemon/heartbeat":
			value := map[string]any{"runtime_id": body.RuntimeID}
			mu.Lock()
			if byID[body.RuntimeID] != "" && !requested[body.RuntimeID] {
				requested[body.RuntimeID] = true
				value["pending_model_list"] = map[string]any{"id": "native-catalog"}
			}
			mu.Unlock()
			output = value
		case strings.Contains(r.URL.Path, "/models/") && strings.HasSuffix(r.URL.Path, "/result"):
			var result struct {
				Status   string            `json:"status"`
				Models   []json.RawMessage `json:"models"`
				Fallback bool              `json:"fallback"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&result); err != nil {
				problem(err)
				return
			}
			mu.Lock()
			provider := ""
			for id, name := range byID {
				if strings.Contains(r.URL.Path, "/"+id+"/") {
					provider = name
				}
			}
			if provider == "" || result.Status != "completed" || len(result.Models) == 0 || result.Fallback {
				problem(fmt.Errorf("installed provider %s did not produce an actual model catalog", provider))
			} else {
				completed[provider] = true
				results <- nil
			}
			mu.Unlock()
		case r.URL.Path == "/api/daemon/tasks/claim":
			output = map[string]any{"tasks": []any{}}
		case strings.HasSuffix(r.URL.Path, "/repos"):
			output = map[string]any{"workspace_id": workspaceID, "repos": []any{}}
		case r.URL.Path == "/api/tokens/current/renew":
			output = map[string]any{"token": "mul_disposable_native_catalog"}
		case strings.Contains(r.URL.Path, "completions") || strings.Contains(r.URL.Path, "responses"):
			problem(errors.New("catalog probe attempted model generation"))
			http.Error(w, "no generation", http.StatusForbidden)
			return
		}
		writeFixtureJSON(w, output)
	}))
	defer backend.Close()
	bridge, err := official.NewBridge(official.BridgeOptions{BackendURL: backend.URL, Store: v.store, RuntimeRef: v.selection.RuntimeRef, Providers: enabledProviders(v.descriptor)})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	env, err := runtimeimage.Vars(v.descriptor, os.Environ(), runtimeimage.Locations{Home: home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot})
	if err != nil {
		return err
	}
	env = append(env, "OPENAI_BASE_URL="+backend.URL+"/v1", "OPENAI_API_KEY=sk-disposable-local-fixture", "MULTICA_GC_ENABLED=false")
	process, err := official.Setup(official.DaemonOptions{CoreRoot: wire.ControllerRoot, Home: home, TokenFile: filepath.Join(wire.ControlRoot, "verifyofficial-token"), DaemonID: uuid.NewString(), Name: "Native catalog fixture", BackendURL: backend.URL, ProxyURL: "http://" + listener.Addr().String(), Capacity: 1, PollInterval: time.Second, HeartbeatInterval: time.Second, Providers: enabledProviders(v.descriptor), RuntimeRef: v.selection.RuntimeRef, Env: env})
	if err != nil {
		return err
	}
	// Direct installed provider paths avoid requiring production admission's
	// not-yet-generated verification record during the prepared image build.
	for i, entry := range process.Env {
		key, _, _ := strings.Cut(entry, "=")
		for id, executable := range v.descriptor.Providers {
			if key == "MULTICA_"+strings.ToUpper(id)+"_PATH" {
				process.Env[i] = key + "=" + executable.Path
			}
		}
	}
	log, err := os.Create(filepath.Join(v.evidence, "native-catalog.log"))
	if err != nil {
		return err
	}
	defer log.Close()
	process.Stdout, process.Stderr = log, log
	done := make(chan error, 1)
	go func() { done <- official.RunDaemon(ctx, listener, bridge, process) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	}()
	for {
		select {
		case err := <-results:
			if err != nil {
				return err
			}
			mu.Lock()
			complete := len(completed) == len(v.descriptor.Providers)
			mu.Unlock()
			if complete {
				v.results = append(v.results, "installed-provider-native-model-catalogs")
				return nil
			}
		case err := <-done:
			done <- err
			return fmt.Errorf("native catalog daemon stopped: %w", err)
		case <-ctx.Done():
			return fmt.Errorf("native provider catalog: %w", ctx.Err())
		}
	}
}
