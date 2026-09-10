package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type homeEntry struct {
	Kind, SHA256, Link string
}
type homeObservation struct {
	Home, CodexHome, ConfigRoot, TaskID string
	Entries                             map[string]homeEntry
}

// homeProvider records the installed daemon's actual prepared filesystem at
// the provider boundary. All credentials and instructions in this probe are
// synthetic. No model protocol or generation service is involved.
func homeProvider(output string, args []string) int {
	if slices.Contains(args, "--version") {
		fmt.Println("codex-cli 0.153.4")
		return 0
	}
	if os.Getenv("MULTICA_TASK_ID") == "" {
		return 0
	}
	o := homeObservation{Home: os.Getenv("HOME"), CodexHome: os.Getenv("CODEX_HOME"), ConfigRoot: os.Getenv("MULTICA_TASK_CONFIG_ROOT"), TaskID: os.Getenv("MULTICA_TASK_ID"), Entries: map[string]homeEntry{}}
	if !filepath.IsAbs(o.CodexHome) {
		return 95
	}
	if slices.Contains(args, "--fixture-private-home-update") {
		if err := writeNativeHomeUpdate(o); err != nil {
			return 98
		}
	}
	if err := filepath.WalkDir(o.CodexHome, func(path string, e fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(o.CodexHome, path)
		if err != nil || rel == "." {
			return err
		}
		entry := homeEntry{}
		switch {
		case e.IsDir():
			entry.Kind = "directory"
		case e.Type()&os.ModeSymlink != 0:
			entry.Kind = "link"
			entry.Link, err = os.Readlink(path)
		case e.Type().IsRegular():
			entry.Kind = "file"
			var raw []byte
			raw, err = os.ReadFile(path)
			entry.SHA256 = core.Digest(raw)
		default:
			return errors.New("unexpected native HOME entry")
		}
		if err != nil {
			return err
		}
		o.Entries[filepath.ToSlash(rel)] = entry
		return nil
	}); err != nil {
		return 96
	}
	raw, err := json.MarshalIndent(o, "", "  ")
	if err != nil || os.WriteFile(output+".pending", raw, 0600) != nil || os.Rename(output+".pending", output) != nil {
		return 97
	}
	return 0
}

func (v *verifier) taskHome(ctx context.Context) (returnErr error) {
	if _, enabled := v.descriptor.Providers["codex"]; !enabled {
		return nil
	}
	fixture, cleanup, err := v.prepareNativeHomeFixture()
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, cleanup()) }()
	var converted []homeConversion
	for _, assigned := range []bool{true, false} {
		observed, err := v.observeTaskHome(ctx, fixture, assigned)
		if err != nil {
			return err
		}
		result, err := v.convertTaskHome(ctx, fixture, observed, assigned)
		if err != nil {
			return err
		}
		converted = append(converted, result)
	}
	raw, err := json.MarshalIndent(converted, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(v.evidence, "home-conversion.json"), append(raw, '\n'), 0600); err != nil {
		return err
	}
	v.results = append(v.results, "installed-daemon-task-home-assignment-and-removal", "installed-daemon-task-home-preparation-consumption-and-private-state-isolation")
	return nil
}

func (v *verifier) observeTaskHome(parent context.Context, fixture *nativeHomeFixture, assigned bool) (observation homeObservation, returnErr error) {
	home := wire.Home
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	phase := "home-unassigned"
	if assigned {
		phase = "home-assigned"
	}
	output := filepath.Join(v.evidence, phase+".json")
	_ = os.Remove(output)
	taskID, runtimeID := uuid.NewString(), uuid.NewString()
	var mu sync.Mutex
	registered, delivered := false, false
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var result any = map[string]any{}
		switch {
		case r.URL.Path == "/api/daemon/ws":
			http.Error(w, "local HTTP task fixture", http.StatusServiceUnavailable)
			return
		case r.URL.Path == "/api/daemon/workspaces":
			result = []any{map[string]any{"id": workspaceID, "slug": "task-home", "name": "Task HOME fixture"}}
		case r.URL.Path == "/api/daemon/register":
			var body struct{ Runtimes []struct{ Type string } }
			_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body)
			runtimes := []any{}
			mu.Lock()
			for _, runtime := range body.Runtimes {
				if runtime.Type == "codex" {
					registered = true
					runtimes = append(runtimes, map[string]any{"id": runtimeID, "provider": "codex", "status": "online", "name": "Task HOME fixture"})
				}
			}
			mu.Unlock()
			result = map[string]any{"runtimes": runtimes, "repos": []any{}}
		case r.URL.Path == "/api/daemon/tasks/claim":
			mu.Lock()
			defer mu.Unlock()
			tasks := []any{}
			if registered && !delivered {
				delivered = true
				skills := []any{}
				if assigned {
					skills = append(skills, map[string]any{"id": uuid.NewString(), "name": "code-review", "content": homeAssignedInstructions, "files": []any{map[string]any{"path": "references/assigned.md", "content": homeAssignedReference}}})
				}
				tasks = append(tasks, map[string]any{"id": taskID, "agent_id": agentID, "runtime_id": runtimeID, "workspace_id": workspaceID, "workspace_slug": "task-home", "auth_token": nativeHomeTaskToken, "chat_session_id": uuid.NewString(), "chat_message": "Inspect local task preparation.", "repos": []any{}, "agent": map[string]any{"id": agentID, "name": "Task HOME fixture", "instructions": "Inspect local task preparation.", "skills": skills}})
			}
			result = map[string]any{"tasks": tasks}
		case r.URL.Path == "/api/daemon/heartbeat":
			result = map[string]any{"runtime_id": runtimeID}
		case strings.HasSuffix(r.URL.Path, "/repos"):
			result = map[string]any{"workspace_id": workspaceID, "repos": []any{}}
		case r.URL.Path == "/api/tokens/current/renew":
			result = map[string]any{"token": "mul_disposable_home_fixture"}
		}
		writeFixtureJSON(w, result)
	}))
	defer backend.Close()
	bridge, err := official.NewBridge(official.BridgeOptions{BackendURL: backend.URL, Store: v.store, RuntimeRef: fixture.ref, Providers: enabledProviders(v.descriptor)})
	if err != nil {
		return homeObservation{}, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return homeObservation{}, err
	}
	defer listener.Close()
	helper := filepath.Join(fixture.directory, "observe-codex-"+phase)
	if err := os.WriteFile(helper, []byte("#!/bin/sh\nexec "+shellQuote(v.helper)+" home-provider "+shellQuote(output)+" \"$@\"\n"), 0500); err != nil {
		return homeObservation{}, err
	}
	env, err := runtimeimage.Vars(v.descriptor, os.Environ(), runtimeimage.Locations{Home: home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot})
	if err != nil {
		return homeObservation{}, err
	}
	env = append(env, "MULTICA_GC_ENABLED=false", "OPENAI_API_KEY=sk-disposable-home-fixture", "OPENAI_BASE_URL="+backend.URL+"/v1")
	process, err := official.Setup(official.DaemonOptions{CoreRoot: wire.ControllerRoot, Home: home, TokenFile: filepath.Join(wire.ControlRoot, "verifyofficial-token"), DaemonID: v.selection.OwnerID, Name: "Task HOME fixture", BackendURL: backend.URL, ProxyURL: "http://" + listener.Addr().String(), Capacity: 1, PollInterval: time.Second, HeartbeatInterval: time.Second, Providers: enabledProviders(v.descriptor), RuntimeRef: fixture.ref, Env: env})
	if err != nil {
		return homeObservation{}, err
	}
	for i, entry := range process.Env {
		key, _, _ := strings.Cut(entry, "=")
		if key == "MULTICA_CODEX_PATH" {
			process.Env[i] = key + "=" + helper
		} else if strings.HasPrefix(key, "MULTICA_") && strings.HasSuffix(key, "_PATH") {
			process.Env[i] = key + "=" + core.Root + "/disabled/fixture"
		}
	}
	log, err := os.Create(filepath.Join(v.evidence, phase+".log"))
	if err != nil {
		return homeObservation{}, err
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
			returnErr = errors.Join(returnErr, errors.New("native HOME daemon did not stop before conversion"))
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if raw, err := os.ReadFile(output); err == nil {
			var observed homeObservation
			if err := json.Unmarshal(raw, &observed); err != nil {
				return homeObservation{}, err
			}
			if observed.TaskID != taskID || observed.Home != home || observed.ConfigRoot == "" {
				return homeObservation{}, errors.New("native HOME evidence does not belong to the claimed task")
			}
			return observed, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return homeObservation{}, err
		}
		select {
		case err := <-done:
			done <- err
			return homeObservation{}, fmt.Errorf("task HOME daemon stopped: %w", err)
		case <-ctx.Done():
			return homeObservation{}, fmt.Errorf("native task HOME: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
