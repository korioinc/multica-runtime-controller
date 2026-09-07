package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/environment"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func runProvider(ctx context.Context, args []string) (returnErr error) {
	if slices.Contains(args, "--version") {
		fmt.Println("0.85.0")
		return nil
	}
	if os.Getenv("MULTICA_TASK_ID") == "" {
		if slices.Contains(args, "--list-models") {
			fmt.Print("provider  model  context  max-out  reasoning  images\nfixture  fixture-model  200000  8192  yes  yes\n")
			return nil
		}
		return errors.New("fixture provider supports only version/model no-task calls")
	}
	if err := fixtureOnly(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	raw, err := os.ReadFile(wire.RequestPath)
	if err != nil {
		return err
	}
	request, err := wire.Decode(raw)
	if err != nil {
		return err
	}
	if slices.Contains(args, "--fixture-transport-hold") {
		return transportHold(ctx, request)
	}
	backend := strings.TrimRight(os.Getenv("VERIFYRUNTIME_BACKEND"), "/")
	if !strings.HasPrefix(backend, "http://") {
		return errors.New("provider requires the local fixture backend")
	}
	session, err := wire.PiSession(request)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	if request.TaskID != os.Getenv("MULTICA_TASK_ID") || cwd != request.WorkDir {
		return errors.New("provider received another task identity or directory")
	}
	report := providerResult{TaskID: request.TaskID, Case: os.Getenv("VERIFYRUNTIME_CASE"), Stage: "started", WorkDir: cwd, Session: session, Storage: request.WorkerSubPath, Environment: request.Environment, Request: raw}
	defer func() {
		if returnErr != nil {
			report.Error = returnErr.Error()
			report.Stage = "failed"
		}
		postCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := postProvider(postCtx, backend, report); returnErr == nil && err != nil {
			returnErr = err
		}
	}()
	prompt, err := io.ReadAll(io.LimitReader(os.Stdin, 8<<20))
	if err != nil {
		return err
	}
	if !bytes.Contains(prompt, []byte("Verify the local runtime repository")) {
		return errors.New("actual official natural-language prompt did not reach the provider")
	}
	report.ContinuityNotice = strings.Contains(strings.ToLower(string(prompt)), "could not be restored")
	if err := verifyMounts(cwd); err != nil {
		return err
	}
	report.ROChecked, report.WritableChecked, report.IsolationChecked = true, true, true
	cache := os.Getenv("FIXTURE_CACHE")
	expectedCache := filepath.Join(cwd, ".cache")
	if override := os.Getenv("VERIFYRUNTIME_EXPECT_CACHE"); override != "" {
		expectedCache = override
	}
	if cache != expectedCache {
		return fmt.Errorf("manifest/task environment expansion selected wrong cache: got %q expected %q", cache, expectedCache)
	}
	if err := os.MkdirAll(cache, 0700); err != nil {
		return err
	}
	cacheFile, err := os.CreateTemp(cache, "fixture-cache-")
	if err != nil {
		return err
	}
	_, err = cacheFile.Write([]byte("task-local cache"))
	cacheFile.Close()
	os.Remove(cacheFile.Name())
	if err != nil {
		return err
	}
	report.CachePath, report.CacheChecked = cache, true
	actualHash, err := core.HashFile(wire.CoreRoot + "/runtime")
	if err != nil {
		return err
	}
	report.CoreHash = actualHash
	if actualHash != request.Environment.Core.Files["runtime"] {
		return errors.New("worker core differs from its immutable request")
	}
	priorSession, err := os.ReadFile(session)
	if err != nil {
		return err
	}
	report.PriorSession = bytes.Contains(priorSession, []byte("runtime fixture transcript"))
	if os.Getenv("VERIFYRUNTIME_HOLD") == "true" {
		if err := postProvider(ctx, backend, report); err != nil {
			return err
		}
		if err := waitForRelease(ctx, backend, request.TaskID); err != nil {
			return err
		}
	}
	if len(request.RepositoryURLs) != 1 {
		return errors.New("fixture expected its single assigned local repository")
	}
	assigned := request.RepositoryURLs[0]
	if _, err := command(ctx, cwd, wire.CoreRoot+"/multica", "repo", "checkout", assigned); err != nil {
		return err
	}
	repository, err := findRepository(cwd)
	if err != nil {
		return err
	}
	report.Repository = repository
	data, err := os.ReadFile(filepath.Join(repository, "tracked.txt"))
	if err != nil {
		return err
	}
	report.PriorWork = bytes.Contains(data, []byte("runtime fixture mutation"))
	report.PriorWorkDigest = core.Digest(data)
	if !report.PriorWork {
		if _, err := command(ctx, repository, "git", "-c", "core.hooksPath=/dev/null", "switch", "-c", "fixture-user-work"); err != nil {
			return err
		}
	}
	branch, err := command(ctx, repository, "git", "-c", "core.hooksPath=/dev/null", "branch", "--show-current")
	if err != nil {
		return err
	}
	report.Branch = strings.TrimSpace(string(branch))
	data = append(data, []byte("runtime fixture mutation "+request.TaskID+" "+request.AttemptID+"\n")...)
	if err := os.WriteFile(filepath.Join(repository, "tracked.txt"), data, 0600); err != nil {
		return err
	}
	report.ModifiedDigest = core.Digest(data)
	userHook := []byte("#!/bin/sh\n# runtime fixture user's hook\nexit 42\n")
	hook := filepath.Join(repository, ".git/hooks/pre-commit")
	if err := os.WriteFile(hook, userHook, 0700); err != nil {
		return err
	}
	if _, err := command(ctx, cwd, wire.CoreRoot+"/multica", "repo", "checkout", assigned); err != nil {
		return err
	}
	after, err := os.ReadFile(filepath.Join(repository, "tracked.txt"))
	if err != nil || !bytes.Equal(after, data) {
		return errors.New("repeated official checkout replaced worker edits")
	}
	afterBranch, err := command(ctx, repository, "git", "-c", "core.hooksPath=/dev/null", "branch", "--show-current")
	if err != nil || strings.TrimSpace(string(afterBranch)) != report.Branch {
		return errors.New("repeated checkout replaced the worker's branch")
	}
	afterHook, err := os.ReadFile(hook)
	if err != nil || !bytes.Equal(afterHook, userHook) {
		return errors.New("repeated checkout replaced a user Git hook")
	}
	report.RepeatCheckoutChecked = true
	if _, err := command(ctx, cwd, wire.CoreRoot+"/multica", "repo", "checkout", backend+"/git/forbidden.git"); err == nil {
		return errors.New("worker checked out a repository outside its observed scope")
	}
	report.ScopeChecked = true
	if _, _, err := environment.Check(wire.EnvironmentRoot, wire.CoreRoot, request.EnvironmentInput, &request.Environment); err != nil {
		return err
	}
	if err := writeSession(session, cwd, len(priorSession) == 0); err != nil {
		return err
	}
	sessionData, err := os.ReadFile(session)
	if err != nil {
		return err
	}
	report.SessionDigest = core.Digest(sessionData)
	if os.Getenv("VERIFYRUNTIME_HOLD_AFTER_WORK") == "true" {
		report.Stage = "held"
		if err := postProvider(ctx, backend, report); err != nil {
			return err
		}
		if err := waitForRelease(ctx, backend, request.TaskID); err != nil {
			return err
		}
	}
	report.Stage = "completed"
	encoded, _ := json.Marshal(map[string]any{"type": "message_update", "assistantMessageEvent": map[string]any{"type": "text_delta", "delta": "Local runtime repository verification completed."}})
	fmt.Println(string(encoded))
	encoded, _ = json.Marshal(map[string]any{"type": "turn_end", "message": map[string]any{"role": "assistant", "stopReason": "stop"}})
	fmt.Println(string(encoded))
	return nil
}

// transportHold is used only by the disposable remote-stream sever fixture.
// It neither edits workspace/session files nor reports a fabricated completion.
func transportHold(ctx context.Context, request wire.Request) error {
	if request.Provider != "pi" || !slices.Contains(request.Args, "--fixture-transport-hold") || request.TaskID != os.Getenv("MULTICA_TASK_ID") {
		return errors.New("invalid transport fixture identity")
	}
	cwd, err := os.Getwd()
	if err != nil || cwd != request.WorkDir {
		return errors.New("transport fixture work directory mismatch")
	}
	if _, err := wire.PiSession(request); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(os.Stdout, "verifystream-ready:%s:%d\n", request.AttemptID, os.Getpid()); err != nil {
		return err
	}
	read := make(chan error, 1)
	go func() { var data [1]byte; _, err := os.Stdin.Read(data[:]); read <- err }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-read:
		if err == nil {
			return errors.New("transport fixture unexpectedly received input")
		}
		return err
	}
}

func command(ctx context.Context, dir, executable string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Dir = dir
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", filepath.Base(executable), err, strings.TrimSpace(string(raw)))
	}
	return raw, nil
}

func verifyMounts(cwd string) error {
	for _, root := range []string{wire.CoreRoot, wire.EnvironmentRoot} {
		file, err := os.CreateTemp(root, "fixture-write-probe-")
		if err == nil {
			file.Close()
			os.Remove(file.Name())
			return errors.New("worker can modify immutable core or tools")
		}
		if !errors.Is(err, os.ErrPermission) && !strings.Contains(strings.ToLower(err.Error()), "read-only") {
			return fmt.Errorf("unexpected immutable mount failure: %w", err)
		}
	}
	for _, root := range []string{wire.Home, "/tmp", cwd} {
		file, err := os.CreateTemp(root, "fixture-writable-")
		if err != nil {
			return err
		}
		_, err = file.Write([]byte("private writable data"))
		file.Close()
		os.Remove(file.Name())
		if err != nil {
			return err
		}
	}
	protected := []string{"/workspace/.multica-runtime/state", "/workspace/.multica-runtime/attempts", "/workspace/.multica-runtime/workers", "/workspace/.repos", "/var/run/secrets/kubernetes.io/serviceaccount/token"}
	if root := os.Getenv("VERIFYRUNTIME_FORBIDDEN_ROOT"); root != "" {
		protected = append(protected, root)
	}
	for _, path := range protected {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("worker can reach unassigned controller or task storage: %s", path)
		}
	}
	return nil
}

func findRepository(root string) (string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	found := ""
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := os.Lstat(filepath.Join(path, ".git"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if !info.IsDir() || found != "" {
			return "", errors.New("checkout did not provide one standalone Git directory")
		}
		for _, name := range []string{"objects/info/alternates", "commondir"} {
			if _, err := os.Stat(filepath.Join(path, ".git", name)); !errors.Is(err, os.ErrNotExist) {
				return "", errors.New("checkout shares external Git state")
			}
		}
		found = path
	}
	if found == "" {
		return "", errors.New("checkout repository was not published")
	}
	return found, nil
}

func writeSession(path, cwd string, fresh bool) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	if fresh {
		if err := encoder.Encode(map[string]any{"type": "session", "version": 3, "id": uuid.NewString(), "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "cwd": cwd}); err != nil {
			return err
		}
	}
	return encoder.Encode(map[string]any{"type": "message", "id": uuid.NewString(), "parentId": nil, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "runtime fixture transcript"}}}})
}

func postProvider(ctx context.Context, backend string, report providerResult) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, backend+"/fixture/provider", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	var result map[string]any
	return readBody(response, &result)
}
func waitForRelease(ctx context.Context, backend, taskID string) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, backend+"/fixture/release/"+taskID, nil)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return err
			}
			var result struct {
				Released bool `json:"released"`
			}
			if err := readBody(response, &result); err != nil {
				return err
			}
			if result.Released {
				return nil
			}
		}
	}
}
