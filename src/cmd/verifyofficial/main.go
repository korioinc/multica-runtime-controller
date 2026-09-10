// verifyofficial exercises the pinned release executable in an isolated local
// container. Its backend and providers never contact a real model or service.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	configurationbundle "github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/execution"
	"github.com/korioinc/multica-runtime-controller/internal/official"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type configuration struct {
	image              bool
	official, evidence string
}
type verifier struct {
	configuration
	store            *workspace.Store
	selection        execution.Selection
	descriptor       runtimeimage.Descriptor
	descriptorDigest string
	fixtureRef       runtimeimage.Ref
	helper           string
	backend          *backend
	server           *httptest.Server
	redirectServer   *httptest.Server
	events           []providerEvent
	results          []string
}
type health struct {
	Status     string            `json:"status"`
	Agents     []string          `json:"agents"`
	Skipped    map[string]string `json:"skipped_agents"`
	Workspaces []struct {
		Runtimes []string `json:"runtimes"`
	} `json:"workspaces"`
}
type running struct {
	cancel context.CancelFunc
	done   chan error
	log    *os.File
}

func main() {
	if len(os.Args) > 2 && os.Args[1] == "home-provider" {
		os.Exit(homeProvider(os.Args[2], os.Args[3:]))
	}
	if len(os.Args) > 1 && (os.Args[1] == "provider" || os.Args[1] == "forbidden") {
		os.Exit(providerHelper(os.Args[1], os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "shim" {
		helper, err := os.Executable()
		if err != nil {
			os.Exit(1)
		}
		result := execution.RunProcess(context.Background(), helper, append([]string{"provider"}, os.Args[2:]...), os.Environ(), "/tmp", time.Second, execution.ProcessStreams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
		if result.Exited {
			os.Exit(result.Code)
		}
		os.Exit(1)
	}
	cfg := configuration{}
	flag.BoolVar(&cfg.image, "image", false, "verify the prepared image and its installed official adapter")
	flag.StringVar(&cfg.evidence, "evidence", "/evidence/official", "local evidence directory")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if err := verify(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "official fixture failed:", err)
		os.Exit(1)
	}
	fmt.Println("official release discovery, registration, claim transports and authorization verified")
}

func verify(ctx context.Context, cfg configuration) error {
	if runtime.GOOS != "linux" || os.Getenv("LOCALVERIFY_DISPOSABLE_CONTAINER") != "true" {
		return errors.New("requires an explicitly disposable Linux container")
	}
	if !cfg.image || flag.NArg() != 0 {
		return errors.New("--image is required; only installed image verification is supported")
	}
	helper, err := os.Executable()
	if err != nil {
		return err
	}
	v := &verifier{configuration: cfg, helper: helper, backend: newBackend()}
	if err := os.MkdirAll(cfg.evidence, 0700); err != nil {
		return err
	}
	v.server = httptest.NewServer(http.HandlerFunc(v.backend.serve))
	defer v.server.Close()
	v.redirectServer = httptest.NewServer(http.HandlerFunc(v.backend.redirectTarget))
	defer v.redirectServer.Close()
	if err := v.prepare(ctx); err != nil {
		return err
	}
	if err := v.stage(ctx, "discovery", false, false, func(ctx context.Context, process official.DaemonProcess, active *running) error {
		if err := v.noTask(ctx, process.Env); err != nil {
			return err
		}
		if err := v.streams(ctx, process.Env); err != nil {
			return err
		}
		ready, err := awaitReady(ctx, active)
		if err != nil {
			return err
		}
		if !slices.Contains(ready.Agents, "pi") {
			return errors.New("official availability did not discover enabled Pi")
		}
		for _, id := range ready.Agents {
			if id != "pi" {
				return errors.New("official availability discovered a disabled builtin")
			}
		}
		v.backend.mutex.Lock()
		initialProfileQueries := v.backend.bridgeProfileRequests
		v.backend.mutex.Unlock()
		if err := v.registrationRequests(ctx, wire.Value(process.Env, "MULTICA_SERVER_URL")); err != nil {
			return err
		}
		for _, mode := range []string{"http", "ws", "failure", "uncertain"} {
			if err := v.transport(ctx, mode); err != nil {
				return err
			}
		}
		if err := v.models(ctx); err != nil {
			return err
		}
		v.backend.mutex.Lock()
		refreshedProfiles := v.backend.bridgeProfileRequests > initialProfileQueries
		v.backend.mutex.Unlock()
		if !refreshedProfiles {
			return errors.New("actual daemon did not repeat custom-profile interception after WS refresh notification")
		}
		v.results = append(v.results, "custom-profile-startup-and-refresh")
		if err := v.localDirectory(ctx, "http"); err != nil {
			return err
		}
		if err := v.localDirectory(ctx, "ws"); err != nil {
			return err
		}
		return v.noForbidden()
	}); err != nil {
		return err
	}
	if err := v.stage(ctx, "version-failure", true, false, func(ctx context.Context, _ official.DaemonProcess, active *running) error {
		ready, err := awaitReady(ctx, active)
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err == nil && ready.Skipped["pi"] == "" {
			return errors.New("fresh daemon did not diagnose failed Pi version probe")
		}
		events, readErr := readEvents()
		if readErr != nil {
			return readErr
		}
		failedProbe := false
		for _, event := range events {
			failedProbe = failedProbe || event.VersionFailed
		}
		if !failedProbe {
			return errors.New("version failure scenario never exercised the provider failure")
		}
		for _, scope := range ready.Workspaces {
			if len(scope.Runtimes) > 0 {
				return errors.New("fresh daemon registered a version-failing provider online")
			}
		}
		v.backend.mutex.Lock()
		defer v.backend.mutex.Unlock()
		for _, reg := range v.backend.registrations {
			if len(reg.Types) > 0 {
				return errors.New("failed version probe reached online registration")
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := v.stage(ctx, "registration-injection", false, true, func(ctx context.Context, _ official.DaemonProcess, active *running) error {
		ready, err := awaitReady(ctx, active)
		v.backend.mutex.Lock()
		injected := false
		for _, reg := range v.backend.registrations {
			injected = injected || reg.Injected
		}
		v.backend.mutex.Unlock()
		if !injected {
			return errors.New("profile injection fixture did not reach the actual registration response")
		}
		if err == nil {
			for _, scope := range ready.Workspaces {
				if len(scope.Runtimes) > 0 {
					return errors.New("profile-bearing response authorized official runtime registration")
				}
			}
		}
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return v.noForbidden()
	}); err != nil {
		return err
	}
	if err := v.noForbidden(); err != nil {
		return err
	}
	if err := v.redirects(ctx); err != nil {
		return err
	}
	if err := v.nativeCatalog(ctx); err != nil {
		return err
	}
	if err := v.taskHome(ctx); err != nil {
		return err
	}
	_, digest, err := runtimeimage.CheckInstalled(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		return err
	}
	if digest != v.descriptorDigest {
		return errors.New("prepared image changed during adapter verification")
	}
	raw, _ := json.MarshalIndent(struct {
		Proofs []string      `json:"proofs"`
		Core   core.Contract `json:"core"`
	}{v.results, v.descriptor.Controller}, "", "  ")
	return os.WriteFile(filepath.Join(cfg.evidence, "result.json"), append(raw, '\n'), 0600)
}

func (v *verifier) prepare(ctx context.Context) error {
	for _, path := range []string{wire.WorkspaceRoot, wire.Home, wire.ControlRoot} {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	d, digest, err := runtimeimage.CheckInstalled(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		return err
	}
	v.descriptor, v.descriptorDigest, v.official = d, digest, d.Daemon.Path
	if _, ok := d.Providers["pi"]; !ok {
		return errors.New("adapter fixture requires installed Pi")
	}
	ref, err := d.Reference("fixture-runtime@sha256:"+core.Digest([]byte(d.ImageBuildID)), digest, configurationbundle.Digest([]configurationbundle.Group{}))
	if err != nil {
		return err
	}
	v.fixtureRef = ref
	v.fixtureRef.Providers = map[string]runtimeimage.Executable{"pi": d.Providers["pi"]}
	owner := uuid.NewString()
	v.store, err = workspace.Open(workspace.Options{Directory: workspace.DefaultDirectory, WorkspaceRoot: wire.WorkspaceRoot, SessionRoot: wire.PiSessionsRoot, OwnerID: owner})
	if err != nil {
		return err
	}
	if err = os.MkdirAll(wire.PiSessionsRoot, 0700); err != nil {
		return err
	}
	v.selection = execution.Selection{OwnerID: owner, RuntimeRef: ref}
	if err = os.WriteFile(filepath.Join(wire.ControlRoot, "verifyofficial-token"), []byte("mul_disposable_official_fixture"), 0600); err != nil {
		return err
	}
	if err = os.WriteFile(fixtureShim, []byte("#!/bin/sh\nexec "+shellQuote(v.helper)+" shim \"$@\"\n"), 0500); err != nil {
		return err
	}
	decoys := "/tmp/verifyofficial-path"
	if err = os.MkdirAll(decoys, 0700); err != nil {
		return err
	}
	for _, name := range []string{"claude", "codex", "copilot", "agy", "omp", "fixture-custom"} {
		if err = os.WriteFile(filepath.Join(decoys, name), []byte("#!/bin/sh\nexec "+shellQuote(v.helper)+" forbidden "+shellQuote(name)+" \"$@\"\n"), 0500); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(wire.Home, ".bash_profile"), []byte("export PATH="+shellQuote(decoys+":/usr/local/bin:/usr/bin:/bin")+"\n"), 0600)
}

const fixtureShim = "/tmp/verifyofficial-provider"

func (v *verifier) stage(ctx context.Context, name string, versionFail, inject bool, action func(context.Context, official.DaemonProcess, *running) error) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	v.backend.mutex.Lock()
	v.backend.mode = "http"
	v.backend.pending = nil
	v.backend.injectProfile = inject
	v.backend.registrations = nil
	v.backend.registered = false
	v.backend.modelSent = false
	v.backend.registerRedirect = ""
	v.backend.claimRedirect = ""
	v.backend.redirectTask = nil
	v.backend.redirectIssued = false
	v.backend.redirectCycleFinished = false
	v.backend.redirectRequests = nil
	if name == "registration-redirect" {
		v.backend.registerRedirect = v.redirectServer.URL + "/register"
	}
	if name == "claim-redirect" {
		v.backend.claimRedirect = v.redirectServer.URL + "/claim"
	}
	v.backend.mutex.Unlock()
	if versionFail {
		if err := os.WriteFile(failVersionFile, []byte("fail"), 0600); err != nil {
			return err
		}
	} else {
		_ = os.Remove(failVersionFile)
	}
	bridge, err := official.NewBridge(official.BridgeOptions{BackendURL: v.server.URL, Store: v.store, RuntimeRef: v.fixtureRef, Providers: []string{"pi"}})
	if err != nil {
		return err
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/runtime-profiles") {
			v.backend.mutex.Lock()
			v.backend.bridgeProfileRequests++
			v.backend.mutex.Unlock()
		}
		bridge.ServeHTTP(w, r)
		if r.URL.Path == "/api/daemon/tasks/claim" || r.URL.Path == "/api/daemon/ws" {
			select {
			case v.backend.bridgeDone <- r.URL.Path:
			default:
			}
		}
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	env := []string{"HOME=" + wire.Home, "PATH=/tmp/verifyofficial-path:/usr/local/bin:/usr/bin:/bin", "SHELL=/bin/bash", "TMPDIR=/tmp", "MULTICA_GC_ENABLED=false", "MULTICA_DAEMON_WS_CLAIM_POLL_INTERVAL=1s", "MULTICA_CODEX_PATH=/tmp/verifyofficial-path/codex"}
	process, err := official.Setup(official.DaemonOptions{CoreRoot: wire.ControllerRoot, Home: wire.Home, TokenFile: filepath.Join(wire.ControlRoot, "verifyofficial-token"), DaemonID: v.selection.OwnerID, Name: "Official fixture", BackendURL: v.server.URL, ProxyURL: "http://" + listener.Addr().String(), Capacity: 1, PollInterval: time.Second, HeartbeatInterval: time.Second, Providers: enabledProviders(v.descriptor), RuntimeRef: v.selection.RuntimeRef, Env: env})
	if err != nil {
		return err
	}
	// The prepared image has no production verification record yet. Test-owned
	// entrypoints exercise the actual daemon adapter; final-image integration
	// separately exercises the production controller shims.
	for i, entry := range process.Env {
		key, _, _ := strings.Cut(entry, "=")
		if key == "MULTICA_PI_PATH" {
			process.Env[i] = key + "=" + fixtureShim
		} else if strings.HasPrefix(key, "MULTICA_") && strings.HasSuffix(key, "_PATH") {
			process.Env[i] = key + "=" + core.Root + "/disabled/fixture"
		}
	}
	if err := injectLocalProfileOverride(); err != nil {
		return err
	}
	log, err := os.Create(filepath.Join(v.evidence, name+".log"))
	if err != nil {
		return err
	}
	process.Stdout, process.Stderr = log, log
	childCtx, stop := context.WithCancel(ctx)
	active := &running{cancel: stop, done: make(chan error, 1), log: log}
	go func() { active.done <- official.RunDaemon(childCtx, listener, handler, process) }()
	defer func() {
		active.cancel()
		select {
		case <-active.done:
		case <-time.After(15 * time.Second):
		}
		active.log.Close()
	}()
	if err := action(ctx, process, active); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	v.results = append(v.results, name)
	fmt.Println("official fixture passed:", name)
	return nil
}

func injectLocalProfileOverride() error {
	path := filepath.Join(wire.Home, ".multica/config.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var values map[string]any
	if err := json.Unmarshal(raw, &values); err != nil {
		return err
	}
	values["profile_command_overrides"] = map[string]string{customProfileID: "/tmp/verifyofficial-path/fixture-custom"}
	raw, err = json.Marshal(values)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0600)
}

func awaitReady(ctx context.Context, active *running) (health, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: time.Second}
	for {
		select {
		case <-ctx.Done():
			return health{}, ctx.Err()
		case err := <-active.done:
			active.done <- err
			return health{}, fmt.Errorf("official process stopped: %w", err)
		case <-ticker.C:
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:19514/health", nil)
			if err != nil {
				return health{}, err
			}
			response, err := client.Do(request)
			if err != nil {
				continue
			}
			var state health
			err = json.NewDecoder(response.Body).Decode(&state)
			response.Body.Close()
			if err == nil && state.Status == "running" {
				return state, nil
			}
		}
	}
}

func (v *verifier) noTask(ctx context.Context, env []string) error {
	probe := exec.CommandContext(ctx, v.official, "daemon", "probe-runtimes")
	probe.Env = env
	raw, err := probe.Output()
	if err != nil {
		return fmt.Errorf("official availability probe: %w", err)
	}
	var availability struct {
		Providers map[string]int `json:"provider_summary"`
	}
	if json.Unmarshal(raw, &availability) != nil || availability.Providers["pi"] < 1 {
		return errors.New("actual release did not discover Pi")
	}
	for id := range availability.Providers {
		if id != "pi" {
			return errors.New("disabled provider discovered by actual release")
		}
	}
	if err := os.WriteFile(filepath.Join(v.evidence, "availability.json"), raw, 0600); err != nil {
		return err
	}
	args := []string{"--echo-protocol", "argument with spaces", "literal-$()-value"}
	input := []byte{0, 1, 'h', 'e', 'l', 'l', 'o', '\n'}
	command := exec.CommandContext(ctx, fixtureShim, args...)
	command.Env = env
	command.Dir = "/tmp"
	command.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	raw, err = command.Output()
	_ = os.WriteFile(filepath.Join(v.evidence, "no-task-stdout.bin"), raw, 0600)
	_ = os.WriteFile(filepath.Join(v.evidence, "no-task-stderr.bin"), stderr.Bytes(), 0600)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 17 {
		return errors.New("no-task provider exit code was not preserved")
	}
	var echoed struct {
		Args  []string
		Input []byte
		Home  string
	}
	if json.Unmarshal(raw, &echoed) != nil || !slices.Equal(echoed.Args, args[1:]) || !bytes.Equal(echoed.Input, input) || echoed.Home != wire.Home || stderr.String() != "fixture stderr bytes\n" {
		return errors.New("no-task provider argv, streams or native HOME were not preserved")
	}
	return nil
}

func (v *verifier) transport(ctx context.Context, mode string) error {
	task := v.backend.enqueue(mode)
	id := task["id"].(string)
	for {
		select {
		case failure := <-v.backend.failures:
			if failure.TaskID != id {
				continue
			}
			if failure.Reason != "runtime_offline" {
				return fmt.Errorf("unregistered runtime failed for unexpected reason %q", failure.Reason)
			}
			if _, err := v.store.Lookup(id, task["auth_token"].(string), workspaceID, agentID); err != nil {
				return fmt.Errorf("successful %s claim was not observed: %w", mode, err)
			}
			if _, err := v.store.Lookup(id, "mat_wrong", workspaceID, agentID); err == nil {
				return errors.New("observed task accepted a mismatched credential")
			}
			v.backend.mutex.Lock()
			transport := v.backend.delivered
			injected := v.backend.injected
			v.backend.mutex.Unlock()
			if mode == "ws" && transport != "ws" || mode == "http" && transport != "http" || (mode == "failure" || mode == "uncertain") && (!injected || transport != "http") {
				return fmt.Errorf("%s did not execute its required official claim fallback (observed %s)", mode, transport)
			}
			v.results = append(v.results, "claim-"+mode)
			fmt.Println("official claim verified:", mode)
			return nil
		case err := <-v.backend.errors:
			return err
		case <-ctx.Done():
			return fmt.Errorf("official %s claim: %w", mode, ctx.Err())
		}
	}
}

func (v *verifier) noForbidden() error {
	raw, err := os.ReadFile(eventFile)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var event providerEvent
		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if event.Mode == "forbidden" || event.TaskID != "" {
			return errors.New("unsupported builtin, custom profile or unregistered task executed a controller provider")
		}
	}
	if err := os.WriteFile(filepath.Join(v.evidence, "provider-events.jsonl"), raw, 0600); err != nil {
		return err
	}
	v.backend.mutex.Lock()
	defer v.backend.mutex.Unlock()
	if v.backend.profileRequests != 0 {
		return errors.New("custom profiles escaped the bridge")
	}
	if v.backend.bridgeProfileRequests == 0 {
		return errors.New("actual daemon never exercised custom-profile interception")
	}
	return nil
}

func readEvents() ([]providerEvent, error) {
	raw, err := os.ReadFile(eventFile)
	if err != nil {
		return nil, err
	}
	var events []providerEvent
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var event providerEvent
		if err := decoder.Decode(&event); err == io.EOF {
			return events, nil
		} else if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
}
func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }

func (v *verifier) models(ctx context.Context) error {
	select {
	case raw := <-v.backend.models:
		if err := os.WriteFile(filepath.Join(v.evidence, "models.json"), raw, 0600); err != nil {
			return err
		}
		var result struct {
			Models []struct {
				ID string `json:"id"`
			} `json:"models"`
		}
		if json.Unmarshal(raw, &result) != nil {
			return errors.New("invalid official model response")
		}
		found := false
		for _, model := range result.Models {
			if model.ID == "fixture/fixture-model" {
				found = true
			}
		}
		if !found {
			return errors.New("actual official model lookup did not return the fixture provider's model")
		}
		events, err := readEvents()
		if err != nil {
			return err
		}
		forwarded := false
		for _, event := range events {
			if slices.Contains(event.Args, "--list-models") && event.Home == wire.Home && event.TaskID == "" {
				forwarded = true
			}
		}
		if !forwarded {
			return errors.New("model lookup did not reach the real no-task provider")
		}
		v.results = append(v.results, "model-lookup")
		return nil
	case err := <-v.backend.errors:
		return err
	case <-ctx.Done():
		return fmt.Errorf("official model query: %w", ctx.Err())
	}
}

func (v *verifier) localDirectory(ctx context.Context, mode string) error {
	private := "/tmp/verifyofficial-user-tree"
	if err := os.MkdirAll(private, 0700); err != nil {
		return err
	}
	canary := filepath.Join(private, "private-data")
	if err := os.WriteFile(canary, []byte("operator's existing file"), 0600); err != nil {
		return err
	}
	task := map[string]any{"id": uuid.NewString(), "agent_id": agentID, "runtime_id": builtinRuntimeID, "workspace_id": workspaceID, "auth_token": "mat_disposable_local_directory", "chat_session_id": uuid.NewString(), "chat_message": "This local-directory task must never execute", "agent": map[string]any{"id": agentID, "name": "Local-directory rejection fixture"}, "repos": []any{}, "project_resources": []any{map[string]any{"resource_type": "local_directory", "resource_ref": map[string]any{"local_path": private, "daemon_id": v.selection.OwnerID, "execution_mode": "worktree"}}}}
	for {
		select {
		case <-v.backend.bridgeDone:
			continue
		default:
			goto drained
		}
	}
drained:
	v.backend.queue(mode, task)
	for {
		select {
		case route := <-v.backend.bridgeDone:
			if mode == "ws" && route != "/api/daemon/ws" || mode == "http" && route != "/api/daemon/tasks/claim" {
				continue
			}
			v.backend.mutex.Lock()
			delivered := v.backend.delivered
			pending := v.backend.pending != nil
			v.backend.mutex.Unlock()
			if pending || delivered != mode {
				continue
			}
			if _, err := v.store.Lookup(task["id"].(string), task["auth_token"].(string), workspaceID, agentID); err == nil {
				return errors.New("explicit local-directory claim granted provider authority")
			}
			data, err := os.ReadFile(canary)
			if err != nil || string(data) != "operator's existing file" {
				return errors.New("rejected local-directory claim modified operator data")
			}
			entries, err := os.ReadDir(private)
			if err != nil {
				return err
			}
			if len(entries) != 1 {
				return errors.New("rejected local-directory claim created controller sidecars")
			}
			v.results = append(v.results, "local-directory-"+mode)
			fmt.Println("local-directory authorization rejected:", mode)
			return nil
		case err := <-v.backend.errors:
			return err
		case <-ctx.Done():
			return fmt.Errorf("local-directory %s rejection: %w", mode, ctx.Err())
		}
	}
}

func (v *verifier) registrationRequests(ctx context.Context, origin string) error {
	type responseEvidence struct {
		Case       string `json:"case"`
		HTTPStatus int    `json:"httpStatus"`
		Body       string `json:"body"`
	}
	evidence := []responseEvidence{}
	for _, candidate := range []struct {
		name    string
		runtime map[string]any
	}{{"disabled-builtin", map[string]any{"type": "claude"}}, {"custom-profile", map[string]any{"type": "pi", "profile_id": customProfileID}}} {
		raw, _ := json.Marshal(map[string]any{"runtimes": []any{candidate.runtime}})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/api/daemon/register", bytes.NewReader(raw))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if err != nil {
			return err
		}
		evidence = append(evidence, responseEvidence{candidate.name, response.StatusCode, string(body)})
		// The backend records an attempted unauthorized runtime mutation when an
		// invalid registration escapes the bridge's request boundary.
		select {
		case err := <-v.backend.errors:
			return err
		default:
		}
	}
	raw, _ := json.MarshalIndent(evidence, "", "  ")
	if err := os.WriteFile(filepath.Join(v.evidence, "registration-rejections.json"), raw, 0600); err != nil {
		return err
	}
	v.results = append(v.results, "registration-request-boundary")
	return nil
}

func enabledProviders(d runtimeimage.Descriptor) []string {
	ids := []string{}
	for id := range d.Providers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
