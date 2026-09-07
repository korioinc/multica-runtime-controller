package official

import (
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/environment"
)

// These descriptors belong to the pinned v0.4.40 adapter, independently of
// which tools an operator installs. Absolute missing paths disable fallback.
var builtinIDs = strings.Fields("claude codex opencode codearts deveco openclaw hermes pi omp cursor copilot kimi reasonix dsh kiro codebuddy antigravity qoder qoderclicn traecli grok qwen qwenpaw dim mcode zeroclaw")

type DaemonOptions struct {
	CoreRoot, Home, TokenFile, DaemonID, Name, BackendURL, ProxyURL string
	Capacity                                                        int
	PollInterval, HeartbeatInterval                                 time.Duration
	Providers                                                       []string
	Environment                                                     environment.Ref
	Env                                                             []string
	Stdin                                                           io.Reader
	Stdout, Stderr                                                  io.Writer
}

func providerSet(providers []string) (map[string]bool, error) {
	enabled := map[string]bool{}
	for _, id := range providers {
		if !environment.SupportedProvider(id) || enabled[id] {
			return nil, errors.New("enabled providers must be unique supported builtin IDs")
		}
		enabled[id] = true
	}
	if len(enabled) == 0 {
		return nil, errors.New("at least one provider must be enabled")
	}
	return enabled, nil
}

// Setup verifies the injected binary then writes the official CLI's native
// configuration. The resulting argv always disables binary update and reload.
func Setup(options DaemonOptions) (DaemonProcess, error) {
	var process DaemonProcess
	if err := options.Environment.Validate(); err != nil {
		return process, err
	}
	contract, err := core.Check(options.CoreRoot, options.Environment.Platform)
	if err != nil {
		return process, err
	}
	if contract.OfficialVersion != "0.4.40" {
		return process, errors.New("official adapter requires release 0.4.40")
	}
	actual, _ := json.Marshal(contract)
	expected, _ := json.Marshal(options.Environment.Core)
	if string(actual) != string(expected) {
		return process, errors.New("official process core differs from its selected environment")
	}
	enabled, err := providerSet(options.Providers)
	if err != nil {
		return process, err
	}
	if len(enabled) != len(options.Environment.Providers) {
		return process, errors.New("daemon providers differ from the verified environment")
	}
	for id := range options.Environment.Providers {
		if !enabled[id] {
			return process, errors.New("daemon providers differ from the verified environment")
		}
	}
	daemonID, err := uuid.Parse(options.DaemonID)
	if err != nil || daemonID.String() != options.DaemonID {
		return process, errors.New("official daemon requires a canonical installation UUID")
	}
	if !filepath.IsAbs(options.Home) || filepath.Clean(options.Home) != options.Home {
		return process, errors.New("official daemon requires a native absolute HOME")
	}
	if options.Name == "" {
		options.Name = "runtime-controller"
	}
	if options.Capacity == 0 {
		options.Capacity = 20
	}
	if options.Capacity < 1 {
		return process, errors.New("daemon capacity must be positive")
	}
	if options.PollInterval == 0 {
		options.PollInterval = 10 * time.Second
	}
	if options.HeartbeatInterval == 0 {
		options.HeartbeatInterval = 15 * time.Second
	}
	if options.PollInterval < 0 || options.HeartbeatInterval < 0 {
		return process, errors.New("daemon intervals must be positive")
	}
	proxy, err := NormalizeBackendURL(options.ProxyURL)
	if err != nil {
		return process, err
	}
	local, _ := url.Parse(proxy)
	if local.Scheme != "http" || local.Hostname() != "127.0.0.1" || local.Path != "" {
		return process, errors.New("official bridge must use the local loopback origin")
	}
	backend, err := NormalizeBackendURL(options.BackendURL)
	if err != nil {
		return process, err
	}
	tokenBytes, err := os.ReadFile(options.TokenFile)
	if err != nil {
		return process, errors.New("read official controller token")
	}
	token := strings.TrimSpace(string(tokenBytes))
	if !strings.HasPrefix(token, "mul_") && !strings.HasPrefix(token, "mcn_") {
		return process, errors.New("official controller token must be a mul_ or mcn_ credential")
	}
	config := struct {
		ServerURL         string `json:"server_url"`
		Token             string `json:"token"`
		DeviceName        string `json:"device_name"`
		RuntimeName       string `json:"runtime_name"`
		Capacity          int    `json:"max_concurrent_tasks"`
		Poll              string `json:"poll_interval"`
		Heartbeat         string `json:"heartbeat_interval"`
		DisableAutoUpdate bool   `json:"disable_auto_update"`
		DisableAutoReload bool   `json:"disable_auto_reload"`
	}{proxy, token, options.Name, options.Name, options.Capacity, options.PollInterval.String(), options.HeartbeatInterval.String(), true, true}
	raw, err := json.Marshal(config)
	if err != nil {
		return process, err
	}
	if err := writeConfig(options.Home, raw); err != nil {
		return process, err
	}
	env := map[string]string{}
	for _, entry := range options.Env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			env[key] = value
		}
	}
	env["HOME"] = options.Home
	env["MULTICA_BASE_URL"] = backend
	env["MULTICA_SERVER_URL"] = proxy
	env["MULTICA_WORKSPACES_ROOT"] = "/workspace"
	env["MULTICA_DAEMON_AUTO_UPDATE"] = "false"
	env["MULTICA_DAEMON_AUTO_RELOAD"] = "false"
	for _, id := range builtinIDs {
		path := filepath.Join(options.CoreRoot, "disabled", id)
		if enabled[id] {
			path = filepath.Join(options.CoreRoot, "shims", environment.Alias(id))
		}
		env["MULTICA_"+strings.ToUpper(id)+"_PATH"] = path
	}
	if env["MULTICA_GC_COMPLETED_TASK_TTL"] == "" {
		origin, _ := url.Parse(backend)
		if strings.EqualFold(origin.Hostname(), "api.multica.ai") {
			env["MULTICA_GC_COMPLETED_TASK_TTL"] = "336h"
		}
	}
	process.Path = filepath.Join(options.CoreRoot, "multica")
	process.Args = []string{"daemon", "start", "--foreground", "--no-auto-update", "--no-auto-reload", "--daemon-id", options.DaemonID, "--runtime-name", options.Name, "--max-concurrent-tasks", strconv.Itoa(options.Capacity), "--poll-interval", options.PollInterval.String(), "--heartbeat-interval", options.HeartbeatInterval.String()}
	for _, key := range slices.Sorted(maps.Keys(env)) {
		process.Env = append(process.Env, key+"="+env[key])
	}
	process.Stdin, process.Stdout, process.Stderr = options.Stdin, options.Stdout, options.Stderr
	return process, nil
}

func writeConfig(home string, raw []byte) error {
	directory := filepath.Join(home, ".multica")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil || canonical != directory {
		return errors.New("official configuration directory must not contain symlinks")
	}
	file, err := os.CreateTemp(directory, ".config-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(file.Name(), filepath.Join(directory, "config.json")); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
