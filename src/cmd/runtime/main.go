package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/intercept"
)

const defaultRuntimeCapacity = "20"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "hold":
			if err := runTaskPod(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			return
		case "provider-worker":
			if err := runProviderWorker(os.Args[2:]); err != nil {
				if code, ok := providerProcessExitCode(err); ok {
					os.Exit(code)
				}
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			return
		case "daemon-proxy":
			if err := runDaemonGateway(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			return
		case "daemon":
			if err := writeOfficialCLIConfig(os.Getenv); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := intercept.PersistLauncherConfig(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			key, err := intercept.ControllerGrantKey(context.Background())
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := os.Setenv("MULTICA_CONTROLLER_GRANT_KEY", key); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}

	path, args, err := commandFor(os.Args[1:], os.Getenv)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	environ := os.Environ()
	if len(os.Args) > 1 && os.Args[1] == "daemon" {
		environ = daemonEnvironment(environ, os.Getenv)
	}
	if err := syscall.Exec(path, append([]string{path}, args...), environ); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func commandFor(args []string, getenv func(string) string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errors.New("usage: multica-runtime <daemon|hold|provider-worker|daemon-proxy>")
	}
	if args[0] != "daemon" {
		return "", nil, fmt.Errorf("unknown mode %q", args[0])
	}
	// The daemon's repository-grant and plan-only checks are mandatory. A
	// legacy MULTICA_BINARY override must not select an unpatched daemon.
	path := "/opt/multica/controller/multica"
	runtimeName := valueOr(getenv("MULTICA_RUNTIME_NAME"), "runtime-controller")
	daemonID, err := configuredDaemonID(getenv)
	if err != nil {
		return "", nil, err
	}
	capacity := valueOr(getenv("MULTICA_RUNTIME_CAPACITY"), defaultRuntimeCapacity)
	pollInterval := valueOr(getenv("MULTICA_POLL_INTERVAL"), "10s")
	heartbeatInterval := valueOr(getenv("MULTICA_HEARTBEAT_INTERVAL"), "15s")
	return path, []string{
		"daemon", "start", "--foreground", "--no-auto-update", "--no-auto-reload",
		"--daemon-id", daemonID,
		"--runtime-name", runtimeName,
		"--max-concurrent-tasks", capacity,
		"--poll-interval", pollInterval,
		"--heartbeat-interval", heartbeatInterval,
	}, nil
}

func configuredDaemonID(getenv func(string) string) (string, error) {
	configured := strings.TrimSpace(getenv("MULTICA_DAEMON_ID"))
	if configured == "" {
		return "", errors.New("MULTICA_DAEMON_ID is required")
	}
	parsed, err := uuid.Parse(configured)
	if err != nil {
		return "", errors.New("MULTICA_DAEMON_ID must be a UUID")
	}
	return parsed.String(), nil
}

type officialCLIConfig struct {
	ServerURL          string `json:"server_url"`
	Token              string `json:"token"`
	DeviceName         string `json:"device_name"`
	RuntimeName        string `json:"runtime_name"`
	MaxConcurrentTasks int    `json:"max_concurrent_tasks"`
	PollInterval       string `json:"poll_interval"`
	HeartbeatInterval  string `json:"heartbeat_interval"`
	DisableAutoUpdate  bool   `json:"disable_auto_update"`
	DisableAutoReload  bool   `json:"disable_auto_reload"`
}

func writeOfficialCLIConfig(getenv func(string) string) error {
	tokenFile := strings.TrimSpace(getenv("MULTICA_CONTROLLER_TOKEN_FILE"))
	if tokenFile == "" {
		return errors.New("MULTICA_CONTROLLER_TOKEN_FILE is required")
	}
	rawToken, err := os.ReadFile(tokenFile)
	if err != nil {
		return fmt.Errorf("read Multica token: %w", err)
	}
	token := strings.TrimSpace(string(rawToken))
	if !strings.HasPrefix(token, "mul_") && !strings.HasPrefix(token, "mcn_") {
		return errors.New("the official Multica CLI requires a mul_ or mcn_ token")
	}
	capacity, err := strconv.Atoi(valueOr(getenv("MULTICA_RUNTIME_CAPACITY"), defaultRuntimeCapacity))
	if err != nil || capacity < 1 {
		return errors.New("MULTICA_RUNTIME_CAPACITY must be a positive integer")
	}
	home := strings.TrimSpace(getenv("HOME"))
	if !filepath.IsAbs(home) {
		return errors.New("HOME must be an absolute path")
	}
	runtimeName := valueOr(getenv("MULTICA_RUNTIME_NAME"), "runtime-controller")
	config := officialCLIConfig{
		ServerURL:          strings.TrimRight(strings.TrimSpace(getenv("MULTICA_BASE_URL")), "/"),
		Token:              token,
		DeviceName:         runtimeName,
		RuntimeName:        runtimeName,
		MaxConcurrentTasks: capacity,
		PollInterval:       valueOr(getenv("MULTICA_POLL_INTERVAL"), "10s"),
		HeartbeatInterval:  valueOr(getenv("MULTICA_HEARTBEAT_INTERVAL"), "15s"),
		DisableAutoUpdate:  true,
		DisableAutoReload:  true,
	}
	if config.ServerURL == "" {
		return errors.New("MULTICA_BASE_URL is required")
	}
	rawConfig, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode official Multica CLI config: %w", err)
	}
	configDir := filepath.Join(home, ".multica")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return fmt.Errorf("create Multica config directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), rawConfig, 0o600); err != nil {
		return fmt.Errorf("write Multica CLI config: %w", err)
	}
	return nil
}

func daemonEnvironment(environ []string, getenv func(string) string) []string {
	values := make(map[string]string, len(environ)+4)
	for _, entry := range environ {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	values["MULTICA_SERVER_URL"] = strings.TrimRight(strings.TrimSpace(getenv("MULTICA_BASE_URL")), "/")
	values["MULTICA_WORKSPACES_ROOT"] = "/workspace"
	values["MULTICA_DAEMON_AUTO_UPDATE"] = "false"
	values["MULTICA_DAEMON_AUTO_RELOAD"] = "false"
	configureGitHubEnvironment(values)
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	return result
}

func configureGitHubEnvironment(values map[string]string) {
	token := strings.TrimSpace(values["GH_TOKEN"])
	if token == "" {
		token = strings.TrimSpace(values["GITHUB_PAT_TOKEN"])
		if token != "" {
			values["GH_TOKEN"] = token
		}
	}

	if name := strings.TrimSpace(values["GITHUB_USER_NAME"]); name != "" {
		appendGitConfigEnvironment(values, "user.name", name)
	}
	if email := strings.TrimSpace(values["GITHUB_USER_EMAIL"]); email != "" {
		appendGitConfigEnvironment(values, "user.email", email)
	}
	if token == "" {
		return
	}

	values["GIT_TERMINAL_PROMPT"] = "0"
	appendGitConfigEnvironment(values, "url.https://github.com/.insteadOf", "git@github.com:")
	appendGitConfigEnvironment(values, "url.https://github.com/.insteadOf", "ssh://git@github.com/")
	appendGitConfigEnvironment(values, "credential.https://github.com.helper", "!gh auth git-credential")
}

func appendGitConfigEnvironment(values map[string]string, key, value string) {
	count, err := strconv.Atoi(values["GIT_CONFIG_COUNT"])
	if err != nil || count < 0 {
		count = 0
	}
	for index := range count {
		suffix := strconv.Itoa(index)
		if values["GIT_CONFIG_KEY_"+suffix] == key && values["GIT_CONFIG_VALUE_"+suffix] == value {
			return
		}
	}
	index := strconv.Itoa(count)
	values["GIT_CONFIG_KEY_"+index] = key
	values["GIT_CONFIG_VALUE_"+index] = value
	values["GIT_CONFIG_COUNT"] = strconv.Itoa(count + 1)
}

func runProviderWorker(args []string) error {
	if len(args) != 1 || !strings.HasPrefix(args[0], "--request-file=") {
		return errors.New("usage: multica-runtime provider-worker --request-file=<path>")
	}
	path := strings.TrimPrefix(args[0], "--request-file=")
	if path != intercept.RequestFilePath() {
		return errors.New("provider request must use the mounted request path")
	}
	request, err := readProviderRequest(path)
	if err != nil {
		return err
	}
	providerPath, err := intercept.ProviderExecutable(request.Provider)
	if err != nil {
		return err
	}
	podLog, err := os.OpenFile("/proc/1/fd/1", os.O_WRONLY, 0)
	if err != nil {
		podLog, err = os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			return fmt.Errorf("open task Pod log output: %w", err)
		}
	}
	defer func() { _ = podLog.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return runProviderProcess(ctx, providerPath, request, intercept.Streams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr}, podLog)
}

func valueOr(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}
