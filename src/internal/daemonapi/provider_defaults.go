package daemonapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/processgroup"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/multica-ai/multica/server/pkg/agent"
)

// ProviderDefaultsInput contains observations supplied by the controller after
// any candidate workspace writer is fenced. No task directory reaches a child.
// Missing files are omitted from a known map; SourcesKnown=false means the
// parent could not establish the native configuration layers or trust policy.
type ProviderDefaultsInput struct {
	Provider     string
	Options      agent.ExecOptions
	Environment  map[string]string
	HomeFiles    map[string][]byte
	ProjectFiles map[string][]byte
	SourcesKnown bool
}

type ProviderDefaults struct {
	Resolved      bool
	Model         string
	ModelProvider string
	ThinkingLevel string
	ServiceTier   *string
	Reason        string
}

type defaultProbePlan struct {
	files       map[string][]byte
	arguments   []string
	environment map[string]string
}

const providerProbeTimeout = 20 * time.Second

var errProviderMetadata = errors.New("provider metadata unavailable")

// ResolveProviderDefaults uses metadata operations only. It never sends a
// prompt, starts a model turn, or exposes task/backend/Git capabilities.
func ResolveProviderDefaults(ctx context.Context, descriptor runtimeimage.Descriptor, input ProviderDefaultsInput) (result ProviderDefaults, resultErr error) {
	unknown := func(reason string) (ProviderDefaults, error) { return ProviderDefaults{Reason: reason}, nil }
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !input.SourcesKnown || input.HomeFiles == nil || input.ProjectFiles == nil {
		return unknown("native_configuration_unobserved")
	}
	if !selectorValue(input.Options.Model, true) || !selectorValue(input.Options.ThinkingLevel, true) || !selectorValue(input.Options.ServiceTier, true) {
		return unknown("native_selector_invalid")
	}
	var total int
	for _, files := range []map[string][]byte{input.HomeFiles, input.ProjectFiles} {
		for _, raw := range files {
			total += len(raw)
			if total > MaxPayload {
				return unknown("native_configuration_too_large")
			}
		}
	}
	var plan defaultProbePlan
	var reason string
	switch input.Provider {
	case "codex":
		if !codexSystemSourcesAbsent() {
			return unknown("codex_system_configuration_unmodeled")
		}
		plan, reason = codexDefaultPlan(input)
	case "pi":
		plan, reason = piDefaultPlan(input)
	case "claude":
		// CLI selectors do not prove the effective native model and effort.
		// Keep execution available while refusing cross-task conversation reuse.
		return unknown("claude_native_defaults_unobserved")
	default:
		return unknown("native_provider_unsupported")
	}
	if reason != "" {
		return unknown(reason)
	}
	executable, ok := descriptor.Providers[input.Provider]
	if !ok || descriptor.Validate(core.HostPlatform()) != nil {
		return result, errors.New("provider defaults require an admitted image")
	}
	resolved, err := filepath.EvalSymlinks(executable.Path)
	if err != nil || !runtimeimage.ImmutablePath(resolved) || !runtimeimage.ImmutablePath(executable.Path) {
		return result, errors.New("provider defaults require an installed executable")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return result, errors.New("provider defaults executable is unavailable")
	}
	sha, err := core.HashFile(resolved)
	if err != nil || sha != executable.SHA256 {
		return result, errors.New("provider defaults executable differs from the admitted image")
	}
	paths := make([]string, 0, len(descriptor.BinDirs))
	for _, directory := range descriptor.BinDirs {
		actual, err := filepath.EvalSymlinks(directory)
		if err != nil || !runtimeimage.ImmutablePath(actual) {
			return result, errors.New("provider defaults PATH differs from the image")
		}
		paths = append(paths, actual)
	}
	scratch, err := os.MkdirTemp("", "multica-provider-defaults-")
	if err != nil {
		return result, err
	}
	removeScratch := true
	defer func() {
		if removeScratch {
			resultErr = errors.Join(resultErr, os.RemoveAll(scratch))
		}
	}()
	for _, directory := range []string{"home", "tmp", "work", "home/.codex", "home/.pi/agent"} {
		if err := os.MkdirAll(filepath.Join(scratch, directory), 0700); err != nil {
			return result, err
		}
	}
	for relative, raw := range plan.files {
		if !filepath.IsLocal(relative) {
			return result, errors.New("provider probe file escaped scratch")
		}
		name := filepath.Join(scratch, relative)
		if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
			return result, err
		}
		if err := os.WriteFile(name, raw, 0600); err != nil {
			return result, err
		}
	}
	environment := map[string]string{"PATH": strings.Join(paths, ":"), "HOME": filepath.Join(scratch, "home"),
		"TMPDIR": filepath.Join(scratch, "tmp"), "LANG": "C.UTF-8"}
	for key, value := range plan.environment {
		environment[key] = value
	}
	if input.Provider == "codex" {
		environment["CODEX_HOME"] = filepath.Join(scratch, "home/.codex")
	} else {
		environment["PI_CODING_AGENT_DIR"] = filepath.Join(scratch, "home/.pi/agent")
		environment["PI_OFFLINE"], environment["PI_TELEMETRY"] = "1", "0"
	}
	probeCtx, cancel := context.WithTimeout(ctx, providerProbeTimeout)
	defer cancel()
	probe, err := startDefaultProbe(probeCtx, executable.Path, plan.arguments, wire.Environment(environment), filepath.Join(scratch, "work"))
	if err != nil {
		return unknown("native_probe_unavailable")
	}
	if input.Provider == "codex" {
		result, err = resolveCodexMetadata(probe, input, filepath.Join(scratch, "work"))
	} else {
		result, err = resolvePiMetadata(probe, input)
	}
	// Kill the whole group, including launcher children, before joining the
	// direct child and removing credentials or scratch files.
	stopErr := processgroup.Stop(probe.command.Process.Pid)
	_ = probe.stdin.Close()
	_ = probe.stdout.Close()
	_ = probe.command.Wait()
	if stopErr != nil {
		removeScratch = false
		return ProviderDefaults{}, errors.Join(errors.New("provider probe writers remain unproven"), stopErr)
	}
	if err := ctx.Err(); err != nil {
		return ProviderDefaults{}, err
	}
	if err != nil || probeCtx.Err() != nil || !result.Resolved {
		return unknown("native_defaults_unresolved")
	}
	return result, nil
}

func codexSystemSourcesAbsent() bool {
	// Linux is the admitted runtime platform. macOS can also load managed
	// preferences, which this controller cannot reproduce in private scratch.
	if runtime.GOOS != "linux" {
		return false
	}
	for _, name := range []string{"/etc/codex/config.toml", "/etc/codex/managed_config.toml", "/etc/codex/requirements.toml"} {
		if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			return false
		}
	}
	return true
}

type defaultProbe struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	decoder *json.Decoder
	serial  int
}

func startDefaultProbe(ctx context.Context, executable string, arguments, environment []string, directory string) (*defaultProbe, error) {
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Dir, command.Env, command.Stderr = directory, environment, io.Discard
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = time.Second
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	return &defaultProbe{command: command, stdin: stdin, stdout: stdout, decoder: json.NewDecoder(io.LimitReader(stdout, MaxPayload+1))}, nil
}

func (p *defaultProbe) send(value any) error {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 64<<10 {
		return errProviderMetadata
	}
	_, err = p.stdin.Write(append(raw, '\n'))
	return err
}

func (p *defaultProbe) rpc(method string, params any, output any) error {
	p.serial++
	id := p.serial
	if err := p.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return err
	}
	for {
		var message struct {
			ID     *int            `json:"id"`
			Method string          `json:"method"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if p.decoder.Decode(&message) != nil {
			return errProviderMetadata
		}
		if message.ID == nil {
			continue
		}
		if *message.ID != id || message.Method != "" || len(message.Error) != 0 || len(message.Result) == 0 || string(message.Result) == "null" {
			return errProviderMetadata
		}
		if output != nil && json.Unmarshal(message.Result, output) != nil {
			return errProviderMetadata
		}
		return nil
	}
}

func selectorValue(value string, empty bool) bool {
	return (empty || value != "") && len(value) <= 512 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

func literalCredential(value string) bool {
	return value != "" && len(value) <= 64<<10 && !strings.HasPrefix(strings.TrimSpace(value), "!") && !strings.ContainsAny(value, "\x00\r\n")
}

func resolvePlainCredential(value string, environment map[string]string) (string, bool) {
	if !literalCredential(value) {
		return "", false
	}
	if strings.Contains(value, "$") {
		name, found := strings.CutPrefix(value, "$")
		if strings.HasPrefix(name, "{") && strings.HasSuffix(name, "}") {
			name = name[1 : len(name)-1]
		}
		if !found || !executionEnvName.MatchString(name) || protectedProbeVariable(name) || environment[name] == "" {
			return "", false
		}
		value = environment[name]
	}
	return value, literalCredential(value) && !protectedProbeCredential(value, environment)
}

func protectedProbeVariable(name string) bool {
	return runtimeimage.Reserved(name) || strings.HasPrefix(name, "GH_") || strings.HasPrefix(name, "GITHUB_") || strings.HasPrefix(name, "GIT_") || strings.HasPrefix(name, "SCM_")
}

func protectedProbeCredential(value string, environment map[string]string) bool {
	for _, prefix := range []string{"mat_", "mdt_", "mtc_", "mts_", "mul_", "mcn_", "ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_"} {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	for name, secret := range environment {
		if secret != "" && secret == value && protectedProbeVariable(name) {
			return true
		}
	}
	return false
}

// These are the installed providers' literal API-key environment inputs.
// Task capabilities, Git credentials, and loader variables are never copied.
var plainProviderKeyVariables = []string{
	"ANT_LING_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "AZURE_OPENAI_API_KEY", "NVIDIA_API_KEY", "DEEPSEEK_API_KEY",
	"GEMINI_API_KEY", "GOOGLE_CLOUD_API_KEY", "GROQ_API_KEY", "CEREBRAS_API_KEY", "XAI_API_KEY", "TYPESAFE_API_KEY", "RADIUS_API_KEY",
	"OPENROUTER_API_KEY", "AI_GATEWAY_API_KEY", "ZAI_API_KEY", "ZAI_CODING_CN_API_KEY", "MISTRAL_API_KEY", "MINIMAX_API_KEY", "MINIMAX_CN_API_KEY",
	"MOONSHOT_API_KEY", "HF_TOKEN", "FIREWORKS_API_KEY", "TOGETHER_API_KEY", "BASETEN_API_KEY", "OPENCODE_API_KEY", "KIMI_API_KEY", "META_API_KEY",
	"CLOUDFLARE_API_KEY", "XIAOMI_API_KEY", "XIAOMI_TOKEN_PLAN_CN_API_KEY", "XIAOMI_TOKEN_PLAN_AMS_API_KEY", "XIAOMI_TOKEN_PLAN_SGP_API_KEY",
	"QWEN_TOKEN_PLAN_API_KEY", "QWEN_TOKEN_PLAN_CN_API_KEY",
}

func probeEnvironment(input ProviderDefaultsInput) (map[string]string, bool) {
	result := make(map[string]string)
	for key, value := range input.Environment {
		if value == "" {
			continue
		}
		if input.Provider == "pi" && key == "PI_CODING_AGENT_DIR" && value != filepath.Join(wire.Home, ".pi/agent") {
			return nil, false
		}
		if key == "AWS_CONFIG_FILE" || key == "AWS_SHARED_CREDENTIALS_FILE" {
			relative := ".aws/config"
			if key == "AWS_SHARED_CREDENTIALS_FILE" {
				relative = ".aws/credentials"
			}
			// The image supplies these standard paths even without AWS auth.
			// Source collection separately proves their files absent. Pi does
			// not discover credentials from either variable alone.
			if value != filepath.Join(wire.Home, relative) {
				return nil, false
			}
			continue
		}
		if slices.Contains([]string{"NODE_OPTIONS", "NODE_PATH", "NODE_EXTRA_CA_CERTS", "OPENSSL_CONF", "BUN_OPTIONS", "BASH_ENV", "ENV", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_OAUTH_TOKEN", "COPILOT_GITHUB_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS", "ANTHROPIC_IDENTITY_TOKEN_FILE", "ANTHROPIC_FEDERATION_RULE_ID"}, key) || strings.HasPrefix(key, "AWS_") || strings.HasPrefix(key, "LD_") || strings.HasPrefix(key, "DYLD_") {
			return nil, false
		}
		if slices.Contains(plainProviderKeyVariables, key) && (input.Provider == "pi" || key == "OPENAI_API_KEY") {
			if !literalCredential(value) || protectedProbeCredential(value, input.Environment) {
				return nil, false
			}
			result[key] = value
		}
		if key == "OPENAI_BASE_URL" || key == "ANTHROPIC_BASE_URL" {
			endpoint, err := url.Parse(value)
			if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.Scheme != "http" && endpoint.Scheme != "https" {
				return nil, false
			}
			result[key] = value
		}
	}
	return result, true
}
