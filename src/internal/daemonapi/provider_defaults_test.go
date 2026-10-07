package daemonapi

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/pelletier/go-toml/v2"
)

func defaultsInput(provider string) ProviderDefaultsInput {
	return ProviderDefaultsInput{Provider: provider, SourcesKnown: true, HomeFiles: map[string][]byte{}, ProjectFiles: map[string][]byte{},
		Environment: map[string]string{"OPENAI_API_KEY": "sk-local-defaults-fixture", "MULTICA_AUTH_TOKEN": "mat_private_task", "GITHUB_TOKEN": "ghs_private_git",
			"AWS_CONFIG_FILE": filepath.Join(wire.Home, ".aws/config"), "AWS_SHARED_CREDENTIALS_FILE": filepath.Join(wire.Home, ".aws/credentials")}}
}

func TestClaudeExplicitSelectorsCannotAuthorizeConversationReuse(t *testing.T) {
	for _, layer := range []string{"explicit", "environment", "home", "project", "custom-model"} {
		t.Run(layer, func(t *testing.T) {
			input := defaultsInput("claude")
			input.Options.Model, input.Options.ThinkingLevel = "claude-sonnet-4-6", "high"
			switch layer {
			case "environment":
				input.Environment["CLAUDE_CODE_EFFORT_LEVEL"] = "auto"
			case "home":
				input.HomeFiles[".claude/settings.json"] = []byte(`{"model":"opus","env":{"CLAUDE_CODE_EFFORT_LEVEL":"max"}}`)
			case "project":
				input.ProjectFiles[".claude/settings.local.json"] = []byte(`{"modelSettings":{"effortLevel":"low"}}`)
			case "custom-model":
				input.Options.CustomArgs = []string{"--model", "opusplan"}
			}
			defaults, err := ResolveProviderDefaults(t.Context(), runtimeimage.Descriptor{}, input)
			if err != nil || defaults.Resolved || defaults.Reason != "claude_native_defaults_unobserved" {
				t.Fatal("unobserved Claude defaults gained cross-task reuse authority", defaults, err)
			}
		})
	}
}

func localCodexProviderArguments() []string {
	var arguments []string
	for _, override := range []string{`model_provider="fixture"`, `model_providers.fixture.name="fixture"`,
		`model_providers.fixture.base_url="http://127.0.0.1:18081/v1"`, `model_providers.fixture.wire_api="responses"`,
		`model_providers.fixture.env_key="OPENAI_API_KEY"`, `model_providers.fixture.requires_openai_auth=false`} {
		arguments = append(arguments, "-c", override)
	}
	return arguments
}

func TestProviderDefaultPlansPreserveDataOnlyNativeSelectors(t *testing.T) {
	t.Run("Codex plain launch selectors and custom API provider", func(t *testing.T) {
		input := defaultsInput("codex")
		input.Options.CustomArgs = append(localCodexProviderArguments(), "-c", "model=gpt-5.4", "-c", `model_reasoning_effort="high"`)
		plan, reason := codexDefaultPlan(input)
		if reason != "" {
			t.Fatal(reason)
		}
		var config map[string]any
		if err := toml.Unmarshal(plan.files["home/.codex/config.toml"], &config); err != nil {
			t.Fatal(err)
		}
		if config["model"] != "gpt-5.4" || config["model_reasoning_effort"] != "high" || config["model_provider"] != "fixture" {
			t.Fatal("native selectors changed during safe reconstruction")
		}
		if plan.environment["MULTICA_AUTH_TOKEN"] != "" || plan.environment["GITHUB_TOKEN"] != "" || plan.environment["OPENAI_API_KEY"] != input.Environment["OPENAI_API_KEY"] {
			t.Fatal("probe environment carried task or Git authority")
		}
	})
	t.Run("Codex credential references cannot enable runtime loaders", func(t *testing.T) {
		input := defaultsInput("codex")
		input.Environment["CUSTOM_PROVIDER_CREDENTIAL"] = "literal-local-fixture-key"
		input.Options.CustomArgs = append(localCodexProviderArguments(), "-c", `model_providers.fixture.env_key="CUSTOM_PROVIDER_CREDENTIAL"`)
		plan, reason := codexDefaultPlan(input)
		if reason != "" {
			t.Fatal(reason)
		}
		var config struct {
			Providers map[string]struct {
				EnvironmentKey string `toml:"env_key"`
			} `toml:"model_providers"`
		}
		if toml.Unmarshal(plan.files["home/.codex/config.toml"], &config) != nil || plan.environment["CUSTOM_PROVIDER_CREDENTIAL"] != "" ||
			plan.environment[config.Providers["fixture"].EnvironmentKey] != input.Environment["CUSTOM_PROVIDER_CREDENTIAL"] {
			t.Fatal("source variable name escaped into the native probe's loader environment")
		}
	})
	t.Run("Pi environment references preserve literal API keys", func(t *testing.T) {
		input := defaultsInput("pi")
		input.Environment["LOCAL_MODEL_KEY"] = "a-literal-api-key"
		input.HomeFiles[".pi/agent/auth.json"] = []byte(`{"openai":{"type":"api_key","key":"$LOCAL_MODEL_KEY"}}`)
		input.HomeFiles[".pi/agent/settings.json"] = []byte(`{"defaultProvider":"openai","defaultModel":"gpt-5.4","defaultThinkingLevel":"high"}`)
		plan, reason := piDefaultPlan(input)
		if reason != "" {
			t.Fatal(reason)
		}
		var auth map[string]map[string]string
		if json.Unmarshal(plan.files["home/.pi/agent/auth.json"], &auth) != nil || plan.environment[strings.TrimSuffix(strings.TrimPrefix(auth["openai"]["key"], "${"), "}")] != input.Environment["LOCAL_MODEL_KEY"] {
			t.Fatal("native credential reference lost its observed value")
		}
		if plan.environment["MULTICA_AUTH_TOKEN"] != "" || plan.environment["GITHUB_TOKEN"] != "" || len(plan.files["home/.pi/agent/settings.json"]) == 0 {
			t.Fatal("reconstruction lost its source boundary")
		}
	})
	t.Run("Pi built-in provider endpoint and credential overrides", func(t *testing.T) {
		input := defaultsInput("pi")
		input.Environment["LOCAL_MODEL_KEY"] = "sk-local-model-override"
		input.HomeFiles[".pi/agent/models.json"] = []byte(`{"providers":{"openai":{"baseUrl":"http://127.0.0.1:18081/v1","apiKey":"${LOCAL_MODEL_KEY}"}}}`)
		plan, reason := piDefaultPlan(input)
		if reason != "" || plan.environment["LOCAL_MODEL_KEY"] != "" {
			t.Fatal("data-only native provider override was not safely reconstructed", reason)
		}
		var models struct {
			Providers map[string]map[string]string `json:"providers"`
		}
		if json.Unmarshal(plan.files["home/.pi/agent/models.json"], &models) != nil || models.Providers["openai"]["baseUrl"] != "http://127.0.0.1:18081/v1" {
			t.Fatal("provider endpoint changed during reconstruction")
		}
		name := strings.TrimSuffix(strings.TrimPrefix(models.Providers["openai"]["apiKey"], "${"), "}")
		if plan.environment[name] != input.Environment["LOCAL_MODEL_KEY"] {
			t.Fatal("provider credential did not retain its observed native value")
		}
	})
}

func TestProviderDefaultPlansRejectExecutableOrOpaqueConfiguration(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "credential-command-ran")
	for _, scenario := range []struct {
		name string
		edit func(*ProviderDefaultsInput)
	}{
		{"credential command", func(input *ProviderDefaultsInput) {
			raw, _ := json.Marshal(map[string]any{"openai": map[string]string{"type": "api_key", "key": "!touch " + marker}})
			input.HomeFiles[".pi/agent/auth.json"] = raw
		}},
		{"startup extension", func(input *ProviderDefaultsInput) {
			input.HomeFiles[".pi/agent/settings.json"] = []byte(`{"extensions":["./extension.ts"]}`)
		}},
		{"package startup", func(input *ProviderDefaultsInput) {
			input.ProjectFiles[".pi/settings.json"] = []byte(`{"packages":["npm:unobserved-package"]}`)
		}},
		{"loader environment", func(input *ProviderDefaultsInput) { input.Environment["NODE_OPTIONS"] = "--import ./extension.mjs" }},
		{"unobserved Pi configuration root", func(input *ProviderDefaultsInput) { input.Environment["PI_CODING_AGENT_DIR"] = "/another/agent" }},
		{"native TLS startup file", func(input *ProviderDefaultsInput) { input.Environment["OPENSSL_CONF"] = "/unobserved/startup.cnf" }},
		{"native additional certificates", func(input *ProviderDefaultsInput) {
			input.Environment["NODE_EXTRA_CA_CERTS"] = "/unobserved/certificates.pem"
		}},
		{"unobserved AWS credential file", func(input *ProviderDefaultsInput) {
			input.Environment["AWS_SHARED_CREDENTIALS_FILE"] = "/unobserved/aws"
		}},
		{"AWS profile authentication", func(input *ProviderDefaultsInput) { input.Environment["AWS_PROFILE"] = "unobserved-profile" }},
		{"task credential reference", func(input *ProviderDefaultsInput) {
			input.HomeFiles[".pi/agent/auth.json"] = []byte(`{"openai":{"type":"api_key","key":"$MULTICA_AUTH_TOKEN"}}`)
		}},
		{"empty credential reference", func(input *ProviderDefaultsInput) {
			input.Environment["EMPTY_KEY"] = ""
			input.HomeFiles[".pi/agent/auth.json"] = []byte(`{"openai":{"type":"api_key","key":"${EMPTY_KEY}"}}`)
		}},
		{"Git credential value", func(input *ProviderDefaultsInput) {
			input.Environment["OPENAI_API_KEY"] = input.Environment["GITHUB_TOKEN"]
		}},
		{"attempt capability value", func(input *ProviderDefaultsInput) { input.Environment["OPENAI_API_KEY"] = "mtc_private_attempt" }},
		{"session capability value", func(input *ProviderDefaultsInput) { input.Environment["OPENAI_API_KEY"] = "mts_private_session" }},
		{"opaque model registry", func(input *ProviderDefaultsInput) {
			input.HomeFiles[".pi/agent/models.json"] = []byte(`{"providers":{"openai":{"apiKey":"!touch marker"}}}`)
		}},
		{"unknown project trust", func(input *ProviderDefaultsInput) {
			input.ProjectFiles[".pi/settings.json"] = []byte(`{"defaultModel":"gpt-5.4"}`)
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			input := defaultsInput("pi")
			scenario.edit(&input)
			// An invalid descriptor makes an accidental path to launch observable.
			result, err := ResolveProviderDefaults(context.Background(), runtimeimage.Descriptor{}, input)
			if err != nil || result.Resolved || result.Reason == "" {
				t.Fatal("untrusted input reached executable admission", result.Reason, err)
			}
		})
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("credential command executed")
	}
	for _, override := range []string{`model_providers.fixture.env_key="GITHUB_TOKEN"`, `model_providers.fixture.requires_openai_auth=true`,
		`model_providers.fixture.experimental_bearer_token="literal"`, `model_providers.fixture.api_key_helper="touch marker"`, `hooks.test="touch marker"`, `profile="unobserved"`} {
		input := defaultsInput("codex")
		input.Options = agent.ExecOptions{CustomArgs: append(localCodexProviderArguments(), "-c", override)}
		if _, reason := codexDefaultPlan(input); reason == "" {
			t.Fatal("unmodeled Codex provider configuration was admitted", strings.Split(override, "=")[0])
		}
	}
}
