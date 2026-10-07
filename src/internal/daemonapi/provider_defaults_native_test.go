//go:build linux

package daemonapi

import (
	"context"
	"os"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/multica-ai/multica/server/pkg/agent"
)

// Run only inside the admitted runtime image with networking disabled and
// dummy API credentials. Every operation is metadata-only; no turn is sent.
func TestInstalledProviderDefaultsUseMetadataOnly(t *testing.T) {
	if os.Getenv("MULTICA_TEST_PROVIDER_DEFAULTS_NATIVE") != "1" {
		t.Skip("requires the installed runtime image")
	}
	var descriptor runtimeimage.Descriptor
	if _, err := runtimeimage.ReadJSON(runtimeimage.DescriptorPath, &descriptor); err != nil {
		t.Fatal(err)
	}
	for _, provider := range []string{"codex", "pi"} {
		t.Run(provider+" plain API key default", func(t *testing.T) {
			result, err := ResolveProviderDefaults(context.Background(), descriptor, defaultsInput(provider))
			if err != nil || !result.Resolved || result.Model == "" || result.ThinkingLevel == "" || result.ModelProvider != "openai" || result.ServiceTier != nil {
				t.Fatalf("default metadata not resolved: %+v, %v", result, err)
			}
			t.Logf("model=%s thinking=%s tier=unset", result.Model, result.ThinkingLevel)
		})
	}
	t.Run("Codex data-only local provider", func(t *testing.T) {
		input := defaultsInput("codex")
		input.Options = agent.ExecOptions{Model: "gpt-5.4", ThinkingLevel: "high", ServiceTier: "priority", CustomArgs: localCodexProviderArguments()}
		result, err := ResolveProviderDefaults(context.Background(), descriptor, input)
		// The SDK sends the tier again on turn/start, including when thread
		// metadata omits the tier for this custom provider.
		if err != nil || !result.Resolved || result.Model != input.Options.Model || result.ModelProvider != "fixture" || result.ThinkingLevel != input.Options.ThinkingLevel || result.ServiceTier == nil || *result.ServiceTier != input.Options.ServiceTier {
			t.Fatalf("local provider metadata not resolved: %+v, %v", result, err)
		}
	})
	for _, custom := range []bool{false, true} {
		name := "Codex configured tier with built-in provider"
		if custom {
			name = "Codex configured tier with custom provider"
		}
		t.Run(name, func(t *testing.T) {
			input := defaultsInput("codex")
			input.HomeFiles[".codex/config.toml"] = []byte("service_tier=\"priority\"\n[features]\nfast_mode=true\n")
			if custom {
				input.Options.CustomArgs = localCodexProviderArguments()
			}
			result, err := ResolveProviderDefaults(context.Background(), descriptor, input)
			if err != nil || !result.Resolved || result.ServiceTier == nil || *result.ServiceTier != "priority" {
				t.Fatalf("configured tier metadata was unavailable: %+v, %v", result, err)
			}
			tier := "unset"
			if result.ServiceTier != nil {
				tier = *result.ServiceTier
			}
			t.Log("native configured service tier:", tier)
		})
	}
	t.Run("Pi global settings select model and thinking", func(t *testing.T) {
		input := defaultsInput("pi")
		input.HomeFiles[".pi/agent/settings.json"] = []byte(`{"defaultProvider":"openai","defaultModel":"gpt-5.4","defaultThinkingLevel":"high"}`)
		result, err := ResolveProviderDefaults(context.Background(), descriptor, input)
		if err != nil || !result.Resolved || result.Model != "openai/gpt-5.4" || result.ThinkingLevel != "high" {
			t.Fatalf("global metadata not resolved: %+v, %v", result, err)
		}
	})
	t.Run("Pi blank task selectors finalize the observed tier model", func(t *testing.T) {
		claim, bootstrap, executable, settings := piTierExecutionFixture(t, "", "priority")
		executable = descriptor.Providers["pi"]
		bootstrap.RuntimeRef.Providers = map[string]runtimeimage.Executable{"pi": executable}
		execution, err := UnresolvedExecutionInput(claim, bootstrap, executable, settings)
		if err != nil || execution.Run.Options.Model != "" || len(execution.ServiceTierConfig) != 0 {
			t.Fatal("task defaults were validated or guessed before native observation", err)
		}
		input := defaultsInput("pi")
		input.Options = execution.Run.Options
		input.HomeFiles[".pi/agent/settings.json"] = []byte(`{"defaultProvider":"openai","defaultModel":"gpt-5.4","defaultThinkingLevel":"high"}`)
		defaults, err := ResolveProviderDefaults(context.Background(), descriptor, input)
		if err != nil || !defaults.Resolved {
			t.Fatal("the current native task defaults were not resolved", err)
		}
		execution, err = FinalizeExecution(execution, defaults)
		if err != nil {
			t.Fatal("observed task defaults could not finalize helper input", err)
		}
		assertPiTierBinding(t, execution, "gpt-5.4", "high", "priority")
	})
	t.Run("Pi auth file resolves a plain environment reference", func(t *testing.T) {
		input := defaultsInput("pi")
		delete(input.Environment, "OPENAI_API_KEY")
		input.Environment["LOCAL_PI_CREDENTIAL"] = "sk-local-fixture-key"
		input.HomeFiles[".pi/agent/auth.json"] = []byte(`{"openai":{"type":"api_key","key":"${LOCAL_PI_CREDENTIAL}"}}`)
		result, err := ResolveProviderDefaults(context.Background(), descriptor, input)
		if err != nil || !result.Resolved || result.ModelProvider != "openai" {
			t.Fatalf("plain auth reference was not resolved: %+v, %v", result, err)
		}
	})
	t.Run("Pi built-in model keeps a data-only endpoint override", func(t *testing.T) {
		input := defaultsInput("pi")
		input.Options.Model = "openai/gpt-5.4"
		input.HomeFiles[".pi/agent/models.json"] = []byte(`{"providers":{"openai":{"baseUrl":"http://127.0.0.1:18081/v1"}}}`)
		result, err := ResolveProviderDefaults(context.Background(), descriptor, input)
		if err != nil || !result.Resolved || result.Model != input.Options.Model || result.ModelProvider != "openai" {
			t.Fatalf("data-only endpoint configuration was not resolved: %+v, %v", result, err)
		}
	})
}
