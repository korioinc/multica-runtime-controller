package daemonapi

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func piTierExecutionFixture(t *testing.T, model, tier string) (Claim, wire.Bootstrap, runtimeimage.Executable, ExecutionSettings) {
	t.Helper()
	claim, bootstrap, executable, settings := executionFixture(t, nil)
	settings.Provider = "pi"
	bootstrap.RuntimeRef.Providers = map[string]runtimeimage.Executable{"pi": executable}
	var fields map[string]any
	if err := json.Unmarshal(claim.Envelope, &fields); err != nil {
		t.Fatal(err)
	}
	agent := fields["agent"].(map[string]any)
	agent["model"], agent["thinking_level"], agent["service_tier"] = model, "", tier
	claim.Envelope, _ = json.Marshal(fields)
	return claim, bootstrap, executable, settings
}

func assertPiTierBinding(t *testing.T, execution Execution, model, thinking, tier string) {
	t.Helper()
	if execution.Run.Options.Model != "openai/"+model || execution.Run.Options.ThinkingLevel != thinking || execution.Run.Options.ServiceTier != tier {
		t.Fatal("final execution did not bind the observed provider options")
	}
	var config struct {
		PersistState    bool     `json:"persistState"`
		Active          bool     `json:"active"`
		ServiceTier     string   `json:"serviceTier"`
		SupportedModels []string `json:"supportedModels"`
	}
	if json.Unmarshal(execution.ServiceTierConfig, &config) != nil || config.PersistState || !config.Active || config.ServiceTier != tier ||
		!slices.Equal(config.SupportedModels, []string{"pi-openai-service-tier:openai-responses/" + model}) {
		t.Fatal("helper tier configuration differs from the finalized execution options")
	}
}

func TestExecutionPiTierRequiresFinalOptionsBeforeHelperPublication(t *testing.T) {
	claim, bootstrap, executable, settings := piTierExecutionFixture(t, "", "priority")
	input, err := UnresolvedExecutionInput(claim, bootstrap, executable, settings)
	if err != nil || input.Run.Options.Model != "" || input.Run.Options.ThinkingLevel != "" || input.Run.Options.ServiceTier != "priority" || len(input.ServiceTierConfig) != 0 {
		t.Fatal("unresolved input guessed native selectors or published a tier helper", err)
	}
	if _, err := ExecutionInput(claim, bootstrap, executable, settings); !errors.Is(err, errPiTierModelUnresolved) {
		t.Fatal("the strict legacy input path accepted an unknown tier model", err)
	}
	if _, err := FinalizeExecution(input, ProviderDefaults{Reason: "native_startup_configuration_unmodeled"}); !errors.Is(err, errPiTierModelUnresolved) {
		t.Fatal("unknown final defaults supplied a guessed model for the tier", err)
	}
	tier := "priority"
	finalized, err := FinalizeExecution(input, ProviderDefaults{Resolved: true, Model: "openai/gpt-5.4", ThinkingLevel: "high", ServiceTier: &tier})
	if err != nil {
		t.Fatal(err)
	}
	assertPiTierBinding(t, finalized, "gpt-5.4", "high", tier)
	// Preparation rebuilds input and binds its frozen choices before writing
	// helper files. Rebinding must also replace an existing generated helper.
	tier = "flex"
	finalized, err = FinalizeExecution(finalized, ProviderDefaults{Resolved: true, Model: "openai/gpt-5.5", ThinkingLevel: "medium", ServiceTier: &tier})
	if err != nil {
		t.Fatal(err)
	}
	assertPiTierBinding(t, finalized, "gpt-5.5", "medium", tier)
	finalized, err = FinalizeExecution(finalized, ProviderDefaults{Resolved: true, Model: "openai/gpt-5.5", ThinkingLevel: "medium"})
	if err != nil || finalized.Run.Options.ServiceTier != "" || string(finalized.ServiceTierConfig) != `{"persistState":false,"active":false}` {
		t.Fatal("proven absence retained the previous active service tier", err)
	}
}

func TestExecutionPiTierRejectsInvalidRawAndFinalChoices(t *testing.T) {
	for _, test := range []struct{ model, tier string }{
		{"", "unsupported"}, {"openai/gpt-unsupported", "priority"}, {"anthropic/claude", "priority"}, {"openai-codex/gpt-5.4", "priority"},
	} {
		t.Run(test.model+"/"+test.tier, func(t *testing.T) {
			claim, bootstrap, executable, settings := piTierExecutionFixture(t, test.model, test.tier)
			if _, err := UnresolvedExecutionInput(claim, bootstrap, executable, settings); err == nil {
				t.Fatal("unresolved builder bypassed an explicit invalid model, tier, or OAuth choice")
			}
			if _, err := ExecutionInput(claim, bootstrap, executable, settings); err == nil {
				t.Fatal("strict input accepted an invalid model, tier, or OAuth choice")
			}
		})
	}
	claim, bootstrap, executable, settings := piTierExecutionFixture(t, "", "priority")
	input, err := UnresolvedExecutionInput(claim, bootstrap, executable, settings)
	if err != nil {
		t.Fatal(err)
	}
	priority := "priority"
	for _, model := range []string{"", "openai/gpt-unsupported", "openai-codex/gpt-5.4", "openai/gpt-5.4\x00"} {
		if _, err := FinalizeExecution(input, ProviderDefaults{Resolved: true, Model: model, ThinkingLevel: "high", ServiceTier: &priority}); err == nil {
			t.Fatal("finalization accepted an unsupported or invalid observed model")
		}
	}
	if _, err := FinalizeExecution(input, ProviderDefaults{Resolved: true, Model: "openai/gpt-5.4", ThinkingLevel: "high\x00", ServiceTier: &priority}); err == nil {
		t.Fatal("finalization bypassed option validation after binding native defaults")
	}
}
