package daemonapi

import (
	"encoding/json"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

func piDefaultPlan(input ProviderDefaultsInput) (defaultProbePlan, string) {
	plan := defaultProbePlan{files: make(map[string][]byte)}
	var ok bool
	plan.environment, ok = probeEnvironment(input)
	if !ok {
		return plan, "native_environment_unmodeled"
	}
	if _, present := input.HomeFiles[".pi/agent/trust.json"]; present {
		return plan, "pi_project_trust_unmodeled"
	}
	for _, layer := range []struct {
		files  map[string][]byte
		source string
		target string
	}{
		{input.HomeFiles, ".pi/agent/settings.json", "home/.pi/agent/settings.json"},
		{input.ProjectFiles, ".pi/settings.json", "work/.pi/settings.json"},
	} {
		if raw, exists := layer.files[layer.source]; exists {
			settings, ok := piSelectorSettings(raw)
			if !ok {
				return plan, "pi_startup_configuration_unmodeled"
			}
			if layer.source == ".pi/settings.json" && len(settings) != 0 {
				// Pi applies this layer only after native project trust resolves.
				// Moving the project to scratch cannot preserve that decision.
				return plan, "pi_project_trust_unmodeled"
			}
			plan.files[layer.target], _ = json.Marshal(settings)
		}
	}
	if raw, exists := input.HomeFiles[".pi/agent/models.json"]; exists {
		models, ok := piDataOnlyProviderOverrides(raw, input.Environment, plan.environment)
		if !ok {
			return plan, "pi_model_registry_unmodeled"
		}
		plan.files["home/.pi/agent/models.json"], _ = json.Marshal(models)
	}
	if raw, exists := input.HomeFiles[".pi/agent/auth.json"]; exists {
		var source map[string]map[string]json.RawMessage
		if json.Unmarshal(raw, &source) != nil || source == nil {
			return plan, "native_auth_invalid"
		}
		auth := make(map[string]map[string]string)
		providers := make([]string, 0, len(source))
		for provider := range source {
			providers = append(providers, provider)
		}
		slices.Sort(providers)
		for index, provider := range providers {
			entry := source[provider]
			var kind, value string
			if !selectorValue(provider, false) || len(entry) != 2 || json.Unmarshal(entry["type"], &kind) != nil || kind != "api_key" || json.Unmarshal(entry["key"], &value) != nil {
				return plan, "native_auth_unmodeled"
			}
			value, ok := resolvePlainCredential(value, input.Environment)
			if !ok {
				return plan, "native_auth_unmodeled"
			}
			// Pi resolves a credential reference once. A fresh private variable
			// preserves literal values that happen to look like another env key.
			reference := "PROVIDER_DEFAULT_KEY_" + strconv.Itoa(index)
			plan.environment[reference] = value
			auth[provider] = map[string]string{"type": "api_key", "key": "${" + reference + "}"}
		}
		plan.files["home/.pi/agent/auth.json"], _ = json.Marshal(auth)
	}
	plan.arguments = []string{"--mode", "rpc", "--no-session", "--offline", "--no-extensions", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files"}
	if input.Options.Model != "" {
		plan.arguments = append(plan.arguments, "--model", input.Options.Model)
	}
	if input.Options.ThinkingLevel != "" {
		plan.arguments = append(plan.arguments, "--thinking", input.Options.ThinkingLevel)
	}
	if len(input.Options.ExtraArgs) != 0 {
		return plan, "pi_launch_arguments_unmodeled"
	}
	for i := 0; i < len(input.Options.CustomArgs); i++ {
		argument := unquoteSelectorArgument(input.Options.CustomArgs[i])
		flag, value, equal := strings.Cut(argument, "=")
		if flag != "--provider" && flag != "--model" && flag != "--models" {
			return plan, "pi_launch_arguments_unmodeled"
		}
		if !equal {
			i++
			if i == len(input.Options.CustomArgs) {
				return plan, "pi_launch_arguments_invalid"
			}
			value = unquoteSelectorArgument(input.Options.CustomArgs[i])
		}
		if !selectorValue(value, false) || strings.HasPrefix(value, "-") || strings.HasPrefix(value, "@") {
			return plan, "pi_launch_arguments_invalid"
		}
		plan.arguments = append(plan.arguments, flag, value)
	}
	return plan, ""
}

func piDataOnlyProviderOverrides(raw []byte, inputEnvironment, probeEnvironment map[string]string) (map[string]any, bool) {
	var document map[string]json.RawMessage
	if json.Unmarshal(raw, &document) != nil || document == nil || len(document) > 1 {
		return nil, false
	}
	providers := map[string]map[string]string{}
	if len(document) != 0 {
		if json.Unmarshal(document["providers"], &providers) != nil || providers == nil {
			return nil, false
		}
	}
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	slices.Sort(names)
	for index, name := range names {
		fields := providers[name]
		if !selectorValue(name, false) || fields == nil || len(fields) == 0 {
			return nil, false
		}
		for field, value := range fields {
			switch field {
			case "baseUrl":
				endpoint, err := url.Parse(value)
				if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.Scheme != "http" && endpoint.Scheme != "https" {
					return nil, false
				}
			case "apiKey":
				credential, ok := resolvePlainCredential(value, inputEnvironment)
				if !ok {
					return nil, false
				}
				reference := "PROVIDER_DEFAULT_MODEL_KEY_" + strconv.Itoa(index)
				probeEnvironment[reference] = credential
				fields[field] = "${" + reference + "}"
			default:
				// Custom model definitions, request headers, OAuth, factories,
				// and discovery configuration need their own native proof.
				return nil, false
			}
		}
	}
	return map[string]any{"providers": providers}, true
}

func unquoteSelectorArgument(value string) string {
	if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') && value[len(value)-1] == value[0] {
		return value[1 : len(value)-1]
	}
	return value
}

func piSelectorSettings(raw []byte) (map[string]any, bool) {
	var source map[string]json.RawMessage
	if json.Unmarshal(raw, &source) != nil || source == nil {
		return nil, false
	}
	result := make(map[string]any)
	for key, value := range source {
		switch key {
		case "defaultProvider", "defaultModel", "defaultThinkingLevel":
			var selection string
			if json.Unmarshal(value, &selection) != nil || !selectorValue(selection, false) {
				return nil, false
			}
			result[key] = selection
		case "enabledModels":
			var models []string
			if json.Unmarshal(value, &models) != nil || models == nil {
				return nil, false
			}
			for _, model := range models {
				if !selectorValue(model, false) {
					return nil, false
				}
			}
			result[key] = models
		case "modelThinkingLevels":
			var levels map[string]string
			if json.Unmarshal(value, &levels) != nil || levels == nil {
				return nil, false
			}
			for model, level := range levels {
				if !selectorValue(model, false) || !selectorValue(level, false) {
					return nil, false
				}
			}
			result[key] = levels
		case "packages", "extensions", "skills", "promptTemplates", "themes":
			var resources []json.RawMessage
			if json.Unmarshal(value, &resources) != nil || len(resources) != 0 {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return result, true
}

func resolvePiMetadata(probe *defaultProbe, input ProviderDefaultsInput) (ProviderDefaults, error) {
	if err := probe.send(map[string]string{"id": "defaults", "type": "get_state"}); err != nil {
		return ProviderDefaults{}, err
	}
	for {
		var response struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Command string `json:"command"`
			Success bool   `json:"success"`
			Data    struct {
				Model *struct {
					Provider string `json:"provider"`
					ID       string `json:"id"`
				} `json:"model"`
				ThinkingLevel string `json:"thinkingLevel"`
			} `json:"data"`
		}
		if probe.decoder.Decode(&response) != nil {
			return ProviderDefaults{}, errProviderMetadata
		}
		if response.Type != "response" {
			if response.Type == "extension_ui_request" {
				return ProviderDefaults{}, errProviderMetadata
			}
			continue
		}
		model := response.Data.Model
		if response.ID != "defaults" || response.Command != "get_state" || !response.Success || model == nil ||
			!selectorValue(model.Provider, false) || !selectorValue(model.ID, false) || !selectorValue(response.Data.ThinkingLevel, false) {
			return ProviderDefaults{}, errProviderMetadata
		}
		var tier *string
		if input.Options.ServiceTier != "" {
			value := input.Options.ServiceTier
			tier = &value
		}
		return ProviderDefaults{Resolved: true, Model: model.Provider + "/" + model.ID, ModelProvider: model.Provider,
			ThinkingLevel: response.Data.ThinkingLevel, ServiceTier: tier}, nil
	}
}
