package daemonapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/pelletier/go-toml/v2"
)

func codexDefaultPlan(input ProviderDefaultsInput) (defaultProbePlan, string) {
	plan := defaultProbePlan{files: make(map[string][]byte)}
	var ok bool
	plan.environment, ok = probeEnvironment(input)
	if !ok {
		return plan, "native_environment_unmodeled"
	}
	for name, raw := range input.ProjectFiles {
		if name != ".codex/config.toml" {
			continue
		}
		var project map[string]any
		if toml.Unmarshal(raw, &project) != nil || len(project) != 0 {
			// Relocating a trusted project to scratch would change Codex's
			// trust/layer resolution. Do not approve or ignore that layer.
			return plan, "codex_project_configuration_unmodeled"
		}
	}
	config := make(map[string]any)
	if raw, exists := input.HomeFiles[".codex/config.toml"]; exists {
		var source map[string]any
		if toml.Unmarshal(raw, &source) != nil {
			return plan, "native_configuration_invalid"
		}
		for key, value := range source {
			switch key {
			case "model", "model_reasoning_effort", "service_tier", "model_provider":
				text, ok := value.(string)
				if !ok || !selectorValue(text, false) {
					return plan, "codex_selector_unmodeled"
				}
				config[key] = text
			case "approval_policy", "sandbox_mode", "model_reasoning_summary", "model_verbosity", "personality":
				text, ok := value.(string)
				if !ok || !selectorValue(text, false) {
					return plan, "native_configuration_invalid"
				}
				config[key] = text
			case "cli_auth_credentials_store":
				if value != "file" {
					return plan, "codex_auth_storage_unmodeled"
				}
				config[key] = value
			case "features":
				features, ok := value.(map[string]any)
				if !ok {
					return plan, "native_configuration_invalid"
				}
				kept := map[string]bool{}
				for name, raw := range features {
					flag, ok := raw.(bool)
					if !ok || name != "fast_mode" && name != "hooks" && name != "plugins" && name != "remote_plugin" && name != "skill_mcp_dependency_install" {
						return plan, "codex_startup_feature_unmodeled"
					}
					if name == "fast_mode" {
						kept[name] = flag
					}
				}
				config[key] = kept
			case "model_providers":
				providers, ok := value.(map[string]any)
				if !ok {
					return plan, "codex_model_provider_unmodeled"
				}
				config[key] = providers
			case "mcp_servers", "hooks", "plugins", "profiles", "projects":
				object, ok := value.(map[string]any)
				if !ok || len(object) != 0 {
					return plan, "codex_startup_configuration_unmodeled"
				}
			default:
				return plan, "codex_configuration_unmodeled"
			}
		}
	}
	if raw, exists := input.HomeFiles[".codex/auth.json"]; exists {
		var source map[string]json.RawMessage
		if json.Unmarshal(raw, &source) != nil || source == nil {
			return plan, "native_auth_invalid"
		}
		auth := make(map[string]string)
		for name, value := range source {
			var text string
			if json.Unmarshal(value, &text) != nil {
				return plan, "native_auth_unmodeled"
			}
			switch name {
			case "OPENAI_API_KEY":
				if !literalCredential(text) || protectedProbeCredential(text, input.Environment) {
					return plan, "native_auth_unmodeled"
				}
			case "auth_mode":
				if text != "apikey" {
					return plan, "native_auth_unmodeled"
				}
			default:
				return plan, "native_auth_unmodeled"
			}
			auth[name] = text
		}
		if len(auth) > 0 && auth["OPENAI_API_KEY"] == "" {
			return plan, "native_auth_unmodeled"
		}
		if len(auth) > 0 {
			plan.files["home/.codex/auth.json"], _ = json.Marshal(auth)
		}
	}
	arguments := agent.NormalizeCodexLaunchArgs(input.Options.ExtraArgs, input.Options.CustomArgs, input.Options.McpConfig, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i := 0; i < len(arguments); i++ {
		argument := arguments[i]
		flag, value, equal := strings.Cut(argument, "=")
		if flag != "--config" && flag != "-c" {
			return plan, "codex_launch_arguments_unmodeled"
		}
		if !equal {
			i++
			if i == len(arguments) {
				return plan, "codex_launch_arguments_invalid"
			}
			value = arguments[i]
		}
		if !applyCodexSelectorOverride(config, value) {
			return plan, "codex_launch_arguments_unmodeled"
		}
	}
	if !codexDataOnlyProviders(config, input.Environment, plan.environment) {
		return plan, "codex_model_provider_unmodeled"
	}
	// Only reconstructed values are serialized. Task MCP/plugin declarations
	// and external command hooks never enter this process's configuration.
	config["mcp_servers"] = map[string]any{}
	plan.files["home/.codex/config.toml"], _ = toml.Marshal(config)
	plan.arguments = []string{"app-server", "--listen", "stdio://", "--disable", "hooks", "--disable", "plugins", "--disable", "remote_plugin", "--disable", "skill_mcp_dependency_install"}
	if input.Options.ServiceTier == "priority" {
		plan.arguments = append(plan.arguments, "--enable", "fast_mode")
	}
	return plan, ""
}

func applyCodexSelectorOverride(config map[string]any, value string) bool {
	key, value, found := strings.Cut(value, "=")
	key, value = strings.TrimSpace(key), strings.TrimSpace(value)
	if !found || !selectorValue(value, false) {
		return false
	}
	var parsed map[string]any
	var selected any = value
	// The native CLI falls back to the raw string for an unquoted TOML value.
	if toml.Unmarshal([]byte("value="+value), &parsed) == nil && len(parsed) == 1 {
		selected = parsed["value"]
	}
	if key == "model" || key == "model_reasoning_effort" || key == "service_tier" || key == "model_provider" {
		text, ok := selected.(string)
		if !ok || !selectorValue(text, false) {
			return false
		}
		config[key] = text
		return true
	}
	parts := strings.Split(key, ".")
	if len(parts) != 3 || parts[0] != "model_providers" || !codexProviderName(parts[1]) {
		return false
	}
	providers, _ := config["model_providers"].(map[string]any)
	if providers == nil {
		providers = make(map[string]any)
		config["model_providers"] = providers
	}
	provider, exists := providers[parts[1]]
	fields, ok := provider.(map[string]any)
	if exists && !ok {
		return false
	}
	if !exists {
		fields = make(map[string]any)
		providers[parts[1]] = fields
	}
	fields[parts[2]] = selected
	return true
}

func codexProviderName(name string) bool {
	return selectorValue(name, false) && len(name) <= 128 && !strings.ContainsAny(name, " .\t/\\\"'")
}

func codexDataOnlyProviders(config map[string]any, inputEnvironment, probeEnvironment map[string]string) bool {
	providers, _ := config["model_providers"].(map[string]any)
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	slices.Sort(names)
	for index, name := range names {
		raw := providers[name]
		provider, ok := raw.(map[string]any)
		if !codexProviderName(name) || !ok {
			return false
		}
		for field, raw := range provider {
			if field == "requires_openai_auth" {
				if enabled, ok := raw.(bool); !ok || enabled {
					return false
				}
				continue
			}
			value, ok := raw.(string)
			if !ok || !selectorValue(value, false) {
				return false
			}
			switch field {
			case "name":
			case "base_url":
				endpoint, err := url.Parse(value)
				if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.Scheme != "http" && endpoint.Scheme != "https" {
					return false
				}
			case "wire_api":
				if value != "responses" {
					return false
				}
			case "env_key":
				credential := inputEnvironment[value]
				if !executionEnvName.MatchString(value) || protectedProbeVariable(value) || !literalCredential(credential) || protectedProbeCredential(credential, inputEnvironment) {
					return false
				}
				// A source variable can also configure a runtime loader. Keep its
				// credential value without reintroducing that variable's behavior.
				reference := "PROVIDER_DEFAULT_KEY_" + strconv.Itoa(index)
				probeEnvironment[reference] = credential
				provider[field] = reference
			default:
				return false
			}
		}
		for _, required := range []string{"name", "base_url", "env_key", "wire_api"} {
			if _, exists := provider[required]; !exists {
				return false
			}
		}
	}
	selected, _ := config["model_provider"].(string)
	return selected == "" || selected == "openai" || providers[selected] != nil
}

func resolveCodexMetadata(probe *defaultProbe, input ProviderDefaultsInput, cwd string) (ProviderDefaults, error) {
	if err := probe.rpc("initialize", map[string]any{"clientInfo": map[string]string{"name": "multica-runtime-defaults", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}, nil); err != nil {
		return ProviderDefaults{}, err
	}
	if err := probe.send(map[string]any{"jsonrpc": "2.0", "method": "initialized", "params": struct{}{}}); err != nil {
		return ProviderDefaults{}, err
	}
	var current struct {
		Config map[string]json.RawMessage `json:"config"`
	}
	if probe.rpc("config/read", map[string]bool{"includeLayers": false}, &current) != nil || current.Config == nil {
		return ProviderDefaults{}, errProviderMetadata
	}
	if _, known := nullableSelector(current.Config, "service_tier"); !known {
		return ProviderDefaults{}, errProviderMetadata
	}
	config := map[string]any{"mcp_servers": map[string]any{}, "features.hooks": false, "features.plugins": false, "features.remote_plugin": false, "features.skill_mcp_dependency_install": false}
	if input.Options.ThinkingLevel != "" {
		config["model_reasoning_effort"] = input.Options.ThinkingLevel
	}
	params := map[string]any{"cwd": cwd, "ephemeral": true, "config": config, "model": nil}
	if input.Options.Model != "" {
		params["model"] = input.Options.Model
	}
	if input.Options.ServiceTier != "" {
		params["serviceTier"] = input.Options.ServiceTier
	}
	var thread map[string]json.RawMessage
	if probe.rpc("thread/start", params, &thread) != nil {
		return ProviderDefaults{}, errProviderMetadata
	}
	model, modelKnown := nullableSelector(thread, "model")
	provider, providerKnown := nullableSelector(thread, "modelProvider")
	effort, effortKnown := nullableSelector(thread, "reasoningEffort")
	tier, tierKnown := nullableSelector(thread, "serviceTier")
	if !modelKnown || model == nil || !providerKnown || provider == nil || !effortKnown || !tierKnown {
		return ProviderDefaults{}, errProviderMetadata
	}
	if input.Options.ThinkingLevel != "" {
		selected := input.Options.ThinkingLevel
		effort = &selected
	}
	if effort == nil {
		effort, effortKnown = nullableSelector(current.Config, "model_reasoning_effort")
		if !effortKnown {
			return ProviderDefaults{}, errProviderMetadata
		}
	}
	if effort == nil {
		cursor := ""
		seen := make(map[string]bool)
		for page := 0; page < 32; page++ {
			params := map[string]any{"limit": 100}
			if cursor != "" {
				params["cursor"] = cursor
			}
			var catalog struct {
				Data []struct {
					Model                  string `json:"model"`
					DefaultReasoningEffort string `json:"defaultReasoningEffort"`
				} `json:"data"`
				NextCursor *string `json:"nextCursor"`
			}
			if probe.rpc("model/list", params, &catalog) != nil || catalog.Data == nil {
				return ProviderDefaults{}, errProviderMetadata
			}
			for _, item := range catalog.Data {
				if item.Model == *model {
					if effort != nil || !selectorValue(item.DefaultReasoningEffort, false) {
						return ProviderDefaults{}, errProviderMetadata
					}
					selected := item.DefaultReasoningEffort
					effort = &selected
				}
			}
			if catalog.NextCursor == nil || *catalog.NextCursor == "" {
				break
			}
			cursor = *catalog.NextCursor
			if len(cursor) > 2048 || seen[cursor] {
				return ProviderDefaults{}, errProviderMetadata
			}
			seen[cursor] = true
			if page == 31 {
				return ProviderDefaults{}, errProviderMetadata
			}
		}
	}
	if effort == nil {
		return ProviderDefaults{}, errProviderMetadata
	}
	if input.Options.ServiceTier != "" {
		// The SDK repeats this explicit override on turn/start. Some providers
		// omit it from thread/start metadata, which cannot cancel that request.
		selected := input.Options.ServiceTier
		tier = &selected
	}
	return ProviderDefaults{Resolved: true, Model: *model, ModelProvider: *provider, ThinkingLevel: *effort, ServiceTier: tier}, nil
}

func nullableSelector(values map[string]json.RawMessage, key string) (*string, bool) {
	raw, exists := values[key]
	if !exists {
		return nil, false
	}
	if string(raw) == "null" {
		return nil, true
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || !selectorValue(value, false) {
		return nil, false
	}
	return &value, true
}
