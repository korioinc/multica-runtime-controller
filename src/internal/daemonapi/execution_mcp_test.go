package daemonapi

import (
	"cmp"
	"encoding/json"
	"slices"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func TestExecutionPiMCPOperatorInputPrecedence(t *testing.T) {
	const currentPath = ".pi/agent/mcp-adapter.json"
	const previousPath = ".pi/agent/mcp.json"
	current := `{"mcpServers":{"current":{"url":"https://current.example/mcp"}}}`
	previous := `{"mcpServers":{"previous":{"url":"https://previous.example/mcp"}}}`
	for _, scenario := range []struct {
		name     string
		files    map[string]string
		expected []string
		invalid  bool
	}{
		{name: "current file", files: map[string]string{currentPath: current}, expected: []string{"current", "task"}},
		{name: "previous input fallback", files: map[string]string{previousPath: previous}, expected: []string{"previous", "task"}},
		{name: "current wins", files: map[string]string{currentPath: current, previousPath: previous}, expected: []string{"current", "task"}},
		{name: "explicit empty wins", files: map[string]string{currentPath: `{"mcpServers":{}}`, previousPath: previous}, expected: []string{"task"}},
		{name: "explicit null wins", files: map[string]string{currentPath: `null`, previousPath: previous}, expected: []string{"task"}},
		{name: "invalid current JSON cannot fall back", files: map[string]string{currentPath: `{`, previousPath: previous}, invalid: true},
		{name: "invalid current schema cannot fall back", files: map[string]string{currentPath: `{"mcpServers":"invalid"}`, previousPath: previous}, invalid: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			claim, bootstrap, executable, settings := executionFixture(t, nil)
			settings.Provider = "pi"
			bootstrap.RuntimeRef.Providers = map[string]runtimeimage.Executable{"pi": executable}
			var fields map[string]any
			if err := json.Unmarshal(claim.Envelope, &fields); err != nil {
				t.Fatal(err)
			}
			fields["agent"].(map[string]any)["mcp_config"] = map[string]any{"mcpServers": map[string]any{"task": map[string]string{"url": "https://task.example/mcp"}}}
			claim.Envelope, _ = json.Marshal(fields)
			bootstrap.Configuration = mcpOperatorBundle(scenario.files)
			execution, err := ExecutionInput(claim, bootstrap, executable, settings)
			if scenario.invalid {
				if err == nil {
					t.Fatal("malformed current operator configuration fell back to older authority")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var document struct {
				Servers map[string]json.RawMessage `json:"mcpServers"`
			}
			if err := json.Unmarshal(execution.Run.Options.McpConfig, &document); err != nil {
				t.Fatal(err)
			}
			if len(document.Servers) != len(scenario.expected) {
				t.Fatal("operator precedence added an unselected MCP server")
			}
			for _, name := range scenario.expected {
				if document.Servers[name] == nil {
					t.Fatal("execution lost the selected operator or current task MCP server", name)
				}
			}
		})
	}
}

func TestCapturedCodexMCPIgnoresPiInputMigration(t *testing.T) {
	bundle := mcpOperatorBundle(map[string]string{
		".pi/agent/mcp-adapter.json": `{`,
		".pi/agent/mcp.json":         `{`,
		".codex/config.toml":         "[mcp_servers.codex]\nurl = 'https://codex.example/mcp'\n[mcp_servers.codex.http_headers]\nAuthorization = 'Bearer operator'\n",
	})
	raw, err := capturedMCP(bundle, "codex")
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Servers map[string]struct {
			Type    string            `json:"type"`
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"mcpServers"`
	}
	if json.Unmarshal(raw, &document) != nil || len(document.Servers) != 1 || document.Servers["codex"].Type != "http" ||
		document.Servers["codex"].URL != "https://codex.example/mcp" || document.Servers["codex"].Headers["Authorization"] != "Bearer operator" {
		t.Fatal("Pi input migration changed the captured Codex server")
	}
}

func mcpOperatorBundle(files map[string]string) configuration.Bundle {
	group := configuration.Group{Name: "operator", Directories: []string{}, Files: []configuration.File{}}
	for path, content := range files {
		data := []byte(content)
		group.Files = append(group.Files, configuration.File{Target: path, Mode: 0600, SHA256: core.Digest(data), Content: data})
	}
	slices.SortFunc(group.Files, func(a, b configuration.File) int {
		return cmp.Compare(a.Target, b.Target)
	})
	groups := []configuration.Group{group}
	return configuration.Bundle{Groups: groups, Digest: configuration.Digest(groups)}
}
