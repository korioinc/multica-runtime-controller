package daemonapi

import (
	"encoding/json"
	"net/http"
	"strings"
)

const (
	blockedEndpointCode    = "runtime_controller_endpoint_blocked"
	blockedEndpointMessage = "This endpoint is blocked by multica-runtime-controller and is unavailable."
	credentialBlockReason  = "Personal credentials and account authentication are unavailable to tasks."
	daemonBlockReason      = "Daemon APIs are reserved for controller operations."
	workspaceBlockReason   = "Workspace creation and deletion are unavailable to tasks."
	integrationBlockReason = "Personal integration authentication is unavailable to tasks."
	machineBlockReason     = "Runtime configuration and machine control are unavailable to tasks."
	onboardingBlockReason  = "Onboarding bootstrap is blocked until backend workspace validation is enforced."
)

// Protected namespaces include the root and every descendant, regardless of
// method. Normal workspace integration and plugin tokens are not personal tokens.
var blockedNamespaces = [...]struct{ path, reason string }{
	{"/api/tokens", credentialBlockReason},
	{"/api/cli-token", credentialBlockReason},
	{"/api/auth", credentialBlockReason},
	{"/auth", credentialBlockReason},
	{"/api/daemon", daemonBlockReason},
}

// A * matches exactly one already-validated path segment. All other API paths
// retain backend authorization; new business endpoints need no allowlist entry.
var blockedEndpoints = [...]struct{ method, path, reason string }{
	{http.MethodPost, "/api/workspaces", workspaceBlockReason},
	{http.MethodDelete, "/api/workspaces/*", workspaceBlockReason},

	{http.MethodPost, "/api/integrations/composio/connect/init", integrationBlockReason},
	{http.MethodGet, "/api/integrations/composio/callback", integrationBlockReason},
	{http.MethodDelete, "/api/integrations/composio/connections/*", integrationBlockReason},
	{http.MethodPost, "/api/lark/binding/redeem", integrationBlockReason},
	{http.MethodPost, "/api/slack/binding/redeem", integrationBlockReason},
	{http.MethodPost, "/api/dingtalk/binding/redeem", integrationBlockReason},
	{http.MethodPost, "/api/wecom/binding/redeem", integrationBlockReason},
	{http.MethodPost, "/api/telegram/binding/redeem", integrationBlockReason},

	{http.MethodDelete, "/api/runtimes/*", machineBlockReason},
	{http.MethodPost, "/api/runtimes/*/unbind-agents-and-delete", machineBlockReason},
	{http.MethodPost, "/api/runtimes/*/archive-agents-and-delete", machineBlockReason},
	{http.MethodPost, "/api/runtimes/*/update", machineBlockReason},
	{http.MethodPost, "/api/runtimes/*/models", machineBlockReason},
	{http.MethodPost, "/api/runtimes/*/local-skills", machineBlockReason},
	{http.MethodPost, "/api/runtimes/*/local-skills/import", machineBlockReason},
	{http.MethodPost, "/api/workspaces/*/runtime-profiles", machineBlockReason},
	{http.MethodPut, "/api/workspaces/*/runtime-profiles/*", machineBlockReason},
	{http.MethodPatch, "/api/workspaces/*/runtime-profiles/*", machineBlockReason},
	{http.MethodDelete, "/api/workspaces/*/runtime-profiles/*", machineBlockReason},
	{http.MethodPost, "/api/cloud-runtime/nodes", machineBlockReason},
	{http.MethodDelete, "/api/cloud-runtime/nodes", machineBlockReason},
	{http.MethodPost, "/api/cloud-runtime/nodes/start", machineBlockReason},
	{http.MethodPost, "/api/cloud-runtime/nodes/stop", machineBlockReason},
	{http.MethodPost, "/api/cloud-runtime/nodes/reboot", machineBlockReason},
	{http.MethodPost, "/api/cloud-runtime/nodes/exec", machineBlockReason},

	// Remove these two restrictions only after the backend checks the task's
	// workspace against the bootstrap request's workspace_id.
	{http.MethodPost, "/api/me/onboarding/runtime-bootstrap", onboardingBlockReason},
	{http.MethodPost, "/api/me/onboarding/no-runtime-bootstrap", onboardingBlockReason},
}

func blockedEndpointReason(method, path string) string {
	for _, rule := range blockedNamespaces {
		if endpointNamespace(path, rule.path) {
			return rule.reason
		}
	}
	// The backend may serve HEAD using its GET handler. A protected GET must not
	// become available through that implicit method alias.
	if method == http.MethodHead {
		method = http.MethodGet
	}
	parts := strings.Split(path, "/")
	for _, rule := range blockedEndpoints {
		if rule.method == method && endpointPath(parts, rule.path) {
			return rule.reason
		}
	}
	return ""
}

func endpointNamespace(path, namespace string) bool {
	return path == namespace || strings.HasPrefix(path, namespace+"/")
}

func endpointPath(parts []string, pattern string) bool {
	expected := strings.Split(pattern, "/")
	if len(parts) != len(expected) {
		return false
	}
	for i, part := range parts {
		if expected[i] != "*" && expected[i] != part {
			return false
		}
	}
	return true
}

func proxyMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func rejectBlockedEndpoint(w http.ResponseWriter, r *http.Request, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_ = json.NewEncoder(w).Encode(struct {
		Error  string `json:"error"`
		Code   string `json:"code"`
		Method string `json:"method"`
		Path   string `json:"path"`
		Reason string `json:"reason"`
	}{blockedEndpointMessage, blockedEndpointCode, r.Method, r.URL.Path, reason})
}
