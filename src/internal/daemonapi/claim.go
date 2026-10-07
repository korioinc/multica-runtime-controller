package daemonapi

import (
	"encoding/json"
	"errors"
	"strings"
)

// Claim keeps authorization identities separate from the execution input parsed
// from Envelope during controller preparation.
type Claim struct {
	Kind                          string           `json:"kind"`
	ID                            string           `json:"id"`
	AgentID                       string           `json:"agent_id"`
	RuntimeID                     string           `json:"runtime_id"`
	DispatchedAt                  string           `json:"dispatched_at"`
	WorkspaceID                   string           `json:"workspace_id"`
	WorkspaceSlug                 string           `json:"workspace_slug"`
	IssueID                       string           `json:"issue_id"`
	IssueIdentifier               string           `json:"issue_identifier"`
	Repos                         []Repository     `json:"repos"`
	AuthToken                     string           `json:"auth_token"`
	RemoteMCPDaemonToken          string           `json:"remote_mcp_daemon_token"`
	PriorSessionID                string           `json:"prior_session_id"`
	PriorWorkDir                  string           `json:"prior_work_dir"`
	StartClaimSupported           bool             `json:"start_claim_supported"`
	CreatedAt                     string           `json:"created_at"`
	ChatSessionID                 string           `json:"chat_session_id"`
	ChatChannelType               string           `json:"chat_channel_type"`
	ChatType                      string           `json:"chat_type"`
	AutopilotRunID                string           `json:"autopilot_run_id"`
	AutopilotID                   string           `json:"autopilot_id"`
	QuickCreatePrompt             string           `json:"quick_create_prompt"`
	NewCommentsDeltaKnown         bool             `json:"new_comments_delta_known"`
	IssueStateDeltaKnown          bool             `json:"issue_state_delta_known"`
	PriorSessionResumeUnavailable bool             `json:"prior_session_resume_unavailable"`
	Attribution                   *TaskAttribution `json:"attribution"`
	InitiatorID                   string           `json:"initiator_id"`
	InitiatorType                 string           `json:"initiator_type"`
	ConnectedApps                 json.RawMessage  `json:"connected_apps"`
	ProjectResources              []struct {
		ID string `json:"id"`
	} `json:"project_resources"`
	RemoteMCPConnections []struct {
		InstallationID string `json:"installation_id"`
		ContributionID string `json:"contribution_id"`
	} `json:"remote_mcp_connections"`
	PluginHookTools []struct {
		InstallationID string `json:"installation_id"`
		HookKey        string `json:"hook_key"`
	} `json:"plugin_hook_tools"`
	Agent struct {
		ID        string            `json:"id"`
		SkillRefs []json.RawMessage `json:"skill_refs"`
		MCPConfig json.RawMessage   `json:"mcp_config"`
	} `json:"agent"`
	Envelope json.RawMessage `json:"-"`
}

func ParseClaim(body json.RawMessage) (Claim, error) {
	var claim Claim
	if len(body) > MaxPayload || json.Unmarshal(body, &claim) != nil || !segment(claim.ID) || !segment(claim.AgentID) || !segment(claim.RuntimeID) || !segment(claim.WorkspaceID) || claim.Agent.ID != claim.AgentID || !strings.HasPrefix(claim.AuthToken, "mat_") || len(claim.AuthToken) <= 4 || strings.ContainsAny(claim.AuthToken, "\r\n") {
		return Claim{}, errors.New("invalid official task claim")
	}
	if len(claim.RemoteMCPConnections) > 0 && claim.RemoteMCPDaemonToken == "" {
		return Claim{}, errors.New("missing task broker credential")
	}
	for _, connection := range claim.RemoteMCPConnections {
		if !segment(connection.InstallationID) || !segment(connection.ContributionID) {
			return Claim{}, errors.New("invalid task contribution scope")
		}
	}
	for _, hook := range claim.PluginHookTools {
		if !segment(hook.InstallationID) || !segment(hook.HookKey) {
			return Claim{}, errors.New("invalid task hook scope")
		}
	}
	claim.Envelope = append(json.RawMessage(nil), body...)
	return claim, nil
}
