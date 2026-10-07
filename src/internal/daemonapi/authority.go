package daemonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

// ObserveAuthority reads workspace principals before assessing native-history
// integrations. Opaque integrations do not erase observed workspace authority.
// Every ordinary endpoint uses the current task token, whose actual principal
// is read from /api/me rather than inferred from token bytes or display names.
func (c *Client) ObserveAuthority(ctx context.Context, claim Claim) (workspace.AuthorityEvidence, error) {
	evidence := workspace.AuthorityEvidence{
		Version: workspace.AuthorityEvidenceVersion, WorkspaceID: claim.WorkspaceID,
		RuntimeID: claim.RuntimeID, AgentID: claim.AgentID,
	}
	unknown := func(reason string) (workspace.AuthorityEvidence, error) {
		evidence.Integrations = workspace.IntegrationEvidence{State: workspace.IntegrationUnknown, Reason: reason}
		return evidence, ctx.Err()
	}
	if !observationClaim(claim) {
		return evidence, errors.New("invalid authority observation scope")
	}
	if !validAttribution(claim.Attribution) || claim.Attribution == nil {
		return unknown("delegation_unknown")
	}
	if claim.Attribution.Originator != nil {
		evidence.OriginatorID = claim.Attribution.Originator.ID
	} else if claim.Attribution.Source != "unattributed" || claim.Attribution.Precise || claim.Attribution.Initiator != nil {
		return unknown("delegation_unknown")
	}
	if (claim.InitiatorID != "" && claim.InitiatorID != evidence.OriginatorID) || (claim.InitiatorType != "" && (claim.InitiatorType != "member" || evidence.OriginatorID == "")) {
		return unknown("delegation_conflict")
	}
	evidence.DelegationKnown = true
	var principal struct {
		ID string `json:"id"`
	}
	if c.taskObservationJSON(ctx, claim, "/api/me", &principal) != nil || !observationID(principal.ID) {
		return unknown("principal_unavailable")
	}
	evidence.PrincipalID = principal.ID
	var agent struct {
		ID          string `json:"id"`
		WorkspaceID string `json:"workspace_id"`
		RuntimeID   string `json:"runtime_id"`
		OwnerID     string `json:"owner_id"`
	}
	if c.taskObservationJSON(ctx, claim, "/api/agents/"+claim.AgentID, &agent) != nil || agent.ID != claim.AgentID || agent.WorkspaceID != claim.WorkspaceID || agent.RuntimeID != claim.RuntimeID || !observationID(agent.OwnerID) {
		return unknown("agent_authority_unavailable")
	}
	evidence.AgentOwnerID = agent.OwnerID
	var members []struct {
		ID          string `json:"id"`
		WorkspaceID string `json:"workspace_id"`
		UserID      string `json:"user_id"`
		Role        string `json:"role"`
	}
	if c.taskObservationJSON(ctx, claim, "/api/workspaces/"+claim.WorkspaceID+"/members", &members) != nil || members == nil {
		return unknown("membership_unavailable")
	}
	wanted := map[string]bool{evidence.PrincipalID: true, evidence.AgentOwnerID: true}
	if evidence.OriginatorID != "" {
		wanted[evidence.OriginatorID] = true
	}
	seenMembers, seenUsers := make(map[string]bool), make(map[string]bool)
	var memberships []workspace.AuthorityMembership
	for _, member := range members {
		if !observationID(member.ID) || member.WorkspaceID != claim.WorkspaceID || !observationID(member.UserID) || seenMembers[member.ID] || seenUsers[member.UserID] || (member.Role != "owner" && member.Role != "admin" && member.Role != "member") {
			return unknown("membership_ambiguous")
		}
		seenMembers[member.ID], seenUsers[member.UserID] = true, true
		if wanted[member.UserID] {
			memberships = append(memberships, workspace.AuthorityMembership{UserID: member.UserID, Role: member.Role})
			delete(wanted, member.UserID)
		}
	}
	if len(wanted) != 0 {
		return unknown("membership_missing")
	}
	slices.SortFunc(memberships, func(a, b workspace.AuthorityMembership) int { return strings.Compare(a.UserID, b.UserID) })
	evidence.Memberships = memberships
	if !emptyMCPDocument(claim.Agent.MCPConfig) || !emptyDeliveryArray(claim.ConnectedApps) || len(claim.RemoteMCPConnections) != 0 || claim.RemoteMCPDaemonToken != "" || len(claim.PluginHookTools) != 0 {
		return unknown("integration_authority_opaque")
	}
	var bindings []json.RawMessage
	if c.taskObservationJSON(ctx, claim, "/api/agents/"+claim.AgentID+"/mcp-servers", &bindings) != nil || bindings == nil {
		return unknown("mcp_inventory_unavailable")
	}
	if len(bindings) != 0 {
		return unknown("integration_authority_opaque")
	}
	raw, _, err := c.sendResponse(ctx, http.MethodGet, "/api/workspaces/"+claim.WorkspaceID+"/plugins", claim.AuthToken, claim.WorkspaceID, nil)
	var responseErr *ResponseError
	var disabled struct {
		Code string `json:"code"`
	}
	if err != nil {
		if !errors.As(err, &responseErr) || responseErr.StatusCode != http.StatusForbidden || json.Unmarshal(raw, &disabled) != nil || disabled.Code != "plugin_api_disabled" {
			return unknown("plugin_inventory_unavailable")
		}
	} else {
		var plugins struct {
			Plugins []json.RawMessage `json:"plugins"`
		}
		if json.Unmarshal(raw, &plugins) != nil || plugins.Plugins == nil {
			return unknown("plugin_inventory_unavailable")
		}
		if len(plugins.Plugins) != 0 {
			return unknown("integration_authority_opaque")
		}
	}
	evidence.Integrations = workspace.IntegrationEvidence{State: workspace.IntegrationVerifiedEmpty}
	if !evidence.ValidForReuse() {
		return unknown("authority_incomplete")
	}
	return evidence, nil
}

func (c *Client) taskObservationJSON(ctx context.Context, claim Claim, endpoint string, output any) error {
	raw, err := c.send(ctx, http.MethodGet, endpoint, claim.AuthToken, claim.WorkspaceID, nil)
	if err != nil {
		return err
	}
	if json.Unmarshal(raw, output) != nil {
		return errors.New("invalid authority response")
	}
	return nil
}

// Absence is the supported claim producer's empty representation. It becomes
// positive evidence only after successful independent inventory observations.
func emptyDeliveryArray(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return true
	}
	var values []json.RawMessage
	return json.Unmarshal(raw, &values) == nil && len(values) == 0
}

func emptyMCPDocument(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return true
	}
	var document map[string]json.RawMessage
	if json.Unmarshal(raw, &document) != nil || document == nil {
		return false
	}
	for key, value := range document {
		if key != "mcpServers" && key != "mcp" {
			return false
		}
		var servers map[string]json.RawMessage
		if json.Unmarshal(value, &servers) != nil || len(servers) != 0 {
			return false
		}
	}
	return true
}

func validAttribution(attribution *TaskAttribution) bool {
	if attribution == nil {
		return true
	}
	switch attribution.Source {
	case "direct_human", "delegation", "comment_source", "trigger_owner", "rule_owner", "owner_fallback", "backfill", "unattributed":
	default:
		return false
	}
	for _, user := range []*AttributionUser{attribution.Initiator, attribution.Originator} {
		if user != nil && !observationID(user.ID) {
			return false
		}
	}
	for _, id := range []string{attribution.DelegatedFromTaskID, attribution.RetryOfTaskID, attribution.RerunOfTaskID, attribution.RuleVersionID} {
		if id != "" && !observationID(id) {
			return false
		}
	}
	return attribution.Evidence == nil || (attribution.Evidence.Kind != "" && (attribution.Evidence.RefID == "" || observationID(attribution.Evidence.RefID)))
}
