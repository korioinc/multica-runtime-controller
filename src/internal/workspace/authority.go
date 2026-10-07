package workspace

// IntegrationState describes observed integration authority for native history.
type IntegrationState string

const (
	AuthorityEvidenceVersion                  = 1
	IntegrationUnknown       IntegrationState = "unknown"
	IntegrationVerifiedEmpty IntegrationState = "verified_empty"
)

type IntegrationEvidence struct {
	State  IntegrationState `json:"state"`
	Reason string           `json:"reason,omitempty"`
}

type AuthorityMembership struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"`
}

// AuthorityEvidence contains stable observations, never rotating bearer tokens.
// Captured operator configuration is validated separately by compatibility.
type AuthorityEvidence struct {
	Version         int                   `json:"version"`
	WorkspaceID     string                `json:"workspace_id"`
	RuntimeID       string                `json:"runtime_id"`
	AgentID         string                `json:"agent_id"`
	PrincipalID     string                `json:"principal_id"`
	AgentOwnerID    string                `json:"agent_owner_id"`
	OriginatorID    string                `json:"originator_id,omitempty"`
	DelegationKnown bool                  `json:"delegation_known"`
	Memberships     []AuthorityMembership `json:"memberships"`
	Integrations    IntegrationEvidence   `json:"integrations"`
}

// ValidForWorkspaceReuse requires one canonical role for each authority-bearing human.
// The observer sorts memberships before publication; journal validation rejects
// duplicates, unrelated identities, and missing roles instead of repairing them.
func (a AuthorityEvidence) ValidForWorkspaceReuse() bool {
	if a.Version != AuthorityEvidenceVersion || !canonicalUUID(a.WorkspaceID) || !canonicalUUID(a.RuntimeID) || !canonicalUUID(a.AgentID) || !canonicalUUID(a.PrincipalID) || !canonicalUUID(a.AgentOwnerID) || !a.DelegationKnown || (a.OriginatorID != "" && !canonicalUUID(a.OriginatorID)) {
		return false
	}
	wanted := map[string]bool{a.PrincipalID: true, a.AgentOwnerID: true}
	if a.OriginatorID != "" {
		wanted[a.OriginatorID] = true
	}
	previous := ""
	for _, member := range a.Memberships {
		if !wanted[member.UserID] || member.UserID <= previous || (member.Role != "owner" && member.Role != "admin" && member.Role != "member") {
			return false
		}
		delete(wanted, member.UserID)
		previous = member.UserID
	}
	return len(wanted) == 0
}

// ValidForReuse also requires proven integration authority for native history.
func (a AuthorityEvidence) ValidForReuse() bool {
	return a.ValidForWorkspaceReuse() && a.Integrations.State == IntegrationVerifiedEmpty && a.Integrations.Reason == ""
}
