package daemonapi

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/skillbundle"
)

// ConversationIdentity follows the current task's official audience and source
// discriminator. A historical channel chat can contain a private native turn.
func ConversationIdentity(ownerID string, claim Claim) workspace.ConversationKey {
	key := workspace.ConversationKey{OwnerID: ownerID, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID,
		Kind: workspace.ConversationTask, SubjectID: claim.ID}
	switch claim.Kind {
	case "direct", "comment":
		if wire.UUID(claim.IssueID) && claim.ChatSessionID == "" && claim.AutopilotRunID == "" && claim.AutopilotID == "" && claim.QuickCreatePrompt == "" {
			key.Kind, key.SubjectID = workspace.ConversationIssue, claim.IssueID
		}
	case "chat":
		if wire.UUID(claim.ChatSessionID) && claim.IssueID == "" && claim.ChatChannelType == "" && claim.ChatType == "" &&
			claim.AutopilotRunID == "" && claim.AutopilotID == "" && claim.QuickCreatePrompt == "" {
			key.Kind, key.SubjectID = workspace.ConversationAgentDM, claim.ChatSessionID
		}
	}
	return key
}

// ExecutionCompatibility records the inputs actually selected for execution.
// The caller resolves native defaults before using this as reuse authority.
func ExecutionCompatibility(claim Claim, key workspace.ConversationKey, bootstrap wire.Bootstrap, input Execution,
	skills []SkillBundle, authority workspace.AuthorityEvidence, repositories []workspace.Repository, scope []string) (workspace.ConversationCompatibilityV1, bool, error) {
	var task executionTask
	if json.Unmarshal(claim.Envelope, &task) != nil || !key.Valid() {
		return workspace.ConversationCompatibilityV1{}, false, errors.New("invalid conversation execution input")
	}
	options := input.Run.Options
	stableMCP := input.Run.Options.McpConfig
	if len(stableMCP) == 0 {
		stableMCP = json.RawMessage(`null`)
	}
	var mcp struct {
		Servers map[string]json.RawMessage `json:"mcpServers"`
	}
	if json.Unmarshal(stableMCP, &mcp) != nil {
		return workspace.ConversationCompatibilityV1{}, false, errors.New("invalid effective MCP configuration")
	}
	eligible := key.Kind != workspace.ConversationTask && claim.StartClaimSupported && authority.ValidForReuse() && len(mcp.Servers) == 0
	if len(mcp.Servers) != 0 {
		// An opaque integration cannot gain eligibility by hashing its rotating
		// credential or generated URL. Its actual execution input stays intact.
		authority.Integrations = workspace.IntegrationEvidence{State: workspace.IntegrationUnknown, Reason: "effective_integrations_unproven"}
		stableMCP = json.RawMessage(`null`)
	}
	result := workspace.ConversationCompatibilityV1{Conversation: key, RuntimeID: claim.RuntimeID, Authority: authority,
		Repositories: repositories, ResourceScope: scope, Provider: input.Run.Provider, RuntimeRef: bootstrap.RuntimeRef,
		ConfigurationDigest: bootstrap.RuntimeRef.ConfigurationDigest, AgentInstructions: task.Agent.Instructions,
		Model: options.Model, ThinkingLevel: options.ThinkingLevel, ServiceTier: options.ServiceTier,
		CustomArgs: append(append([]string{}, options.ExtraArgs...), options.CustomArgs...), CustomEnv: task.Agent.CustomEnv,
		StableMCP: stableMCP, StablePlugins: json.RawMessage(`[]`)}
	for _, skill := range skills {
		files := make([]skillbundle.File, 0, len(skill.Files))
		for _, file := range skill.Files {
			files = append(files, skillbundle.File{Path: file.Path, Content: file.Content})
		}
		manifest := skillbundle.BuildManifest(skillbundle.Skill{ID: skill.ID, Source: skill.Source, Name: skill.Name,
			Description: skill.Description, Content: skill.Content, Files: files})
		id := skill.Source + ":" + skill.ID
		if skill.ID == "" {
			id = "inline:" + skill.Name
		}
		result.SkillHashes = append(result.SkillHashes, workspace.SkillContentHash{ID: id, Digest: strings.TrimPrefix(manifest.Hash, "sha256:")})
	}
	for _, skill := range task.Agent.DisabledRuntimeSkills {
		raw, _ := json.Marshal([]string{skill.RuntimeID, skill.Provider, skill.Root, skill.Key, skill.Plugin})
		result.DisabledRuntimeSkills = append(result.DisabledRuntimeSkills, string(raw))
	}
	return result, eligible, nil
}
