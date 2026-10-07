package workspace

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

type Conversation struct {
	Key              ConversationKey `json:"key"`
	CurrentStorageID string          `json:"currentStorageID,omitempty"`
	CurrentSessionID string          `json:"currentSessionID,omitempty"`
	Revision         uint64          `json:"revision"`
}

type SkillContentHash struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

// ConversationCompatibilityV1 describes stable authority and execution inputs.
// Per-turn prompts, task tokens, reply targets, and generated relay URLs do not belong here.
type ConversationCompatibilityV1 struct {
	Conversation            ConversationKey    `json:"conversation"`
	RuntimeID               string             `json:"runtimeID"`
	Authority               AuthorityEvidence  `json:"authority"`
	Repositories            []Repository       `json:"repositories"`
	ResourceScope           []string           `json:"resourceScope"`
	Provider                string             `json:"provider"`
	ProviderOptionsResolved bool               `json:"providerOptionsResolved"`
	RuntimeRef              runtimeimage.Ref   `json:"runtimeRef"`
	ConfigurationDigest     string             `json:"configurationDigest"`
	AgentInstructions       string             `json:"agentInstructions"`
	Model                   string             `json:"model"`
	ThinkingLevel           string             `json:"thinkingLevel"`
	ServiceTier             string             `json:"serviceTier"`
	CustomArgs              []string           `json:"customArgs"`
	CustomEnv               map[string]string  `json:"customEnv"`
	SkillHashes             []SkillContentHash `json:"skillHashes"`
	DisabledRuntimeSkills   []string           `json:"disabledRuntimeSkills"`
	StableMCP               json.RawMessage    `json:"stableMCP"`
	StablePlugins           json.RawMessage    `json:"stablePlugins"`
}

func (c ConversationCompatibilityV1) normalizedWorkspace() (ConversationCompatibilityV1, error) {
	if !c.Conversation.Valid() || !canonicalUUID(c.RuntimeID) || c.RuntimeRef.Validate() != nil ||
		!fingerprint(c.ConfigurationDigest) || c.ConfigurationDigest != c.RuntimeRef.ConfigurationDigest ||
		!SupportedProvider(c.Provider) {
		return c, invalid("conversation compatibility")
	}
	if c.Authority.WorkspaceID != "" && c.Authority.WorkspaceID != c.Conversation.WorkspaceID ||
		c.Authority.AgentID != "" && c.Authority.AgentID != c.Conversation.AgentID || c.Authority.RuntimeID != "" && c.Authority.RuntimeID != c.RuntimeID {
		return c, invalid("conversation authority")
	}
	c.Repositories = slices.Clone(c.Repositories)
	slices.SortFunc(c.Repositories, func(a, b Repository) int {
		if result := strings.Compare(a.URL, b.URL); result != 0 {
			return result
		}
		if result := strings.Compare(a.Ref, b.Ref); result != 0 {
			return result
		}
		return strings.Compare(a.ID, b.ID)
	})
	for i := range c.Repositories {
		if !validOpaque(c.Repositories[i].URL) {
			return c, invalid("conversation repository")
		}
		c.Repositories[i].Description = ""
	}
	c.ResourceScope = slices.Clone(c.ResourceScope)
	slices.Sort(c.ResourceScope)
	c.ResourceScope = slices.Compact(c.ResourceScope)
	if c.Repositories == nil {
		c.Repositories = []Repository{}
	}
	if c.ResourceScope == nil {
		c.ResourceScope = []string{}
	}
	return c, nil
}

// WorkspaceDigest binds the authority and fixed environment of the anchored root.
// Native options and refreshed per-turn integration inputs do not own that root.
func (c ConversationCompatibilityV1) WorkspaceDigest() (string, error) {
	c, err := c.normalizedWorkspace()
	if err != nil {
		return "", err
	}
	authority := c.Authority
	authority.Integrations = IntegrationEvidence{}
	raw, err := json.Marshal([]any{"multica-workspace-compatibility-v1", c.Conversation, c.RuntimeID, authority,
		c.Repositories, c.ResourceScope, c.Provider, c.RuntimeRef, c.ConfigurationDigest})
	if err != nil {
		return "", err
	}
	return digest(raw), nil
}

func (c ConversationCompatibilityV1) Digest() (string, error) {
	c, err := c.normalizedWorkspace()
	if err != nil {
		return "", err
	}
	if !json.Valid(c.StableMCP) || !json.Valid(c.StablePlugins) {
		return "", invalid("conversation integrations")
	}
	c.DisabledRuntimeSkills = slices.Clone(c.DisabledRuntimeSkills)
	slices.Sort(c.DisabledRuntimeSkills)
	c.DisabledRuntimeSkills = slices.Compact(c.DisabledRuntimeSkills)
	c.SkillHashes = slices.Clone(c.SkillHashes)
	slices.SortFunc(c.SkillHashes, func(a, b SkillContentHash) int { return strings.Compare(a.ID, b.ID) })
	for i, skill := range c.SkillHashes {
		if !validOpaque(skill.ID) || !fingerprint(skill.Digest) || i > 0 && c.SkillHashes[i-1].ID == skill.ID {
			return "", invalid("conversation skill")
		}
	}
	c.CustomEnv = maps.Clone(c.CustomEnv)
	if c.CustomEnv == nil {
		c.CustomEnv = map[string]string{}
	}
	if c.CustomArgs == nil {
		c.CustomArgs = []string{}
	}
	if c.SkillHashes == nil {
		c.SkillHashes = []SkillContentHash{}
	}
	if c.DisabledRuntimeSkills == nil {
		c.DisabledRuntimeSkills = []string{}
	}
	c.StableMCP = canonicalJSON(c.StableMCP)
	c.StablePlugins = canonicalJSON(c.StablePlugins)
	raw, err := json.Marshal([]any{"multica-conversation-compatibility-v1", c})
	if err != nil {
		return "", err
	}
	return digest(raw), nil
}

// RecordCompatibility retains typed observations so later turns can compare
// principal and static configuration after the previous task token expires.
func (s *Store) RecordCompatibility(id string, compatibility ConversationCompatibilityV1) error {
	calculated, err := compatibility.Digest()
	if err != nil {
		return err
	}
	workspaceDigest, err := compatibility.WorkspaceDigest()
	if err != nil {
		return err
	}
	return s.updateGrant(id, func(_ *registry, g *TaskGrant) error {
		if !g.SessionProtocol || g.Legacy || g.ExecutionRevoked || (g.State != "waiting_storage" && g.State != "intent") ||
			compatibility.Conversation.OwnerID != g.OwnerID || compatibility.Conversation.WorkspaceID != g.WorkspaceID ||
			compatibility.Conversation.AgentID != g.AgentID || compatibility.RuntimeID != g.RuntimeID || !compatibility.RuntimeRef.Equal(g.RuntimeRef) ||
			compatibility.Conversation.Kind == ConversationTask && compatibility.Conversation.SubjectID != g.TaskID ||
			g.CompatibilityDigest != "" && g.CompatibilityDigest != calculated || g.Conversation != (ConversationKey{}) && g.Conversation != compatibility.Conversation ||
			!compatibilityScopeMatches(*g, compatibility, workspaceDigest) {
			return ErrConflict
		}
		if g.Selection != nil && (g.Selection.WorkspaceReuseEligible && !compatibility.Authority.ValidForWorkspaceReuse() ||
			g.Selection.ReuseEligible && (!g.Selection.WorkspaceReuseEligible || !compatibility.ProviderOptionsResolved || !compatibility.Authority.ValidForReuse())) {
			return ErrConflict
		}
		if g.Compatibility != nil {
			previous, err := g.Compatibility.Digest()
			if err != nil || previous != calculated {
				return ErrConflict
			}
			return nil
		}
		g.Compatibility, g.CompatibilityDigest, g.Conversation = cloneCompatibility(&compatibility), calculated, compatibility.Conversation
		g.WorkspaceCompatibilityDigest = workspaceDigest
		return nil
	})
}

func provenCompatibility(g TaskGrant, selection Selection, expected string) bool {
	if g.Compatibility == nil || g.CompatibilityDigest != expected || g.Compatibility.Conversation != selection.Conversation ||
		g.Compatibility.RuntimeID != g.RuntimeID || !g.Compatibility.RuntimeRef.Equal(g.RuntimeRef) {
		return false
	}
	calculated, err := g.Compatibility.Digest()
	workspaceDigest, workspaceErr := g.Compatibility.WorkspaceDigest()
	return err == nil && calculated == expected && workspaceErr == nil && workspaceDigest == g.WorkspaceCompatibilityDigest &&
		compatibilityScopeMatches(g, *g.Compatibility, workspaceDigest) &&
		(!selection.WorkspaceReuseEligible || g.Compatibility.Authority.ValidForWorkspaceReuse()) &&
		(!selection.ReuseEligible || selection.WorkspaceReuseEligible && g.Compatibility.ProviderOptionsResolved && g.Compatibility.Authority.ValidForReuse())
}

func compatibilityScopeMatches(g TaskGrant, compatibility ConversationCompatibilityV1, expected string) bool {
	compatibility.Repositories, compatibility.ResourceScope = g.Repositories, g.ResourceScope
	calculated, err := compatibility.WorkspaceDigest()
	return err == nil && calculated == expected
}

type SelectionMode string

const (
	SelectionResume         SelectionMode = "resume"
	SelectionFreshSession   SelectionMode = "fresh_session"
	SelectionFreshWorkspace SelectionMode = "fresh_workspace"
)

type CheckpointSource struct {
	TaskID    string `json:"taskID"`
	AttemptID string `json:"attemptID"`
}

func (s CheckpointSource) valid() bool { return canonicalUUID(s.TaskID) && canonicalUUID(s.AttemptID) }

// Selection records the backend-selected sources separately from the latest
// local writer. A named rerun may select A after a later turn B used the same files.
type Selection struct {
	Conversation           ConversationKey  `json:"conversation"`
	ReuseEligible          bool             `json:"reuseEligible"`
	WorkspaceReuseEligible bool             `json:"workspaceReuseEligible"`
	Mode                   SelectionMode    `json:"mode"`
	StorageID              string           `json:"storageID,omitempty"`
	SessionSource          CheckpointSource `json:"sessionSource,omitempty"`
	WorkspaceSource        CheckpointSource `json:"workspaceSource,omitempty"`
	LatestWriter           CheckpointSource `json:"latestWriter,omitempty"`
	SessionID              string           `json:"sessionID,omitempty"`
	WorkDir                string           `json:"workDir,omitempty"`
	EvidenceDigest         string           `json:"evidenceDigest,omitempty"`
	Reason                 string           `json:"reason,omitempty"`
}

// CompletionWitness is recorded only after an authenticated terminal is
// accepted by the unchanged backend. A transport timeout is not acceptance.
type CompletionWitness struct {
	TaskID          string    `json:"taskID"`
	AttemptID       string    `json:"attemptID"`
	WorkerSessionID string    `json:"workerSessionID"`
	StorageID       string    `json:"storageID"`
	TurnSequence    uint64    `json:"turnSequence"`
	RequestDigest   string    `json:"requestDigest"`
	ResultDigest    string    `json:"resultDigest"`
	Status          string    `json:"status"`
	SessionID       string    `json:"sessionID,omitempty"`
	WorkDir         string    `json:"workDir,omitempty"`
	AcknowledgedAt  time.Time `json:"acknowledgedAt"`
}

type SessionCheckpoint struct {
	SealingSessionID             string           `json:"sealingSessionID,omitempty"`
	SealedAt                     time.Time        `json:"sealedAt,omitempty"`
	Source                       CheckpointSource `json:"source"`
	SessionSource                CheckpointSource `json:"sessionSource,omitempty"`
	WorkspaceSource              CheckpointSource `json:"workspaceSource,omitempty"`
	WorkerSessionID              string           `json:"workerSessionID"`
	TurnSequence                 uint64           `json:"turnSequence"`
	CompatibilityDigest          string           `json:"compatibilityDigest"`
	WorkspaceCompatibilityDigest string           `json:"workspaceCompatibilityDigest"`
	SessionID                    string           `json:"sessionID,omitempty"`
	WorkDir                      string           `json:"workDir"`
	PublishedAt                  time.Time        `json:"publishedAt"`
}

// TurnInputDigest is shared with wire without importing wire into this package.
// The top-level inputDigest field is excluded; all remaining JSON is canonical.
func TurnInputDigest(raw json.RawMessage) (string, error) {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return "", errors.New("turn assignment must be an object")
	}
	delete(object, "inputDigest")
	encoded, err := json.Marshal(object)
	if err != nil {
		return "", err
	}
	return digest(canonicalJSON(encoded)), nil
}

func canonicalJSON(raw []byte) []byte {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return nil
	}
	encoded, _ := json.Marshal(value)
	return encoded
}
