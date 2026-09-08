package migration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

// Schema 1 is consumed only by this explicit migration boundary. Retain these
// DTOs while schema-1 installations are supported; normal writers never use them.
type legacyCore struct {
	ContractVersion int               `json:"contractVersion"`
	BuildID         string            `json:"buildID"`
	Platform        string            `json:"platform"`
	OfficialVersion string            `json:"officialVersion"`
	OfficialSHA256  string            `json:"officialSHA256"`
	Files           map[string]string `json:"files"`
}
type legacyFingerprint struct {
	Entrypoint    string `json:"entrypoint"`
	SHA256        string `json:"sha256"`
	VersionOutput string `json:"versionOutput"`
}
type legacyRef struct {
	SchemaVersion    int                          `json:"schemaVersion"`
	EnvironmentID    string                       `json:"environmentID"`
	ContentDigest    string                       `json:"contentDigest"`
	ManifestDigest   string                       `json:"manifestDigest"`
	Core             legacyCore                   `json:"core"`
	CoreImage        string                       `json:"coreImage"`
	EnvironmentImage string                       `json:"environmentImage"`
	Platform         string                       `json:"platform"`
	Providers        map[string]legacyFingerprint `json:"providers"`
}

func (r legacyRef) validate() error {
	if r.SchemaVersion != 1 || !core.ValidSHA(r.EnvironmentID) || !core.ValidSHA(r.ContentDigest) || !core.ValidSHA(r.ManifestDigest) || !core.PinnedImage(r.CoreImage) || !core.PinnedImage(r.EnvironmentImage) {
		return errors.New("invalid legacy environment reference")
	}
	c := r.Core
	if c.ContractVersion != 1 || c.BuildID == "" || !core.SupportedPlatform(c.Platform) || c.Platform != r.Platform || c.OfficialVersion == "" || !core.ValidSHA(c.OfficialSHA256) || len(c.Files) != 2 || c.Files["multica"] != c.OfficialSHA256 || !core.ValidSHA(c.Files["runtime"]) {
		return errors.New("invalid legacy controller artifact contract")
	}
	if len(r.Providers) == 0 {
		return errors.New("legacy environment has no provider fingerprints")
	}
	for id, fingerprint := range r.Providers {
		if (id != "pi" && id != "codex" && id != "copilot" && id != "antigravity") || fingerprint.Entrypoint == "" || !core.ValidSHA(fingerprint.SHA256) {
			return errors.New("invalid legacy provider fingerprint")
		}
	}
	return nil
}
func (r legacyRef) equal(other legacyRef) bool {
	a, _ := json.Marshal(r)
	b, _ := json.Marshal(other)
	return bytes.Equal(a, b)
}

type legacyClaim struct {
	ID             string    `json:"id"`
	WorkspaceID    string    `json:"workspaceID"`
	AgentID        string    `json:"agentID"`
	IssueID        string    `json:"issueID,omitempty"`
	ChatID         string    `json:"chatID,omitempty"`
	ProjectID      string    `json:"projectID,omitempty"`
	Grant          string    `json:"scope"`
	TokenHash      string    `json:"credentialFingerprint"`
	PriorWorkDir   string    `json:"priorWorkDir,omitempty"`
	PriorSession   string    `json:"priorSession,omitempty"`
	BoundRoot      string    `json:"boundRoot,omitempty"`
	WorkerSubPath  string    `json:"workerSubPath,omitempty"`
	RepositoryURLs []string  `json:"repositoryURLs"`
	TaskEnvKeys    []string  `json:"taskEnvKeys"`
	Environment    legacyRef `json:"environment"`
	Denied         bool      `json:"denied,omitempty"`
	ObservedAt     time.Time `json:"observedAt"`
}
type legacyBinding struct {
	Root          string               `json:"root"`
	Identity      string               `json:"identity"`
	Grant         string               `json:"scope"`
	WorkerSubPath string               `json:"workerSubPath"`
	Sessions      map[string]legacyRef `json:"sessions"`
}
type legacyRegistry struct {
	SchemaVersion int                      `json:"schemaVersion"`
	OwnerID       string                   `json:"ownerID"`
	WorkspaceRoot string                   `json:"workspaceRoot"`
	Claims        map[string]legacyClaim   `json:"claims"`
	Bindings      map[string]legacyBinding `json:"bindings"`
	Retired       map[string]string        `json:"retired"`
}

func convert(options workspace.Options, source legacyRegistry) (workspace.Registry, error) {
	target := workspace.Registry{SchemaVersion: workspace.SchemaVersion, OwnerID: source.OwnerID, WorkspaceRoot: source.WorkspaceRoot, Claims: map[string]workspace.Claim{}, Bindings: map[string]workspace.Binding{}, Retired: source.Retired}
	if source.SchemaVersion != 1 || source.Claims == nil || source.Bindings == nil {
		return target, errors.New("unsupported legacy workspace registry")
	}
	for key, claim := range source.Claims {
		if err := claim.Environment.validate(); err != nil {
			return target, fmt.Errorf("legacy claim reference: %w", err)
		}
		target.Claims[key] = workspace.Claim{ID: claim.ID, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID, IssueID: claim.IssueID, ChatID: claim.ChatID, ProjectID: claim.ProjectID, Grant: claim.Grant, TokenHash: claim.TokenHash, PriorWorkDir: claim.PriorWorkDir, PriorSession: claim.PriorSession, BoundRoot: claim.BoundRoot, WorkerSubPath: claim.WorkerSubPath, RepositoryURLs: claim.RepositoryURLs, TaskEnvKeys: claim.TaskEnvKeys, ExecutionState: "unobserved", RuntimeRef: nil, Denied: claim.Denied, ObservedAt: claim.ObservedAt}
	}
	type sessionAuthority struct {
		storage   string
		reference legacyRef
	}
	sessions := map[string]sessionAuthority{}
	for key, binding := range source.Bindings {
		if binding.Sessions == nil {
			return target, errors.New("legacy binding has no session authority")
		}
		converted := workspace.Binding{Root: binding.Root, Identity: binding.Identity, Grant: binding.Grant, WorkerSubPath: binding.WorkerSubPath, Sessions: map[string]workspace.SessionRecord{}}
		for session, reference := range binding.Sessions {
			if err := reference.validate(); err != nil {
				return target, fmt.Errorf("legacy session reference: %w", err)
			}
			if prior, known := sessions[session]; known && (prior.storage != binding.WorkerSubPath || !prior.reference.equal(reference)) {
				return target, errors.New("legacy session has conflicting storage or environment authority")
			}
			sessions[session] = sessionAuthority{binding.WorkerSubPath, reference}
			converted.Sessions[session] = workspace.SessionRecord{State: "archived", RuntimeRef: nil}
		}
		target.Bindings[key] = converted
	}
	return target, workspace.ValidateRegistry(options, target)
}

// A legacy registry is validated before discarding its execution references.
// Reject duplicate object keys as well as unknown fields and trailing input.
func strictDecode(raw []byte, value any) error {
	tokens := json.NewDecoder(bytes.NewReader(raw))
	var scan func() error
	scan = func() error {
		token, err := tokens.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'):
			seen := map[string]bool{}
			for tokens.More() {
				key, err := tokens.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate or invalid authority object key")
				}
				seen[name] = true
				if err := scan(); err != nil {
					return err
				}
			}
			_, err = tokens.Token()
			return err
		case json.Delim('['):
			for tokens.More() {
				if err := scan(); err != nil {
					return err
				}
			}
			_, err = tokens.Token()
			return err
		}
		return nil
	}
	if err := scan(); err != nil {
		return err
	}
	if _, err := tokens.Token(); err != io.EOF {
		return errors.New("trailing authority input")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}
