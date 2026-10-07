package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type turnInputs struct {
	claim     daemonapi.Claim
	provider  string
	skills    []daemonapi.SkillBundle
	mcp       json.RawMessage
	authority workspace.AuthorityEvidence
}

// Inputs are joined before native configuration or task files are inspected.
func (c *Controller) acquireTurnInputs(ctx context.Context, g workspace.TaskGrant) (turnInputs, error) {
	var input turnInputs
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		return input, err
	}
	metadata, err := c.sessionMetadata(g)
	if err != nil {
		return input, err
	}
	input.claim, input.provider = claim, metadata.Runtime.Provider
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 2)
	go func() {
		var err error
		input.skills, err = c.API.ExecutionSkills(ctx, claim)
		if err != nil {
			cancel()
		}
		finished <- err
	}()
	go func() {
		var err error
		input.authority, err = c.API.ObserveAuthority(ctx, claim)
		if err != nil {
			cancel()
		}
		finished <- err
	}()
	input.mcp, err = c.API.PrepareMCP(ctx, claim)
	if err != nil {
		cancel()
	}
	err = errors.Join(err, <-finished, <-finished)
	return input, err
}

func (c *Controller) turnExecution(g workspace.TaskGrant, b wire.Bootstrap, input turnInputs) (daemonapi.Execution, error) {
	executable, ok := c.Descriptor.Providers[input.provider]
	if !ok {
		return daemonapi.Execution{}, errors.New("task provider is not installed")
	}
	return daemonapi.UnresolvedExecutionInput(input.claim, b, executable, daemonapi.ExecutionSettings{Provider: input.provider,
		TaskRoot: b.TaskRoot, SessionID: g.ResumeSession, SourceProven: g.Selection != nil && g.Selection.Mode == workspace.SelectionResume,
		Skills: input.skills, MCPConfig: input.mcp})
}

func (c *Controller) activateTurn(ctx context.Context, g workspace.TaskGrant) error {
	if g.WorkerSessionID == "" && !g.RuntimeRef.Equal(c.RuntimeRef) {
		return c.recordFailure(g, errors.New("queued task image does not support the current worker session protocol"))
	}
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		return err
	}
	key := daemonapi.ConversationIdentity(g.OwnerID, claim)
	gate := c.attemptLock("conversation:" + key.Digest())
	if !gate.TryLock() {
		return nil
	}
	defer gate.Unlock()
	grants, err := c.Store.List()
	if err != nil {
		return err
	}
	for _, other := range grants {
		if other.AttemptID == g.AttemptID || other.State != "waiting_storage" || !other.SessionProtocol || other.ExecutionRevoked ||
			other.CreatedAt.After(g.CreatedAt) || other.CreatedAt.Equal(g.CreatedAt) && other.AttemptID > g.AttemptID {
			continue
		}
		otherClaim, err := daemonapi.ParseClaim(other.Envelope)
		if err == nil && daemonapi.ConversationIdentity(other.OwnerID, otherClaim) == key {
			c.enqueueAttempt(other.AttemptID)
			return nil
		}
	}
	if g.Selection == nil {
		if err := c.selectTurn(ctx, g); err != nil {
			if errors.Is(err, workspace.ErrStorageBusy) || transientObservation(err) {
				return nil
			}
			return c.recordFailure(g, err)
		}
		var err error
		g, err = c.Store.Get(g.AttemptID)
		if err != nil {
			return err
		}
	}
	residentLimit := c.MaxResidentPods
	if residentLimit == 0 {
		residentLimit = c.Capacity
	}
	reserved, session, err := c.Store.ReserveTurn(g.AttemptID, *g.Selection, g.CompatibilityDigest, c.ConversationIdleTimeout, residentLimit)
	if errors.Is(err, workspace.ErrStorageBusy) || errors.Is(err, workspace.ErrResidentCapacity) {
		// The periodic session sweep observes any committed eviction. An
		// unchanged denial must not immediately requeue this waiting claim.
		return nil
	}
	if err != nil {
		return c.recordFailure(g, err)
	}
	slog.Info("conversation turn reserved", append(turnAttributes(reserved), "mode", reserved.Selection.Mode, "reason", reserved.Selection.Reason)...)
	c.enqueueSession(session.ID)
	if err := c.provision(ctx, reserved); err != nil && !errors.Is(err, errPreparePending) && !transientObservation(err) {
		return c.recordFailure(reserved, err)
	}
	return nil
}

func (c *Controller) selectTurn(ctx context.Context, g workspace.TaskGrant) error {
	input, err := c.acquireTurnInputs(ctx, g)
	if err != nil {
		return err
	}
	key := daemonapi.ConversationIdentity(g.OwnerID, input.claim)
	root, err := workspace.TaskRoot(wire.WorkspaceRoot, g.WorkspaceID, g.TaskID, input.claim.WorkspaceSlug, input.claim.IssueIdentifier)
	if err != nil {
		return err
	}
	// This provisional value is only used to interpret options. It grants no
	// session identity and does not read or create the proposed native root.
	b := wire.Bootstrap{OwnerID: g.OwnerID, TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID,
		AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, TaskRoot: root, Configuration: c.Configuration, Environment: c.Environment}
	execution, err := c.turnExecution(g, b, input)
	if err != nil {
		return err
	}
	compatibility, eligible, err := daemonapi.ExecutionCompatibility(input.claim, key, b, execution, input.skills, input.authority, g.Repositories, g.ResourceScope)
	if err != nil {
		return err
	}
	var selection workspace.Selection
	if g.BackendSelection != nil {
		selection = *g.BackendSelection
	} else {
		selection, err = c.backendSelection(ctx, input.claim, compatibility, eligible)
		if err != nil {
			return err
		}
		if err := c.Store.RecordBackendSelection(g.AttemptID, selection); err != nil {
			return err
		}
	}
	selection, err = c.selectWorkspace(g, input.claim, compatibility, selection)
	if err != nil {
		return err
	}
	projectFiles := map[string][]byte{}
	sourcesKnown := true
	if selection.Mode != workspace.SelectionFreshWorkspace {
		storage, err := c.Store.GetStorage(selection.StorageID)
		if err != nil {
			return err
		}
		if selection.Mode == workspace.SelectionResume && workspace.ValidateSession(*storage.Prepared, selection.SessionID) != nil {
			selection = freshNativeSelection(selection, "selected_session_unavailable")
		}
		root = storage.TaskRoot
		projectFiles, sourcesKnown = nativeSelectorFiles(root+"/workdir", []string{".codex/config.toml", ".pi/settings.json", "config.toml"})
		// The provider sees the anchored root as its only NFS mount.
		// Ancestor configuration inside that root remains a native layer.
		ancestor, known := nativeSelectorFiles(root, []string{".codex/config.toml"})
		sourcesKnown = sourcesKnown && known && len(ancestor) == 0
		if _, legacy := projectFiles["config.toml"]; legacy && input.provider == "codex" {
			sourcesKnown = false
		}
		delete(projectFiles, "config.toml")
	}
	homeFiles, homeKnown := nativeSelectorFiles(wire.Home, []string{".codex/config.toml", ".codex/auth.json", ".pi/agent/settings.json", ".pi/agent/models.json", ".pi/agent/auth.json", ".pi/agent/trust.json",
		".aws/config", ".aws/credentials", ".config/gcloud/application_default_credentials.json"})
	for _, name := range []string{".aws/config", ".aws/credentials", ".config/gcloud/application_default_credentials.json"} {
		if _, found := homeFiles[name]; found && input.provider == "pi" {
			sourcesKnown = false
		}
		delete(homeFiles, name)
	}
	if _, found := homeFiles[".pi/agent/trust.json"]; found && input.provider == "pi" {
		sourcesKnown = false
	}
	environment, err := runtimeimage.Vars(c.Descriptor, c.Environment, runtimeimage.Locations{Home: wire.Home, TmpDir: "/tmp", Workspace: root})
	if err != nil {
		return err
	}
	values := map[string]string{}
	for _, entry := range environment {
		if name, value, ok := strings.Cut(entry, "="); ok {
			values[name] = value
		}
	}
	for name, value := range compatibility.CustomEnv {
		values[name] = value
	}
	defaults, err := daemonapi.ResolveProviderDefaults(ctx, c.Descriptor, daemonapi.ProviderDefaultsInput{Provider: input.provider,
		Options: execution.Run.Options, Environment: values, HomeFiles: homeFiles, ProjectFiles: projectFiles, SourcesKnown: sourcesKnown && homeKnown})
	if err != nil {
		return err
	}
	execution, err = daemonapi.FinalizeExecution(execution, defaults)
	if err != nil {
		return err
	}
	if defaults.Resolved {
		compatibility.ProviderOptionsResolved = true
		compatibility.Model, compatibility.ThinkingLevel = execution.Run.Options.Model, execution.Run.Options.ThinkingLevel
		compatibility.ServiceTier = execution.Run.Options.ServiceTier
	} else {
		eligible = false
		selection = freshNativeSelection(selection, defaults.Reason)
	}
	digest, err := compatibility.Digest()
	if err != nil {
		return err
	}
	if selection.Mode == workspace.SelectionResume {
		source, err := c.Store.Get(selection.SessionSource.AttemptID)
		if err != nil {
			return err
		}
		if !eligible || digest != source.CompatibilityDigest {
			selection = freshNativeSelection(selection, "execution_configuration_changed")
		}
	}
	selection.ReuseEligible = eligible && defaults.Resolved
	if err := c.Store.RecordCompatibility(g.AttemptID, compatibility); err != nil {
		return err
	}
	if err := c.Store.RecordSelection(g.AttemptID, selection, digest); err != nil {
		return err
	}
	slog.Info("conversation continuity selected", "task", g.TaskID, "attempt", g.AttemptID,
		"conversation", key.Digest(), "mode", selection.Mode, "reason", selection.Reason,
		"workspaceReuseEligible", selection.WorkspaceReuseEligible, "reuseEligible", selection.ReuseEligible)
	return nil
}

func freshSelection(key workspace.ConversationKey, eligible bool, reason string) workspace.Selection {
	return workspace.Selection{Conversation: key, ReuseEligible: eligible, WorkspaceReuseEligible: eligible, Mode: workspace.SelectionFreshWorkspace, Reason: reason}
}

func freshNativeSelection(selection workspace.Selection, reason string) workspace.Selection {
	if selection.Mode != workspace.SelectionFreshWorkspace {
		selection.Mode = workspace.SelectionFreshSession
	}
	selection.SessionID, selection.SessionSource = "", workspace.CheckpointSource{}
	selection.ReuseEligible, selection.Reason = false, reason
	return selection
}

func (c *Controller) sameStaticCompatibility(current workspace.ConversationCompatibilityV1, prior workspace.TaskGrant) bool {
	if prior.Compatibility == nil || !prior.Compatibility.ProviderOptionsResolved || !prior.Compatibility.Authority.ValidForReuse() {
		return false
	}
	claim, err := daemonapi.ParseClaim(prior.Envelope)
	if err != nil {
		return false
	}
	b, err := wire.DecodeBootstrap(prior.Bootstrap)
	if err != nil {
		return false
	}
	original, err := daemonapi.UnresolvedExecutionInput(claim, b, b.RuntimeRef.Providers[prior.Compatibility.Provider], daemonapi.ExecutionSettings{
		Provider: prior.Compatibility.Provider, TaskRoot: prior.TaskRoot, Skills: []daemonapi.SkillBundle{}})
	if err != nil {
		return false
	}
	previous := *prior.Compatibility
	previous.ProviderOptionsResolved = false
	previous.Model, previous.ThinkingLevel, previous.ServiceTier = original.Run.Options.Model, original.Run.Options.ThinkingLevel, original.Run.Options.ServiceTier
	left, leftErr := current.Digest()
	right, rightErr := previous.Digest()
	return leftErr == nil && rightErr == nil && left == right
}

func (c *Controller) backendSelection(ctx context.Context, claim daemonapi.Claim, compatibility workspace.ConversationCompatibilityV1, eligible bool) (workspace.Selection, error) {
	key := compatibility.Conversation
	fresh := freshSelection(key, eligible, "selected_workspace_unavailable")
	if !eligible || claim.PriorWorkDir == "" {
		if !eligible {
			fresh.Reason = "continuity_authority_unknown"
		}
		return fresh, nil
	}
	var evidence continuityEvidence
	var err error
	evidence.Grants, err = c.Store.List()
	if err != nil {
		return fresh, err
	}
	evidence.Storages, err = c.Store.ListStorages()
	if err != nil {
		return fresh, err
	}
	evidence.Terminals = map[string]workspace.Terminal{}
	for _, grant := range evidence.Grants {
		if grant.Conversation != key {
			continue
		}
		terminal, err := c.Store.Terminal(grant.AttemptID)
		if err == nil {
			evidence.Terminals[grant.AttemptID] = terminal
		} else if !errors.Is(err, workspace.ErrUnauthorized) {
			return fresh, err
		}
	}
	if key.Kind == workspace.ConversationIssue {
		evidence.History, err = c.API.IssueTaskHistory(ctx, claim)
	} else {
		evidence.History, err = c.API.AgentTaskHistory(ctx, claim)
	}
	evidence.HistoryKnown = err == nil
	if ctx.Err() != nil {
		return fresh, ctx.Err()
	}
	if evidence.HistoryKnown {
		if err := c.reconcileTurnCompletion(ctx, claim, evidence.History); err != nil {
			return fresh, err
		}
		// A completed predecessor may have committed upstream before its
		// callback response arrived. Select from the reconciled durable witness.
		evidence.Grants, err = c.Store.List()
		if err != nil {
			return fresh, err
		}
		evidence.Storages, err = c.Store.ListStorages()
		if err != nil {
			return fresh, err
		}
		for _, grant := range evidence.Grants {
			if grant.Conversation != key {
				continue
			}
			terminal, err := c.Store.Terminal(grant.AttemptID)
			if err == nil {
				evidence.Terminals[grant.AttemptID] = terminal
			} else if !errors.Is(err, workspace.ErrUnauthorized) {
				return fresh, err
			}
		}
	}
	evidence.WarmSessions = map[string]workspace.WorkerSession{}
	for _, storage := range evidence.Storages {
		if storage.WriterSessionID == "" || storage.ActiveAttempt != "" {
			continue
		}
		owner, err := c.Store.WarmSession(storage.ID)
		if err == nil {
			evidence.WarmSessions[storage.ID] = owner
		} else if !errors.Is(err, workspace.ErrStorageBusy) && !errors.Is(err, workspace.ErrConflict) {
			return fresh, err
		}
	}
	seen := map[string]bool{}
	if claim.PriorSessionID != "" {
		for _, grant := range evidence.Grants {
			if grant.Conversation != key || grant.PendingResume == nil || !grant.PendingResume.ResumeRejectedTransient ||
				grant.ResumeSession != claim.PriorSessionID || grant.CompletionWitness != nil {
				continue
			}
			for _, storage := range evidence.Storages {
				if storage.ID == grant.StorageID && storage.LatestWriter == grantSource(grant) &&
					(claim.PriorWorkDir == "" || claim.PriorWorkDir == storage.TaskRoot+"/workdir") {
					c.enqueueDelivery(grant.AttemptID)
					return fresh, workspace.ErrStorageBusy
				}
			}
		}
	}
	for _, grant := range evidence.Grants {
		if grant.Conversation != key || seen[grant.CompatibilityDigest] || !c.sameStaticCompatibility(compatibility, grant) {
			continue
		}
		seen[grant.CompatibilityDigest] = true
		selected := selectConversationSource(claim, key, grant.CompatibilityDigest, eligible, evidence)
		if selected.Mode == workspace.SelectionFreshWorkspace {
			if fresh.Mode == workspace.SelectionFreshWorkspace {
				fresh = selected
			}
			continue
		}
		if fresh.Mode != workspace.SelectionFreshWorkspace {
			return freshSelection(key, eligible, "selected_generation_ambiguous"), nil
		}
		fresh = selected
	}
	return fresh, nil
}

// Only bounded regular files under an observed root enter the metadata probe.
// An absent file in a known directory is different from an unreadable source.
func nativeSelectorFiles(directory string, names []string) (map[string][]byte, bool) {
	files := map[string][]byte{}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return files, false
	}
	anchor, err := os.OpenRoot("/")
	if err != nil {
		return files, false
	}
	defer anchor.Close()
	root, err := workspace.OpenNativeDirectory(anchor, strings.TrimPrefix(directory, "/"))
	if err != nil {
		return files, false
	}
	defer root.Close()
	for _, name := range names {
		if !filepath.IsLocal(name) || filepath.Clean(name) != name {
			return files, false
		}
		prefix := ""
		parts := strings.Split(name, string(filepath.Separator))
		missing := false
		for _, component := range parts[:len(parts)-1] {
			prefix = filepath.Join(prefix, component)
			info, err := root.Lstat(prefix)
			if errors.Is(err, os.ErrNotExist) {
				missing = true
				break
			}
			if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return files, false
			}
		}
		if missing {
			continue
		}
		info, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() || info.Size() > wire.MaxRequestBytes {
			return files, false
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return files, false
		}
		file, err := root.Open(name)
		if err != nil {
			return files, false
		}
		opened, err := file.Stat()
		if err != nil || !os.SameFile(info, opened) {
			_ = file.Close()
			return files, false
		}
		raw, readErr := io.ReadAll(io.LimitReader(file, wire.MaxRequestBytes+1))
		if errors.Join(readErr, file.Close()) != nil || len(raw) > wire.MaxRequestBytes {
			return files, false
		}
		files[name] = raw
	}
	return files, true
}
