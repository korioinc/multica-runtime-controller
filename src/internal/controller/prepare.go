package controller

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"io/fs"
	"log/slog"
	"os"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

// startPreparation is called with the short coordination lock. The journal
// reservation outlives the goroutine and prevents restart from duplicating it.
func (c *Controller) startPreparation(ctx context.Context, g workspace.TaskGrant, b wire.Bootstrap) error {
	c.mu.Lock()
	current, err := c.Store.Get(g.AttemptID)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	// Preparation can finish while provision is renewing its lease. Observe
	// durable completion together with this process's preparation ownership.
	if current.State != "intent" || current.Stop != nil || current.Prepared != nil {
		c.mu.Unlock()
		return errPreparePending
	}
	g = current
	if g.PreparationStarted {
		active := c.preparing[g.AttemptID]
		c.mu.Unlock()
		if active {
			return errPreparePending
		}
		return workspace.ErrPreparationWritersUnproven
	}
	active := 0
	for _, running := range c.preparing {
		if running {
			active++
		}
	}
	if active >= c.Policy.Storage.MaxConcurrentPreparations {
		c.mu.Unlock()
		return errPreparePending
	}
	if c.preparing == nil {
		c.preparing = map[string]bool{}
	}
	c.preparing[g.AttemptID] = true
	c.mu.Unlock()
	launched := false
	defer func() {
		if !launched {
			c.mu.Lock()
			delete(c.preparing, g.AttemptID)
			c.mu.Unlock()
		}
	}()
	process, err := c.Kube.ControllerProcess(ctx, c.Owner)
	if err != nil {
		if errors.Is(err, kubernetes.ErrControllerObservationPending) {
			return errPreparePending
		}
		return err
	}
	if err := c.Store.BeginPreparation(g.AttemptID, process); err != nil {
		return err
	}
	parent := c.prepareContext
	if parent == nil {
		parent = context.WithoutCancel(ctx)
	}
	job, cancel := context.WithDeadline(parent, g.CreatedAt.Add(time.Duration(c.Policy.Worker.PreparationTimeoutSeconds)*time.Second))
	c.mu.Lock()
	if c.prepareCancels == nil {
		c.prepareCancels = map[string]context.CancelFunc{}
	}
	c.prepareCancels[g.AttemptID] = cancel
	c.mu.Unlock()
	// A stop can commit between reserving preparation and publishing cancel.
	if current, err := c.Store.Get(g.AttemptID); err != nil || current.Stop != nil {
		cancel()
	}
	launched = true
	c.prepareWG.Add(1)
	go func() {
		defer c.prepareWG.Done()
		defer cancel()
		// Completion and slot release both wake pending attempts. This runs
		// after the journal transition and after releasing the coordination lock.
		defer func() {
			c.mu.Lock()
			// Retain ownership even after this process's helper has finished.
			c.preparing[g.AttemptID] = false
			delete(c.prepareCancels, g.AttemptID)
			c.mu.Unlock()
			c.enqueueAttempt(g.AttemptID)
			c.wakePending()
		}()
		started := time.Now()
		slog.Info("task preparation started", "task", g.TaskID, "attempt", g.AttemptID)
		err := c.prepare(job, g, b)
		if err == nil {
			slog.Info("task preparation completed", "task", g.TaskID, "attempt", g.AttemptID, "elapsed", time.Since(started))
		}
		if err != nil {
			reason := "preparation_failed"
			var diagnostic *diagnostics.Error
			if errors.As(err, &diagnostic) {
				reason = diagnostic.Reason
			}
			if errors.Is(err, workspace.ErrPreparationWritersUnproven) {
				reason = "preparation_writers_unproven"
			}
			slog.Warn("task preparation failed", "task", g.TaskID, "attempt", g.AttemptID, "reason", reason)
			current, readErr := c.Store.Get(g.AttemptID)
			if readErr != nil {
				return
			}
			if !current.PreparationStopped && !errors.Is(err, workspace.ErrPreparationWritersUnproven) {
				if stopErr := c.Store.CompletePreparation(g.AttemptID, nil, nil); stopErr != nil {
					return
				}
			}
			if current.Stop == nil {
				_ = c.recordFailure(current, err)
			}
		}
	}()
	return errPreparePending
}

func (c *Controller) prepare(ctx context.Context, g workspace.TaskGrant, b wire.Bootstrap) error {
	if g.PreparationStarted {
		return errors.New("previous preparation termination is unresolved")
	}
	if err := c.workspaceBudget(); err != nil {
		return err
	}
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		return err
	}
	var metadata daemonapi.Bootstrap
	if err := json.Unmarshal(g.Metadata, &metadata); err != nil {
		return err
	}
	provider := metadata.Runtime.Provider
	executable, ok := c.Descriptor.Providers[provider]
	if !ok {
		return errors.New("task provider is not installed")
	}
	prior, err := c.Store.PreviousPrepared(g.AttemptID)
	if err != nil {
		return err
	}
	if g.ResumeSession != "" {
		if prior == nil {
			return errors.New("session has no task preparation binding")
		}
		if err := workspace.ValidateSession(*prior, g.ResumeSession); err != nil {
			return err
		}
	}
	// Independent remote inputs share the preparation deadline. Join both paths
	// before any filesystem work, including when either acquisition fails.
	inputsCtx, cancelInputs := context.WithCancel(ctx)
	defer cancelInputs()
	var skills []daemonapi.SkillBundle
	skillsDone := make(chan error, 1)
	go func() {
		finish := diagnostics.StartPhase("task_skills", diagnostics.TaskAttributes(g.TaskID, g.AttemptID)...)
		var err error
		skills, err = c.API.ExecutionSkills(inputsCtx, claim)
		finish(err)
		if err != nil {
			cancelInputs()
		}
		skillsDone <- err
	}()
	finishMCP := diagnostics.StartPhase("task_mcp", diagnostics.TaskAttributes(g.TaskID, g.AttemptID)...)
	mcp, mcpErr := c.API.PrepareMCP(inputsCtx, claim)
	finishMCP(mcpErr)
	if mcpErr != nil {
		cancelInputs()
	}
	if err := errors.Join(<-skillsDone, mcpErr); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	execution, err := daemonapi.UnresolvedExecutionInput(claim, b, executable, daemonapi.ExecutionSettings{Provider: provider, TaskRoot: g.TaskRoot, SessionID: g.ResumeSession,
		SourceProven: g.Selection != nil && g.Selection.Mode == workspace.SelectionResume, Skills: skills, MCPConfig: mcp})
	if err != nil {
		return err
	}
	defaults := daemonapi.ProviderDefaults{}
	if g.SessionProtocol {
		if g.Compatibility == nil {
			return errors.New("turn has no frozen execution compatibility")
		}
		if g.Compatibility.ProviderOptionsResolved {
			defaults = daemonapi.ProviderDefaults{Resolved: true, Model: g.Compatibility.Model, ThinkingLevel: g.Compatibility.ThinkingLevel,
				ServiceTier: &g.Compatibility.ServiceTier}
		}
	}
	execution, err = daemonapi.FinalizeExecution(execution, defaults)
	if err != nil {
		return err
	}
	if g.SessionProtocol {
		authority := g.Compatibility.Authority
		if g.Selection != nil && g.Selection.WorkspaceReuseEligible {
			var err error
			authority, err = c.API.ObserveAuthority(ctx, claim)
			if err != nil {
				return err
			}
		}
		current, _, err := daemonapi.ExecutionCompatibility(claim, g.Conversation, b, execution, skills, authority, g.Repositories, g.ResourceScope)
		if err != nil {
			return err
		}
		current.ProviderOptionsResolved = g.Compatibility.ProviderOptionsResolved
		digest, err := current.Digest()
		if err != nil || digest != g.CompatibilityDigest {
			return errors.New("turn authority or execution inputs changed before preparation")
		}
	}
	environment, err := runtimeimage.Vars(c.Descriptor, c.Environment, runtimeimage.Locations{Home: wire.Home, TmpDir: "/tmp", Workspace: wire.WorkspaceRoot})
	if err != nil {
		return err
	}
	skillIDs := make([]string, 0, len(skills))
	for _, skill := range skills {
		skillIDs = append(skillIDs, skill.ID+":"+skill.Hash)
	}
	finishEnvironment := diagnostics.StartPhase("task_environment", diagnostics.TaskAttributes(g.TaskID, g.AttemptID)...)
	var codexHelper runtimeimage.Executable
	if provider == "codex" && g.Reuse && !g.SessionProtocol {
		codexHelper, err = runtimeimage.CodexHelperExecutable(c.Descriptor)
		if err != nil {
			finishEnvironment(err)
			return err
		}
	}
	var sessionID string
	var turnSequence uint64
	liveReuse := false
	if g.SessionProtocol {
		session, err := c.Store.GetSession(g.WorkerSessionID)
		if err != nil {
			return err
		}
		if session.ProtocolVersion != workspace.SessionProtocolVersion || session.ActiveAttempt != g.AttemptID || session.TurnSequence != g.TurnSequence {
			return errors.New("preparation requires the current v2 session reservation")
		}
		sessionID, turnSequence = session.ID, g.TurnSequence
		liveReuse = prior != nil && prior.NativeMetadata != nil && prior.NativeMetadata.WorkerSessionID == session.ID &&
			session.PodUID != "" && session.LastCompleteAttempt == prior.AttemptID
	}
	var checkpoint func(workspace.Prepared) error
	if !g.SessionProtocol {
		checkpoint = func(prepared workspace.Prepared) error { return c.Store.CheckpointPreparation(g.AttemptID, prepared) }
	}
	prepared, err := workspace.PrepareTask(ctx, workspace.Preparation{
		Conversation: g.Conversation, WorkspaceAnchorTaskID: g.WorkspaceAnchorTaskID,
		WorkerSessionID: sessionID, TurnSequence: turnSequence, ScratchRoot: c.Store.PreparationDirectory(), LiveReuse: liveReuse,
		OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID, Generation: g.Generation,
		PVCUID: g.PVCUID, WorkspacesRoot: wire.WorkspaceRoot, TaskRoot: g.TaskRoot, Provider: provider, Executable: executable.Path,
		RuntimeDigest: g.Fingerprint, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, Command: c.Descriptor.Daemon.Path,
		CodexVersion: c.Descriptor.Providers["codex"].Version, CodexHelper: codexHelper, ResumeSessionID: g.ResumeSession, CustomArgs: execution.Run.Options.CustomArgs,
		WorkspaceSlug: claim.WorkspaceSlug, IssueIdentifier: claim.IssueIdentifier,
		Environment: environment, Task: execution.HelperTask, Repositories: g.Repositories, SkillIDs: skillIDs, IsReuse: g.Reuse, Prior: prior,
		Instructions: []byte(execution.Run.RuntimeBrief), ServiceTierConfig: execution.ServiceTierConfig,
		Checkpoint: checkpoint,
	})
	if err == nil {
		err = c.workspaceBudget()
	}
	finishEnvironment(err)
	if err != nil {
		if !errors.Is(err, workspace.ErrPreparationWritersUnproven) {
			if recordErr := c.Store.CompletePreparation(g.AttemptID, nil, nil); recordErr != nil {
				return errors.Join(err, recordErr)
			}
		}
		return err
	}
	execution.Run.Options.Cwd = prepared.Environment.WorkDir
	execution.Run.Options.ClaudeSettingsPath = prepared.Environment.ClaudeSettingsPath
	currentPrompt := execution.Run.Prompt
	if g.SessionProtocol {
		execution.Run.NativeMetadata = prepared.NativeMetadata
		execution.Run.Prompt = execution.Run.RuntimeBrief + "\n\n" + execution.Run.Prompt
	}
	if execution.Run.Options.Timeout <= 0 || execution.Run.Options.Timeout > time.Duration(c.Policy.Worker.TaskDeadlineSeconds)*time.Second {
		execution.Run.Options.Timeout = time.Duration(c.Policy.Worker.TaskDeadlineSeconds) * time.Second
	}
	raw, err := json.Marshal(execution.Run)
	if err != nil {
		return err
	}
	if g.SessionProtocol {
		if !turnPreparationFits(b, g, prepared, execution.Run) && len(claim.ProjectResources) > 0 {
			brief, err := daemonapi.RuntimeBriefWithoutResources(claim, skills)
			if err != nil {
				return err
			}
			prepared = prepared.OmitNativeResources()
			execution.Run.NativeMetadata = prepared.NativeMetadata
			execution.Run.RuntimeBrief = brief
			execution.Run.Prompt = brief + "\n\n" + currentPrompt
			raw, err = json.Marshal(execution.Run)
			if err != nil {
				return err
			}
		}
		if !turnPreparationFits(b, g, prepared, execution.Run) {
			return errors.New("mandatory turn configuration exceeds assignment budget")
		}
		if err := c.Store.CheckpointPreparation(g.AttemptID, prepared); err != nil {
			return err
		}
	}
	return c.Store.CompletePreparation(g.AttemptID, &prepared, raw)
}

func turnPreparationFits(b wire.Bootstrap, g workspace.TaskGrant, p workspace.Prepared, run wire.Run) bool {
	b.PreparedDigest, b.AllowedLinks = p.Digest, p.AllowedLinks
	b.NativeMetadataDigest = p.NativeMetadata.Digest()
	// UUID and SHA lengths are fixed; the actual Pod may not exist yet.
	assignment := wire.TurnAssignment{WorkerSessionID: g.WorkerSessionID, PodUID: uuid.Nil.String(), TurnSequence: g.TurnSequence,
		InputDigest: wire.Digest(nil), Bootstrap: b, Run: run, Deadline: g.CreatedAt.Add(run.Options.Timeout)}
	raw, err := json.Marshal(assignment)
	return err == nil && len(raw) <= wire.MaxAssignmentBytes
}

func (c *Controller) workspaceBudget() error {
	root, err := os.OpenRoot(wire.WorkspaceRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	var size int64
	// ponytail: scan installation data at admission; use accounted writes if the
	// installation grows enough that scanning becomes a measured bottleneck.
	return fs.WalkDir(root.FS(), ".", func(_ string, entry fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			size += info.Size()
			if size > c.Policy.Storage.MaxBytes {
				return workspace.ErrAdmissionBudget
			}
		}
		return nil
	})
}
