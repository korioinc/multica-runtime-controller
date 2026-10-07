// Package controller owns registration, dispatch and durable completion delivery.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/repocache"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type Controller struct {
	prepareContext                  context.Context
	preparing                       map[string]bool
	prepareCancels                  map[string]context.CancelFunc
	attemptLocks                    sync.Map
	idleDeadlines                   sync.Map
	claimMu                         sync.Mutex
	prepareWG                       sync.WaitGroup
	Store                           *workspace.Store
	API                             *daemonapi.Client
	Kube                            *kubernetes.Client
	Policy                          kubernetes.Config
	Owner                           kubernetes.Owner
	OwnerID                         string
	Descriptor                      runtimeimage.Descriptor
	RuntimeRef                      runtimeimage.Ref
	Configuration                   configuration.Bundle
	Environment                     []string
	GatewayURL, Name                string
	NFSServer                       string
	Capacity                        int
	ConversationIdleTimeout         time.Duration
	MaxResidentPods                 int
	PollInterval, HeartbeatInterval time.Duration
	App                             *githubapp.Manager
	Cache                           *repocache.Manager
	checkoutMu                      sync.Mutex
	checkoutOps                     map[string]*checkoutOperation
	checkoutClosed                  bool
	checkoutWG                      sync.WaitGroup
	mu                              sync.Mutex
	lastCapacity                    *capacityObservation
	dispatch                        *dispatchQueues
	runtimes                        map[string]daemonapi.Bootstrap
}

const DefaultConversationIdleTimeout = 10 * time.Minute

func (c *Controller) Run(ctx context.Context) error {
	ctx, cancelRun := context.WithCancel(ctx)
	c.prepareContext = ctx
	defer func() { cancelRun(); c.closeCheckouts(); c.prepareWG.Wait() }()
	if c.Capacity < 1 || c.PollInterval <= 0 || c.HeartbeatInterval <= 0 || c.ConversationIdleTimeout < 0 || c.MaxResidentPods < 0 {
		return errors.New("invalid dispatcher settings")
	}
	if c.MaxResidentPods == 0 {
		c.MaxResidentPods = c.Capacity
	}
	// These inputs alone must fit before registration can expose the runtime
	// to claims. Per-attempt identities and routes are checked during provision.
	inputs, err := json.Marshal(wire.Bootstrap{OwnerID: c.OwnerID, RuntimeRef: c.RuntimeRef, Configuration: c.Configuration, Environment: c.Environment, GatewayURL: c.GatewayURL, GitHubApp: c.App != nil, TerminationGraceSeconds: int(c.Policy.Worker.TerminationGraceSeconds)})
	if err != nil {
		return err
	}
	if len(inputs) > wire.MaxRequestBytes {
		return errors.New("controller configuration exceeds worker bootstrap payload limit")
	}
	if err := c.register(ctx); err != nil {
		return diagnostics.Wrap("controller_registration_failed", err)
	}
	queues := newDispatchQueues()
	c.mu.Lock()
	c.dispatch = queues
	c.mu.Unlock()
	slog.Info("controller dispatch started", "runtimes", len(c.runtimes), "capacity", c.Capacity, "residentLimit", c.MaxResidentPods, "conversationIdleTimeout", c.ConversationIdleTimeout, "pollInterval", c.PollInterval, "heartbeatInterval", c.HeartbeatInterval)
	var loops sync.WaitGroup
	defer func() {
		cancelRun()
		queues.attempts.ShutDown()
		queues.deliveries.ShutDown()
		queues.sessions.ShutDown()
		loops.Wait()
	}()
	for range 4 {
		loops.Go(func() { c.runQueue(ctx, queues.attempts, c.reconcileAttempt) })
		loops.Go(func() { c.runQueue(ctx, queues.sessions, c.reconcileSession) })
	}
	// Slow delivery or cleanup for one task must not hold another task's reply.
	for range c.Capacity {
		loops.Go(func() { c.runQueue(ctx, queues.deliveries, c.reconcileDelivery) })
	}
	loops.Go(func() { c.runRecovery(ctx, queues) })
	loops.Go(func() {
		c.periodic(ctx, c.HeartbeatInterval, func() {
			for id := range c.runtimes {
				if err := c.API.Heartbeat(ctx, id); err != nil {
					slog.Warn("runtime heartbeat unavailable", append([]any{"runtime", id}, failureAttributes(err, "heartbeat_unavailable")...)...)
				}
			}
		})
	})
	loops.Go(func() { c.runClaims(ctx, queues) })
	loops.Go(func() {
		c.API.WatchTasks(ctx, c.runtimeIDs(), c.wakeClaim, func() {
			c.wakeClaim()
			c.wakePending()
		})
	})
	loops.Go(func() { c.Kube.WatchWorkers(ctx, c.OwnerID, c.enqueueAttempt, c.enqueueSession) })
	c.wakePending()
	c.wakeClaim()
	<-ctx.Done()
	return ctx.Err()
}

func (c *Controller) register(ctx context.Context) error {
	slog.Info("runtime registration started", "providers", len(c.Descriptor.Providers))
	workspaces, err := c.API.Workspaces(ctx)
	if err != nil {
		slog.Warn("runtime workspace discovery failed", failureAttributes(err, "workspace_discovery_failed")...)
		return diagnostics.Wrap("controller_workspace_discovery_failed", err)
	}
	slog.Info("runtime workspaces discovered", "workspaces", len(workspaces))
	inventory := []daemonapi.Inventory{}
	for id, executable := range c.Descriptor.Providers {
		inventory = append(inventory, daemonapi.Inventory{Name: id, Type: id, Version: executable.Version, Status: "online"})
	}
	slices.SortFunc(inventory, func(a, b daemonapi.Inventory) int {
		if a.Type < b.Type {
			return -1
		}
		if a.Type > b.Type {
			return 1
		}
		return 0
	})
	c.runtimes = map[string]daemonapi.Bootstrap{}
	for _, w := range workspaces {
		registration, err := c.API.Register(ctx, daemonapi.Registration{WorkspaceID: w.ID, DaemonID: c.OwnerID, DeviceName: c.Name, Runtimes: inventory})
		if err != nil {
			slog.Warn("runtime registration failed", failureAttributes(err, "registration_failed")...)
			return err
		}
		for _, runtime := range registration.Runtimes {
			if _, ok := c.Descriptor.Providers[runtime.Provider]; !ok || runtime.ID == "" {
				return errors.New("registered runtime differs from image inventory")
			}
			c.runtimes[runtime.ID] = daemonapi.Bootstrap{Workspace: w, Runtime: runtime, ReposVersion: registration.ReposVersion, Settings: registration.Settings}
		}
	}
	slog.Info("runtime registration completed", "workspaces", len(workspaces), "runtimes", len(c.runtimes), "providers", len(inventory))
	if len(c.runtimes) == 0 {
		slog.Warn("controller has no registered runtimes", "reason", "no_registered_runtimes")
	}
	return nil
}

func (c *Controller) claim(ctx context.Context) error {
	c.claimMu.Lock()
	defer c.claimMu.Unlock()
	grants, err := c.Store.List()
	if err != nil {
		return err
	}
	active := 0
	for _, g := range grants {
		// An uncertain worker still occupies capacity and retains its task's
		// storage lock, but does not block admission to unrelated task roots.
		if g.State != "closed" && !(g.SessionProtocol && g.TurnReceipt != nil) {
			active++
		}
	}
	if active >= c.Capacity || len(c.runtimes) == 0 {
		return nil
	}
	started := time.Now()
	tasks, err := c.API.Claim(ctx, c.OwnerID, c.runtimeIDs(), c.Capacity-active)
	if err != nil {
		return err
	}
	queued := []workspace.TaskGrant{}
	for _, raw := range tasks {
		claim, err := daemonapi.ParseClaim(raw)
		if err != nil {
			slog.Warn("invalid claimed task refused", "reason", "claim_invalid")
			continue
		}
		metadata, ok := c.runtimes[claim.RuntimeID]
		if !ok || metadata.Workspace.ID != claim.WorkspaceID {
			slog.Warn("invalid claimed task refused", "reason", "claim_scope_invalid")
			continue
		}
		known := false
		for _, existing := range grants {
			if existing.TaskID == claim.ID && existing.State != "closed" {
				known = true
				break
			}
		}
		if known {
			continue
		}
		g := workspace.TaskGrant{SessionProtocol: true, OwnerID: c.OwnerID, TaskID: claim.ID, RuntimeID: claim.RuntimeID, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID, RuntimeRef: c.RuntimeRef, Envelope: raw}
		if claim.IssueID != "" {
			g.ResourceScope = append(g.ResourceScope, "issue:"+claim.IssueID)
		}
		for _, repo := range claim.Repos {
			g.Repositories = append(g.Repositories, workspace.Repository{URL: repo.URL, Ref: repo.Ref, Description: repo.Description})
		}
		for _, resource := range claim.ProjectResources {
			g.ResourceScope = append(g.ResourceScope, "project:"+resource.ID)
		}
		for _, connection := range claim.RemoteMCPConnections {
			g.ResourceScope = append(g.ResourceScope, "remote:"+connection.InstallationID+":"+connection.ContributionID)
		}
		for _, hook := range claim.PluginHookTools {
			g.ResourceScope = append(g.ResourceScope, "hook:"+hook.InstallationID+":"+hook.HookKey)
		}
		for _, skill := range claim.Agent.SkillRefs {
			g.ResourceScope = append(g.ResourceScope, "skill:"+wire.Digest(skill))
		}
		g.Metadata, _ = json.Marshal(metadata)
		g, err = c.Store.QueueClaim(g)
		if errors.Is(err, workspace.ErrInvalidClaim) || errors.Is(err, workspace.ErrConflict) || errors.Is(err, workspace.ErrStorageBusy) {
			slog.Warn("claimed task admission refused", "task", claim.ID, "reason", "claim_conflict")
			continue
		}
		if err != nil {
			return err
		}
		queued = append(queued, g)
		grants = append(grants, g)
		slog.Info("task claim accepted", "task", g.TaskID, "attempt", g.AttemptID, "claimElapsed", time.Since(started))
	}
	// The complete accepted batch is durable before any member reserves storage.
	for _, g := range queued {
		c.enqueueAttempt(g.AttemptID)
	}
	if len(queued) > 0 {
		// A backend batch may contain only one task per agent. Continue filling
		// available capacity without waiting for another notification or poll.
		c.wakeClaim()
	}
	return nil
}

func (c *Controller) runtimeIDs() []string {
	ids := make([]string, 0, len(c.runtimes))
	for id := range c.runtimes {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (c *Controller) periodic(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}
