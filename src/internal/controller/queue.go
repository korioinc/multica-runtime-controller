package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"

	"k8s.io/client-go/util/workqueue"
)

// Queues contain hints only. The journal and live admission own every decision.
// workqueue retains one follow-up when an event arrives during processing.
type dispatchQueues struct {
	attempts   workqueue.TypedInterface[string]
	deliveries workqueue.TypedInterface[string]
	sessions   workqueue.TypedInterface[string]
	claim      chan struct{}
	pending    chan struct{}
}

func newDispatchQueues() *dispatchQueues {
	return &dispatchQueues{attempts: workqueue.NewTyped[string](), deliveries: workqueue.NewTyped[string](), sessions: workqueue.NewTyped[string](), claim: make(chan struct{}, 1), pending: make(chan struct{}, 1)}
}

func (c *Controller) enqueueSession(id string) {
	c.mu.Lock()
	q := c.dispatch
	c.mu.Unlock()
	if q != nil && id != "" {
		q.sessions.Add(id)
	}
}

func (c *Controller) enqueueAttempt(id string) {
	c.mu.Lock()
	q := c.dispatch
	c.mu.Unlock()
	if q != nil {
		q.attempts.Add(id)
	}
}

func (c *Controller) wakeClaim() {
	c.mu.Lock()
	q := c.dispatch
	c.mu.Unlock()
	if q != nil {
		select {
		case q.claim <- struct{}{}:
		default:
		}
	}
}

func (c *Controller) enqueueDelivery(id string) {
	c.mu.Lock()
	q := c.dispatch
	c.mu.Unlock()
	if q != nil {
		q.deliveries.Add(id)
	}
}

// Rescan when preparation slots or task storage become available, including
// work whose own Pod has not been created and cannot produce a watch event.
func (c *Controller) wakePending() {
	c.mu.Lock()
	q := c.dispatch
	c.mu.Unlock()
	if q != nil {
		select {
		case q.pending <- struct{}{}:
		default:
		}
	}
}

func (c *Controller) runClaims(ctx context.Context, q *dispatchQueues) {
	ticker := time.NewTicker(c.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.claim:
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return
		}
		if err := c.claim(ctx); err != nil {
			slog.Warn("task dispatch deferred", failureAttributes(err, "dispatch_unavailable")...)
		}
	}
}

func (c *Controller) runRecovery(ctx context.Context, q *dispatchQueues) {
	// Cancellation, preparation leases and stop deadlines also progress without
	// Pod events. This sweep is independent of the normal event-driven path.
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.pending:
		case <-ticker.C:
		}
		if ctx.Err() != nil {
			return
		}
		if err := c.enqueuePending(); err != nil {
			slog.Warn("controller reconciliation deferred", "reason", "journal_unavailable")
		}
	}
}

func (c *Controller) enqueuePending() error {
	grants, err := c.Store.ReconcileGrants()
	if err != nil {
		return err
	}
	for _, g := range grants {
		if g.State == "closed" {
			c.enqueueDelivery(g.AttemptID)
		} else {
			c.enqueueAttempt(g.AttemptID)
		}
	}
	sessions, err := c.Store.ListSessions()
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if !session.ResourcesCleaned {
			c.enqueueSession(session.ID)
		}
	}
	c.observeCapacity(grants, sessions)
	return nil
}

type capacityObservation struct {
	active, idle, uncertain, resident int
}

// Emit counts only when a recovery sweep observes a change. The journal still
// owns admission; these independently observed queue inputs are diagnostics.
func (c *Controller) observeCapacity(grants []workspace.TaskGrant, sessions []workspace.WorkerSession) {
	var observed capacityObservation
	for _, grant := range grants {
		if grant.State != "closed" && !(grant.SessionProtocol && grant.TurnReceipt != nil) {
			observed.active++
		}
		if grant.WorkerSessionID == "" && grant.StorageID != "" && !(grant.State == "closed" && grant.CleanupComplete) {
			resources, err := record(grant)
			if err == nil && (grant.PodUID != "" || resources.PodCreateRequested || grant.State == "intent" && grant.Stop == nil) {
				observed.resident++
				if grant.State == "quarantined" {
					observed.uncertain++
				}
			}
		}
	}
	for _, session := range sessions {
		if session.ResourcesCleaned {
			continue
		}
		observed.resident++
		if session.State == workspace.SessionIdle {
			observed.idle++
		}
		if session.State == workspace.SessionQuarantined {
			observed.uncertain++
		}
	}
	c.mu.Lock()
	changed := c.lastCapacity == nil || *c.lastCapacity != observed
	if changed {
		c.lastCapacity = &observed
	}
	c.mu.Unlock()
	if changed {
		slog.Info("runtime capacity observed", "active_turns", observed.active, "idle_pods", observed.idle,
			"uncertain_pods", observed.uncertain, "resident_pods", observed.resident, "execution_limit", c.Capacity, "resident_limit", c.MaxResidentPods)
	}
}

func (c *Controller) runQueue(ctx context.Context, q workqueue.TypedInterface[string], reconcile func(context.Context, string) error) {
	for {
		id, shutdown := q.Get()
		if shutdown {
			return
		}
		if ctx.Err() != nil {
			q.Done(id)
			return
		}
		if err := reconcile(ctx, id); err != nil && ctx.Err() == nil {
			slog.Warn("attempt reconciliation deferred", "attempt", id, "reason", "observation_or_cleanup_unavailable")
		}
		q.Done(id)
	}
}
