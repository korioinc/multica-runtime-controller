package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (c *Controller) reconcileAttempt(ctx context.Context, id string) error {
	lock := c.attemptLock(id)
	if !lock.TryLock() {
		// A closed attempt may still have a Pod event while its delivery owns
		// this lock. Never let that event occupy an active attempt worker.
		g, err := c.Store.Get(id)
		if err != nil {
			return err
		}
		if g.State == "closed" {
			c.enqueueDelivery(id)
		}
		return nil // The periodic pending sweep also recovers a busy active owner.
	}
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	g, err := c.Store.Get(id)
	if err != nil {
		return err
	}
	if g.State == "closed" {
		c.enqueueDelivery(id)
		return nil
	}
	if err := c.recoverCheckoutWriter(ctx, id); err != nil {
		return err
	}
	g, err = c.Store.Get(id)
	if err != nil {
		return err
	}
	c.mu.Lock()
	_, startedHere := c.preparing[id]
	preparing := c.preparing[id]
	c.mu.Unlock()
	if g.PreparationProcess != nil && g.PreparationStarted && !g.PreparationStopped && !startedHere {
		stopped, err := c.Kube.PreparationStopped(ctx, *g.PreparationProcess)
		if err != nil {
			return err
		}
		if stopped {
			if err := c.Store.InterruptPreparation(id, *g.PreparationProcess); err != nil {
				return err
			}
			g, err = c.Store.Get(id)
			if err != nil {
				return err
			}
			if g.Stop == nil && g.State == "intent" {
				if err := c.recordFailure(g, diagnostics.Wrap("preparation_controller_restarted", errors.New("preparation container terminated"))); err != nil {
					return err
				}
				g, err = c.Store.Get(id)
				if err != nil {
					return err
				}
			}
		}
	}
	if g.Stop != nil {
		return c.reconcileStop(ctx, g)
	}
	// Cancellation is observed independently of preparation lease renewal and
	// worker control polling, including before its first input request.
	if stopped, err := c.observeBackendStop(ctx, g); stopped {
		if err != nil {
			return err
		}
		g, err = c.Store.Get(id)
		if err != nil {
			return err
		}
		return c.reconcileStop(ctx, g)
	}
	if g.State == "quarantined" {
		g, err = c.requestStop(g, "worker_unavailable")
		if err != nil {
			return err
		}
		return c.reconcileStop(ctx, g)
	}
	if g.State == "unexecuted" {
		g, err = c.requestStop(g, "worker_unavailable")
		if err != nil {
			return err
		}
		return c.reconcileStop(ctx, g)
	}
	if g.State == "waiting_storage" {
		return c.activate(ctx, g)
	}
	if g.State == "intent" {
		if time.Since(g.CreatedAt) > time.Duration(c.Policy.Worker.PreparationTimeoutSeconds)*time.Second {
			if preparing {
				return nil
			}
			return c.recordFailure(g, nil)
		}
		if err := c.provision(ctx, g); err != nil && !errors.Is(err, errPreparePending) && !transientObservation(err) {
			if stopped, stopErr := c.observeBackendStop(ctx, g); stopped {
				return stopErr
			}
			if errors.Is(err, errStartupPodStopped) {
				return c.stopFailedStartup(ctx, g, err)
			}
			return c.recordFailure(g, err)
		}
		return nil
	}
	if g.State == "terminal_received" {
		t, err := c.Store.Terminal(id)
		if err != nil {
			return err
		}
		if t.State == "received" && t.ResultReceipt == nil {
			r, err := record(g)
			if err != nil {
				return err
			}
			observed, err := c.Kube.ObserveTermination(ctx, r.Reference)
			if err != nil {
				return err
			}
			if observed.Stopped {
				g, err = c.requestStop(g, "worker_unsealed")
				if err != nil {
					return err
				}
				return c.reconcileStop(ctx, g)
			}
		}
		return c.deliver(ctx, g)
	}
	if g.State == "assigned" || g.State == "ready" || g.State == "offered" || g.State == "starting" {
		if time.Since(g.CreatedAt) > time.Duration(c.Policy.Worker.PreparationTimeoutSeconds)*time.Second {
			return c.recordFailure(g, nil)
		}
		if err := c.API.PrepareLease(ctx, g.RuntimeID, g.TaskID); err != nil {
			current, readErr := c.Store.Get(id)
			if readErr != nil {
				return readErr
			}
			if current.State == "started" || current.State == "terminal_received" {
				return nil
			}
			if stopped, stopErr := c.observeBackendStop(ctx, current); stopped {
				return stopErr
			}
			if current.State == "starting" && c.sameAssignment(ctx, current) {
				status, statusErr := c.API.TaskStatus(ctx, current.TaskID)
				if statusErr == nil && status == "running" {
					return nil
				}
			}
			if leaseRefused(err) {
				return c.recordFailure(current, err)
			}
		}
	}
	if g.WorkerSessionID != "" && g.State == "started" {
		assignment, err := wire.DecodeTurnAssignment(g.Assignment)
		if err != nil {
			return err
		}
		if !time.Now().Before(assignment.Deadline) {
			g, err = c.requestStop(g, "task_deadline_exceeded")
			if err != nil {
				return err
			}
			return c.reconcileStop(ctx, g)
		}
	}
	r, err := record(g)
	if err != nil {
		return err
	}
	pod, err := c.Kube.PodState(ctx, r.Reference)
	if transientObservation(err) {
		return err
	}
	if err != nil || pod.DeletionTimestamp != nil || pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
		return c.recordFailure(g, nil)
	}
	return nil
}

func (c *Controller) stopFailedStartup(ctx context.Context, observed workspace.TaskGrant, cause error) error {
	current, err := c.Store.Get(observed.AttemptID)
	if err != nil {
		return err
	}
	// Admission, cancellation, or a native result may have committed while the
	// Pod observation was in flight. Let that current authority own recovery.
	if current.State != "intent" || current.Stop != nil {
		c.enqueueAttempt(current.AttemptID)
		return nil
	}
	if err := c.recordFailure(current, cause); err != nil {
		return err
	}
	current, err = c.Store.Get(current.AttemptID)
	if err != nil {
		return err
	}
	if current.State != "quarantined" && current.State != "unexecuted" {
		c.enqueueAttempt(current.AttemptID)
		return nil
	}
	current, err = c.requestStop(current, failureReason(cause, "worker_startup_failed"))
	if err != nil {
		return err
	}
	return c.reconcileStop(ctx, current)
}

// Delivery is serialized per attempt by the delivery workqueue. An active
// result must not wait for the attempt lock held by checkout or Pod shutdown.
func (c *Controller) reconcileDelivery(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	g, err := c.Store.Get(id)
	if err != nil {
		return err
	}
	if g.WorkerSessionID != "" && g.State == "closed" {
		// A completed turn has no authority to delete its shared session Pod.
		if err := c.deliverResult(ctx, g); err != nil {
			return err
		}
		return c.finishTurn(ctx, g)
	}
	if g.State == "terminal_received" {
		t, err := c.Store.Terminal(id)
		if err != nil {
			return err
		}
		if t.Source != "worker" || t.Seal == nil && t.ResultReceipt == nil ||
			!g.StartConfirmed && !(t.Kind == "cancel-ack" && g.ExecutionRevoked) {
			return nil
		}
		return c.deliverResult(ctx, g)
	}
	if g.State != "closed" {
		return nil
	}
	lock := c.attemptLock(id)
	lock.Lock()
	defer lock.Unlock()
	g, err = c.Store.Get(id)
	if err != nil {
		return err
	}
	return c.reconcileClosed(ctx, g)
}

func (c *Controller) recordFailure(g workspace.TaskGrant, cause error) error {
	body := []byte(`{"error":"Runtime worker unavailable; task storage retained"}`)
	reason := failureReason(cause, "worker_unavailable")
	var diagnostic *diagnostics.Error
	if errors.As(cause, &diagnostic) {
		reason = diagnostic.Reason
		switch diagnostic.Reason {
		case "preparation_controller_restarted":
			body = []byte(`{"error":"Preparation interrupted by controller restart; task storage retained"}`)
		case "worker_initialization_failed":
			body = []byte(`{"error":"Worker initialization failed before execution admission; task storage retained","failure_reason":"agent_error.unknown"}`)
		case "worker_startup_failed":
			body = []byte(`{"error":"Worker stopped before execution admission; task storage retained","failure_reason":"agent_error.unknown"}`)
		}
	}
	if errors.Is(cause, workspace.ErrAdmissionBudget) {
		reason = "admission_budget_exhausted"
		body = []byte(`{"error":"Task admission budget exhausted; existing data retained"}`)
	}
	c.checkoutMu.Lock()
	_, err := c.Store.ReceiveFailure(g.AttemptID, body)
	// A failure observation stops current readers even if journal publication
	// fails. A later reader must independently pass durable authorization.
	c.cancelCheckoutsLocked(g.AttemptID)
	c.checkoutMu.Unlock()
	if errors.Is(err, workspace.ErrAdmissionBudget) {
		slog.Warn("task admission deferred", "attempt", g.AttemptID, "reason", "admission_budget_exhausted")
	}
	if errors.Is(err, workspace.ErrConflict) {
		// Never replace an actual native result with an inferred failure.
		return nil
	}
	if err == nil {
		slog.Warn("task execution failed", "task", g.TaskID, "attempt", g.AttemptID, "reason", reason)
	}
	return err
}

func (c *Controller) deliverFailure(ctx context.Context, g workspace.TaskGrant) error {
	t, err := c.Store.Terminal(g.AttemptID)
	if errors.Is(err, workspace.ErrUnauthorized) {
		return nil
	}
	if err != nil {
		return err
	}
	if t.Source != "controller" {
		return nil
	}
	if t.State == "delivered" || t.State == "rejected" {
		return nil
	}
	if t.State != "received" && t.State != "uncertain" && t.State != "forwarding" {
		return nil
	}
	status, err := c.deliveryStatus(ctx, g)
	if errors.Is(err, errAssignmentChanged) || err == nil && !terminalAllowed("fail", status) {
		if err := c.Store.RejectFailure(g.AttemptID); err != nil {
			return err
		}
		c.wakePending()
		c.wakeClaim()
		return nil
	}
	if err != nil {
		return err
	}
	if t.State == "uncertain" || t.State == "forwarding" {
		if !g.StartConfirmed || status != "running" {
			return nil
		}
		if err := c.Store.ResumeForward(g.AttemptID); err != nil {
			return err
		}
	}
	// The existing backend has no assignment compare-and-set on /fail. This
	// check detects reassignment; pre-start ambiguous transport is not retried.
	if err := c.Store.BeginFailureForward(g.AttemptID); err != nil {
		return err
	}
	err = c.API.Terminal(ctx, g.TaskID, "fail", t.Body)
	outcome := forwardOutcome(err)
	if err := c.Store.FinishForward(g.AttemptID, outcome); err != nil {
		return err
	}
	if outcome == "delivered" || outcome == "rejected" {
		c.wakePending()
		c.wakeClaim()
	}
	slog.Info("task terminal delivery finished", "task", g.TaskID, "attempt", g.AttemptID, "source", "controller", "kind", "fail", "result", outcome)
	return nil
}

func (c *Controller) deliver(ctx context.Context, g workspace.TaskGrant) error {
	g, err := c.Store.Get(g.AttemptID)
	if err != nil {
		return err
	}
	if g.Stop != nil {
		return c.reconcileStop(ctx, g)
	}
	if g.State != "terminal_received" {
		return errors.New("terminal attempt inactive")
	}
	t, err := c.Store.Terminal(g.AttemptID)
	if err != nil {
		return err
	}
	if t.Seal == nil && t.ResultReceipt == nil {
		return nil
	}
	if g.WorkerSessionID != "" && turnCanSettle(t) {
		c.enqueueDelivery(g.AttemptID)
		return c.finishTurn(ctx, g)
	}
	// Authentication permits result delivery. Physical termination and clean
	// storage remain separate decisions owned by stop reconciliation.
	c.enqueueDelivery(g.AttemptID)
	g, err = c.requestStop(g, "execution_finished")
	if err != nil {
		return err
	}
	return c.reconcileStop(ctx, g)
}

var errAssignmentChanged = errors.New("upstream task assignment changed")

func (c *Controller) assignment(ctx context.Context, g workspace.TaskGrant) (daemonapi.TaskAssignment, error) {
	var claim struct {
		DispatchedAt string `json:"dispatched_at"`
	}
	if json.Unmarshal(g.Envelope, &claim) != nil || claim.DispatchedAt == "" {
		return daemonapi.TaskAssignment{}, errors.New("original dispatch identity unavailable")
	}
	var current daemonapi.TaskAssignment
	var err error
	if g.SessionProtocol {
		original, parseErr := daemonapi.ParseClaim(g.Envelope)
		if parseErr != nil {
			return current, parseErr
		}
		current, err = c.API.ClaimTaskAssignment(ctx, original)
	} else {
		current, err = c.API.TaskAssignment(ctx, g.WorkspaceID, g.AgentID, g.TaskID)
	}
	if err != nil {
		return current, err
	}
	claimedAt, claimErr := time.Parse(time.RFC3339, claim.DispatchedAt)
	currentAt, currentErr := time.Parse(time.RFC3339, current.DispatchedAt)
	// Task history exposes dispatch times only to seconds; claims retain fractions.
	// This owner-visible observation is not a dispatch-generation mutation fence.
	if current.RuntimeID != g.RuntimeID || claimErr != nil || currentErr != nil || !currentAt.Truncate(time.Second).Equal(claimedAt.Truncate(time.Second)) {
		return current, errAssignmentChanged
	}
	return current, nil
}

func (c *Controller) sameAssignment(ctx context.Context, g workspace.TaskGrant) bool {
	_, err := c.assignment(ctx, g)
	return err == nil
}

// Terminal transitions can revoke the task token. Their control-plane status
// can end delivery retries, but cannot prove which payload the backend accepted.
func (c *Controller) deliveryStatus(ctx context.Context, g workspace.TaskGrant) (string, error) {
	if g.SessionProtocol {
		status, err := c.API.TaskStatus(ctx, g.TaskID)
		if err == nil && (status == "cancelled" || status == "completed" || status == "failed") {
			return status, nil
		}
	}
	assignment, err := c.assignment(ctx, g)
	return assignment.Status, err
}

func terminalAllowed(kind, status string) bool {
	if kind == "complete" {
		return status == "running"
	}
	if kind == "cancel-ack" {
		return status == "cancelled"
	}
	return status == "dispatched" || status == "running" || status == "waiting_local_directory"
}

func forwardOutcome(err error) string {
	if err == nil {
		return "delivered"
	}
	if daemonapi.RequestNotSent(err) {
		return "not-sent"
	}
	var response *daemonapi.ResponseError
	if errors.As(err, &response) && response.StatusCode >= 400 && response.StatusCode < 500 {
		return "rejected"
	}
	return "uncertain"
}

// Failed observations leave existing authority in place, while missing objects
// and confirmed identity mismatches still quarantine the unresolved writer.
func transientObservation(err error) bool {
	if err == nil {
		return false
	}
	var network net.Error
	var status apierrors.APIStatus
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &network) || errors.As(err, &status) && (status.Status().Code == 408 || status.Status().Code == 429 || status.Status().Code >= 500)
}
