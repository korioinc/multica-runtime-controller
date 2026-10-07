package controller

import (
	"context"
	"crypto/ed25519"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func (c *Controller) attemptLock(id string) *sync.Mutex {
	lock, _ := c.attemptLocks.LoadOrStore(id, new(sync.Mutex))
	return lock.(*sync.Mutex)
}

func (c *Controller) requestStop(g workspace.TaskGrant, reason string) (workspace.TaskGrant, error) {
	// Journal transitions atomically reject a later Offer/BeginStart/MarkStarted.
	// Do not wait for an in-flight task API body or remote call to drain before
	// recording cancellation; already authorized operations may finish.
	c.checkoutMu.Lock()
	current, err := c.Store.RequestStop(g.AttemptID, reason)
	c.cancelCheckoutsLocked(g.AttemptID)
	c.checkoutMu.Unlock()
	if err != nil {
		return current, err
	}
	c.mu.Lock()
	if cancel := c.prepareCancels[g.AttemptID]; cancel != nil {
		cancel()
	}
	c.mu.Unlock()
	if g.Stop == nil {
		slog.Info("task stop requested", "task", g.TaskID, "attempt", g.AttemptID, "reason", reason)
		c.enqueueAttempt(g.AttemptID)
	}
	if g.WorkerSessionID != "" {
		c.enqueueSession(g.WorkerSessionID)
	}
	return current, nil
}

func (c *Controller) observeBackendStop(ctx context.Context, g workspace.TaskGrant) (bool, error) {
	var statusErr error
	if g.SessionProtocol {
		// Cancellation revokes the task token. The daemon status endpoint
		// remains a stop-only signal; it conveys no assignment authority.
		var status string
		status, statusErr = c.API.TaskStatus(ctx, g.TaskID)
		current, err := c.Store.Get(g.AttemptID)
		if err != nil {
			return false, err
		}
		g = current
		if g.State == "closed" || g.TurnComplete {
			return false, nil
		}
		if statusErr == nil && (status == "cancelled" || status == "completed" || status == "failed") {
			reason := backendStopReason(g, status)
			if reason == "" {
				return false, nil
			}
			_, err := c.requestStop(g, reason)
			return true, err
		}
	}
	assignment, err := c.assignment(ctx, g)
	if g.SessionProtocol {
		current, getErr := c.Store.Get(g.AttemptID)
		if getErr != nil {
			return false, getErr
		}
		g = current
		if g.State == "closed" || g.TurnComplete {
			return false, nil
		}
	}
	reason := ""
	if errors.Is(err, errAssignmentChanged) {
		reason = "assignment_changed"
	} else if err != nil {
		return false, errors.Join(statusErr, err)
	} else {
		reason = backendStopReason(g, assignment.Status)
	}
	if reason == "" {
		return false, statusErr
	}
	_, err = c.requestStop(g, reason)
	return true, err
}

func backendStopReason(g workspace.TaskGrant, status string) string {
	switch status {
	case "cancelled":
		return "backend_cancelled"
	case "failed", "completed":
		// Recorded native results retain their delivery owner.
		if g.State != "terminal_received" {
			return "backend_terminal"
		}
	}
	return ""
}

func (c *Controller) reconcileStop(ctx context.Context, g workspace.TaskGrant) error {
	if g.Stop == nil {
		return errors.New("stop intent missing")
	}
	if g.WorkerSessionID != "" {
		c.enqueueSession(g.WorkerSessionID)
		c.enqueueDelivery(g.AttemptID)
		return nil
	}
	if g.State == "terminal_received" {
		// Restart recovery and cancellation may enter here before delivery. Do
		// not make an authenticated outcome wait for checkout or Pod cleanup.
		c.enqueueDelivery(g.AttemptID)
	}
	if err := c.stopCheckoutWrites(ctx, g.AttemptID); err != nil {
		return err
	}
	var err error
	g, err = c.Store.Get(g.AttemptID)
	if err != nil {
		return err
	}
	if g.State != "closed" {
		c.mu.Lock()
		if cancel := c.prepareCancels[g.AttemptID]; cancel != nil {
			cancel()
		}
		c.mu.Unlock()
		if g.PreparationStarted && !g.PreparationStopped {
			return nil
		}
		r, err := record(g)
		if err != nil {
			return err
		}
		if r.SecretCreateRequested && r.Reference.SecretUID == "" {
			uid, err := c.Kube.ResolveCleanupSecret(ctx, r.Reference)
			if err != nil {
				return err
			}
			if uid != "" {
				r.Reference.SecretUID = uid
				if err := c.saveResources(g.AttemptID, r); err != nil {
					return err
				}
			}
		}
		if g.Stop.Evidence == nil {
			if g.PodUID == "" && r.PodCreateRequested {
				uid, err := c.Kube.ResolveCleanupPod(ctx, r.Reference)
				if err != nil {
					return err
				}
				if uid == "" {
					// Complete only the request issued before stop. Its finalizer
					// and fixed name coalesce any late POST; revoked execution
					// remains revoked while the Pod supplies termination evidence.
					b, err := wire.DecodeBootstrap(g.Bootstrap)
					if err != nil {
						return err
					}
					r.Reference.Owner = c.Owner
					uid, err = c.provisionPod(ctx, g, &r, b)
					if err != nil {
						return err
					}
					if uid == "" {
						return errors.New("worker creation outcome is unresolved")
					}
				}
				if err := c.Store.BindPod(g.AttemptID, g.PodName, uid, ""); err != nil {
					return err
				}
				g, err = c.Store.Get(g.AttemptID)
				if err != nil {
					return err
				}
			}
			kind := "no-worker"
			observedAt := time.Now().UTC()
			if g.PodUID != "" {
				r.Reference.PodUID, r.Reference.PVCUID = g.PodUID, g.PVCUID
				if err := c.saveResources(g.AttemptID, r); err != nil {
					return err
				}
				observation, err := c.Kube.ObserveTermination(ctx, r.Reference)
				if err != nil {
					return err
				}
				if !observation.Stopped {
					// Kubernetes retains the Pod's configured shutdown grace and
					// our finalizer. Pre-worker init observes stop cooperatively;
					// DeletePod preserves that startup window before signalling.
					if err := c.Kube.DeletePod(ctx, r.Reference); err != nil {
						return err
					}
					return nil
				}
				kind, observedAt = "terminated", observation.ObservedAt
				if observation.NeverStarted {
					kind = "never-started"
				}
			}
			evidence := workspace.StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: kind, ObservedAt: observedAt}
			if kind != "terminated" && g.Prepared != nil && g.PreparationStopped {
				// Only a root with no worker writes can use the controller's
				// local flush. It cannot recover an interrupted NFS client.
				evidence.PreparedFlushOK = workspace.SyncTaskFilesystem(g.TaskRoot) == nil
			}
			if err := c.Store.ObserveStop(g.AttemptID, evidence); err != nil {
				return err
			}
			slog.Info("task stop evidence recorded", "task", g.TaskID, "attempt", g.AttemptID, "podUID", g.PodUID, "evidence", kind)
		}
		g, err = c.Store.Get(g.AttemptID)
		if err != nil {
			return err
		}
		if waiting, err := c.recoverUnsealedResult(ctx, g); waiting || err != nil {
			return err
		}
		if err := c.Store.CloseStopped(g.AttemptID); err != nil {
			return err
		}
		c.wakeClaim()
	}
	c.enqueueDelivery(g.AttemptID)
	return nil
}

func (c *Controller) reconcileClosed(ctx context.Context, g workspace.TaskGrant) error {
	if g.WorkerSessionID != "" {
		return c.deliverResult(ctx, g)
	}
	if g.Stop == nil {
		if g.CleanupComplete {
			return nil
		}
		r, err := record(g)
		if err != nil {
			return err
		}
		if err := c.Kube.Cleanup(ctx, r.Reference); err != nil {
			return err
		}
		if err := c.Store.MarkCleaned(g.AttemptID); err != nil {
			return err
		}
		c.wakePending()
		return nil
	}
	if waiting, err := c.recoverUnsealedResult(ctx, g); waiting || err != nil {
		return err
	}
	// Resource cleanup is independent of a possibly unavailable backend. Its
	// outbox remains durable and continues to block unsafe same-task retries.
	var cleanupErr error
	if !g.CleanupComplete {
		r, err := record(g)
		if err != nil {
			return err
		}
		if r.SecretCreateRequested || r.PodCreateRequested {
			cleanupErr = c.Kube.Cleanup(ctx, r.Reference)
		}
		if cleanupErr == nil {
			cleanupErr = c.Store.MarkCleaned(g.AttemptID)
			if cleanupErr == nil {
				slog.Info("task cleanup completed", "task", g.TaskID, "attempt", g.AttemptID)
				c.wakePending()
			}
		}
	}
	return errors.Join(cleanupErr, c.deliverResult(ctx, g))
}

func (c *Controller) deliverResult(ctx context.Context, g workspace.TaskGrant) error {
	t, err := c.Store.Terminal(g.AttemptID)
	if errors.Is(err, workspace.ErrUnauthorized) {
		return nil
	}
	if err != nil {
		return err
	}
	if t.Source == "controller" {
		return c.deliverFailure(ctx, g)
	}
	if t.RecoveryFailure != nil {
		return c.deliverRecoveryFailure(ctx, g, t)
	}
	if g.WorkerSessionID != "" && turnCanSettle(t) {
		return c.deliverTurnResult(ctx, g)
	}
	if t.State != "sealed" && t.State != "uncertain" && t.State != "forwarding" && !(t.State == "received" && t.ResultReceipt != nil) {
		return nil
	}
	status, err := c.deliveryStatus(ctx, g)
	if errors.Is(err, errAssignmentChanged) || err == nil && !terminalAllowed(t.Kind, status) {
		if err := c.Store.RejectTerminal(g.AttemptID); err != nil {
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
		if !(g.StartConfirmed && status == "running") && !(t.Kind == "cancel-ack" && status == "cancelled") {
			return nil
		}
		if err := c.Store.ResumeForward(g.AttemptID); err != nil {
			return err
		}
	}
	if err := c.Store.BeginForward(g.AttemptID); err != nil {
		return err
	}
	err = c.API.Terminal(ctx, g.TaskID, t.Kind, t.Body)
	outcome := forwardOutcome(err)
	if err := c.Store.FinishForward(g.AttemptID, outcome); err != nil {
		return err
	}
	if outcome == "delivered" || outcome == "rejected" {
		c.wakePending()
		// The capacity wake may have found this task still running upstream.
		c.wakeClaim()
	}
	return nil
}

func (c *Controller) recoverUnsealedResult(ctx context.Context, g workspace.TaskGrant) (bool, error) {
	t, err := c.Store.Terminal(g.AttemptID)
	if errors.Is(err, workspace.ErrUnauthorized) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if t.Source != "worker" || t.Seal != nil || t.RecoveryFailure != nil || t.State != "received" && t.ResultReceipt == nil {
		return false, nil
	}
	if t.ResultReceipt != nil && g.Stop != nil && g.Stop.Receipt != nil && g.Stop.Receipt.WritersStopped && g.Stop.Receipt.FlushOK {
		return false, nil
	}
	if g.Stop == nil || g.Stop.Evidence == nil {
		return true, nil
	}
	// Keep the original Pod and its credential available for an in-flight seal.
	// Its positive termination observation starts the bounded receipt window.
	grace := time.Duration(c.Policy.Worker.TerminationGraceSeconds) * time.Second
	if g.WorkerSessionID != "" {
		session, err := c.Store.GetSession(g.WorkerSessionID)
		if err != nil {
			return true, err
		}
		grace, err = c.sessionTerminationGrace(session)
		if err != nil {
			return true, err
		}
		if t.ResultReceipt != nil && session.Stop != nil && session.Stop.ActiveAttempt == g.AttemptID && session.Stop.ControllerWritersStopped {
			receipt := session.Stop.Receipt
			// The Store authenticates the final receipt. It replaces the late
			// per-attempt seal only for the same terminated, authenticated turn.
			if receipt != nil && receipt.WritersStopped && receipt.FlushOK && receipt.WorkerSessionID == g.WorkerSessionID &&
				receipt.AttemptID == g.AttemptID && receipt.TurnSequence == g.TurnSequence && receipt.InputDigest == g.InputDigest &&
				receipt.PodUID == g.PodUID && receipt.PVCUID == g.PVCUID {
				return false, nil
			}
		}
	}
	if time.Since(g.Stop.Evidence.ObservedAt) < grace {
		return true, nil
	}
	if t.ResultReceipt != nil {
		// Keep the authenticated outcome. Missing storage proof marks the root
		// dirty at CloseStopped rather than replacing an already delivered result.
		return false, nil
	}
	if err := c.stopCheckoutWriters(ctx, g.AttemptID); err != nil {
		return true, err
	}
	body := []byte(`{"error":"Execution result was recorded, but worker shutdown proof was not received; task data retained","failure_reason":"agent_error.unknown"}`)
	if err := c.Store.RecordRecoveryFailure(g.AttemptID, body); err != nil {
		if errors.Is(err, workspace.ErrConflict) {
			current, readErr := c.Store.Terminal(g.AttemptID)
			if readErr == nil && (current.Seal != nil || current.ResultReceipt != nil) {
				return false, nil
			}
		}
		return true, err
	}
	slog.Warn("task result recovery selected", "task", g.TaskID, "attempt", g.AttemptID, "reason", "shutdown_receipt_missing")
	return false, nil
}

func (c *Controller) deliverRecoveryFailure(ctx context.Context, g workspace.TaskGrant, t workspace.Terminal) error {
	recovery := t.RecoveryFailure
	if recovery.State == "delivered" || recovery.State == "rejected" {
		return nil
	}
	status, err := c.deliveryStatus(ctx, g)
	if errors.Is(err, errAssignmentChanged) || err == nil && !terminalAllowed("fail", status) {
		if err := c.Store.RejectRecoveryFailure(g.AttemptID); err != nil {
			return err
		}
		c.wakePending()
		c.wakeClaim()
		return nil
	}
	if err != nil {
		return err
	}
	if recovery.State == "uncertain" || recovery.State == "forwarding" {
		if !g.StartConfirmed || status != "running" {
			return nil
		}
		if err := c.Store.ResumeForward(g.AttemptID); err != nil {
			return err
		}
	}
	if err := c.Store.BeginRecoveryForward(g.AttemptID); err != nil {
		return err
	}
	err = c.API.Terminal(ctx, g.TaskID, "fail", recovery.Body)
	outcome := forwardOutcome(err)
	if err := c.Store.FinishRecoveryForward(g.AttemptID, outcome); err != nil {
		return err
	}
	if outcome == "delivered" || outcome == "rejected" {
		c.wakePending()
		c.wakeClaim()
	}
	slog.Info("task recovery failure delivery finished", "task", g.TaskID, "attempt", g.AttemptID, "reason", "shutdown_receipt_missing", "result", outcome)
	return nil
}

func stopCommand(g workspace.TaskGrant) wire.StopCommand {
	if g.Stop == nil {
		return wire.StopCommand{}
	}
	return wire.StopCommand{Cancel: true, Revision: g.Stop.Revision, Nonce: g.Stop.Nonce}
}

// The stop audience cannot read execution input, start work, acquire task API
// authority, or replace a result. It can report the old writer's termination.
func (c *Controller) stopControl(w http.ResponseWriter, r *http.Request, id, action string) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	g, err := c.Store.AuthorizeStop(token)
	if errors.Is(err, workspace.ErrPodBindingPending) && g.AttemptID == id && (action == "stop-admit" && r.Method == http.MethodPost || action == "stop-control" && r.Method == http.MethodGet) {
		http.Error(w, "Pod binding observation pending", http.StatusServiceUnavailable)
		return
	}
	if err != nil || g.AttemptID != id {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	if action == "stop-receipt" && r.Method == http.MethodPost {
		var receipt workspace.StopReceipt
		if decodeBody(r, &receipt) != nil || c.Store.ReceiveStopReceipt(id, receipt) != nil {
			http.Error(w, "invalid stop receipt", http.StatusConflict)
			return
		}
		c.enqueueAttempt(id)
		writeJSON(w, map[string]bool{"accepted": true})
		return
	}
	if g.State == "closed" {
		writeJSON(w, stopCommand(g))
		return
	}
	if action == "stop-admit" && r.Method == http.MethodPost {
		var input struct {
			PublicKey       []byte `json:"publicKey"`
			PodUID          string `json:"podUID"`
			PVCUID          string `json:"pvcUID"`
			BootstrapDigest string `json:"bootstrapDigest"`
		}
		if decodeBody(r, &input) != nil || len(input.PublicKey) != ed25519.PublicKeySize || input.PodUID != g.PodUID || input.PVCUID != g.PVCUID || input.BootstrapDigest != g.BootstrapDigest {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		rec, err := record(g)
		if err != nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		rec.Reference.PodUID, rec.Reference.PVCUID = g.PodUID, g.PVCUID
		pod, err := c.Kube.AuthorizeStop(r.Context(), rec.Reference)
		if err != nil {
			http.Error(w, "admission observation pending", http.StatusServiceUnavailable)
			return
		}
		if pod.Spec.NodeName == "" {
			http.Error(w, "scheduling pending", http.StatusServiceUnavailable)
			return
		}
		if err := c.Store.PinStopSupervisor(id, input.PodUID, input.PVCUID, pod.Spec.NodeName, input.PublicKey); err != nil {
			http.Error(w, "stop supervisor conflict", http.StatusConflict)
			return
		}
	} else if action == "stop-request" && r.Method == http.MethodPost {
		var input struct {
			Reason string `json:"reason"`
		}
		if decodeBody(r, &input) != nil || input.Reason != "worker_startup_failed" && input.Reason != "worker_shutdown" {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		// Retain a controller-only failure when no backend cancellation was
		// observed. A late worker report cannot replace an existing outcome.
		if stopped, err := c.observeBackendStop(r.Context(), g); stopped && err != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		g, err = c.Store.Get(id)
		if err != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		if g.Stop == nil {
			if err := c.recordFailure(g, nil); err != nil {
				http.Error(w, "unavailable", 503)
				return
			}
			g, err = c.requestStop(g, input.Reason)
			if err != nil {
				http.Error(w, "unavailable", 503)
				return
			}
		}
	} else if action != "stop-control" || r.Method != http.MethodGet {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	g, err = c.Store.Get(id)
	if err != nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, stopCommand(g))
}
