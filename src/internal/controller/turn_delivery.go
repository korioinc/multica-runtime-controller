package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

// deliverTurnResult keeps backend acceptance separate from transport success.
// The authenticated provider result remains immutable through every failure.
func (c *Controller) deliverTurnResult(ctx context.Context, grant workspace.TaskGrant) error {
	g, err := c.Store.Get(grant.AttemptID)
	if err != nil {
		return err
	}
	terminal, err := c.Store.Terminal(g.AttemptID)
	if errors.Is(err, workspace.ErrUnauthorized) {
		return nil
	}
	if err != nil {
		return err
	}
	if !g.SessionProtocol || g.WorkerSessionID == "" || terminal.Source != "worker" || !turnCanSettle(terminal) {
		return errors.New("completion does not belong to a conversation turn")
	}
	if terminal.RecoveryFailure != nil {
		return c.deliverRecoveryFailure(ctx, g, terminal)
	}
	if terminal.ResultReceipt == nil {
		return nil
	}
	if terminal.State == "rejected" {
		return c.stopUnacceptedTurn(g)
	}
	if g.CompletionWitness != nil {
		return c.finishTurn(ctx, g)
	}
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		return err
	}
	observed, found, err := c.observeTurnTask(ctx, g, claim)
	if err != nil || !found {
		// A cancelled task's revoked token cannot expose its result row. The
		// control endpoint proves non-acceptance, never a native checkpoint.
		status, statusErr := c.API.TaskStatus(ctx, g.TaskID)
		if statusErr == nil && (status == "cancelled" || terminal.Kind == "fail" && (status == "failed" || status == "completed")) {
			if terminal.State != "delivered" {
				if err := c.Store.RejectTerminal(g.AttemptID); err != nil {
					return err
				}
				c.wakeClaim()
			}
			return c.stopUnacceptedTurn(g)
		}
	}
	if err != nil {
		return err
	}
	if !found {
		return errors.New("completion assignment observation unavailable")
	}
	if recorded, err := c.recordObservedTurnCompletion(g, observed); recorded || err != nil {
		if err != nil {
			return err
		}
		return c.finishTurn(ctx, g)
	}
	if !sameObservedTurn(claim, observed) || observed.Status != "running" {
		if terminal.State == "delivered" {
			// A historical transport-only acknowledgement is not a witness.
			return c.stopUnacceptedTurn(g)
		}
		if err := c.Store.RejectTerminal(g.AttemptID); err != nil {
			return err
		}
		return c.stopUnacceptedTurn(g)
	}
	if terminal.State == "delivered" {
		return errors.New("transport acknowledgement lacks an accepted completion")
	}
	if terminal.State == "uncertain" || terminal.State == "forwarding" {
		if !g.StartConfirmed {
			return nil
		}
		if err := c.Store.ResumeForward(g.AttemptID); err != nil {
			return err
		}
	}
	if err := c.Store.BeginForward(g.AttemptID); err != nil {
		return err
	}
	observed, sendErr := c.API.TerminalObservation(ctx, g.TaskID, terminal.Kind, terminal.Body)
	if sendErr == nil {
		if recorded, err := c.recordObservedTurnCompletion(g, observed); err != nil {
			return err
		} else if recorded {
			return c.finishTurn(ctx, g)
		}
		// A successful HTTP response containing another result is definitive
		// non-acceptance of this request, not permission to publish a checkpoint.
		if err := c.Store.FinishForward(g.AttemptID, "rejected"); err != nil {
			return err
		}
		return c.stopUnacceptedTurn(g)
	}
	outcome := forwardOutcome(sendErr)
	if err := c.Store.FinishForward(g.AttemptID, outcome); err != nil {
		return err
	}
	if outcome == "rejected" {
		return c.stopUnacceptedTurn(g)
	}
	return nil
}

func sameObservedTurn(claim daemonapi.Claim, row daemonapi.TaskObservation) bool {
	return row.MatchesResultClaim(claim)
}

// Task credentials remain the first choice for result observation. Controller
// authority can recover the original row when those credentials are unavailable.
// This read belongs only to signed result delivery, never execution admission.
func (c *Controller) observeTurnTask(ctx context.Context, g workspace.TaskGrant, original daemonapi.Claim) (daemonapi.TaskObservation, bool, error) {
	claims := []daemonapi.Claim{original}
	grants, err := c.Store.List()
	if err != nil {
		return daemonapi.TaskObservation{}, false, err
	}
	seen := map[string]bool{original.AuthToken: true}
	for i := len(grants) - 1; i >= 0; i-- {
		candidate := grants[i]
		if candidate.AttemptID == g.AttemptID || candidate.State == "closed" || candidate.WorkspaceID != g.WorkspaceID || candidate.AgentID != g.AgentID {
			continue
		}
		claim, err := daemonapi.ParseClaim(candidate.Envelope)
		if err != nil || seen[claim.AuthToken] {
			continue
		}
		seen[claim.AuthToken] = true
		claims = append(claims, claim)
	}
	var lastError error
	for _, claim := range claims {
		row, err := c.API.AgentTask(ctx, claim, original.ID)
		if err == nil {
			return row, true, nil
		}
		var rows []daemonapi.TaskObservation
		if claim.IssueID != "" && claim.IssueID == original.IssueID {
			rows, err = c.API.IssueTaskHistory(ctx, claim)
		}
		if err != nil {
			lastError = err
			continue
		}
		for _, row := range rows {
			if row.ID == original.ID {
				return row, true, nil
			}
		}
	}
	row, err := c.API.ControllerTask(ctx, original)
	if err != nil {
		return daemonapi.TaskObservation{}, false, errors.Join(lastError, err)
	}
	return row, true, nil
}

// recordObservedTurnCompletion accepts only the exact supported backend result
// and the original generation-bound start plus supervisor result signature.
func (c *Controller) recordObservedTurnCompletion(grant workspace.TaskGrant, row daemonapi.TaskObservation) (bool, error) {
	g, err := c.Store.Get(grant.AttemptID)
	if err != nil {
		return false, err
	}
	terminal, err := c.Store.Terminal(g.AttemptID)
	if errors.Is(err, workspace.ErrUnauthorized) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if g.WorkerSessionID == "" || !g.StartConfirmed || g.PendingResume == nil || terminal.Source != "worker" || !turnCanSettle(terminal) ||
		terminal.ResultReceipt == nil || terminal.RecoveryFailure != nil || terminal.State == "rejected" {
		return false, nil
	}
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil {
		return false, err
	}
	accepted := row.AcceptsCompletion(claim, terminal.Body)
	if terminal.Kind == "fail" {
		accepted = row.AcceptsFailure(claim, terminal.Body)
		if g.PendingResume.ResumeRejectedTransient {
			accepted = row.AcceptsTransientFailure(claim, terminal.Body)
		}
	}
	if !accepted {
		return false, nil
	}
	if g.CompletionWitness != nil && terminal.State == "delivered" {
		return true, nil
	}
	sessionID := g.PendingResume.SessionID
	if terminal.Kind == "fail" || g.PendingResume.SessionRolloutMissing || sessionID == g.PendingResume.RetiredSessionID {
		sessionID = ""
	}
	witness := workspace.CompletionWitness{TaskID: g.TaskID, AttemptID: g.AttemptID, WorkerSessionID: g.WorkerSessionID,
		StorageID: g.StorageID, TurnSequence: g.TurnSequence, RequestDigest: terminal.RequestDigest, ResultDigest: terminal.ResultDigest,
		Status: row.Status, SessionID: sessionID, WorkDir: g.PendingResume.WorkDir, AcknowledgedAt: time.Now().UTC()}
	if err := c.Store.RecordCompletionWitness(g.AttemptID, witness); err != nil {
		return false, err
	}
	c.enqueueAttempt(g.AttemptID)
	c.wakeClaim()
	return true, nil
}

// reconcileTurnCompletion reuses a current claim's already-authorized history
// before source selection. It never substitutes status equality for acceptance.
func (c *Controller) reconcileTurnCompletion(ctx context.Context, current daemonapi.Claim, rows []daemonapi.TaskObservation) error {
	grants, err := c.Store.List()
	if err != nil {
		return err
	}
	storages, err := c.Store.ListStorages()
	if err != nil {
		return err
	}
	byStorage := make(map[string]workspace.Storage, len(storages))
	for _, storage := range storages {
		byStorage[storage.ID] = storage
	}
	byID := make(map[string]daemonapi.TaskObservation, len(rows))
	for _, row := range rows {
		if row.WorkspaceID == current.WorkspaceID && row.AgentID == current.AgentID {
			byID[row.ID] = row
		}
	}
	for _, grant := range grants {
		if grant.WorkerSessionID == "" || grant.WorkspaceID != current.WorkspaceID || grant.AgentID != current.AgentID {
			continue
		}
		if grant.TurnComplete && grant.CompletionWitness != nil {
			session, err := c.Store.GetSession(grant.WorkerSessionID)
			if err != nil {
				return err
			}
			if session.State != workspace.SessionClosed {
				continue
			}
			storage := byStorage[grant.StorageID]
			if grant.Selection == nil || !grant.Selection.WorkspaceReuseEligible || storage.LatestWriter.AttemptID != grant.AttemptID || storage.Checkpoint != nil {
				continue
			}
		}
		row, ok := byID[grant.TaskID]
		if !ok {
			continue
		}
		if recorded, err := c.recordObservedTurnCompletion(grant, row); err != nil {
			return err
		} else if recorded {
			if err := c.finishTurn(ctx, grant); err != nil {
				return err
			}
		}
	}
	return nil
}

// finishTurn joins independent evidence without owning Pod destruction. A
// closed session can acquire a late checkpoint without reopening its deadline.
func (c *Controller) finishTurn(ctx context.Context, grant workspace.TaskGrant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g, err := c.Store.Get(grant.AttemptID)
	if err != nil {
		return err
	}
	terminal, err := c.Store.Terminal(g.AttemptID)
	if errors.Is(err, workspace.ErrUnauthorized) {
		return nil
	}
	if err != nil {
		return err
	}
	if g.WorkerSessionID == "" || terminal.Source != "worker" || !turnCanSettle(terminal) || terminal.RecoveryFailure != nil {
		return nil
	}
	if terminal.State == "rejected" {
		return c.stopUnacceptedTurn(g)
	}
	for _, event := range g.Events {
		if event.State == "uncertain" {
			if _, err := c.Store.RequestSessionStop(g.WorkerSessionID, "event_delivery_uncertain"); err != nil {
				return err
			}
			c.enqueueSession(g.WorkerSessionID)
			if terminal.State != "delivered" && terminal.State != "rejected" {
				c.enqueueDelivery(g.AttemptID)
			}
			return nil
		}
		if event.State != "delivered" && event.State != "local" {
			return nil
		}
	}
	if g.CompletionWitness == nil || terminal.State != "delivered" {
		c.enqueueDelivery(g.AttemptID)
		return nil
	}
	session, err := c.Store.GetSession(g.WorkerSessionID)
	if err != nil {
		return err
	}
	if g.TurnComplete {
		if session.State != workspace.SessionClosed {
			return nil
		}
		storage, err := c.Store.GetStorage(g.StorageID)
		if err != nil {
			return err
		}
		if g.Selection == nil || !g.Selection.WorkspaceReuseEligible || storage.LatestWriter.AttemptID != g.AttemptID || storage.Checkpoint != nil {
			return nil
		}
	}
	if g.TurnReceipt == nil && g.TurnExecutionReceipt == nil && session.State != workspace.SessionClosed {
		return nil
	}
	if err := c.Store.CompleteTurn(g.AttemptID); err != nil {
		if errors.Is(err, workspace.ErrConflict) {
			// Cleanup or a late final receipt may still be outstanding.
			c.enqueueSession(g.WorkerSessionID)
			return nil
		}
		return err
	}
	if !g.TurnComplete {
		if completed, err := c.Store.GetSession(g.WorkerSessionID); err == nil {
			slog.Info("conversation turn settled", append(turnAttributes(g), "state", completed.State,
				"idle_deadline", completed.IdleDeadline, "sinceResult", time.Since(terminal.ReceivedAt))...)
		}
	}
	c.enqueueSession(g.WorkerSessionID)
	c.wakePending()
	c.wakeClaim()
	return nil
}

func turnCanSettle(terminal workspace.Terminal) bool {
	return terminal.Kind == "complete" || terminal.Kind == "fail"
}

func (c *Controller) stopUnacceptedTurn(g workspace.TaskGrant) error {
	session, err := c.Store.GetSession(g.WorkerSessionID)
	if err != nil {
		return err
	}
	if session.Stop != nil {
		return nil
	}
	if _, err := c.Store.RequestSessionStop(g.WorkerSessionID, "completion_not_accepted"); err != nil {
		return err
	}
	c.enqueueSession(g.WorkerSessionID)
	c.wakeClaim()
	return nil
}
