package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/kubernetes"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type idleDeadline struct {
	revision  uint64
	recorded  time.Time
	monotonic time.Time
}

func (c *Controller) reconcileSession(ctx context.Context, id string) error {
	lock := c.attemptLock("session:" + id)
	if !lock.TryLock() {
		return nil
	}
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	session, err := c.Store.GetSession(id)
	if err != nil {
		return err
	}
	if session.State == workspace.SessionIdle {
		deadline := session.IdleDeadline
		if shorter := session.IdleSince.Add(c.ConversationIdleTimeout); shorter.Before(deadline) {
			deadline = shorter
		}
		now := time.Now()
		if !deadline.Equal(session.IdleDeadline) || now.Before(session.IdleSince) || !now.Before(deadline) {
			if _, err := c.Store.ExpireSession(id, c.ConversationIdleTimeout); err != nil {
				return err
			}
		}
		session, err = c.Store.GetSession(id)
		if err != nil {
			return err
		}
		if session.State == workspace.SessionIdle {
			now := time.Now()
			value, found := c.idleDeadlines.Load(id)
			deadline, _ := value.(idleDeadline)
			if !found || deadline.revision != session.Revision || !deadline.recorded.Equal(session.IdleDeadline) {
				deadline = idleDeadline{revision: session.Revision, recorded: session.IdleDeadline,
					monotonic: now.Add(max(time.Duration(0), session.IdleDeadline.Sub(now)))}
				c.idleDeadlines.Store(id, deadline)
			}
			if !now.Before(deadline.monotonic) {
				if _, err := c.Store.DrainIdleSession(id, deadline.revision, "idle_expired"); err != nil {
					return err
				}
			}
			// The existing monotonic recovery ticker checks this deadline.
			// Store reservation also enforces the persisted UTC cutoff exactly.
		}
	}
	session, err = c.Store.GetSession(id)
	if err != nil {
		return err
	}
	if session.State != workspace.SessionIdle {
		c.idleDeadlines.Delete(id)
	}
	if session.Stop == nil {
		if session.PodUID != "" {
			record, err := sessionRecord(session)
			if err != nil {
				return err
			}
			pod, err := c.Kube.PodState(ctx, record.Reference)
			if errors.Is(err, kubernetes.ErrWriterUnknown) {
				return c.Store.QuarantineSession(id)
			}
			if err != nil {
				return err
			}
			if pod.DeletionTimestamp != nil || pod.Status.Phase == "Succeeded" || pod.Status.Phase == "Failed" {
				if _, err := c.Store.RequestSessionStop(id, "worker_stopped"); err != nil {
					return err
				}
				c.enqueueSession(id)
			}
		}
		return nil
	}
	return c.reconcileSessionStop(ctx, session)
}

func (c *Controller) reconcileSessionStop(ctx context.Context, session workspace.WorkerSession) error {
	if session.ResourcesCleaned {
		return nil
	}
	if session.State != workspace.SessionClosed {
		// Recover only issued requests; never create a new incarnation to
		// compensate for an unresolved POST from this session.
		if len(session.Resources) != 0 {
			if err := c.ensureSessionResources(ctx, session.ID); err != nil {
				return err
			}
		}
		if _, err := c.sessionStopCommand(ctx, session); err != nil {
			return err
		}
		var err error
		session, err = c.Store.GetSession(session.ID)
		if err != nil {
			return err
		}
		stopped, err := c.stopSessionControllerWriters(ctx, session)
		if err != nil {
			return err
		}
		if !stopped {
			return nil
		}
		if session.Stop.Evidence == nil {
			evidence := workspace.StopEvidence{PodUID: session.PodUID, PVCUID: session.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()}
			if session.PodUID != "" {
				record, err := sessionRecord(session)
				if err != nil {
					return err
				}
				observation, err := c.Kube.ObserveTermination(ctx, record.Reference)
				if errors.Is(err, kubernetes.ErrWriterUnknown) {
					return c.Store.QuarantineSession(session.ID)
				}
				if err != nil {
					return err
				}
				if !observation.Stopped {
					return c.Kube.DeletePod(ctx, record.Reference)
				}
				evidence.Kind, evidence.ObservedAt = "terminated", observation.ObservedAt
				if observation.NeverStarted {
					evidence.Kind = "never-started"
				}
			}
			if evidence.Kind != "terminated" && session.ActiveAttempt != "" {
				grant, err := c.Store.Get(session.ActiveAttempt)
				if err != nil {
					return err
				}
				evidence.PreparedFlushOK = grant.Prepared != nil && grant.PreparationStopped && session.Stop.ControllerWritersStopped
			}
			if err := c.Store.ObserveSessionStop(session.ID, evidence); err != nil {
				return err
			}
			session, err = c.Store.GetSession(session.ID)
			if err != nil {
				return err
			}
			slog.Info("worker session termination proven", "worker_session_id", session.ID, "podUID", session.PodUID,
				"conversation_kind", session.Conversation.Kind, "conversation", session.Conversation.Digest(), "storage", session.StorageID,
				"turn_sequence", session.TurnSequence, "idle_deadline", session.IdleDeadline, "reason", session.Stop.Reason,
				"evidence", evidence.Kind, "sinceStop", time.Since(session.Stop.RequestedAt))
		}
		grace, err := c.sessionTerminationGrace(session)
		if err != nil {
			return err
		}
		if session.Stop.Evidence.Kind == "terminated" && session.Stop.Receipt == nil && time.Since(session.Stop.Evidence.ObservedAt) < grace {
			return nil
		}
		if session.ActiveAttempt != "" {
			grant, err := c.Store.Get(session.ActiveAttempt)
			if err != nil {
				return err
			}
			if waiting, err := c.recoverUnsealedResult(ctx, grant); waiting || err != nil {
				return err
			}
			if _, err := c.Store.Terminal(grant.AttemptID); errors.Is(err, workspace.ErrUnauthorized) {
				cause := errors.New("worker session stopped without a terminal result")
				if !grant.StartConfirmed {
					cause = diagnostics.Wrap("worker_startup_failed", cause)
				}
				// Closing the grant first would permanently lose its failure outbox.
				// ReceiveFailure preserves any native result committed after this read.
				if err := c.recordFailure(grant, cause); err != nil {
					return err
				}
			} else if err != nil {
				return err
			}
			c.enqueueDelivery(grant.AttemptID)
		}
		if err := c.Store.CloseSession(session.ID); err != nil {
			return err
		}
		c.wakeClaim()
	}
	session, err := c.Store.GetSession(session.ID)
	if err != nil {
		return err
	}
	if len(session.Resources) != 0 {
		record, err := sessionRecord(session)
		if err != nil {
			return err
		}
		if err := c.Kube.Cleanup(ctx, record.Reference); err != nil {
			return err
		}
	}
	if err := c.Store.MarkSessionCleaned(session.ID); err != nil {
		return err
	}
	c.wakePending()
	c.wakeClaim()
	return nil
}
