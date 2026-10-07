package worker

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

type sessionSupervisor struct {
	gateway           sessionGateway
	admitted          bool
	state             workspace.SessionState
	sequence          uint64
	inputDigest       string
	completed         uint64
	settled           uint64
	receiptAccepted   bool
	finishingDeadline time.Time
	lifecycle         *workerLifecycle
	home              *managedHome
	desktopMu         sync.Mutex
	desktop           *residentDesktop
	failureReason     string
	stopMu            sync.Mutex
	stop              wire.SessionStopCommand
}

func (s *sessionSupervisor) currentDesktop() *residentDesktop {
	s.desktopMu.Lock()
	defer s.desktopMu.Unlock()
	return s.desktop
}

func (s *sessionSupervisor) ensureDesktop(ctx context.Context, descriptor runtimeimage.Descriptor, taskRoot string) error {
	if desktop := s.currentDesktop(); desktop != nil {
		return desktop.health(ctx)
	}
	desktop, err := startResidentDesktop(ctx, descriptor, taskRoot)
	s.desktopMu.Lock()
	s.desktop = desktop
	s.desktopMu.Unlock()
	return err
}

func serveSession(ctx context.Context, gateway sessionGateway) (resultErr error) {
	supervisor := &sessionSupervisor{gateway: gateway, state: workspace.SessionCreating}
	ctx, cancel := context.WithCancelCause(ctx)
	var watched <-chan struct{}
	defer func() {
		resultErr = errors.Join(context.Cause(ctx), resultErr)
		var diagnostic *diagnostics.Error
		if errors.As(resultErr, &diagnostic) {
			supervisor.failureReason = diagnostic.Reason
		}
		cancel(nil)
		if watched != nil {
			<-watched
		}
		resultErr = errors.Join(resultErr, supervisor.finalStop())
	}()
	admissionCtx, stopAdmission := context.WithTimeout(ctx, time.Minute)
	err := gateway.enroll(admissionCtx)
	stopAdmission()
	if err != nil {
		return err
	}
	supervisor.admitted = true
	slog.Info("worker session supervisor admitted", "worker_session_id", gateway.bootstrap.WorkerSessionID)
	done := make(chan struct{})
	watched = done
	go func() { defer close(done); supervisor.watch(ctx, cancel) }()
	return supervisor.run(ctx)
}

// One watchdog spans the whole incarnation. It never uses a previous turn's
// idle deadline or credentials to cancel a current reservation or execution.
func (s *sessionSupervisor) watch(ctx context.Context, cancel context.CancelCauseFunc) {
	lastContact := time.Now()
	for ctx.Err() == nil {
		if desktop := s.currentDesktop(); desktop != nil {
			healthCtx, stopHealth := context.WithTimeout(ctx, 10*time.Second)
			err := desktop.health(healthCtx)
			stopHealth()
			if err != nil && ctx.Err() == nil {
				cancel(err)
				return
			}
		}
		requestCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		var command wire.SessionStopCommand
		err := s.gateway.read(requestCtx, workspace.SessionOperationStopControl, struct{}{}, &command, 0)
		stop()
		if err == nil {
			lastContact = time.Now()
			if command.Revision != 0 {
				if err := s.rememberStop(command); err != nil {
					cancel(err)
				} else {
					cancel(errStopRequested)
				}
				return
			}
		} else if time.Since(lastContact) >= time.Minute {
			cancel(errors.New("worker session controller contact lost"))
			return
		}
		if waitControl(ctx) != nil {
			return
		}
	}
}

func (s *sessionSupervisor) run(ctx context.Context) error {
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		deadline := time.Now().Add(15 * time.Second)
		if s.completed > s.settled && !s.finishingDeadline.IsZero() && s.finishingDeadline.Before(deadline) {
			deadline = s.finishingDeadline
		}
		requestCtx, stop := context.WithDeadline(ctx, deadline)
		var response wire.SessionPollResponse
		err := s.gateway.read(requestCtx, workspace.SessionOperationPoll, wire.SessionPoll{TurnSequence: s.sequence, State: s.state}, &response, s.sequence)
		stop()
		if s.completed > s.settled && !s.finishingDeadline.IsZero() && !time.Now().Before(s.finishingDeadline) {
			return errors.New("worker turn settlement remained unproven")
		}
		if err != nil {
			if !errors.Is(err, errGatewayUnavailable) && !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if err := waitControl(ctx); err != nil {
				return context.Cause(ctx)
			}
			continue
		}
		if response.WorkerSessionID != s.gateway.bootstrap.WorkerSessionID || response.TurnSequence < s.sequence || response.SettledTurnSequence > response.TurnSequence || response.SettledTurnSequence < s.settled {
			return errors.New("session poll changed the accepted turn binding")
		}
		if response.Stop != nil {
			if err := s.rememberStop(*response.Stop); err != nil {
				return err
			}
			return errStopRequested
		}
		s.settled = response.SettledTurnSequence
		switch response.State {
		case workspace.SessionDraining, workspace.SessionQuarantined, workspace.SessionClosed:
			return errStopRequested
		case workspace.SessionCreating, workspace.SessionReserved, workspace.SessionRunning, workspace.SessionFinishing, workspace.SessionIdle:
		default:
			return errors.New("unsupported worker session state")
		}
		if response.Assignment != nil {
			assignment := *response.Assignment
			if response.State != workspace.SessionReserved || assignment.TurnSequence != response.TurnSequence {
				return errors.New("assignment is not the reserved session turn")
			}
			if assignment.TurnSequence <= s.sequence {
				if assignment.TurnSequence != s.sequence || assignment.InputDigest != s.inputDigest {
					return errors.New("session replay changed an accepted assignment")
				}
			} else if s.completed == 0 || s.receiptAccepted && s.settled >= s.completed {
				if err := s.execute(ctx, assignment); err != nil {
					return err
				}
				continue
			}
		} else if response.State == workspace.SessionIdle && (s.completed == 0 || s.receiptAccepted && s.settled >= s.completed) {
			s.state = workspace.SessionIdle
			if err := os.WriteFile(wire.ControlRoot+"/ready", []byte(s.gateway.bootstrap.WorkerSessionID), 0600); err != nil {
				return err
			}
		} else if s.sequence == 0 && (response.State == workspace.SessionRunning || response.State == workspace.SessionFinishing) {
			return errors.New("session execution exists without local acceptance")
		}
		// Idle deadlines only prompt a new authenticated observation. The
		// controller may have reserved a new turn since the previous response.
		if err := waitControl(ctx); err != nil {
			return context.Cause(ctx)
		}
	}
}

func (s *sessionSupervisor) execute(ctx context.Context, assignment wire.TurnAssignment) error {
	if err := assignment.Validate(s.gateway.bootstrap, s.gateway.identity.PodUID); err != nil {
		return err
	}
	if assignment.TurnSequence <= s.sequence || !time.Now().Before(assignment.Deadline) {
		return errors.New("assignment was consumed or expired")
	}
	// Consume locally before a retryable acceptance. An uncertain response
	// drains this incarnation instead of invoking the SDK from another poll.
	s.sequence, s.inputDigest, s.state = assignment.TurnSequence, assignment.InputDigest, workspace.SessionReserved
	s.receiptAccepted = false
	if err := os.Remove(wire.ControlRoot + "/ready"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	turnCtx, cancel := context.WithDeadline(ctx, assignment.Deadline)
	defer cancel()
	request, err := s.gateway.proof(turnCtx, workspace.SessionOperationAccept, wire.SessionAccept{TurnSequence: assignment.TurnSequence, InputDigest: assignment.InputDigest}, assignment.TurnSequence)
	if err != nil {
		return err
	}
	if request.Proof.TurnSequence != assignment.TurnSequence || request.Proof.AttemptID != assignment.Bootstrap.AttemptID {
		return errors.New("acceptance challenge belongs to another assignment")
	}
	if err := s.gateway.submit(turnCtx, request, nil); err != nil {
		return err
	}
	raw, err := json.Marshal(assignment.Bootstrap)
	if err != nil {
		return err
	}
	lifecycle := &workerLifecycle{bootstrap: assignment.Bootstrap, client: s.gateway.client, signingKey: s.gateway.key, home: s.home, session: s,
		identity: workerIdentity{PublicKey: s.gateway.identity.PublicKey, PodUID: s.gateway.identity.PodUID,
			PVCUID: s.gateway.bootstrap.PVCUID, BootstrapDigest: wire.Digest(raw)}}
	s.lifecycle = lifecycle
	var result *agent.Result
	err = lifecycle.admit(turnCtx)
	if err == nil {
		s.state = workspace.SessionRunning
		result, err = lifecycle.execute(turnCtx, cancel, &assignment.Run)
	}
	s.home = lifecycle.home
	interrupted := turnCtx.Err() != nil || ctx.Err() != nil
	cancel()
	s.state = workspace.SessionFinishing
	s.finishingDeadline = time.Now().Add(time.Duration(assignment.Bootstrap.TerminationGraceSeconds+60) * time.Second)
	finishCtx, stop := context.WithDeadline(context.Background(), s.finishingDeadline)
	defer stop()
	finishFence := diagnostics.StartPhase("worker_turn_fence", append(diagnostics.TaskAttributes(assignment.Bootstrap.TaskID, assignment.Bootstrap.AttemptID),
		"worker_session_id", assignment.WorkerSessionID, "podUID", s.gateway.identity.PodUID, "turn_sequence", assignment.TurnSequence)...)
	cleanup, command, reportErr := lifecycle.settle(finishCtx, result)
	executionSettled := result != nil && !interrupted && err == nil && cleanup.err == nil && cleanup.taskProcessesStopped && cleanup.localRequestsClosed && cleanup.privateStateCleared
	if executionSettled && reportErr == nil {
		if desktop := s.currentDesktop(); desktop != nil {
			reportErr = desktop.health(finishCtx)
		}
	}
	if executionSettled && reportErr == nil {
		receipt := workspace.TurnExecutionReceipt{WorkerSessionID: assignment.WorkerSessionID, Conversation: s.gateway.bootstrap.Conversation,
			StorageID: s.gateway.bootstrap.StorageID, TaskID: assignment.Bootstrap.TaskID,
			AttemptID: assignment.Bootstrap.AttemptID, Generation: assignment.Bootstrap.Generation, TurnSequence: assignment.TurnSequence,
			InputDigest: assignment.InputDigest, PodUID: s.gateway.identity.PodUID, PVCUID: s.gateway.bootstrap.PVCUID,
			ResultDigest: lifecycle.resultDigest, RequestDigest: command.RequestDigest, Nonce: command.Nonce,
			TaskProcessesStopped: true, LocalRequestsClosed: true, PrivateStateCleared: true}
		receipt.Signature = ed25519.Sign(s.gateway.key, workspace.TurnExecutionReceiptMessage(receipt))
		request, proofErr := s.gateway.proof(finishCtx, workspace.SessionOperationTurnExecutionReceipt, receipt, assignment.TurnSequence)
		reportErr = proofErr
		if proofErr == nil {
			reportErr = s.gateway.submit(finishCtx, request, nil)
		}
		s.receiptAccepted = reportErr == nil
	}
	if result == nil {
		err = errors.Join(err, errors.New("worker turn has no observed provider result"))
	}
	if interrupted {
		err = errors.Join(err, errors.New("worker turn was interrupted"))
	}
	finishFence(errors.Join(err, cleanup.err, reportErr), "taskProcessesStopped", cleanup.taskProcessesStopped,
		"localRequestsClosed", cleanup.localRequestsClosed, "privateStateCleared", cleanup.privateStateCleared)
	if err != nil || cleanup.err != nil || reportErr != nil {
		return errors.Join(err, cleanup.err, reportErr)
	}
	s.completed = assignment.TurnSequence
	return nil
}

func validSessionStop(command wire.SessionStopCommand) bool {
	return command.Revision != 0 && wire.UUID(command.Nonce) && (command.AttemptID == "" || wire.UUID(command.AttemptID)) &&
		(command.InputDigest == "" || core.ValidSHA(command.InputDigest))
}

func (s *sessionSupervisor) rememberStop(command wire.SessionStopCommand) error {
	if !validSessionStop(command) {
		return errors.New("invalid session stop command")
	}
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	if s.stop.Revision != 0 && (s.stop.Revision != command.Revision || s.stop.Nonce != command.Nonce || s.stop.TurnSequence != command.TurnSequence || s.stop.AttemptID != command.AttemptID || s.stop.InputDigest != command.InputDigest) {
		return errors.New("session stop changed its writer binding")
	}
	command.ControllerWritersStopped = command.ControllerWritersStopped || s.stop.ControllerWritersStopped
	s.stop = command
	return nil
}

func (s *sessionSupervisor) stopCommand() wire.SessionStopCommand {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	return s.stop
}

func (s *sessionSupervisor) finalStop() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.gateway.bootstrap.TerminationGraceSeconds+60)*time.Second)
	defer cancel()
	lifecycle := s.lifecycle
	if lifecycle == nil {
		lifecycle = &workerLifecycle{client: s.gateway.client, home: s.home, session: s,
			bootstrap: wire.Bootstrap{TaskRoot: s.gateway.bootstrap.TaskRoot, TerminationGraceSeconds: s.gateway.bootstrap.TerminationGraceSeconds}}
	}
	reason := s.stopCommand().Reason
	if reason == "" {
		reason = s.failureReason
		if reason == "" {
			reason = "worker_shutdown"
		}
	}
	slog.Info("worker session final shutdown", "worker_session_id", s.gateway.bootstrap.WorkerSessionID, "reason", reason)
	fence := func(ctx context.Context) error {
		if !s.admitted {
			if err := s.gateway.enroll(ctx); err != nil {
				return err
			}
			s.admitted = true
		}
		command := s.stopCommand()
		if command.Revision == 0 {
			request, err := s.gateway.proof(ctx, workspace.SessionOperationStopRequest, wire.SessionStopRequest{Reason: reason}, s.sequence)
			if err != nil {
				return err
			}
			if err := s.gateway.submit(ctx, request, &command); err != nil {
				return err
			}
			if err := s.rememberStop(command); err != nil {
				return err
			}
		}
		for {
			if command.TurnSequence < s.sequence {
				return errors.New("session stop omitted an accepted turn")
			}
			if command.ControllerWritersStopped {
				return nil
			}
			if err := s.gateway.read(ctx, workspace.SessionOperationStopControl, struct{}{}, &command, s.sequence); err != nil {
				return err
			}
			if err := s.rememberStop(command); err != nil {
				return err
			}
			if command.ControllerWritersStopped {
				return nil
			}
			if err := waitControl(ctx); err != nil {
				return err
			}
		}
	}
	cleanup := lifecycle.cleanup(ctx, fence)
	command := s.stopCommand()
	if !s.admitted || !validSessionStop(command) {
		return errors.Join(cleanup.err, errors.New("session stop could not be authenticated"))
	}
	receipt := workspace.SessionStopReceipt{WorkerSessionID: s.gateway.bootstrap.WorkerSessionID, AttemptID: command.AttemptID,
		InputDigest: command.InputDigest, TurnSequence: command.TurnSequence, PodUID: s.gateway.identity.PodUID, PVCUID: s.gateway.bootstrap.PVCUID,
		Revision: command.Revision, Nonce: command.Nonce, WritersStopped: cleanup.writersStopped, FlushOK: cleanup.flushOK}
	receipt.Signature = ed25519.Sign(s.gateway.key, workspace.SessionStopReceiptMessage(receipt))
	request, err := s.gateway.proof(ctx, workspace.SessionOperationStopReceipt, receipt, s.sequence)
	if err != nil {
		return errors.Join(cleanup.err, err)
	}
	return errors.Join(cleanup.err, s.gateway.submit(ctx, request, nil))
}
