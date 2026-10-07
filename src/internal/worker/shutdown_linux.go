package worker

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/multica-ai/multica/server/pkg/redact"
)

type workerLifecycle struct {
	bootstrap    wire.Bootstrap
	client       *http.Client
	signingKey   ed25519.PrivateKey
	identity     workerIdentity
	admitted     bool
	taskAPI      *taskRelay
	server       *http.Server
	serverDone   chan struct{}
	runner       *providerRunner
	desktop      *residentDesktop
	session      *sessionSupervisor
	home         *managedHome
	resultDigest string
}

type workerIdentity struct {
	PublicKey       ed25519.PublicKey `json:"publicKey"`
	PodUID          string            `json:"podUID"`
	PVCUID          string            `json:"pvcUID"`
	BootstrapDigest string            `json:"bootstrapDigest"`
}

func (s *workerLifecycle) admit(ctx context.Context) error {
	var command wire.StopCommand
	if err := startupControl(ctx, s.client, s.bootstrap, "stop-admit", s.identity, &command); err != nil {
		return err
	}
	s.admitted = true
	if command.Cancel {
		return errStopRequested
	}
	return nil
}

func watchStop(ctx context.Context, cancel context.CancelFunc, client *http.Client, b wire.Bootstrap) {
	lastContact := time.Now()
	for ctx.Err() == nil {
		var command wire.StopCommand
		err := control(ctx, client, b, http.MethodGet, "stop-control", nil, &command)
		if err == nil {
			lastContact = time.Now()
			if command.Cancel {
				cancel()
				return
			}
		} else if !errors.Is(err, errGatewayUnavailable) || time.Since(lastContact) > time.Minute {
			cancel()
			return
		}
		if err := waitControl(ctx); err != nil {
			return
		}
	}
}

// The complete runtime's existing entrypoint also owns its optional desktop
// setup. Calling that fixed image path keeps services under the admitted PID 1.
func startDesktop(ctx context.Context, environment map[string]string) error {
	if _, err := os.Stat("/etc/multica/desktop-supervisord.conf"); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "/opt/multica/runtime/entrypoint", "--worker-desktop")
	command.Env = wire.Environment(environment)
	if _, provided := environment["MULTICA_DESKTOP_SCREEN"]; !provided {
		command.Env = append(command.Env, "MULTICA_DESKTOP_SCREEN="+os.Getenv("MULTICA_DESKTOP_SCREEN"))
	}
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	return command.Run()
}

func configureTaskPackages(ctx context.Context, environment map[string]string) error {
	if _, err := os.Stat("/opt/multica/runtime/entrypoint"); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "/opt/multica/runtime/entrypoint", "--worker-task")
	command.Env = wire.Environment(environment)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	return command.Run()
}

// All trusted bootstrap paths converge here, even when no provider was started.
// Shutdown has its own deadline because SIGTERM cancels the execution context.
func (s *workerLifecycle) finish(result *agent.Result, cause error, interrupted bool) error {
	b := s.bootstrap
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(b.TerminationGraceSeconds+60)*time.Second)
	defer cancel()
	cleanup, command, terminalErr := s.settle(ctx, result)
	if !s.admitted {
		// Admission may have reached the controller just before its response was
		// lost or SIGTERM arrived. Reuse the same identity and signing key.
		if err := s.admit(ctx); err != nil && !errors.Is(err, errStopRequested) {
			return errors.Join(cleanup.err, terminalErr, err)
		}
	}
	needStop := cause != nil || interrupted || result == nil || cleanup.err != nil
	if command.RequestDigest != "" && command.Nonce != "" && cleanup.writersStopped && cleanup.flushOK {
		sealCtx, stopSeal := context.WithTimeout(ctx, 30*time.Second)
		terminalErr = errors.Join(terminalErr, s.reportStorageReceipt(sealCtx, command))
		stopSeal()
	}
	var stopErr error
	if needStop || terminalErr != nil {
		stopErr = s.reportStop(ctx, cleanup.writersStopped, cleanup.flushOK, result != nil || cause == nil || interrupted)
	}
	return errors.Join(cleanup.err, stopErr, terminalErr)
}

type cleanupOutcome struct {
	writersStopped       bool
	flushOK              bool
	taskProcessesStopped bool
	localRequestsClosed  bool
	privateStateCleared  bool
	err                  error
}

// Result publication and cleanup proceed independently. A slow or failed fence
// cannot manufacture a different provider result or prevent its authentication.
func (s *workerLifecycle) settle(ctx context.Context, result *agent.Result) (cleanupOutcome, wire.SealCommand, error) {
	type reported struct {
		command wire.SealCommand
		err     error
	}
	done := make(chan reported, 1)
	if result == nil {
		done <- reported{}
	} else {
		go func(observed agent.Result) {
			resultCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			command, err := s.reportResult(resultCtx, observed)
			done <- reported{command, err}
		}(*result)
	}
	cleanup := s.cleanup(ctx, nil)
	report := <-done
	return cleanup, report.command, report.err
}

// cleanup is the common writer fence for turn completion and final shutdown.
// Final shutdown supplies a session fence for any unseen reserved attempt.
func (s *workerLifecycle) cleanup(ctx context.Context, sessionFence func(context.Context) error) cleanupOutcome {
	b := s.bootstrap
	attributes := diagnostics.TaskAttributes(b.TaskID, b.AttemptID)
	var localErr error
	turnOnly := s.session != nil && sessionFence == nil
	if s.desktop == nil && s.session != nil {
		s.desktop = s.session.currentDesktop()
	}
	if s.desktop != nil {
		if turnOnly {
			s.desktop.endTurn()
		} else {
			s.desktop.closeLaunches()
		}
	}
	if err := os.Remove(wire.ControlRoot + "/ready"); err != nil && !errors.Is(err, os.ErrNotExist) {
		localErr = err
	}
	if s.server != nil {
		localErr = errors.Join(localErr, s.server.Close())
	}
	if s.serverDone != nil {
		select {
		case <-s.serverDone:
		case <-ctx.Done():
			localErr = errors.Join(localErr, ctx.Err())
		}
	}
	checkouts := make(chan error, 1)
	go func() {
		finish := diagnostics.StartPhase("worker_checkout_stop", attributes...)
		var err error
		if s.taskAPI != nil {
			err = s.taskAPI.stopRequests(ctx)
		}
		if err == nil {
			if sessionFence != nil {
				err = sessionFence(ctx)
			} else {
				err = retryControl(ctx, s.client, b, "checkout-stop", struct{}{}, nil)
			}
		}
		finish(err)
		checkouts <- err
	}()
	finishWriters := diagnostics.StartPhase("worker_writer_stop", attributes...)
	var writerErr error
	if turnOnly && s.desktop != nil {
		writerErr = stopTaskProcesses(ctx, time.Duration(b.TerminationGraceSeconds)*time.Second, s.runner, s.desktop)
	} else {
		writerErr = stopWriters(ctx, time.Duration(b.TerminationGraceSeconds)*time.Second, s.runner, s.desktop)
	}
	finishWriters(writerErr)
	if s.runner != nil {
		s.runner.Close()
	}
	checkoutErr := <-checkouts
	writersStopped := checkoutErr == nil && writerErr == nil
	requestsClosed := checkoutErr == nil && localErr == nil
	privateCleared := false
	var flushErr error
	flushOK := false
	if writersStopped {
		privateErr := errors.Join(clearTurnPrivateFiles(), s.home.restore())
		privateCleared = privateErr == nil
		localErr = errors.Join(localErr, privateErr)
		if !turnOnly && privateCleared {
			finishFlush := diagnostics.StartPhase("worker_filesystem_flush", attributes...)
			flushErr = workspace.SyncTaskFilesystem(b.TaskRoot)
			flushOK = flushErr == nil
			finishFlush(flushErr)
		}
	}
	if !turnOnly && s.desktop != nil {
		s.desktop.Close()
	}
	return cleanupOutcome{writersStopped: !turnOnly && writersStopped, flushOK: flushOK,
		taskProcessesStopped: writerErr == nil, localRequestsClosed: requestsClosed, privateStateCleared: privateCleared,
		err: errors.Join(localErr, checkoutErr, writerErr, flushErr)}
}

func (s *workerLifecycle) reportStop(ctx context.Context, writersStopped, flushOK, interrupted bool) error {
	reason := "worker_startup_failed"
	if interrupted {
		reason = "worker_shutdown"
	}
	var command wire.StopCommand
	attributes := diagnostics.TaskAttributes(s.bootstrap.TaskID, s.bootstrap.AttemptID)
	finishRequest := diagnostics.StartPhase("worker_stop_request", attributes...)
	err := retryControl(ctx, s.client, s.bootstrap, "stop-request", map[string]string{"reason": reason}, &command)
	finishRequest(err)
	if err != nil {
		return err
	}
	if !command.Cancel || command.Revision == 0 || command.Nonce == "" {
		return errors.New("stop challenge missing")
	}
	b := s.bootstrap
	receipt := workspace.StopReceipt{TaskID: b.TaskID, AttemptID: b.AttemptID, Generation: b.Generation,
		PodUID: s.identity.PodUID, PVCUID: b.PVCUID, Revision: command.Revision, Nonce: command.Nonce,
		WritersStopped: writersStopped, FlushOK: flushOK}
	receipt.Signature = ed25519.Sign(s.signingKey, workspace.StopReceiptMessage(receipt))
	finishReceipt := diagnostics.StartPhase("worker_stop_receipt", attributes...)
	err = retryControl(ctx, s.client, b, "stop-receipt", receipt, nil)
	finishReceipt(err)
	return err
}

func (s *workerLifecycle) reportResult(ctx context.Context, result agent.Result) (wire.SealCommand, error) {
	result.Output, result.Error = redact.Text(result.Output), redact.Text(result.Error)
	payload := wire.ProviderResult{Result: result}
	raw, err := json.Marshal(payload)
	if err != nil {
		return wire.SealCommand{}, err
	}
	s.resultDigest = wire.Digest(raw)
	var command wire.SealCommand
	attributes := diagnostics.TaskAttributes(s.bootstrap.TaskID, s.bootstrap.AttemptID)
	finishResult := diagnostics.StartPhase("worker_result", attributes...)
	err = retryControl(ctx, s.client, s.bootstrap, "result", payload, &command)
	finishResult(err)
	if err != nil {
		return wire.SealCommand{}, err
	}
	if command.RequestDigest == "" || command.Nonce == "" {
		return wire.SealCommand{}, errors.New("terminal seal challenge missing")
	}
	b := s.bootstrap
	receipt := workspace.ResultReceipt{TaskID: b.TaskID, AttemptID: b.AttemptID, RequestDigest: command.RequestDigest,
		WorkerSessionID: b.WorkerSessionID, TurnSequence: b.TurnSequence,
		PodUID: s.identity.PodUID, PVCUID: b.PVCUID, Nonce: command.Nonce}
	receipt.Signature = ed25519.Sign(s.signingKey, workspace.ResultReceiptMessage(receipt))
	finishReceipt := diagnostics.StartPhase("worker_result_authentication", attributes...)
	err = retryControl(ctx, s.client, b, "result-receipt", receipt, nil)
	finishReceipt(err)
	return command, err
}

func (s *workerLifecycle) reportStorageReceipt(ctx context.Context, command wire.SealCommand) error {
	b := s.bootstrap
	receipt := workspace.SignedReceipt{TaskID: b.TaskID, AttemptID: b.AttemptID, RequestDigest: command.RequestDigest,
		PodUID: s.identity.PodUID, PVCUID: b.PVCUID, Nonce: command.Nonce, WritersStopped: true, FlushOK: true}
	receipt.Signature = ed25519.Sign(s.signingKey, workspace.ReceiptMessage(receipt))
	finishReceipt := diagnostics.StartPhase("worker_result_receipt", diagnostics.TaskAttributes(b.TaskID, b.AttemptID)...)
	err := retryControl(ctx, s.client, b, "receipt", receipt, nil)
	finishReceipt(err)
	return err
}
