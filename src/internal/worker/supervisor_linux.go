package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/multica-ai/multica/server/pkg/agent"
	"golang.org/x/sys/unix"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"
)

func Serve(ctx context.Context) (resultErr error) {
	if os.Getpid() != 1 {
		return errors.New("worker supervisor requires Linux PID 1")
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return err
	}
	b, session, raw, err := readPodBootstrap(wire.RequestPath)
	if err != nil {
		return err
	}
	if wire.Digest(raw) != os.Getenv("MULTICA_REQUEST_DIGEST") || os.Getenv("POD_UID") == "" {
		return errors.New("worker request or Pod identity differs from admission")
	}
	publicKey, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	client := gatewayClient()
	defer client.CloseIdleConnections()
	defer syscall.Kill(-1, syscall.SIGKILL)
	if session != nil {
		gateway := sessionGateway{bootstrap: *session, client: client, key: signingKey,
			identity: wire.SessionAdmission{WorkerSessionID: session.WorkerSessionID, PublicKey: publicKey,
				PodUID: os.Getenv("POD_UID"), PVCUID: session.PVCUID, BootstrapDigest: wire.Digest(raw)}}
		return serveSession(ctx, gateway)
	}
	slog.Info("worker supervisor started", diagnostics.TaskAttributes(b.TaskID, b.AttemptID)...)
	lifecycle := workerLifecycle{bootstrap: b, client: client, signingKey: signingKey,
		identity: workerIdentity{PublicKey: publicKey, PodUID: os.Getenv("POD_UID"), PVCUID: b.PVCUID, BootstrapDigest: wire.Digest(raw)}}
	providerCtx, cancelProvider := context.WithCancel(ctx)
	var result *agent.Result
	var watched <-chan struct{}
	defer func() {
		interrupted := providerCtx.Err() != nil
		cancelProvider()
		if watched != nil {
			<-watched
		}
		resultErr = errors.Join(resultErr, lifecycle.finish(result, resultErr, interrupted))
	}()
	// The stop key is pinned before image checks, input, HOME writes, or desktop
	// startup. Failure anywhere after this point still has a cleanup channel.
	if err := lifecycle.admit(providerCtx); err != nil {
		return err
	}
	done := make(chan struct{})
	watched = done
	go func() { defer close(done); watchStop(providerCtx, cancelProvider, client, b) }()
	result, resultErr = lifecycle.execute(providerCtx, cancelProvider, nil)
	return resultErr
}

// execute is the only provider launch path for legacy attempts and warm turns.
// The caller owns final cleanup and never calls this twice for one assignment.
func (lifecycle *workerLifecycle) execute(providerCtx context.Context, cancelProvider context.CancelFunc, assigned *wire.Run) (*agent.Result, error) {
	b, client := lifecycle.bootstrap, lifecycle.client
	attributes := diagnostics.TaskAttributes(b.TaskID, b.AttemptID)
	finishImage := diagnostics.StartPhase("worker_image_verification", attributes...)
	d, digest, err := runtimeimage.Check(providerCtx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err == nil {
		err = runtimeimage.Match(d, digest, b.RuntimeRef)
	}
	if err == nil {
		err = runtimeimage.CheckReceipt(wire.ControlRoot, d, digest)
	}
	finishImage(err)
	if err != nil {
		return nil, err
	}
	var run wire.Run
	if err := startupControl(providerCtx, client, b, "input", nil, &run); err != nil {
		return nil, err
	}
	if assigned != nil {
		want, wantErr := json.Marshal(assigned)
		got, gotErr := json.Marshal(run)
		if wantErr != nil || gotErr != nil || !bytes.Equal(want, got) {
			return nil, errors.New("execution input differs from the accepted assignment")
		}
	}
	if err := providerCtx.Err(); err != nil {
		return nil, err
	}
	if lifecycle.session != nil {
		if err := configuration.ValidateHomeConfiguration(b.Configuration); err != nil {
			return nil, err
		}
	}
	if lifecycle.home == nil {
		lifecycle.home, err = captureManagedHome(wire.Home, b.Configuration)
		if err != nil {
			return nil, err
		}
	}
	if err := lifecycle.home.restore(); err != nil {
		return nil, err
	}
	if err := clearTurnPrivateFiles(); err != nil {
		return nil, err
	}
	environment, err := initializeTask(providerCtx, d, b, &run, lifecycle.home)
	if err != nil {
		return nil, err
	}
	if err := configureTaskPackages(providerCtx, environment); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:9080")
	if err != nil {
		return nil, err
	}
	lifecycle.taskAPI = relay(b, client)
	lifecycle.server = &http.Server{Handler: lifecycle.taskAPI, ReadHeaderTimeout: 10 * time.Second, MaxHeaderBytes: 64 << 10}
	lifecycle.serverDone = make(chan struct{})
	go func() {
		defer close(lifecycle.serverDone)
		if err := lifecycle.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cancelProvider()
		}
	}()
	if err := startupControl(providerCtx, client, b, "admit", lifecycle.identity, nil); err != nil {
		return nil, err
	}
	executable, ok := d.Providers[run.Provider]
	if !ok || run.Provider != b.Provider {
		return nil, errors.New("provider differs from admitted image")
	}
	// This permission is intentionally consumed once. A lost response cannot
	// authorize another provider process.
	finishStart := diagnostics.StartPhase("worker_execution_permission", attributes...)
	err = control(providerCtx, client, b, http.MethodPost, "start", struct{}{}, nil)
	finishStart(err)
	if err != nil {
		return nil, err
	}
	if err := providerCtx.Err(); err != nil {
		return nil, err
	}
	finishDesktop := diagnostics.StartPhase("worker_desktop", attributes...)
	if lifecycle.session != nil {
		err = lifecycle.session.ensureDesktop(providerCtx, d, b.TaskRoot)
		if err == nil {
			lifecycle.desktop = lifecycle.session.currentDesktop()
			lifecycle.desktop.overlay(environment)
		}
	} else {
		err = startDesktop(providerCtx, environment)
	}
	finishDesktop(err)
	if err != nil {
		return nil, err
	}
	if err := providerCtx.Err(); err != nil {
		return nil, err
	}
	finishProvider := diagnostics.StartPhase("worker_provider_startup", attributes...)
	providerStarted := time.Now()
	runner, err := startRunner(providerCtx, runnerRequest{
		Provider: run.Provider, ExecutablePath: executable.Path, CLIVersion: executable.Version,
		Environment: environment, Prompt: run.Prompt, Options: run.Options,
		TaskID: b.TaskID, RuntimeID: b.RuntimeID, DaemonVersion: d.Daemon.Version,
		CodexVersion: d.Providers["codex"].Version,
	}, lifecycle.desktop)
	// Even a failed handshake may have started init. Keep its sole Wait owner
	// attached to shutdown before returning through any failure path.
	lifecycle.runner = runner
	if err != nil {
		finishProvider(err)
		return nil, err
	}
	session := runner.Session
	if session == nil || session.Result == nil {
		err = errors.New("provider returned no execution session")
		finishProvider(err)
		return nil, err
	}
	if err := os.WriteFile(wire.ControlRoot+"/ready", []byte(b.AttemptID), 0600); err != nil {
		finishProvider(err)
		return nil, err
	}
	finishProvider(nil)
	finishDrain := diagnostics.StartPhase("worker_provider_drain", attributes...)
	result, err := drainProvider(providerCtx, cancelProvider, session, client, b, run, providerStarted)
	finishDrain(err)
	return result, err
}

func drainProvider(ctx context.Context, cancel context.CancelFunc, session *agent.Session, client *http.Client, b wire.Bootstrap, run wire.Run, providerStarted time.Time) (*agent.Result, error) {
	messages := session.Messages
	results := session.Result
	var final *agent.Result
	var deliveryErr error
	cancelled := ctx.Done()
	var stopDeadline <-chan time.Time
	var stopTimer *time.Timer
	defer func() {
		if stopTimer != nil {
			stopTimer.Stop()
		}
	}()
	var sequence uint64
	firstOutput := true
	lastActivity := time.Now()
	var inFlight int32
	watchdogFired := false
	toolBudget := run.Options.IdleWatchdogTimeout
	if value := run.Environment["MULTICA_AGENT_TOOL_WATCHDOG"]; value != "" {
		var err error
		toolBudget, err = time.ParseDuration(value)
		if err != nil || toolBudget < 0 {
			return nil, errors.New("invalid tool watchdog")
		}
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	send := func(event wire.ProviderEvent) error {
		sequence++
		event.Sequence = sequence
		event = event.Redacted()
		sendCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		for {
			err := control(sendCtx, client, b, http.MethodPost, "event", event, nil)
			if err == nil || !errors.Is(err, errGatewayUnavailable) {
				return diagnostics.Wrap("worker_event_delivery_failed", err)
			}
			if err := waitControl(sendCtx); err != nil {
				return diagnostics.Wrap("worker_event_delivery_failed", err)
			}
		}
	}
	for {
		if messages == nil && final != nil {
			if deliveryErr == nil && len(final.Usage) > 0 {
				if err := send(wire.ProviderEvent{Usage: final.Usage}); err != nil {
					deliveryErr = err
				}
			}
			if watchdogFired && final.Status != "completed" && (session.TerminalObserved == nil || !session.TerminalObserved()) {
				final.Status = "timeout"
				final.Error = "provider inactivity deadline exceeded"
			}
			return final, deliveryErr
		}
		select {
		case <-cancelled:
			cancelled = nil
			stopTimer = time.NewTimer(time.Duration(b.TerminationGraceSeconds) * time.Second)
			stopDeadline = stopTimer.C
		case <-stopDeadline:
			return final, errors.Join(deliveryErr, diagnostics.Wrap("worker_provider_termination_unproven", errors.New("provider termination remained unproven")))
		case message, ok := <-messages:
			if !ok {
				messages = nil
				continue
			}
			if deliveryErr != nil {
				// Keep draining after cancellation so a blocked adapter can publish
				// its actual outcome even when telemetry is no longer available.
				continue
			}
			lastActivity = time.Now()
			if firstOutput && message.Type == agent.MessageText && message.Content != "" {
				firstOutput = false
				slog.Info("worker provider first output", append(diagnostics.TaskAttributes(b.TaskID, b.AttemptID), "elapsed", time.Since(providerStarted))...)
			}
			if message.Type == agent.MessageToolUse {
				inFlight++
			}
			if message.Type == agent.MessageToolResult && inFlight > 0 {
				inFlight--
			}
			if err := send(wire.ProviderEvent{Message: &message}); err != nil {
				deliveryErr = err
				cancel()
			}
		case result, ok := <-results:
			if !ok {
				return nil, errors.Join(deliveryErr, diagnostics.Wrap("worker_provider_result_missing", errors.New("provider ended without a result")))
			}
			final = &result
			results = nil
			slog.Info("worker provider terminal observed", append(diagnostics.TaskAttributes(b.TaskID, b.AttemptID), "completed", result.Status == "completed", "elapsed", time.Since(providerStarted))...)
		case <-tick.C:
			if run.Options.IdleWatchdogTimeout <= 0 || final != nil || len(messages) > 0 || session.TerminalObserved != nil && session.TerminalObserved() {
				continue
			}
			activity := lastActivity
			active := inFlight
			if session.ToolActivity != nil {
				count, last := session.ToolActivity()
				active = count
				if last.After(activity) {
					activity = last
				}
			}
			budget := run.Options.IdleWatchdogTimeout
			if active > 0 {
				budget = toolBudget
			}
			if budget > 0 && time.Since(activity) > budget {
				if active > 0 && session.InterruptBackgroundTools != nil && session.InterruptBackgroundTools() {
					lastActivity = time.Now()
					continue
				}
				watchdogFired = true
				cancel()
			}
		}
	}
}

func reap() error {
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		if err != nil && err != unix.ECHILD && err != unix.EINTR {
			return err
		}
		if pid <= 0 {
			return nil
		}
	}
}

// Stop the whole PID namespace, including desktop services and orphaned tools.
// Freeze before the final scan so a fork/parent-exit race cannot hide a writer.
func stopWriters(ctx context.Context, grace time.Duration, runner *providerRunner, desktop *residentDesktop) error {
	if os.Getpid() != 1 {
		return errors.New("writer fencing requires PID 1")
	}
	if err := syscall.Kill(-1, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	reapChildren := func() (bool, error) {
		if runner != nil {
			select {
			case <-runner.Done:
				if !runner.Reaped() {
					return false, diagnostics.Wrap("worker_runner_reaping_unproven", errors.New("runner supervisor exit was not reaped"))
				}
				runner = nil
			default:
				// cmd.Wait owns the direct init child. wait4(-1) must not
				// steal that child's status, even after a namespace kill.
				return false, nil
			}
		}
		if desktop != nil {
			select {
			case <-desktop.Done:
				if desktop.command.ProcessState == nil {
					return false, diagnostics.Wrap("worker_desktop_reaping_unproven", errors.New("desktop supervisor exit was not reaped"))
				}
				desktop = nil
			default:
				return false, nil
			}
		}
		return true, reap()
	}
	deadline := time.Now().Add(grace)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		ready, err := reapChildren()
		if err != nil {
			return err
		}
		if ready {
			count, err := writerCount()
			if err != nil {
				return err
			}
			if count == 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	if err := syscall.Kill(-1, syscall.SIGSTOP); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	for {
		if err := syscall.Kill(-1, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		ready, err := reapChildren()
		if err != nil {
			return err
		}
		if ready {
			count, err := writerCount()
			if err != nil {
				return err
			}
			if count == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func writerCount() (int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	count := 0
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err == nil && pid > 1 {
			count++
		}
	}
	return count, nil
}
