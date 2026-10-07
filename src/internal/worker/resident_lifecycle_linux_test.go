package worker

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"golang.org/x/sys/unix"
)

func TestPID1ResidentCancellationDuringStartup(t *testing.T) { residentLifecycleCase(t, "startup") }
func TestPID1ResidentCancellationWhileRunning(t *testing.T)  { residentLifecycleCase(t, "running") }
func TestPID1ResidentCancellationDuringSettlement(t *testing.T) {
	residentLifecycleCase(t, "settlement")
}
func TestPID1ResidentCancellationWhileIdle(t *testing.T)    { residentLifecycleCase(t, "idle") }
func TestPID1ResidentRootLossDrainsWithReason(t *testing.T) { residentLifecycleCase(t, "root-loss") }
func TestPID1ResidentDisplayLossDrainsWithReason(t *testing.T) {
	residentLifecycleCase(t, "display-loss")
}

// The installed desktop and real SDK runner exercise the watchdog and final
// fence. Assignment admission and actual provider behavior have separate proof.
func residentLifecycleCase(t *testing.T, phase string) {
	if os.Getpid() != 1 {
		t.Skip("requires the installed runtime in a fresh private Linux PID namespace")
	}
	descriptor, _, err := runtimeimage.Check(t.Context(), runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	s, control := residentStopFixture(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	watched := make(chan struct{})
	go func() { defer close(watched); s.watch(ctx, cancel) }()
	var startup <-chan error
	if phase == "startup" {
		done := make(chan error, 1)
		startup = done
		go func() { done <- s.ensureDesktop(ctx, descriptor, s.gateway.bootstrap.TaskRoot) }()
		residentAwait(t, func() bool {
			processes, err := processList()
			if err != nil {
				return false
			}
			for _, process := range processes {
				args, _ := os.ReadFile(filepath.Join("/proc", strconv.Itoa(process.pid), "cmdline"))
				if strings.Contains(string(args), "\x00init\x00worker\x00desktop\x00") {
					return true
				}
			}
			return false
		}, "the real desktop init did not start")
		if s.currentDesktop() != nil {
			t.Fatal("startup completed before cancellation")
		}
	} else if err := s.ensureDesktop(ctx, descriptor, s.gateway.bootstrap.TaskRoot); err != nil {
		t.Fatal(err)
	}
	var runner *providerRunner
	if phase == "running" || phase == "settlement" || strings.HasSuffix(phase, "-loss") {
		request := runnerFixtureRequest(t, phase != "settlement")
		if phase == "settlement" {
			request.Environment["PROVIDER_LATE_WRITE"] = filepath.Join(request.Options.Cwd, "late-writer")
		}
		runner = startRunnerFixture(t, request)
		s.sequence = 1
		s.lifecycle = &workerLifecycle{client: s.gateway.client, session: s, runner: runner, desktop: s.currentDesktop(),
			bootstrap: wire.Bootstrap{TaskRoot: s.gateway.bootstrap.TaskRoot, TerminationGraceSeconds: 1}}
		_ = runnerFixturePID(t, request.Options.Cwd)
		if phase == "settlement" {
			for range runner.Session.Messages {
			}
			if result, ok := <-runner.Session.Result; !ok || result.Status != "completed" {
				t.Fatal("settlement did not follow an observed SDK result")
			}
		} else {
			go func() {
				for range runner.Session.Messages {
				}
				for range runner.Session.Result {
				}
			}()
		}
	}
	if phase == "startup" {
		cancel(errStopRequested)
	} else if strings.HasSuffix(phase, "-loss") {
		identity := s.currentDesktop().root.identity
		if phase == "display-loss" {
			services, err := desktopServices(ctx, s.currentDesktop().env, s.currentDesktop().root)
			if err != nil {
				t.Fatal(err)
			}
			identity = services["xvfb"]
		}
		if err := signalProcess(identity, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
	} else {
		control.mu.Lock()
		control.cancel = true
		control.mu.Unlock()
	}
	select {
	case <-watched:
	case <-time.After(15 * time.Second):
		t.Fatal("session watchdog did not cancel within its health budget")
	}
	if startup != nil {
		select {
		case err := <-startup:
			if err == nil {
				t.Fatal("cancelled desktop startup reported readiness")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("cancelled desktop startup did not join")
		}
	}
	if strings.HasSuffix(phase, "-loss") {
		var diagnostic *diagnostics.Error
		if !errors.As(context.Cause(ctx), &diagnostic) || !strings.HasPrefix(diagnostic.Reason, "worker_desktop_") {
			t.Fatal("desktop loss had no diagnostic cause", context.Cause(ctx))
		}
		s.failureReason = diagnostic.Reason
	} else if !errors.Is(context.Cause(ctx), errStopRequested) {
		t.Fatal("authenticated cancellation lost its cause", context.Cause(ctx))
	}
	if err := s.finalStop(); err != nil {
		t.Fatal("final stop did not join and flush both trees", err)
	}
	if desktop := s.currentDesktop(); desktop == nil || desktop.live() || desktop.command.ProcessState == nil {
		t.Fatal("desktop command wait was not preserved")
	}
	if runner != nil && !runner.Reaped() {
		t.Fatal("runner command wait was not preserved")
	}
	processes, err := processList()
	if err != nil || len(processes) != 0 {
		t.Fatal("final stop left namespace children", processes, err)
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.err != nil || control.receipt == nil || !control.receipt.WritersStopped || !control.receipt.FlushOK {
		t.Fatal("no valid final storage receipt", control.err)
	}
	if strings.HasSuffix(phase, "-loss") && control.reason != s.failureReason {
		t.Fatal("final stop did not communicate the watchdog reason", control.reason)
	}
}

func TestPID1ExitedProcReadIsDisappearance(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires a fresh Linux PID namespace")
	}
	command := exec.Command("/bin/sleep", "60")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "stat"))
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal(err)
	}
	defer file.Close()
	_ = command.Process.Kill()
	_ = command.Wait()
	_, err = io.ReadAll(file)
	if !errors.Is(err, unix.ESRCH) || !processGone(err) {
		t.Fatal("an exited proc read could abort task fencing", err)
	}
}

func TestPID1VanishedParentCannotCertifyTaskSettlement(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires a fresh private Linux PID namespace")
	}
	descriptor, _, err := runtimeimage.Check(t.Context(), runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	s, _ := residentStopFixture(t)
	if err := s.ensureDesktop(t.Context(), descriptor, s.gateway.bootstrap.TaskRoot); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.finalStop(); err != nil {
			t.Error(err)
		}
	})
	root := t.TempDir()
	command := exec.Command("/usr/bin/python3", "-c", `
import os,pathlib,signal,sys,time
root=pathlib.Path(sys.argv[1])
(root/'ready').write_text('ready')
while not (root/'release').exists():time.sleep(.001)
if os.fork():os._exit(0)
os.setsid();signal.signal(signal.SIGTERM,signal.SIG_IGN)
(root/'child').write_text(str(os.getpid()))
while True:time.sleep(.01)
`, root)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	residentAwait(t, func() bool { _, err := os.Stat(filepath.Join(root, "ready")); return err == nil }, "the outsider parent did not reach its fork barrier")
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	capturedParent := false
	for _, entry := range entries {
		capturedParent = capturedParent || entry.Name() == strconv.Itoa(command.Process.Pid)
	}
	if !capturedParent {
		t.Fatal("the real parent was not captured before its fork")
	}
	if err := os.WriteFile(filepath.Join(root, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	childPID := residentFixturePID(t, root, "child")
	child, err := readProcess(childPID)
	if err != nil || child.parent != 1 {
		t.Fatal("the uncaptured child was not alive and adopted", child, err)
	}
	for _, entry := range entries {
		if entry.Name() == strconv.Itoa(childPID) {
			t.Fatal("the child existed before the enumeration barrier")
		}
	}
	if _, err := observeProcessEntries(entries); !errors.Is(err, errAncestryChanged) {
		t.Fatal("a vanished parent certified an incomplete process scan", err)
	}
	residentAwait(t, func() bool {
		processes, err := processList()
		if err != nil {
			return false
		}
		for _, process := range processes {
			if process.pid == childPID && process.start == child.start {
				return true
			}
		}
		return false
	}, "a fresh process scan did not find the uncaptured child")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := stopTaskProcesses(ctx, 50*time.Millisecond, nil, s.currentDesktop()); err != nil {
		t.Fatal(err)
	}
	if _, err := readProcess(childPID); !processGone(err) {
		t.Fatal("the new outsider survived task fencing", err)
	}
	if !s.currentDesktop().live() {
		t.Fatal("task fencing terminated the resident owner")
	}
}

type residentStopControl struct {
	mu      sync.Mutex
	cancel  bool
	reason  string
	receipt *workspace.SessionStopReceipt
	err     error
}

func residentStopFixture(t *testing.T) (*sessionSupervisor, *residentStopControl) {
	t.Helper()
	public, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	session, assignment := pollAssignment(t, 10<<10)
	if os.Getpid() != 1 {
		session.TaskRoot = t.TempDir()
	}
	if err := os.MkdirAll(session.TaskRoot, 0700); err != nil {
		t.Fatal(err)
	}
	control := &residentStopControl{}
	command := wire.SessionStopCommand{Revision: 1, Nonce: uuid.NewString(), TurnSequence: 1, ControllerWritersStopped: true, Reason: "native_proof_cancel"}
	client := &http.Client{Transport: enrollmentTransport(func(r *http.Request) (*http.Response, error) {
		control.mu.Lock()
		defer control.mu.Unlock()
		action := filepath.Base(r.URL.Path)
		var response any = struct{}{}
		if action == "challenge" {
			var request wire.SessionChallengeRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				return nil, err
			}
			response = workspace.SessionChallenge{Operation: request.Operation, BodyDigest: request.BodyDigest, WorkerSessionID: session.WorkerSessionID,
				PodUID: assignment.PodUID, PVCUID: session.PVCUID, TurnSequence: 1, Nonce: uuid.NewString(), ExpiresAt: time.Now().Add(time.Minute)}
		} else if action != "admit" {
			var request wire.SessionControlRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				return nil, err
			}
			if request.Proof.Operation != action || request.Proof.BodyDigest != wire.Digest(request.Body) || !ed25519.Verify(public, workspace.SessionProofMessage(request.Proof), request.Proof.Signature) {
				control.err = errors.New("invalid signed control request")
			}
			switch action {
			case workspace.SessionOperationStopControl:
				response = wire.SessionStopCommand{}
				if control.cancel {
					response = command
				}
			case workspace.SessionOperationStopRequest:
				var body wire.SessionStopRequest
				if err := json.Unmarshal(request.Body, &body); err != nil {
					return nil, err
				}
				control.reason, command.Reason = body.Reason, body.Reason
				response = command
			case workspace.SessionOperationStopReceipt:
				var receipt workspace.SessionStopReceipt
				if err := json.Unmarshal(request.Body, &receipt); err != nil {
					return nil, err
				}
				if receipt.WorkerSessionID != session.WorkerSessionID || receipt.PodUID != assignment.PodUID || receipt.PVCUID != session.PVCUID || receipt.Nonce != command.Nonce || !ed25519.Verify(public, workspace.SessionStopReceiptMessage(receipt), receipt.Signature) {
					control.err = errors.New("invalid final storage receipt")
				}
				control.receipt = &receipt
			default:
				control.err = errors.New("unexpected session operation: " + action)
			}
		}
		raw, err := json.Marshal(response)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})}
	t.Cleanup(client.CloseIdleConnections)
	return &sessionSupervisor{admitted: true, gateway: sessionGateway{bootstrap: session, identity: wire.SessionAdmission{PodUID: assignment.PodUID, PublicKey: public}, key: key, client: client}}, control
}
