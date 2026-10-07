package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/multica-ai/multica/server/pkg/agent"
	"golang.org/x/sys/unix"
)

func TestPID1RunnerExitWithoutResultCannotComplete(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1")
	}
	for _, ending := range []string{"empty", "truncated"} {
		t.Run(ending, func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "fixture-result-"+ending)
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			_ = listener.SetDeadline(time.Now().Add(10 * time.Second))
			command := exec.Command(os.Args[0], "init", "worker", "run", socket)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			runner := &providerRunner{command: command, Done: done, closed: make(chan struct{})}
			go func() { runner.waitErr = command.Wait(); close(done) }()
			t.Cleanup(func() {
				runner.Cancel()
				runner.Close()
			})
			conn, err := listener.AcceptUnix()
			if err != nil {
				t.Fatal(err)
			}
			runner.conn = conn
			if err := authenticateRunner(conn, runner); err != nil {
				t.Fatal(err)
			}
			if err := writeRunnerPacket(conn, runnerRequest{}, runnerInputLimit); err != nil {
				t.Fatal(err)
			}
			var ready runnerFrame
			if err := readRunnerPacket(conn, &ready, runnerFrameLimit); err != nil {
				t.Fatal(err)
			}
			messages, results := make(chan agent.Message), make(chan agent.Result, 1)
			runner.received = make(chan struct{})
			go runner.receive(messages, results)
			result, err := drainProvider(t.Context(), func() {}, &agent.Session{Messages: messages, Result: results}, http.DefaultClient, wire.Bootstrap{}, wire.Run{}, time.Now())
			if result != nil || err == nil {
				t.Fatal("an empty or truncated result connection manufactured completion", err)
			}
			<-done
			if !runner.Reaped() || runner.waitErr != nil {
				t.Fatal("fixture did not exercise a successful init exit without a result", runner.waitErr)
			}
		})
	}
}

// This child deliberately exits successfully after handshake without an SDK
// result. Production RunProvider remains covered by the other runner fixtures.
func runMissingResultFixture(socket string) int {
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return 1
	}
	defer conn.Close()
	var request runnerRequest
	if readRunnerPacket(conn, &request, runnerInputLimit) != nil || writeRunnerPacket(conn, runnerFrame{Kind: "ready"}, runnerFrameLimit) != nil {
		return 1
	}
	if strings.HasSuffix(socket, "truncated") {
		if _, err := conn.Write([]byte{0, 0}); err != nil {
			return 1
		}
	}
	return 0
}

func TestPID1SDKStartupFailureCannotComplete(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1")
	}
	request := runnerFixtureRequest(t, false)
	request.ExecutablePath = filepath.Join(request.Options.Cwd, "unavailable-provider")
	runner := startRunnerFixture(t, request)
	result, err := drainProvider(t.Context(), func() {}, runner.Session, http.DefaultClient, wire.Bootstrap{}, wire.Run{}, time.Now())
	if err != nil || result == nil || result.Status != "failed" {
		t.Fatal("SDK startup failure did not preserve its failed outcome", result, err)
	}
}

func TestPID1NestedInitDeathFencesLiveRunner(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1 and Python")
	}
	request := runnerFixtureRequest(t, true)
	request.Environment["PROVIDER_LATE_WRITE"] = filepath.Join(request.Options.Cwd, "late-writer")
	runner := startRunnerFixture(t, request)
	writer := runnerFixturePID(t, request.Options.Cwd)
	peer := killRunnerInit(t, runner)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := wire.Bootstrap{TaskRoot: request.Options.Cwd, TerminationGraceSeconds: 1}
	result, err := drainProvider(ctx, cancel, runner.Session, http.DefaultClient, b, wire.Run{}, time.Now())
	if err == nil || result != nil {
		t.Fatal("init death with an open connection manufactured a provider outcome", err)
	}
	cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	lifecycle := workerLifecycle{bootstrap: b, runner: runner}
	cleanup := lifecycle.cleanup(cleanupCtx, func(context.Context) error { return nil })
	if cleanup.err != nil || !cleanup.writersStopped || !cleanup.flushOK {
		t.Fatal("init death prevented the existing cancellation and storage fence", cleanup.err)
	}
	for _, pid := range []int{peer, writer} {
		if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("init death left a runner or detached writer after the fence", pid, err)
		}
	}
}

func TestPID1RunnerRejectsUnrelatedAndDeadInitPeers(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1 and Python")
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	for _, deadInit := range []bool{false, true} {
		t.Run(fmt.Sprintf("dead-init-%t", deadInit), func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "peer.sock")
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("/bin/sleep", "60")
			if deadInit {
				command = exec.Command(os.Args[0], "init", "worker", "run", socket)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			runner := &providerRunner{command: command, Done: done, closed: make(chan struct{})}
			go func() { runner.waitErr = command.Wait(); close(done) }()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := stopWriters(ctx, 50*time.Millisecond, runner, nil); err != nil {
					t.Error(err)
				}
			})
			var unrelated *exec.Cmd
			if !deadInit {
				unrelated = exec.Command("/usr/bin/python3", "-c", "import socket,sys; s=socket.socket(socket.AF_UNIX); s.connect(sys.argv[1]); sys.exit(9 if s.recv(1) else 0)", socket)
				if err := unrelated.Start(); err != nil {
					t.Fatal(err)
				}
			}
			conn, err := listener.AcceptUnix()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			runner.conn = conn
			if deadInit {
				killRunnerInit(t, runner)
			}
			if err := authenticateRunner(conn, runner); err == nil {
				t.Fatal("an unrelated or reparented peer gained execution input authority")
			}
			_ = conn.Close()
			if unrelated != nil {
				if err := unrelated.Wait(); err != nil {
					t.Fatal("unrelated peer obtained execution input", err)
				}
			}
		})
	}
}

func killRunnerInit(t *testing.T, runner *providerRunner) int {
	t.Helper()
	raw, err := runner.conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var peer *unix.Ucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		peer, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil || credentialErr != nil || peer == nil {
		t.Fatal("runner peer identity unavailable", err, credentialErr)
	}
	if err := runner.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runner.Done:
	case <-t.Context().Done():
		t.Fatal("killed init was not joined")
	}
	if !runner.Reaped() || runner.waitErr == nil {
		t.Fatal("nonzero init exit lost its direct wait evidence")
	}
	if err := syscall.Kill(int(peer.Pid), 0); err != nil {
		t.Fatal("fixture did not leave the SDK runner alive after init death", err)
	}
	return int(peer.Pid)
}
