//go:build linux

package initprocess

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Timing logs report two seconds of container scheduling tolerance alongside
// the production drain bound. Functional assertions do not freeze that timing.
const schedulingTolerance = 2 * time.Second

func TestInitClosedContract(t *testing.T) {
	for _, args := range [][]string{
		nil, {"/bin/true"}, {"init", "controller"}, {"worker", "serve"},
		{"controller", "extra"}, {"worker", "run"}, {"worker", "run", ""},
		{"worker", "run", "private-socket", "extra"},
		{"worker", "desktop"}, {"worker", "desktop", ""},
		{"worker", "desktop", "private-socket", "extra"},
	} {
		t.Run(strings.Join(args, "/"), func(t *testing.T) {
			fixture := startFixture(t, "inspect", args, nil)
			fixture.wait(t, exitFailure)
			if _, err := os.Stat(filepath.Join(fixture.root, "primary.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid arguments started a primary: %v", err)
			}
		})
	}
	for _, args := range [][]string{{"controller"}, {"worker", "run", "private-socket"}, {"worker", "desktop", "private-socket"}} {
		t.Run(strings.Join(args, "/"), func(t *testing.T) {
			fixture := startFixture(t, "inspect", args, nil)
			fixture.wait(t, 0)
			var actual fixtureIdentity
			readJSON(t, filepath.Join(fixture.root, "primary.json"), &actual)
			executable, _ := os.Executable()
			if actual.Executable != executable || strings.Join(actual.Args, "\x00") != strings.Join(args, "\x00") {
				t.Fatalf("launch changed executable or arguments: %+v", actual)
			}
			if actual.PGID != actual.PID || actual.PPID != fixture.command.Process.Pid {
				t.Fatalf("launch did not preserve its process boundary: %+v", actual)
			}
		})
	}
}

func TestInitPrimaryStatus(t *testing.T) {
	for _, code := range []int{0, 7, 143, 255} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			fixture := startFixture(t, "exit-"+strconv.Itoa(code), []string{"controller"}, []string{"RUNTIME_INIT_TEST_EXIT_BEFORE_SPAWN_RETURN=1"})
			fixture.wait(t, code)
		})
	}
	t.Run("signal", func(t *testing.T) {
		fixture := startFixture(t, "signal-default", []string{"controller"}, nil)
		fixture.await(t, "ready")
		if err := fixture.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		fixture.wait(t, signalExitOffset+int(syscall.SIGTERM))
	})
}

func TestInitSpawnErrors(t *testing.T) {
	for name, code := range map[string]int{"missing": exitNotFound, "denied": exitCannotExecute, "internal": exitFailure} {
		t.Run(name, func(t *testing.T) {
			fixture := startFixture(t, "inspect", []string{"controller"}, []string{"RUNTIME_INIT_TEST_SPAWN_ERROR=" + name})
			fixture.wait(t, code)
			if _, err := os.Stat(filepath.Join(fixture.root, "primary.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("spawn failure ran primary: %v", err)
			}
		})
	}
}

func TestInitSignalsAndSpawnOrdering(t *testing.T) {
	for _, control := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT} {
		for _, duringSpawn := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/spawn=%v", control, duringSpawn), func(t *testing.T) {
				env := []string{"TINI_KILL_PROCESS_GROUP=1"}
				if duringSpawn {
					env = append(env, "RUNTIME_INIT_TEST_HOLD_SPAWN=1")
				}
				fixture := startFixture(t, "controlled", []string{"controller"}, env)
				fixture.await(t, "ready")
				if duringSpawn {
					fixture.await(t, "spawn-held")
				}
				if err := fixture.command.Process.Signal(control); err != nil {
					t.Fatal(err)
				}
				if duringSpawn {
					fixture.publish(t, "spawn-release")
				}
				fixture.await(t, "signal-"+strconv.Itoa(int(control)))
				select {
				case err := <-fixture.done:
					fixture.waited = true
					t.Fatalf("init exited before application cleanup: %v", err)
				default:
				}
				fixture.publish(t, "finish")
				fixture.wait(t, 7)
				if _, err := os.Stat(filepath.Join(fixture.root, "witness-signal")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("init forwarded to the process group: %v", err)
				}
			})
		}
	}
	t.Run("terminal-signals-discarded", func(t *testing.T) {
		fixture := startFixture(t, "controlled", []string{"controller"}, nil)
		fixture.await(t, "ready")
		for _, control := range []syscall.Signal{syscall.SIGTTIN, syscall.SIGTTOU} {
			if err := fixture.command.Process.Signal(control); err != nil {
				t.Fatal(err)
			}
		}
		// The subsequent TERM must be the first signal the primary observes.
		if err := fixture.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		fixture.await(t, "signal-"+strconv.Itoa(int(syscall.SIGTERM)))
		fixture.publish(t, "finish")
		fixture.wait(t, 7)
	})
}

func TestInitActiveOrphansAndControllerHelpers(t *testing.T) {
	fixture := startFixture(t, "controller-helpers", []string{"controller"}, nil)
	fixture.await(t, "ready")
	var pids []int
	readJSON(t, filepath.Join(fixture.root, "orphan-pids.json"), &pids)
	await(t, func() bool {
		for _, pid := range pids {
			if !errors.Is(unix.Kill(pid, 0), unix.ESRCH) {
				return false
			}
		}
		return true
	}, "adopted zombies remained while primary was active")
	select {
	case err := <-fixture.done:
		fixture.waited = true
		t.Fatalf("primary did not remain active: %v", err)
	default:
	}
	if err := fixture.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	fixture.wait(t, 0)
	var results []int
	readJSON(t, filepath.Join(fixture.root, "helpers.json"), &results)
	if fmt.Sprint(results) != "[17 19 23]" {
		t.Fatalf("primary lost independent helper wait statuses: %v", results)
	}
}

func TestInitOrphanStormForwardsCancellation(t *testing.T) {
	requirePID1(t)
	fixture := startFixture(t, "storm", []string{"controller"}, nil)
	fixture.await(t, "storm-active")
	started := time.Now()
	if err := fixture.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	fixture.await(t, "signal-"+strconv.Itoa(int(syscall.SIGTERM)))
	t.Logf("orphan-storm cancellation observed after %s; drain budget=%s, scheduling tolerance=%s", time.Since(started), reapWindow, schedulingTolerance)
	fixture.publish(t, "finish")
	fixture.wait(t, 7)
}

func TestInitPrimaryExitBounded(t *testing.T) {
	requirePID1(t)
	for _, mode := range []string{"live-orphan", "storm-exit"} {
		t.Run(mode, func(t *testing.T) {
			fixture := startFixture(t, mode, []string{"controller"}, nil)
			fixture.await(t, "ready")
			if mode == "storm-exit" {
				fixture.await(t, "storm-active")
			}
			started := time.Now()
			fixture.publish(t, "finish")
			fixture.wait(t, 7)
			t.Logf("primary exit with %s observed after %s; drain budget=%s, scheduling tolerance=%s", mode, time.Since(started), reapWindow, schedulingTolerance)
			var pids []int
			readJSON(t, filepath.Join(fixture.root, "orphan-pids.json"), &pids)
			if len(pids) != 1 || unix.Kill(pids[0], 0) != nil {
				t.Fatalf("init waited for or killed the remaining live adoptee: %v", pids)
			}
		})
	}
}

func TestInitOwnershipFailures(t *testing.T) {
	for _, scenario := range []string{"lost", "wait-error", "signal-error", "esrch", "process-done", "final-drain-error", "busy-drain", "final-drain-stream", "interrupted-wait", "fallback"} {
		t.Run(scenario, func(t *testing.T) {
			executable, _ := os.Executable()
			command := exec.Command(executable, "init-fixture", "ownership", scenario)
			command.Env = append(os.Environ(), "RUNTIME_INIT_TEST_DIR="+t.TempDir())
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("ownership fixture failed: %v\n%s", err, output)
			}
			t.Logf("%s", output)
		})
	}
}

type processFixture struct {
	command *exec.Cmd
	done    chan error
	waited  bool
	root    string
	log     string
}

func startFixture(t *testing.T, mode string, args, extraEnv []string) *processFixture {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	logPath := filepath.Join(root, "output.log")
	output, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, append([]string{"init"}, args...)...)
	command.Dir = root
	command.Env = append(os.Environ(), "RUNTIME_INIT_TEST_MODE="+mode, "RUNTIME_INIT_TEST_DIR="+root)
	command.Env = append(command.Env, extraEnv...)
	command.Stdout, command.Stderr = output, output
	fixture := &processFixture{command: command, done: make(chan error, 1), root: root, log: logPath}
	if err := command.Start(); err != nil {
		output.Close()
		t.Fatal(err)
	}
	go func() { fixture.done <- command.Wait(); output.Close() }()
	t.Cleanup(func() {
		if !fixture.waited {
			_ = command.Process.Kill()
			<-fixture.done
		}
		var identity fixtureIdentity
		data, _ := os.ReadFile(filepath.Join(root, "primary.json"))
		if json.Unmarshal(data, &identity) == nil && identity.PGID > 1 {
			_ = unix.Kill(-identity.PGID, syscall.SIGKILL)
		}
		if os.Getpid() == 1 {
			deadline := time.Now().Add(reapWindow + schedulingTolerance)
			for time.Now().Before(deadline) {
				var status unix.WaitStatus
				_, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
				if errors.Is(err, unix.ECHILD) {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
	})
	return fixture
}

func (f *processFixture) await(t *testing.T, marker string) {
	t.Helper()
	await(t, func() bool { _, err := os.Stat(filepath.Join(f.root, marker)); return err == nil }, "missing fixture marker: "+marker)
}

func (f *processFixture) publish(t *testing.T, marker string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.root, marker), nil, 0600); err != nil {
		t.Fatal(err)
	}
}

func (f *processFixture) wait(t *testing.T, expected int) {
	t.Helper()
	select {
	case err := <-f.done:
		f.waited = true
		code := 0
		if err != nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatal(err)
			}
			code = exit.ExitCode()
		}
		if code != expected {
			log, _ := os.ReadFile(f.log)
			t.Fatalf("exit code %d, want %d\n%s", code, expected, log)
		}
	case <-time.After(10 * time.Second):
		log, _ := os.ReadFile(f.log)
		t.Fatalf("init did not exit\n%s", log)
	}
}

func await(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(message)
}

func readJSON(t *testing.T, path string, target any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatal(err)
	}
}

func requirePID1(t *testing.T) {
	t.Helper()
	if os.Getpid() != 1 {
		t.Skip("requires a fresh isolated Linux PID namespace with the test binary as PID 1")
	}
}
