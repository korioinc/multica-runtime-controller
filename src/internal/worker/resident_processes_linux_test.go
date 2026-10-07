package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"

	"golang.org/x/sys/unix"
)

// These launch checks use the installed Chrome wrapper and browser. The private
// worker desktop supplies X11; process ancestry decides launch ownership.
func TestPID1ChromeLaunchModesPreserveOwnershipAndRefuseFallback(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires the installed runtime in a private Linux PID namespace")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var desktop *residentDesktop
	var task *providerRunner
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := stopWriters(stopCtx, 100*time.Millisecond, task, desktop); err != nil {
			t.Error(err)
		}
		if task != nil {
			task.Close()
		}
		if desktop != nil {
			desktop.Close()
		}
	})
	version := exec.CommandContext(ctx, "google-chrome-stable", "--version")
	if output, err := version.CombinedOutput(); err != nil {
		t.Fatal("installed Chrome version inspection failed", err, string(output))
	}
	// The version command already completed its owned wait. Reap Chrome's
	// exited auxiliary children before checking for unintended live services.
	if err := reap(); err != nil {
		t.Fatal(err)
	}
	if count, err := writerCount(); err != nil || count != 0 {
		t.Fatal("version inspection started a desktop or browser", count, err)
	}
	t.Log("exact --version succeeded without a bootstrap or owner; no child process remained")
	descriptor, digest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	legacy, session := chromeModeBootstrap(t, descriptor, digest)
	if err := os.MkdirAll(legacy.TaskRoot, 0700); err != nil {
		t.Fatal(err)
	}
	desktop, err = startResidentDesktop(ctx, descriptor, legacy.TaskRoot)
	if err != nil {
		t.Fatal(err)
	}
	verifyChromeModeDisplay(t, ctx, desktop)
	if browsers, err := chromeModeProcesses(); err != nil || len(browsers) != 0 {
		t.Fatal("display fixture already contained Chrome", err)
	}
	for _, invalid := range []struct {
		name       string
		raw        []byte
		unreadable bool
	}{
		{name: "malformed", raw: []byte(`{"version":`)},
		{name: "unreadable", raw: []byte(`{}`), unreadable: true},
		{name: "unsupported", raw: []byte(`{"version":` + strconv.Itoa(wire.SessionProtocolVersion+1) + `}`)},
	} {
		writeChromeModeBootstrap(t, invalid.raw)
		if invalid.unreadable {
			if err := os.Chmod(wire.RequestPath, 0000); err != nil {
				t.Fatal(err)
			}
		}
		refuseChromeMode(t, ctx, desktop, invalid.name)
	}
	// Losing the exact resident init invalidates ownership while the real display
	// stays available. A broken v2 fallback would therefore start real Chrome.
	if err := signalProcess(desktop.root.identity, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case <-desktop.Done:
	case <-ctx.Done():
		t.Fatal("resident init did not exit")
	}
	verifyChromeModeDisplay(t, ctx, desktop)
	raw, _ := json.Marshal(session)
	writeChromeModeBootstrap(t, raw)
	refuseChromeMode(t, ctx, desktop, "v2 owner loss")
	if desktop.live() {
		t.Fatal("lost owner was silently replaced")
	}
	raw, _ = json.Marshal(legacy)
	writeChromeModeBootstrap(t, raw)
	command := exec.CommandContext(ctx, "/bin/sh", "-c", chromeModeBackgroundStart+"\nwait \"$!\"")
	command.Env = wire.Environment(desktop.env)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	task = &providerRunner{command: command, Done: done, closed: make(chan struct{})}
	go func() { task.waitErr = command.Wait(); close(done) }()
	task.root, err = pinProcess(command.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	var browser processIdentity
	residentAwait(t, func() bool {
		browsers, err := chromeModeProcesses()
		if err == nil && len(browsers) != 0 {
			browser = browsers[0]
			return true
		}
		return false
	}, "typed legacy launch did not start the installed Chrome browser")
	if member, err := beneath(browser.pid, task.root); err != nil || !member {
		t.Fatal("legacy Chrome escaped its task ancestry", err)
	}
	if member, err := traceAncestry(browser.pid, desktop.root.identity); err == nil && member {
		t.Fatal("legacy Chrome became resident-owned")
	}
	t.Logf("legacy Chrome pid=%d start=%d task_root_pid=%d start=%d", browser.pid, browser.start, task.root.identity.pid, task.root.identity.start)
	if err := stopWriters(ctx, 100*time.Millisecond, task, desktop); err != nil {
		t.Fatal(err)
	}
	if !task.Reaped() {
		t.Fatal("legacy full stop lost the task command wait")
	}
	if browsers, err := chromeModeProcesses(); err != nil || len(browsers) != 0 {
		t.Fatal("full stop left actual Chrome running", err)
	}
	if current, err := readProcess(browser.pid); err == nil && current.start == browser.start {
		t.Fatal("legacy browser survived the full writer fence")
	}
	t.Log("typed legacy browser remained task-owned and full stop removed it")
}

// This is the isolated-worker background recovery command from open-browser-use.
// No profile, headless mode, sandbox bypass or browser UI control is used.
const chromeModeBackgroundStart = `OBU_CHROME_STARTUP_LOG=$(mktemp /tmp/obu-chrome-startup.XXXXXX)
nohup google-chrome-stable > "$OBU_CHROME_STARTUP_LOG" 2>&1 < /dev/null &`

func chromeModeBootstrap(t *testing.T, descriptor runtimeimage.Descriptor, digest string) (wire.Bootstrap, wire.SessionBootstrap) {
	t.Helper()
	workspaceID, taskID := uuid.NewString(), uuid.NewString()
	root, err := workspace.TaskRoot(wire.WorkspaceRoot, workspaceID, taskID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	bundle := configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)}
	ref, err := descriptor.Reference("fixture.invalid/runtime@sha256:"+digest, digest, configuration.ExecutionDigest(bundle, nil))
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("fixture", 8)
	b := wire.Bootstrap{OwnerID: uuid.NewString(), WorkspaceID: workspaceID, TaskID: taskID, AttemptID: uuid.NewString(), AgentID: uuid.NewString(),
		StorageID: uuid.NewString(), RuntimeID: uuid.NewString(), Generation: 1, TaskRoot: root, NFSServer: "127.0.0.1", Provider: "codex",
		PreparedDigest: core.Digest([]byte("typed Chrome mode fixture")), PVCName: "fixture-volume", PVCUID: uuid.NewString(), RuntimeRef: ref,
		GatewayURL: "http://127.0.0.1", APICapability: token, SupervisorCapability: token, CacheCapability: token, StopCapability: token,
		ExpiresAt: time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano), Configuration: bundle, TerminationGraceSeconds: 1}
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	session := wire.SessionBootstrap{Version: wire.SessionProtocolVersion, WorkerSessionID: uuid.NewString(),
		Conversation: workspace.ConversationKey{OwnerID: b.OwnerID, WorkspaceID: workspaceID, AgentID: b.AgentID, Kind: workspace.ConversationIssue, SubjectID: uuid.NewString()},
		StorageID:    b.StorageID, WorkspaceAnchorTaskID: taskID, CompatibilityDigest: core.Digest([]byte("Chrome owner fixture")),
		TaskRoot: root, PVCName: b.PVCName, PVCUID: b.PVCUID, NFSServer: b.NFSServer, Provider: b.Provider,
		RuntimeID: b.RuntimeID, RuntimeRef: ref, GatewayURL: b.GatewayURL, ControlCapability: token, TerminationGraceSeconds: b.TerminationGraceSeconds}
	if err := session.Validate(); err != nil {
		t.Fatal(err)
	}
	return b, session
}

func writeChromeModeBootstrap(t *testing.T, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(wire.RequestPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(wire.RequestPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err := os.WriteFile(wire.RequestPath, raw, 0400); err != nil {
		t.Fatal(err)
	}
}

func verifyChromeModeDisplay(t *testing.T, ctx context.Context, desktop *residentDesktop) {
	t.Helper()
	command := exec.CommandContext(ctx, "/bin/sh", "-c", `xdpyinfo -display "$DISPLAY" >/dev/null`)
	command.Env = wire.Environment(desktop.env)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatal("isolated worker X11 unavailable", err, string(output))
	}
}

func refuseChromeMode(t *testing.T, ctx context.Context, desktop *residentDesktop, reason string) {
	t.Helper()
	requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	command := exec.CommandContext(requestCtx, "/bin/sh", "-c", chromeModeBackgroundStart+"\nwait \"$!\"")
	command.Env = wire.Environment(desktop.env)
	if err := command.Run(); err == nil || requestCtx.Err() != nil {
		t.Fatal("Chrome was not explicitly refused", reason, err, requestCtx.Err())
	}
	if browsers, err := chromeModeProcesses(); err != nil || len(browsers) != 0 {
		t.Fatal("refused Chrome launch started a replacement", reason, err)
	}
	t.Log("Chrome refused without replacement:", reason)
}

func chromeModeProcesses() ([]processIdentity, error) {
	var processes []processIdentity
	var err error
	deadline := time.Now().Add(10 * time.Second)
	for {
		processes, err = processList()
		if !errors.Is(err, errAncestryChanged) || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil {
		return nil, err
	}
	var browsers []processIdentity
	for _, process := range processes {
		executable, err := os.Readlink("/proc/" + strconv.Itoa(process.pid) + "/exe")
		if err != nil || executable != filepath.Join(filepath.Dir(wire.ChromeOriginal), "chrome") {
			continue
		}
		args, err := os.ReadFile("/proc/" + strconv.Itoa(process.pid) + "/cmdline")
		if err != nil {
			return nil, err
		}
		if !bytes.Contains(args, []byte("--type=")) {
			browsers = append(browsers, process)
		}
	}
	return browsers, nil
}

func TestPID1DesktopBusActivationRetainsResidentOwnership(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires the installed desktop in a private Linux PID namespace")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	descriptor, _, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(wire.WorkspaceRoot, "dbus-owner-")
	if err != nil {
		t.Fatal(err)
	}
	desktop, err := startResidentDesktop(ctx, descriptor, root)
	if desktop != nil {
		t.Cleanup(func() {
			stopCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := stopWriters(stopCtx, 100*time.Millisecond, nil, desktop); err != nil {
				t.Error(err)
			}
			desktop.Close()
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	const service = "ca.desrt.dconf"
	bus := func(method string, arguments ...string) ([]byte, error) {
		args := append([]string{"call", "--session", "--dest", "org.freedesktop.DBus", "--object-path", "/org/freedesktop/DBus", "--method", "org.freedesktop.DBus." + method}, arguments...)
		command := exec.CommandContext(ctx, "gdbus", args...)
		command.Env = wire.Environment(desktop.env)
		return command.CombinedOutput()
	}
	servicePID := func() (int, error) {
		raw, err := bus("GetConnectionUnixProcessID", service)
		if err != nil {
			return 0, err
		}
		return strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(string(raw)), "(uint32 "), ",)"))
	}
	if pid, err := servicePID(); err == nil {
		identity, err := readProcess(pid)
		if err != nil {
			t.Fatal(err)
		}
		if member, err := beneath(pid, desktop.root); err != nil || !member {
			t.Fatal("preexisting real D-Bus service had a foreign owner", err)
		}
		if err := signalProcess(identity, syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		residentAwait(t, func() bool { _, err := servicePID(); return err != nil }, "D-Bus service did not release its name for activation")
	}
	if output, err := bus("StartServiceByName", service, "0"); err != nil {
		t.Fatal("installed D-Bus service activation failed", err, string(output))
	}
	pid, err := servicePID()
	if err != nil {
		t.Fatal(err)
	}
	activated, err := readProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	if member, err := beneath(pid, desktop.root); err != nil || !member {
		t.Fatal("real D-Bus activation escaped resident ancestry", err)
	}
	if err := stopTaskProcesses(ctx, 100*time.Millisecond, nil, desktop); err != nil {
		t.Fatal(err)
	}
	retained, err := readProcess(pid)
	if err != nil || retained.start != activated.start {
		t.Fatal("task fence removed or replaced the activated service", err)
	}
	if member, err := beneath(pid, desktop.root); err != nil || !member {
		t.Fatal("retained activated service lost resident ownership", err)
	}
	t.Logf("actual D-Bus activated service pid=%d start=%d retained beneath resident root pid=%d", pid, activated.start, desktop.root.identity.pid)
}

func TestPID1ResidentDesktopRejectsLostDisplayContinuity(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires a fresh Linux PID namespace and the installed desktop image")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	descriptor, _, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(wire.WorkspaceRoot, "desktop-health-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	desktop, startErr := startResidentDesktop(ctx, descriptor, root)
	if desktop != nil {
		t.Cleanup(func() {
			stopCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := stopWriters(stopCtx, 100*time.Millisecond, nil, desktop); err != nil {
				t.Error(err)
			}
			desktop.Close()
		})
	}
	if startErr != nil {
		t.Fatal(startErr)
	}
	services, err := desktopServices(ctx, desktop.env, desktop.root)
	if err != nil {
		t.Fatal(err)
	}
	if err := desktop.health(ctx); err != nil {
		t.Fatal("installed services did not establish a healthy owner", err)
	}
	if err := signalProcess(services["xvfb"], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	residentAwait(t, func() bool {
		current, err := readProcess(services["xvfb"].pid)
		return err != nil || current.state == 'Z' || current.start != services["xvfb"].start
	}, "the display loss fixture did not terminate Xvfb")
	if err := desktop.health(ctx); err == nil {
		t.Fatal("a replacement display generation authorized warm continuation")
	}
	if !desktop.live() {
		t.Fatal("display continuity was tested only after losing the entire owner")
	}
}

// This isolates ownership and destructive fencing in real kernel process trees.
// Actual Cua, Chrome and unsaved-window retention use the graphical fixture.
func TestPID1ResidentTreeSurvivesTaskFencesAndBothWaitsRemainOwned(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires a fresh private Linux PID namespace")
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	resident, done := residentProcessCommand(t, root, "resident")
	pinned, err := pinProcess(resident.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	desktop := &residentDesktop{command: resident, Done: done, root: pinned}
	var runner *providerRunner
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = stopWriters(ctx, 50*time.Millisecond, runner, desktop)
		desktop.Close()
	})
	residentPID := residentFixturePID(t, root, "resident.child")
	residentBefore, err := readProcess(residentPID)
	if err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 2; turn++ {
		name := "task-" + strconv.Itoa(turn)
		command, done := residentProcessCommand(t, root, name)
		pinnedTask, err := pinProcess(command.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		runner = &providerRunner{command: command, Done: done, root: pinnedTask, closed: make(chan struct{})}
		taskPID := residentFixturePID(t, root, name+".child")
		peer, err := readProcess(taskPID)
		if err != nil {
			t.Fatal(err)
		}
		if err := desktop.beginTurn(runner); err != nil {
			t.Fatal(err)
		}
		if err := desktop.authorizeLaunch(peer); err != nil {
			t.Fatal("current task lost its launch authority", err)
		}
		desktop.endTurn()
		if err := desktop.authorizeLaunch(peer); err == nil {
			t.Fatal("settled task retained launch authority")
		}
		if err := desktop.authorizeLaunch(residentBefore); err != nil {
			t.Fatal("resident app could not launch while idle", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = stopTaskProcesses(ctx, 50*time.Millisecond, runner, desktop)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if !runner.Reaped() || command.ProcessState.ExitCode() != 19 {
			t.Fatal("selective reaping stole the task init's primary status")
		}
		if _, err := readProcess(taskPID); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("a detached task process survived settlement", err)
		}
		current, err := readProcess(residentPID)
		if err != nil || current.start != residentBefore.start || !desktop.live() {
			t.Fatal("turn cleanup replaced a resident application", err)
		}
		before, err := os.ReadFile(filepath.Join(root, "resident.writes"))
		if err != nil {
			t.Fatal(err)
		}
		residentAwait(t, func() bool {
			after, _ := os.ReadFile(filepath.Join(root, "resident.writes"))
			return len(after) > len(before)
		}, "resident activity stopped during idle")
		processes, err := processList()
		if err != nil {
			t.Fatal(err)
		}
		for _, process := range processes {
			if member, err := beneath(process.pid, desktop.root); err != nil || !member {
				t.Fatal("task or unclassified process survived settlement", process.pid, err)
			}
		}
		runner.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := stopWriters(ctx, 50*time.Millisecond, runner, desktop); err != nil {
		t.Fatal(err)
	}
	if resident.ProcessState == nil || resident.ProcessState.ExitCode() != 17 || !runner.Reaped() {
		t.Fatal("final reaping stole an owned primary status")
	}
	if desktop.live() {
		t.Fatal("dead resident root authorized continuity")
	}
}

func residentProcessCommand(t *testing.T, root, name string) (*exec.Cmd, <-chan struct{}) {
	t.Helper()
	command := exec.Command("/usr/bin/python3", "-c", residentProcessFixture, root, name)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	residentAwait(t, func() bool { _, err := os.Stat(filepath.Join(root, name+".ready")); return err == nil }, "process fixture did not start")
	return command, done
}

func residentFixturePID(t *testing.T, root, name string) int {
	t.Helper()
	var pid int
	residentAwait(t, func() bool {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err == nil {
			pid, err = strconv.Atoi(strings.TrimSpace(string(raw)))
		}
		return err == nil && pid > 1
	}, "descendant did not publish its identity")
	return pid
}

func residentAwait(t *testing.T, ready func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}

const residentProcessFixture = `
import ctypes,os,signal,sys,threading,time
root,name=sys.argv[1:]
resident=name=='resident'
libc=ctypes.CDLL(None)
if resident:libc.prctl(36,1,0,0,0)
def publish(suffix,value):
 with open(root+'/'+name+suffix,'w') as f:f.write(str(value))
def writer():
 os.setsid()
 signal.signal(signal.SIGTERM,signal.SIG_IGN)
 # Service-like names and environment markers do not grant resident ownership.
 libc.prctl(15,b'Xvfb',0,0,0)
 os.environ['MULTICA_DESKTOP_OWNED']='1'
 publish('.child',os.getpid())
 def write_forever():
  while True:
   with open(root+'/'+name+'.writes','a') as f:f.write('accepted activity\n')
   time.sleep(.005)
 if resident:
  # A zombie thread-group leader is not proof that its sibling writer stopped.
  threading.Thread(target=write_forever).start()
  libc.syscall(93 if os.uname().machine=='aarch64' else 60,0)
 write_forever()
child=os.fork()
if child==0:
 if resident and os.fork()!=0:os._exit(0)
 writer()
if resident:os.waitpid(child,0)
def finish(sig,frame):
 if not resident and os.fork()==0:
  signal.signal(signal.SIGTERM,signal.SIG_IGN)
  os.setsid()
  while True:
   with open(root+'/'+name+'.late','a') as f:f.write('late task child\n')
   time.sleep(.001)
 sys.exit(17 if resident else 19)
signal.signal(signal.SIGTERM,finish)
publish('.ready',os.getpid())
while True:time.sleep(.01)
`
