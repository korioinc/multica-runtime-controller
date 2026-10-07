package worker

import (
	"context"
	"errors"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"golang.org/x/sys/unix"
)

const desktopPacketLimit = 64 << 10

type desktopInput struct {
	Environment map[string]string `json:"environment"`
	TaskRoot    string            `json:"taskRoot"`
}

type desktopRequest struct {
	Kind       string   `json:"kind"`
	Executable string   `json:"executable,omitempty"`
	Arguments  []string `json:"arguments,omitempty"`
	Workdir    string   `json:"workdir,omitempty"`
}

type desktopResponse struct {
	OK bool `json:"ok"`
}

type residentDesktop struct {
	command *exec.Cmd
	Done    <-chan struct{}
	waitErr error
	root    *processRoot
	conn    *net.UnixConn
	env     map[string]string

	controlMu  sync.Mutex
	launchMu   sync.Mutex
	runner     *providerRunner
	closed     bool
	listener   *net.UnixListener
	current    *net.UnixConn
	launchDone chan struct{}
	cancel     sync.Once
}

func (d *residentDesktop) live() bool {
	if d == nil || d.root == nil {
		return false
	}
	select {
	case <-d.Done:
		return false
	default:
		return d.root.alive()
	}
}

func (d *residentDesktop) Cancel() {
	if d != nil {
		d.cancel.Do(func() { _ = d.command.Process.Signal(syscall.SIGTERM) })
	}
}

func (d *residentDesktop) endTurn() {
	if d != nil {
		d.launchMu.Lock()
		d.runner = nil
		d.launchMu.Unlock()
	}
}

func (d *residentDesktop) beginTurn(runner *providerRunner) error {
	d.launchMu.Lock()
	defer d.launchMu.Unlock()
	if d.closed || !d.live() || d.runner != nil || runner.root == nil || !runner.root.alive() {
		return errors.New("resident turn launch admission unavailable")
	}
	d.runner = runner
	return nil
}

func (d *residentDesktop) closeLaunches() {
	if d == nil {
		return
	}
	d.launchMu.Lock()
	d.closed, d.runner = true, nil
	if d.listener != nil {
		_ = d.listener.Close()
	}
	if d.current != nil {
		_ = d.current.Close()
	}
	d.launchMu.Unlock()
	if d.launchDone != nil {
		<-d.launchDone
	}
}

func (d *residentDesktop) Close() {
	if d != nil {
		d.closeLaunches()
		if d.conn != nil {
			_ = d.conn.Close()
		}
		d.root.Close()
	}
}

func (d *residentDesktop) request(ctx context.Context, request desktopRequest) error {
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !d.live() || d.conn == nil {
		return diagnostics.Wrap("worker_desktop_owner_lost", errors.New("resident desktop unavailable"))
	}
	deadline := time.Now().Add(10 * time.Second)
	if bound, ok := ctx.Deadline(); ok && bound.Before(deadline) {
		deadline = bound
	}
	_ = d.conn.SetDeadline(deadline)
	defer d.conn.SetDeadline(time.Time{})
	if err := writeRunnerPacket(d.conn, request, desktopPacketLimit); err != nil {
		return diagnostics.Wrap("worker_desktop_control_lost", err)
	}
	var response desktopResponse
	if err := readRunnerPacket(d.conn, &response, desktopPacketLimit); err != nil {
		return diagnostics.Wrap("worker_desktop_control_lost", err)
	}
	if !response.OK {
		return diagnostics.Wrap("worker_desktop_continuity_lost", errors.New("resident desktop refused request"))
	}
	return nil
}

func (d *residentDesktop) health(ctx context.Context) error {
	return d.request(ctx, desktopRequest{Kind: "health"})
}

// Only endpoint settings cross into task execution. Task HOME and configuration
// remain private to that task, while Cua and Obu retain their stable discovery.
func (d *residentDesktop) overlay(environment map[string]string) {
	for _, name := range []string{"DISPLAY", "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR", "XAUTHORITY", "XDG_SESSION_TYPE", "GDK_BACKEND", "ACCESSIBILITY_ENABLED", "NO_AT_BRIDGE"} {
		if value, exists := d.env[name]; exists {
			environment[name] = value
		}
	}
}

func desktopEnvironment(descriptor runtimeimage.Descriptor, taskRoot string) (map[string]string, error) {
	root, err := os.OpenRoot(wire.ControlRoot)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	for _, path := range []string{wire.DesktopRoot, wire.DesktopHome, wire.DesktopRoot + "/tmp", wire.DesktopRoot + "/run", wire.DesktopHome + "/.cache/cua-driver", wire.DesktopHome + "/.config", wire.DesktopHome + "/.local/share", wire.DesktopHome + "/.local/state"} {
		if err := root.MkdirAll(strings.TrimPrefix(path, wire.ControlRoot+"/"), 0700); err != nil {
			return nil, err
		}
	}
	entries, err := runtimeimage.Vars(descriptor, nil, runtimeimage.Locations{Home: wire.DesktopHome, TmpDir: wire.DesktopRoot + "/tmp", Workspace: taskRoot})
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, entry := range entries {
		key, value, _ := strings.Cut(entry, "=")
		values[key] = value
	}
	values["LANG"] = "C.UTF-8"
	values["XDG_RUNTIME_DIR"] = wire.DesktopRoot + "/run"
	values["DBUS_SESSION_BUS_ADDRESS"] = "unix:path=" + values["XDG_RUNTIME_DIR"] + "/bus"
	values["XAUTHORITY"] = values["XDG_RUNTIME_DIR"] + "/Xauthority"
	values["XDG_CONFIG_HOME"] = wire.DesktopHome + "/.config"
	values["XDG_CACHE_HOME"] = wire.DesktopHome + "/.cache"
	values["XDG_DATA_HOME"] = wire.DesktopHome + "/.local/share"
	values["XDG_STATE_HOME"] = wire.DesktopHome + "/.local/state"
	values["MULTICA_DESKTOP_SCREEN"] = os.Getenv("MULTICA_DESKTOP_SCREEN")
	if values["MULTICA_DESKTOP_SCREEN"] == "" || values["DISPLAY"] == "" {
		return nil, errors.New("admitted desktop settings unavailable")
	}
	// Pinned Cua uses HOME rather than XDG to discover its daemon socket and PID.
	home, err := os.OpenRoot(wire.Home)
	if err != nil {
		return nil, err
	}
	defer home.Close()
	if err := home.MkdirAll(".cache", 0700); err != nil {
		return nil, err
	}
	const cache = ".cache/cua-driver"
	target := wire.DesktopHome + "/" + cache
	if existing, err := home.Readlink(cache); err == nil {
		if existing != target {
			return nil, errors.New("Cua discovery projection has an unowned collision")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("Cua discovery projection has an unowned collision")
	} else if err := home.Symlink(target, cache); err != nil {
		return nil, err
	}
	return values, nil
}

func startResidentDesktop(ctx context.Context, descriptor runtimeimage.Descriptor, taskRoot string) (_ *residentDesktop, resultErr error) {
	environment, err := desktopEnvironment(descriptor, taskRoot)
	if err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp(wire.ControlRoot, "desktop-control-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	socket := filepath.Join(directory, "owner.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return nil, err
	}
	defer listener.Close()
	if err := os.Chmod(socket, 0600); err != nil {
		return nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	command := exec.Command(executable, "init", "worker", "desktop", socket)
	command.Env = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=" + wire.DesktopHome, "LANG=C.UTF-8"}
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	done := make(chan struct{})
	desktop := &residentDesktop{command: command, Done: done, env: environment}
	go func() { desktop.waitErr = command.Wait(); close(done); _ = listener.Close() }()
	desktop.root, err = pinProcess(command.Process.Pid)
	if err != nil {
		desktop.Cancel()
		return desktop, err
	}
	defer func() {
		if resultErr != nil {
			desktop.Cancel()
		}
	}()
	startup, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	deadline, _ := startup.Deadline()
	_ = listener.SetDeadline(deadline)
	stopAccept := context.AfterFunc(startup, func() { _ = listener.Close() })
	defer stopAccept()
	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			return desktop, diagnostics.Wrap("worker_desktop_handshake_failed", err)
		}
		if err := authenticateChild(conn, command.Process.Pid, desktop.live); err != nil {
			_ = conn.Close()
			continue
		}
		desktop.conn = conn
		break
	}
	// No desktop service has started. Remove the rendezvous before control input.
	_ = listener.Close()
	if err := os.RemoveAll(directory); err != nil {
		return desktop, err
	}
	stopConnection := context.AfterFunc(startup, func() { _ = desktop.conn.Close() })
	defer stopConnection()
	_ = desktop.conn.SetDeadline(deadline)
	if err := writeRunnerPacket(desktop.conn, desktopInput{Environment: environment, TaskRoot: taskRoot}, desktopPacketLimit); err != nil {
		return desktop, err
	}
	var ready desktopResponse
	if err := readRunnerPacket(desktop.conn, &ready, desktopPacketLimit); err != nil || !ready.OK {
		return desktop, diagnostics.Wrap("worker_desktop_startup_failed", errors.Join(err, errors.New("desktop readiness unavailable")))
	}
	if !stopConnection() || startup.Err() != nil {
		return desktop, startup.Err()
	}
	_ = desktop.conn.SetDeadline(time.Time{})
	desktop.listener, err = net.ListenUnix("unix", &net.UnixAddr{Name: wire.DesktopLaunchPath, Net: "unix"})
	if err != nil {
		return desktop, err
	}
	if err := os.Chmod(wire.DesktopLaunchPath, 0600); err != nil {
		_ = desktop.listener.Close()
		return desktop, err
	}
	desktop.launchDone = make(chan struct{})
	go desktop.serveLaunches()
	return desktop, nil
}

func (d *residentDesktop) serveLaunches() {
	defer close(d.launchDone)
	for {
		conn, err := d.listener.AcceptUnix()
		if err != nil {
			return
		}
		d.launchMu.Lock()
		if d.closed {
			d.launchMu.Unlock()
			_ = conn.Close()
			return
		}
		d.current = conn
		d.launchMu.Unlock()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		peer, err := unixPeer(conn)
		var identity processIdentity
		if err == nil {
			identity, err = readProcess(int(peer.Pid))
		}
		var request desktopRequest
		if err == nil {
			err = readRunnerPacket(conn, &request, desktopPacketLimit)
		}
		d.launchMu.Lock()
		if err == nil && !d.closed {
			err = d.authorizeLaunch(identity)
			if err == nil && request.Kind == "launch" && request.Executable == wire.ChromeOriginal {
				err = d.request(context.Background(), request)
			} else if err == nil {
				err = errors.New("unsupported resident launch")
			}
		} else if err == nil {
			err = errors.New("resident launch endpoint closed")
		}
		_ = writeRunnerPacket(conn, desktopResponse{OK: err == nil}, desktopPacketLimit)
		d.current = nil
		d.launchMu.Unlock()
		_ = conn.Close()
	}
}

func (d *residentDesktop) authorizeLaunch(identity processIdentity) error {
	current, err := readProcess(identity.pid)
	if err != nil || current.start != identity.start {
		return errors.New("resident launch peer changed")
	}
	if resident, err := beneath(identity.pid, d.root); err != nil {
		return err
	} else if resident {
		return nil
	}
	if d.runner != nil && d.runner.live() && d.runner.root != nil {
		if current, err := beneath(identity.pid, d.runner.root); err == nil && current {
			return nil
		}
	}
	return errors.New("resident launch peer has no current turn authority")
}

// Chrome selects its launch domain only from the typed, read-only bootstrap.
func Chrome(ctx context.Context, arguments []string) error {
	if len(arguments) == 1 && arguments[0] == "--version" {
		return syscall.Exec(wire.ChromeOriginal, append([]string{wire.ChromeOriginal}, arguments...), os.Environ())
	}
	_, session, _, err := readPodBootstrap(wire.RequestPath)
	if err != nil {
		return err
	}
	if session == nil {
		return syscall.Exec(wire.ChromeOriginal, append([]string{wire.ChromeOriginal}, arguments...), os.Environ())
	}
	if session.Version != wire.SessionProtocolVersion {
		return errors.New("resident Chrome launch requires current session protocol")
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", wire.DesktopLaunchPath)
	if err != nil {
		return diagnostics.Wrap("worker_desktop_launch_unavailable", err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
	workdir, err := os.Getwd()
	if err != nil {
		return err
	}
	if workdir != session.TaskRoot && !strings.HasPrefix(workdir, session.TaskRoot+"/") {
		workdir = ""
	}
	if err := writeRunnerPacket(connection, desktopRequest{Kind: "launch", Executable: wire.ChromeOriginal, Arguments: arguments, Workdir: workdir}, desktopPacketLimit); err != nil {
		return err
	}
	var response desktopResponse
	if err := readRunnerPacket(connection, &response, desktopPacketLimit); err != nil {
		return err
	}
	if !response.OK {
		return errors.New("resident Chrome launch refused")
	}
	return nil
}

func unixPeer(conn *net.UnixConn) (*unix.Ucred, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var peer *unix.Ucred
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		unix.CloseOnExec(int(fd))
		peer, credentialErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return nil, err
	}
	if credentialErr != nil {
		return nil, credentialErr
	}
	if peer == nil || peer.Uid != uint32(os.Getuid()) || peer.Gid != uint32(os.Getgid()) || peer.Pid <= 0 {
		return nil, errors.New("worker local peer identity differs")
	}
	return peer, nil
}

func authenticateChild(conn *net.UnixConn, parent int, live func() bool) error {
	if !live() {
		return errors.New("worker child supervisor has exited")
	}
	peer, err := unixPeer(conn)
	if err != nil {
		return err
	}
	identity, err := readProcess(int(peer.Pid))
	if err != nil {
		return err
	}
	if identity.parent != parent || !live() {
		return errors.New("worker private peer parent differs")
	}
	return nil
}

// RunDesktop owns neutral services and application launch, without task authority.
func RunDesktop(ctx context.Context, socketPath string) error {
	if os.Getpid() == 1 {
		return errors.New("desktop owner requires a supervisor")
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return err
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 0, 0, 0, 0); err != nil {
		return err
	}
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return err
	}
	defer connection.Close()
	conn := connection.(*net.UnixConn)
	peer, err := unixPeer(conn)
	if err != nil || peer.Pid != 1 {
		return errors.New("desktop control peer is not PID 1")
	}
	stopConnection := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopConnection()
	_ = conn.SetDeadline(time.Now().Add(time.Minute))
	var input desktopInput
	if err := readRunnerPacket(conn, &input, desktopPacketLimit); err != nil {
		return err
	}
	root, err := pinProcess(os.Getppid())
	if err != nil {
		return err
	}
	defer root.Close()
	if err := startDesktop(ctx, input.Environment); err != nil {
		return err
	}
	services, err := desktopServices(ctx, input.Environment, root)
	if err != nil {
		return err
	}
	if err := writeRunnerPacket(conn, desktopResponse{OK: true}, desktopPacketLimit); err != nil {
		return err
	}
	_ = conn.SetDeadline(time.Time{})
	for {
		var request desktopRequest
		if err := readRunnerPacket(conn, &request, desktopPacketLimit); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		var requestErr error
		switch request.Kind {
		case "health":
			requestErr = checkDesktopServices(requestCtx, input.Environment, root, services)
		case "launch":
			requestErr = launchChrome(input, request)
		default:
			requestErr = errors.New("unsupported desktop control")
		}
		cancel()
		if err := writeRunnerPacket(conn, desktopResponse{OK: requestErr == nil}, desktopPacketLimit); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func desktopServices(ctx context.Context, environment map[string]string, root *processRoot) (map[string]processIdentity, error) {
	services := map[string]processIdentity{}
	for _, name := range []string{"", "xvfb", "dbus", "openbox"} {
		command := exec.CommandContext(ctx, "/usr/bin/supervisorctl", "-c", "/etc/multica/desktop-supervisord.conf", "pid")
		if name != "" {
			command.Args = append(command.Args, name)
		}
		command.Env = wire.Environment(environment)
		output, err := command.Output()
		if err != nil {
			return nil, errors.New("desktop service identity unavailable")
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(output)))
		if err != nil || pid <= 1 {
			return nil, errors.New("desktop service identity invalid")
		}
		member, err := beneath(pid, root)
		if err != nil || !member {
			return nil, errors.New("desktop service owner differs")
		}
		identity, err := readProcess(pid)
		if err != nil || identity.state == 'Z' {
			return nil, errors.New("desktop service is not live")
		}
		services[name] = identity
	}
	return services, nil
}

func checkDesktopServices(ctx context.Context, environment map[string]string, root *processRoot, services map[string]processIdentity) error {
	if !root.alive() {
		return errors.New("desktop process owner lost")
	}
	for _, expected := range services {
		current, err := readProcess(expected.pid)
		if err != nil || current.start != expected.start || current.state == 'Z' {
			return errors.New("desktop service generation lost")
		}
		if member, err := beneath(expected.pid, root); err != nil || !member {
			return errors.New("desktop service owner lost")
		}
	}
	for _, args := range [][]string{
		{"/usr/bin/xdpyinfo", "-display", environment["DISPLAY"]},
		{"/usr/bin/dbus-send", "--session", "--print-reply", "--reply-timeout=1000", "--dest=org.freedesktop.DBus", "/", "org.freedesktop.DBus.ListNames"},
		{"/opt/multica/tools/bin/cua-driver", "call", "list_apps", "{}"},
	} {
		command := exec.CommandContext(ctx, args[0], args[1:]...)
		command.Env = wire.Environment(environment)
		if err := command.Run(); err != nil {
			return errors.New("desktop endpoint health unproven")
		}
	}
	return nil
}

func launchChrome(input desktopInput, request desktopRequest) error {
	if request.Executable != wire.ChromeOriginal || len(request.Arguments) > 1024 {
		return errors.New("unsupported desktop executable")
	}
	for _, argument := range request.Arguments {
		if strings.ContainsRune(argument, 0) {
			return errors.New("invalid desktop argument")
		}
	}
	workdir := input.TaskRoot
	if request.Workdir != "" {
		canonical, err := filepath.EvalSymlinks(request.Workdir)
		if err != nil || canonical != request.Workdir || canonical != input.TaskRoot && !strings.HasPrefix(canonical, input.TaskRoot+"/") {
			return errors.New("desktop workdir escaped the task mount")
		}
		workdir = canonical
	}
	command := exec.Command(wire.ChromeOriginal, request.Arguments...)
	environment := maps.Clone(input.Environment)
	// Chrome keeps the image's existing profile while its task-neutral HOME
	// and the resident Cua/application environment remain separate.
	environment["XDG_CONFIG_HOME"] = filepath.Dir(wire.ChromeProfileRoot)
	command.Env = wire.Environment(environment)
	command.Dir = workdir
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	// Nil streams are /dev/null. No task descriptor or control channel is inherited.
	if err := command.Start(); err != nil {
		return err
	}
	go func() { _ = command.Wait() }()
	return nil
}
