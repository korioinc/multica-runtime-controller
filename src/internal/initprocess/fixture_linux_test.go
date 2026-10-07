//go:build linux

package initprocess

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type fixtureIdentity struct {
	PID, PPID, PGID       int
	Executable, Directory string
	Args                  []string
}

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "init" {
		if os.Getenv("RUNTIME_INIT_TEST_HOLD_SPAWN") != "" || os.Getenv("RUNTIME_INIT_TEST_SPAWN_ERROR") != "" || os.Getenv("RUNTIME_INIT_TEST_EXIT_BEFORE_SPAWN_RETURN") != "" {
			os.Exit(run(os.Args[2:], fixtureSpawn))
		}
		os.Exit(Run(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "init-fixture" {
		os.Exit(fixtureHelper(os.Args[2:]))
	}
	if len(os.Args) > 1 && role(os.Args[1:]) != "" && os.Getenv("RUNTIME_INIT_TEST_MODE") != "" {
		os.Exit(fixturePrimary())
	}
	os.Exit(m.Run())
}

func fixtureSpawn(executable string, args []string, attr *os.ProcAttr) (*os.Process, error) {
	root := os.Getenv("RUNTIME_INIT_TEST_DIR")
	switch os.Getenv("RUNTIME_INIT_TEST_SPAWN_ERROR") {
	case "missing":
		return os.StartProcess(filepath.Join(root, "missing-executable"), args, attr)
	case "denied":
		path := filepath.Join(root, "denied-executable")
		fixturePublish(root, "denied-executable", []byte("not executable"))
		return os.StartProcess(path, args, attr)
	case "internal":
		return nil, unix.EIO
	}
	process, err := os.StartProcess(executable, args, attr)
	if err == nil && os.Getenv("RUNTIME_INIT_TEST_EXIT_BEFORE_SPAWN_RETURN") != "" {
		// Observe without consuming: production's first drain must own this status.
		for {
			var status unix.Siginfo
			waitErr := unix.Waitid(unix.P_PID, process.Pid, &status, unix.WEXITED|unix.WNOWAIT, nil)
			if errors.Is(waitErr, unix.EINTR) {
				continue
			}
			if waitErr != nil {
				panic(waitErr)
			}
			break
		}
	}
	if err == nil && os.Getenv("RUNTIME_INIT_TEST_HOLD_SPAWN") != "" {
		fixtureWait(root, "ready")
		fixturePublish(root, "spawn-held", nil)
		fixtureWait(root, "spawn-release")
	}
	return process, err
}

func fixturePrimary() int {
	mode, root := os.Getenv("RUNTIME_INIT_TEST_MODE"), os.Getenv("RUNTIME_INIT_TEST_DIR")
	if strings.HasPrefix(mode, "exit-") {
		code, _ := strconv.Atoi(strings.TrimPrefix(mode, "exit-"))
		return code
	}
	controls := make(chan os.Signal, len(controlSignals))
	if mode != "signal-default" {
		signal.Notify(controls, controlSignals...)
		defer signal.Stop(controls)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		panic(err)
	}
	executable, _ := os.Executable()
	directory, _ := os.Getwd()
	pgid, _ := unix.Getpgid(0)
	identity, _ := json.Marshal(fixtureIdentity{
		PID: os.Getpid(), PPID: os.Getppid(), PGID: pgid,
		Executable: executable, Directory: directory, Args: os.Args[1:],
	})
	fixturePublish(root, "primary.json", identity)
	if mode == "inspect" {
		return 0
	}
	if mode == "controller-helpers" {
		return fixtureController(root, controls)
	}
	var witness *exec.Cmd
	if mode == "controlled" {
		witness = fixtureCommand(root, "witness")
		if err := witness.Start(); err != nil {
			panic(err)
		}
		fixtureWait(root, "witness-ready")
	}
	if mode == "live-orphan" {
		fixtureOrphans(root, "live", 1)
	}
	if mode == "storm" || mode == "storm-exit" {
		fixtureOrphans(root, "storm", 1)
	}
	fixturePublish(root, "ready", nil)
	if mode == "signal-default" {
		// A pending timer prevents deadlock detection without handling SIGTERM.
		for {
			time.Sleep(time.Hour)
		}
	}
	if mode == "live-orphan" || mode == "storm-exit" {
		fixtureWait(root, "finish")
		return 7
	}
	control := <-controls
	fixturePublish(root, "signal-"+strconv.Itoa(int(control.(syscall.Signal))), nil)
	fixtureWait(root, "finish")
	if witness != nil {
		fixturePublish(root, "witness-finish", nil)
		if err := witness.Wait(); err != nil {
			return 1
		}
	}
	return 7
}

func fixtureController(root string, controls <-chan os.Signal) int {
	commands := make([]*exec.Cmd, 0, 3)
	for _, code := range []int{17, 19, 23} {
		command := fixtureCommand(root, "managed", strconv.Itoa(code))
		if err := command.Start(); err != nil {
			panic(err)
		}
		commands = append(commands, command)
	}
	fixtureOrphans(root, "exit", 8)
	fixturePublish(root, "ready", nil)
	fmt.Println("INIT_FIXTURE_READY")
	control := <-controls
	fixturePublish(root, "signal-"+strconv.Itoa(int(control.(syscall.Signal))), nil)
	fixturePublish(root, "helpers-release", nil)
	results := make([]int, len(commands))
	for i, command := range commands {
		err := command.Wait()
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			return 1
		}
		results[i] = exit.ExitCode()
	}
	data, _ := json.Marshal(results)
	fixturePublish(root, "helpers.json", data)
	fmt.Printf("INIT_FIXTURE_COMPLETE signal=%d helpers=%s\n", control, data)
	if string(data) != "[17,19,23]" || control != syscall.SIGTERM {
		return 1
	}
	return 0
}

func fixtureHelper(args []string) int {
	root := os.Getenv("RUNTIME_INIT_TEST_DIR")
	switch args[0] {
	case "managed":
		fixtureWait(root, "helpers-release")
		code, _ := strconv.Atoi(args[1])
		return code
	case "witness":
		controls := make(chan os.Signal, len(controlSignals))
		signal.Notify(controls, controlSignals...)
		fixturePublish(root, "witness-ready", nil)
		for {
			select {
			case <-controls:
				fixturePublish(root, "witness-signal", nil)
			default:
			}
			if _, err := os.Stat(filepath.Join(root, "witness-finish")); err == nil {
				return 0
			}
			time.Sleep(time.Millisecond)
		}
	case "orphan-parent":
		count, _ := strconv.Atoi(args[2])
		pids := make([]int, 0, count)
		for range count {
			command := fixtureCommand(root, args[1])
			if err := command.Start(); err != nil {
				panic(err)
			}
			pids = append(pids, command.Process.Pid)
			// This short-lived intermediary deliberately leaves its children.
			_ = command.Process.Release()
		}
		data, _ := json.Marshal(pids)
		fixturePublish(root, "orphan-pids.json", data)
		return 0
	case "exit":
		fixtureWait(root, "orphans-release")
		return 29
	case "live":
		fixtureWait(root, "orphans-release")
		for {
			time.Sleep(time.Second)
		}
	case "storm":
		fixtureWait(root, "orphans-release")
		for batch := 0; ; batch++ {
			directory, err := os.MkdirTemp(root, "batch-")
			if err != nil {
				panic(err)
			}
			fixtureOrphans(directory, "exit", 8)
			if batch == 2 {
				fixturePublish(root, "storm-active", nil)
			}
			time.Sleep(5 * time.Millisecond)
		}
	case "owned":
		fixturePublish(root, "owned-ready", nil)
		for {
			time.Sleep(time.Second)
		}
	case "ownership":
		return fixtureOwnership(root, args[1])
	}
	return 1
}

func fixtureOrphans(root, kind string, count int) {
	command := fixtureCommand(root, "orphan-parent", kind, strconv.Itoa(count))
	if err := command.Run(); err != nil {
		panic(err)
	}
	// Wait for the intermediary before releasing children, proving adoption.
	fixturePublish(root, "orphans-release", nil)
}

func fixtureCommand(root string, args ...string) *exec.Cmd {
	executable, _ := os.Executable()
	command := exec.Command(executable, append([]string{"init-fixture"}, args...)...)
	command.Env = append(os.Environ(), "RUNTIME_INIT_TEST_DIR="+root)
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	return command
}

func fixturePublish(root, name string, data []byte) {
	path := filepath.Join(root, name)
	if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
		panic(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		panic(err)
	}
}

func fixtureWait(root, name string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(root, name)); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("fixture barrier timed out: " + name)
}

func fixtureOwnership(root, scenario string) int {
	executable, _ := os.Executable()
	process, err := os.StartProcess(executable, []string{executable, "init-fixture", "owned"}, &os.ProcAttr{
		Env: os.Environ(), Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Sys: &syscall.SysProcAttr{Setpgid: true},
	})
	if err != nil {
		panic(err)
	}
	pid := process.Pid
	fixtureWait(root, "owned-ready")
	childExits := make(chan os.Signal, 1)
	controls := make(chan os.Signal, 1)
	signal.Notify(childExits, syscall.SIGCHLD)
	defer signal.Stop(childExits)
	var notifications <-chan os.Signal = childExits
	wait := waitCall(unix.Wait4)
	sends := []os.Signal{}
	send := func(control os.Signal) error {
		sends = append(sends, control)
		return process.Signal(control)
	}
	wantCode := exitFailure
	wantSignals := "[killed]"
	switch scenario {
	case "lost":
		_ = process.Kill()
		var status unix.WaitStatus
		_, _ = unix.Wait4(pid, &status, 0, nil)
		wantSignals = "[]"
	case "wait-error":
		first := true
		wait = func(pid int, status *unix.WaitStatus, options int, usage *unix.Rusage) (int, error) {
			if first {
				first = false
				return -1, unix.EIO
			}
			return unix.Wait4(pid, status, options, usage)
		}
	case "signal-error":
		controls <- syscall.SIGTERM
		wantSignals = "[terminated killed]"
		send = func(control os.Signal) error {
			sends = append(sends, control)
			if control == syscall.SIGTERM {
				return unix.EPERM
			}
			return process.Signal(control)
		}
	case "esrch", "process-done":
		controls <- syscall.SIGTERM
		wantCode, wantSignals = signalExitOffset+int(syscall.SIGTERM), "[terminated]"
		send = func(control os.Signal) error {
			sends = append(sends, control)
			if err := process.Signal(control); err != nil {
				panic(err)
			}
			if scenario == "esrch" {
				return unix.ESRCH
			}
			return os.ErrProcessDone
		}
	case "final-drain-error", "final-drain-stream":
		_ = process.Signal(syscall.SIGTERM)
		primarySeen := false
		wait = func(target int, status *unix.WaitStatus, options int, usage *unix.Rusage) (int, error) {
			if primarySeen {
				if scenario == "final-drain-stream" {
					return pid + 1, nil
				}
				return -1, unix.EIO
			}
			result, err := unix.Wait4(target, status, options, usage)
			primarySeen = result == pid
			return result, err
		}
		wantCode, wantSignals = signalExitOffset+int(syscall.SIGTERM), "[]"
	case "busy-drain":
		controls <- syscall.SIGTERM
		wantCode, wantSignals = signalExitOffset+int(syscall.SIGTERM), "[terminated]"
		wait = func(target int, status *unix.WaitStatus, options int, usage *unix.Rusage) (int, error) {
			result, err := unix.Wait4(target, status, options, usage)
			if result == 0 && len(sends) == 0 {
				// A never-empty orphan queue exercises the pass budget. The
				// separate storm fixture proves real adoption and forwarding.
				return pid + 1, nil
			}
			return result, err
		}
	case "interrupted-wait":
		controls <- syscall.SIGTERM
		wantCode, wantSignals = signalExitOffset+int(syscall.SIGTERM), "[terminated]"
		remaining := 3
		wait = func(target int, status *unix.WaitStatus, options int, usage *unix.Rusage) (int, error) {
			if remaining > 0 {
				remaining--
				return -1, unix.EINTR
			}
			return unix.Wait4(target, status, options, usage)
		}
	case "fallback":
		notifications = nil
		wantCode, wantSignals = signalExitOffset+int(syscall.SIGTERM), "[]"
		wait = func(target int, status *unix.WaitStatus, options int, usage *unix.Rusage) (int, error) {
			result, err := unix.Wait4(target, status, options, usage)
			if result == 0 {
				_ = process.Signal(syscall.SIGTERM)
			}
			return result, err
		}
	}
	started := time.Now()
	code := waitPrimary("controller", process, notifications, controls, wait, send)
	fmt.Printf("ownership case %s: elapsed=%s drain_budget=%s scheduling_tolerance=%s\n", scenario, time.Since(started), reapWindow, schedulingTolerance)
	if code != wantCode || fmt.Sprint(sends) != wantSignals {
		fmt.Fprintf(os.Stderr, "ownership case %s: code=%d signals=%v elapsed=%s\n", scenario, code, sends, time.Since(started))
		return 1
	}
	var status unix.WaitStatus
	if _, err := unix.Wait4(pid, &status, unix.WNOHANG, nil); !errors.Is(err, unix.ECHILD) {
		fmt.Fprintf(os.Stderr, "primary not reaped: %v\n", err)
		return 1
	}
	return 0
}
