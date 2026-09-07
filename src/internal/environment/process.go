package environment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type boundedOutput struct{ data bytes.Buffer }

func (b *boundedOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := 65536 - b.data.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.data.Write(p)
	}
	return n, nil
}

// runTree keeps the preparer alive as the Linux subreaper until every script or
// probe descendant exits. Reparented and setsid children cannot outrun READY.
func runTree(ctx context.Context, argv, env []string, dir string, output io.Writer) (string, error) {
	baseline, err := beginSupervision()
	if err != nil {
		return "", err
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	captured := &boundedOutput{}
	var writer io.Writer = captured
	if output != nil {
		writer = output
	}
	cmd.Stdout = writer
	cmd.Stderr = writer
	// Pipes copied by os/exec otherwise keep Wait blocked on descendants. Our own
	// deadline terminates those descendants before waiting for the copy goroutines.
	if err = cmd.Start(); err != nil {
		return "", err
	}
	pid := cmd.Process.Pid
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	tracked := map[int]bool{pid: true}
	finished := false
	var result error
	for {
		children := supervisedChildren(pid, baseline, tracked)
		for _, child := range children {
			tracked[child] = true
		}
		if finished {
			reapChildren(children, pid)
			children = supervisedChildren(pid, baseline, tracked)
			if len(children) == 0 {
				return captured.data.String(), result
			}
		}
		select {
		case result = <-wait:
			finished = true
			wait = nil
		case <-ticker.C:
		case <-ctx.Done():
			// The generation remains non-READY even when the shell itself succeeded.
			_ = syscall.Kill(-pid, syscall.SIGTERM)
			for _, child := range children {
				_ = syscall.Kill(child, syscall.SIGTERM)
			}
			grace := time.NewTimer(200 * time.Millisecond)
			<-grace.C
			for {
				children = supervisedChildren(pid, baseline, tracked)
				for _, child := range children {
					_ = syscall.Kill(child, syscall.SIGKILL)
				}
				_ = syscall.Kill(-pid, syscall.SIGKILL)
				if !finished {
					result = <-wait
					finished = true
				}
				reapChildren(children, pid)
				if len(supervisedChildren(pid, baseline, tracked)) == 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			return captured.data.String(), fmt.Errorf("environment process tree incomplete: %w", ctx.Err())
		}
	}
}

func safeInstallEnv(current []string) []string {
	result := []string{}
	for _, entry := range current {
		key := entry
		for i, c := range key {
			if c == '=' {
				key = key[:i]
				break
			}
		}
		if Reserved(key) && key != "PATH" {
			continue
		}
		result = append(result, entry)
	}
	return result
}
func writableDirectory(path string) error {
	if path == "" {
		return errors.New("missing writable directory")
	}
	f, err := os.CreateTemp(path, ".multica-write-check-")
	if err != nil {
		return err
	}
	name := f.Name()
	if err = f.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Remove(name)
}
