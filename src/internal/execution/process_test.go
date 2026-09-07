package execution

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func awaitFile(t *testing.T, path string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("child process did not reach the required file operation")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestCancellationLetsChildFinishItsPendingWrite(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	script := `/bin/bash -c '
trap '\''printf stopping > "$STOPPING"; while [ ! -f "$RELEASE" ]; do sleep 0.01; done; printf committed > "$RESULT"; exit 0'\'' TERM
printf ready > "$READY"
while :; do sleep 1; done
' &
trap 'exit 42' TERM
wait
`
	env := append(os.Environ(), "READY="+filepath.Join(dir, "ready"), "STOPPING="+filepath.Join(dir, "stopping"), "RELEASE="+filepath.Join(dir, "release"), "RESULT="+filepath.Join(dir, "result"))
	done := make(chan ProcessResult, 1)
	go func() {
		done <- RunProcess(ctx, "/bin/bash", []string{"-c", script}, env, dir, 2*time.Second, ProcessStreams{Stdout: io.Discard, Stderr: io.Discard})
	}()
	awaitFile(t, filepath.Join(dir, "ready"))
	cancel()
	awaitFile(t, filepath.Join(dir, "stopping"))
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if !result.Exited || result.Code != 42 {
			t.Fatalf("lost the provider's observed cancellation result: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process group was not cleaned up")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "result"))
	if err != nil || string(raw) != "committed" {
		t.Fatal("provider leader exit killed a child's pending write before grace completed")
	}
}

func TestProviderExitDoesNotDependOnUpstreamEOF(t *testing.T) {
	input, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result := RunProcess(ctx, "/bin/sh", []string{"-c", "exit 37"}, os.Environ(), t.TempDir(), time.Second, ProcessStreams{Stdin: input, Stdout: io.Discard, Stderr: io.Discard})
	if !result.Exited || result.Code != 37 || ctx.Err() != nil {
		t.Fatalf("provider completion was replaced by an upstream EOF wait: %+v", result)
	}
}

func TestInputReadFailureCannotBecomeSuccessfulExecution(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux /proc provides a deterministic readable descriptor whose data read fails")
	}
	input, err := os.Open("/proc/self/mem")
	if err != nil {
		t.Fatal(err)
	}
	result := RunProcess(t.Context(), "/bin/sh", []string{"-c", "cat >/dev/null"}, os.Environ(), t.TempDir(), time.Second, ProcessStreams{Stdin: input, Stdout: io.Discard, Stderr: io.Discard})
	if result.Err == nil {
		t.Fatal("provider EOF success hid a failed input read")
	}
}
