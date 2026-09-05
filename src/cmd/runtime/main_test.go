package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/intercept"
)

func TestRunProviderProcessMirrorsProtocolActivityWithoutLoggingPayloads(t *testing.T) {
	const (
		taskID = "11111111-2222-3333-4444-555555555555"
		input  = `{"jsonrpc":"2.0","id":7,"method":"initialize","params":{"token":"secret-value"}}` + "\n"
	)
	request := intercept.Request{
		Provider: "codex",
		Args:     []string{"-c", `IFS= read -r line; printf '%s\n' "$line"; exit 7`},
		Env:      append(os.Environ(), "MULTICA_TASK_ID="+taskID),
		WorkDir:  t.TempDir(),
	}
	var stdout bytes.Buffer
	var podLog bytes.Buffer
	err := runProviderProcess(context.Background(), "/bin/sh", request, intercept.Streams{
		Stdin: strings.NewReader(input), Stdout: &stdout, Stderr: &bytes.Buffer{},
	}, &podLog)
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 {
		t.Fatalf("runProviderProcess() error = %v, want exit code 7", err)
	}
	if stdout.String() != input {
		t.Fatalf("provider stdout = %q, want exact protocol payload %q", stdout.String(), input)
	}
	logs := podLog.String()
	for _, want := range []string{
		`"msg":"provider execution started"`,
		`"direction":"controller_to_provider"`,
		`"direction":"provider_to_controller"`,
		`"method":"initialize"`,
		`"id":7`,
		`"exit_code":7`,
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("task Pod log does not contain %q: %s", want, logs)
		}
	}
	if strings.Contains(logs, "secret-value") {
		t.Fatalf("task Pod log leaked protocol payload: %s", logs)
	}
}
