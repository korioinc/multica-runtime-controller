package main

import (
	"errors"
	"fmt"
	"testing"

	utilexec "k8s.io/client-go/util/exec"
)

func TestProviderExitCode(t *testing.T) {
	remoteExit := utilexec.CodeExitError{Err: errors.New("provider failed"), Code: 42}
	if got := providerExitCode(fmt.Errorf("stream provider: %w", remoteExit)); got != 42 {
		t.Fatalf("provider exit code = %d, want 42", got)
	}
	if got := providerExitCode(errors.New("transport failed")); got != 1 {
		t.Fatalf("transport failure exit code = %d, want 1", got)
	}
}
