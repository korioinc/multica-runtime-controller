package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

func runProvider(ctx context.Context, args []string) (returnErr error) {
	if os.Getenv("MULTICA_TASK_ID") == "" {
		if slices.Contains(args, "--version") {
			fmt.Println("0.85.0")
			return nil
		}
		if slices.Contains(args, "--list-models") {
			fmt.Print("provider  model  context  max-out  reasoning  images\nfixture  fixture-model  200000  8192  yes  yes\n")
			return nil
		}
		return errors.New("fixture provider supports only version/model no-task calls")
	}
	if err := fixtureOnly(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()
	raw, err := os.ReadFile(wire.RequestPath)
	if err != nil {
		return err
	}
	request, err := wire.Decode(raw)
	if err != nil {
		return err
	}
	backend := strings.TrimRight(os.Getenv("VERIFYRUNTIME_BACKEND"), "/")
	if !strings.HasPrefix(backend, "http://") {
		return errors.New("provider requires the local fixture backend")
	}
	session, err := wire.PiSession(request)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	if request.TaskID != os.Getenv("MULTICA_TASK_ID") || cwd != request.WorkDir {
		return errors.New("provider received another task identity or directory")
	}
	if err := captureProviderHome(ctx, backend, request, raw); err != nil {
		return err
	}
	if slices.Contains(args, "--version") {
		fmt.Println("0.85.0")
		return nil
	}
	if slices.Contains(args, "--fixture-transport-hold") {
		return transportHold(ctx, request)
	}
	report := providerResult{TaskID: request.TaskID, Case: os.Getenv("VERIFYRUNTIME_CASE"), Stage: "started", WorkDir: cwd, Session: session, Storage: request.WorkerSubPath, RuntimeRef: request.RuntimeRef, Request: raw}
	defer func() { returnErr = finishProvider(backend, report, returnErr) }()
	if err := verifyProviderPrompt(&report); err != nil {
		return err
	}
	if err := verifyProviderEnvironment(&report); err != nil {
		return err
	}
	freshSession, err := inspectProviderSession(&report)
	if err != nil {
		return err
	}
	if os.Getenv("VERIFYRUNTIME_HOLD") == "true" {
		if err := holdProvider(ctx, backend, report); err != nil {
			return err
		}
	}
	if err := verifyProviderCheckout(ctx, request, backend, &report); err != nil {
		return err
	}
	if descriptor, digest, err := runtimeimage.Check(ctx, runtimeimage.Root, wire.ControllerRoot, request.RuntimeRef.Platform); err != nil {
		return err
	} else if err := runtimeimage.Match(descriptor, digest, request.RuntimeRef); err != nil {
		return err
	}
	if err := recordProviderSession(&report, freshSession); err != nil {
		return err
	}
	if os.Getenv("VERIFYRUNTIME_HOLD_AFTER_WORK") == "true" {
		report.Stage = "held"
		if err := holdProvider(ctx, backend, report); err != nil {
			return err
		}
	}
	report.Stage = "completed"
	encoded, _ := json.Marshal(map[string]any{"type": "message_update", "assistantMessageEvent": map[string]any{"type": "text_delta", "delta": "Local runtime repository verification completed."}})
	fmt.Println(string(encoded))
	encoded, _ = json.Marshal(map[string]any{"type": "turn_end", "message": map[string]any{"role": "assistant", "stopReason": "stop"}})
	fmt.Println(string(encoded))
	return nil
}

// transportHold is used only by the disposable remote-stream sever fixture.
// It neither edits workspace/session files nor reports a fabricated completion.
func transportHold(ctx context.Context, request wire.Request) error {
	if request.Provider != "pi" || !slices.Contains(request.Args, "--fixture-transport-hold") || request.TaskID != os.Getenv("MULTICA_TASK_ID") {
		return errors.New("invalid transport fixture identity")
	}
	cwd, err := os.Getwd()
	if err != nil || cwd != request.WorkDir {
		return errors.New("transport fixture work directory mismatch")
	}
	if _, err := wire.PiSession(request); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(os.Stdout, "verifystream-ready:%s:%d\n", request.AttemptID, os.Getpid()); err != nil {
		return err
	}
	read := make(chan error, 1)
	go func() { var data [1]byte; _, err := os.Stdin.Read(data[:]); read <- err }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-read:
		if err == nil {
			return errors.New("transport fixture unexpectedly received input")
		}
		return err
	}
}
