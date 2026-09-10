package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/official"
)

// The second HTTP origin is deliberately outside the bridge. The actual
// official HTTP client would follow a 307 there if the guarded response escaped.
func (v *verifier) redirects(ctx context.Context) error {
	if err := v.stage(ctx, "registration-redirect", false, false, func(ctx context.Context, _ official.DaemonProcess, active *running) error {
		state, err := awaitReady(ctx, active)
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if err == nil {
			for _, workspace := range state.Workspaces {
				if len(workspace.Runtimes) > 0 {
					return errors.New("redirected registration authorized an unvalidated runtime")
				}
			}
		}
		if err := v.redirectEvidence("registration-redirect"); err != nil {
			return err
		}
		return v.noForbidden()
	}); err != nil {
		return err
	}
	return v.stage(ctx, "claim-redirect", false, false, func(ctx context.Context, _ official.DaemonProcess, active *running) error {
		if _, err := awaitReady(ctx, active); err != nil {
			return err
		}
		task := map[string]any{"id": uuid.NewString(), "agent_id": agentID, "runtime_id": builtinRuntimeID, "workspace_id": workspaceID, "auth_token": "mat_disposable_redirect_task", "repos": []any{}, "agent": map[string]any{"id": agentID, "name": "Redirect fixture"}, "chat_session_id": uuid.NewString(), "chat_message": "A redirected task must never be authorized"}
		v.backend.mutex.Lock()
		v.backend.redirectTask = task
		v.backend.pending = task
		v.backend.mutex.Unlock()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("official redirect claim cycle: %w", ctx.Err())
			case err := <-active.done:
				active.done <- err
				return fmt.Errorf("official daemon stopped during claim refusal: %w", err)
			case <-ticker.C:
				v.backend.mutex.Lock()
				finished := v.backend.redirectCycleFinished
				v.backend.mutex.Unlock()
				if !finished {
					continue
				}
				if _, err := v.store.Lookup(task["id"].(string), task["auth_token"].(string), workspaceID, agentID); err == nil {
					return errors.New("redirected task acquired claim authority")
				}
				if err := v.redirectEvidence("claim-redirect"); err != nil {
					return err
				}
				return v.noForbidden()
			}
		}
	})
}

func (v *verifier) redirectEvidence(name string) error {
	v.backend.mutex.Lock()
	issued := v.backend.redirectIssued
	requests := append([]redirectRequest{}, v.backend.redirectRequests...)
	v.backend.mutex.Unlock()
	if !issued {
		return errors.New("actual official client never received the redirect scenario")
	}
	raw, _ := json.MarshalIndent(struct {
		RedirectIssued      bool              `json:"redirectIssued"`
		DestinationRequests []redirectRequest `json:"destinationRequests"`
	}{issued, requests}, "", "  ")
	if err := os.WriteFile(filepath.Join(v.evidence, name+".json"), append(raw, '\n'), 0600); err != nil {
		return err
	}
	if len(requests) > 0 {
		return errors.New("official client followed a redirect outside the guarded registration/claim bridge")
	}
	return nil
}
