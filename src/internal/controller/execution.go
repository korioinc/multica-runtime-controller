package controller

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/multica-ai/multica/server/pkg/redact"
)

func (c *Controller) receiveEvent(ctx context.Context, g workspace.TaskGrant, input wire.ProviderEvent) error {
	if (input.Message == nil) == (len(input.Usage) == 0) {
		return errors.New("event must contain one provider observation")
	}
	input = input.Redacted()
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	event, err := c.Store.ReceiveEvent(g.AttemptID, input.Sequence, raw)
	if err != nil {
		return err
	}
	if g.ExecutionRevoked && event.State == "received" {
		return c.Store.EventDelivery(g.AttemptID, input.Sequence, "received", "local")
	}
	if event.State != "received" {
		return nil
	}
	kind := "messages"
	var body any
	if input.Message != nil {
		m := input.Message
		messageType := string(m.Type)
		switch m.Type {
		case agent.MessageToolUse:
			messageType = "tool_use"
		case agent.MessageToolResult:
			messageType = "tool_result"
			if m.Tool == "" && m.CallID != "" {
				for i := len(g.Events) - 1; i >= 0; i-- {
					var prior wire.ProviderEvent
					if json.Unmarshal(g.Events[i].Body, &prior) == nil && prior.Message != nil && prior.Message.Type == agent.MessageToolUse && prior.Message.CallID == m.CallID {
						m.Tool = prior.Message.Tool
						break
					}
				}
			}
		case agent.MessageText, agent.MessageThinking, agent.MessageError:
		case agent.MessageStatus, agent.MessageLog:
			return c.Store.EventDelivery(g.AttemptID, input.Sequence, "received", "local")
		default:
			return errors.New("unsupported provider message")
		}
		body = map[string]any{"messages": []any{map[string]any{"seq": event.UpstreamSequence, "type": messageType, "tool": m.Tool, "content": m.Content, "input": m.Input, "output": m.Output, "output_truncated": false, "created_at": time.Now().UTC()}}}
	} else {
		kind = "usage"
		usage := make([]any, 0, len(input.Usage))
		for model, u := range input.Usage {
			if u.InputTokens < 0 || u.OutputTokens < 0 || u.CacheReadTokens < 0 || u.CacheWriteTokens < 0 || u.CostUSDTicks < 0 {
				return errors.New("invalid provider usage")
			}
			usage = append(usage, map[string]any{"provider": g.Prepared.Provider, "model": model, "input_tokens": u.InputTokens, "output_tokens": u.OutputTokens, "cache_read_tokens": u.CacheReadTokens, "cache_write_tokens": u.CacheWriteTokens, "cost_usd_ticks": u.CostUSDTicks})
		}
		body = map[string]any{"usage": usage}
	}
	if err := c.Store.EventDelivery(g.AttemptID, input.Sequence, "received", "forwarding"); err != nil {
		return err
	}
	err = c.API.Report(ctx, g.TaskID, kind, body)
	outcome := "delivered"
	if err != nil {
		outcome = "uncertain"
		if daemonapi.RequestNotSent(err) {
			outcome = "received"
		}
	}
	if saveErr := c.Store.EventDelivery(g.AttemptID, input.Sequence, "forwarding", outcome); saveErr != nil {
		return saveErr
	}
	// The backend appends messages without a deduplication key. Keep ambiguous
	// delivery in the private journal; never send the same event twice.
	if outcome == "received" {
		return err
	}
	return nil
}

func (c *Controller) receiveResult(g workspace.TaskGrant, input wire.ProviderResult) (wire.SealCommand, error) {
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return wire.SealCommand{}, err
	}
	resultDigest := wire.Digest(inputBytes)
	if terminal, err := c.Store.Terminal(g.AttemptID); err == nil {
		if terminal.Source != "worker" || terminal.ResultDigest != resultDigest {
			return wire.SealCommand{}, workspace.ErrConflict
		}
		c.cancelCheckouts(g.AttemptID)
		return wire.SealCommand{RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}, nil
	} else if !errors.Is(err, workspace.ErrUnauthorized) {
		return wire.SealCommand{}, err
	}
	result := input.Result
	kind := "fail"
	body := map[string]any{"work_dir": g.TaskRoot + "/workdir"}
	switch result.Status {
	case "completed":
		kind = "complete"
		body["output"] = redact.Text(result.Output)
	case "cancelled", "aborted":
		// Cleanup also revokes execution; only recorded backend cancellation
		// authorizes a cancellation acknowledgement.
		if g.Stop != nil && g.Stop.Reason == "backend_cancelled" {
			kind = "cancel-ack"
			body["error_message"] = redact.Text(result.Error)
		} else {
			body["error"] = "provider interrupted before completion"
		}
	case "failed", "timeout":
		body["error"] = redact.Text(result.Error)
	default:
		return wire.SealCommand{}, errors.New("unsupported provider terminal")
	}
	resume := workspace.ResumePointers{SessionID: result.SessionID, WorkDir: g.TaskRoot + "/workdir"}
	// Transient checkpoint retention requires a modern worker-session binding.
	resume.ResumeRejectedTransient = g.WorkerSessionID != "" && result.ResumeRejectedTransient && !result.ResumeRejected && result.Status == "failed" && result.SessionID == "" &&
		g.Prepared != nil && g.Prepared.Provider == "pi" && g.ResumeSession != ""
	if result.SessionID != "" {
		if g.Prepared == nil {
			return wire.SealCommand{}, errors.New("terminal has no prepared task")
		}
		if err := workspace.ValidateSession(*g.Prepared, result.SessionID); err != nil {
			// A preallocated file is not a resumable session. Preserve the task's
			// files and actual execution result without publishing that pointer.
			resume.MissingSessionID = result.SessionID
			resume.SessionID = ""
			resume.SessionRolloutMissing = true
			body["session_rollout_missing"] = true
		} else {
			body["session_id"] = result.SessionID
		}
	}
	if result.ResumeRejected {
		var run wire.Run
		if err := json.Unmarshal(g.Execution, &run); err != nil {
			return wire.SealCommand{}, err
		}
		resume.RetiredSessionID = run.Options.ResumeSessionID
		body["retired_session_id"] = run.Options.ResumeSessionID
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return wire.SealCommand{}, err
	}
	terminal, err := c.Store.ReceiveTerminal(g.AttemptID, kind, raw, resume, resultDigest)
	if err != nil {
		return wire.SealCommand{}, err
	}
	c.cancelCheckouts(g.AttemptID)
	return wire.SealCommand{RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}, nil
}
