package daemonapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	agentTasksNextCursor  = "X-Agent-Tasks-Next-Cursor"
	maxHistoryPages       = 64
	maxHistoryBytes       = 4 * MaxPayload
	maxHistoryCursorBytes = 2048
)

func observationClaim(claim Claim) bool {
	return observationID(claim.ID) && observationID(claim.WorkspaceID) && observationID(claim.AgentID) && observationID(claim.RuntimeID) && claim.Agent.ID == claim.AgentID && strings.HasPrefix(claim.AuthToken, "mat_") && len(claim.AuthToken) > 4 && !strings.ContainsAny(claim.AuthToken, "\r\n")
}

// AgentTaskHistory consumes every page exposed by the ordinary agent endpoint.
// The endpoint filters some legacy rows; pagination completion does not prove
// universal producer visibility. Errors never return a usable partial history.
func (c *Client) AgentTaskHistory(ctx context.Context, claim Claim) ([]TaskObservation, error) {
	return c.agentTaskHistory(ctx, claim, "", claim.AuthToken)
}

// AgentTask observes one task with the current claim's ordinary API authority.
// A later claim may inspect an earlier task in the same workspace and agent.
// Unvisited pages cannot supply continuity evidence for this lookup.
func (c *Client) AgentTask(ctx context.Context, claim Claim, taskID string) (TaskObservation, error) {
	return c.agentTask(ctx, claim, taskID, claim.AuthToken)
}

// ControllerTask reads the original task with the controller's own authority.
// Result reconciliation uses this after task credentials become unavailable;
// it does not restore the task's execution or business API authority.
func (c *Client) ControllerTask(ctx context.Context, claim Claim) (TaskObservation, error) {
	return c.agentTask(ctx, claim, claim.ID, c.token)
}

func (c *Client) agentTask(ctx context.Context, claim Claim, taskID, token string) (TaskObservation, error) {
	if !observationID(taskID) {
		return TaskObservation{}, errors.New("invalid task observation target")
	}
	tasks, err := c.agentTaskHistory(ctx, claim, taskID, token)
	if err != nil {
		return TaskObservation{}, err
	}
	if len(tasks) == 0 {
		return TaskObservation{}, errors.New("task observation unavailable")
	}
	return tasks[0], nil
}

func (c *Client) agentTaskHistory(ctx context.Context, claim Claim, taskID, token string) ([]TaskObservation, error) {
	if !observationClaim(claim) {
		return nil, errors.New("invalid task history scope")
	}
	var result []TaskObservation
	seenIDs := make(map[string]bool)
	seenCursors := make(map[string]bool)
	cursor, total := "", 0
	for page := 0; page < maxHistoryPages; page++ {
		endpoint := "/api/agents/" + claim.AgentID + "/tasks"
		if cursor != "" {
			endpoint += "?before=" + url.QueryEscape(cursor)
		}
		raw, headers, err := c.sendResponse(ctx, http.MethodGet, endpoint, token, claim.WorkspaceID, nil)
		if err != nil {
			return nil, err
		}
		total += len(raw)
		if total > maxHistoryBytes {
			return nil, errors.New("task history budget exhausted")
		}
		tasks, err := parseTaskHistory(raw, claim, false, seenIDs)
		if err != nil {
			return nil, err
		}
		if len(headers.Values(agentTasksNextCursor)) > 1 {
			return nil, errors.New("ambiguous task history cursor")
		}
		next := headers.Get(agentTasksNextCursor)
		if next != "" && (len(tasks) == 0 || len(next) > maxHistoryCursorBytes || next != strings.TrimSpace(next) || strings.ContainsAny(next, "\x00\r\n") || seenCursors[next]) {
			return nil, errors.New("invalid task history cursor")
		}
		if taskID == "" {
			result = append(result, tasks...)
		} else {
			for _, task := range tasks {
				if task.ID == taskID {
					return []TaskObservation{task}, nil
				}
			}
		}
		if next == "" {
			return result, nil
		}
		seenCursors[next] = true
		cursor = next
	}
	return nil, errors.New("task history page budget exhausted")
}

// IssueTaskHistory reads the complete ordinary execution log for one issue.
// It includes other agents in that issue; callers select their agent explicitly.
// Coordination summaries and active-only views cannot establish this evidence.
func (c *Client) IssueTaskHistory(ctx context.Context, claim Claim) ([]TaskObservation, error) {
	if !observationClaim(claim) || !observationID(claim.IssueID) {
		return nil, errors.New("invalid issue history scope")
	}
	raw, headers, err := c.sendResponse(ctx, http.MethodGet, "/api/issues/"+claim.IssueID+"/task-runs", claim.AuthToken, claim.WorkspaceID, nil)
	if err != nil {
		return nil, err
	}
	if headers.Get(agentTasksNextCursor) != "" {
		return nil, errors.New("unsupported issue history pagination")
	}
	return parseTaskHistory(raw, claim, true, make(map[string]bool))
}

func parseTaskHistory(raw []byte, claim Claim, issue bool, seenIDs map[string]bool) ([]TaskObservation, error) {
	var tasks []TaskObservation
	if json.Unmarshal(raw, &tasks) != nil || tasks == nil {
		return nil, errors.New("invalid task history response")
	}
	for _, task := range tasks {
		if !task.validHistoryRow() || task.WorkspaceID != claim.WorkspaceID || (issue && task.IssueID != claim.IssueID) || (!issue && task.AgentID != claim.AgentID) || seenIDs[task.ID] {
			return nil, errors.New("task history identity unavailable")
		}
		seenIDs[task.ID] = true
	}
	return tasks, nil
}

func (t TaskObservation) validHistoryRow() bool {
	if !observationID(t.ID) || !observationID(t.WorkspaceID) || !observationID(t.AgentID) || t.Kind == "" || t.Status == "" {
		return false
	}
	for _, id := range []string{t.RuntimeID, t.IssueID, t.ChatSessionID} {
		if id != "" && !observationID(id) {
			return false
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, t.CreatedAt); err != nil {
		return false
	}
	for _, timestamp := range []string{t.DispatchedAt, t.StartedAt, t.CompletedAt} {
		if timestamp != "" {
			if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
				return false
			}
		}
	}
	return validAttribution(t.Attribution)
}

// ClaimTaskAssignment keeps ordinary API authentication on the current task
// token. The old TaskAssignment method remains for persisted legacy grants.
func (c *Client) ClaimTaskAssignment(ctx context.Context, claim Claim) (TaskAssignment, error) {
	task, err := c.AgentTask(ctx, claim, claim.ID)
	if err != nil {
		return TaskAssignment{}, err
	}
	if !observationID(task.RuntimeID) || task.DispatchedAt == "" {
		return TaskAssignment{}, errors.New("task assignment unavailable")
	}
	return TaskAssignment{RuntimeID: task.RuntimeID, DispatchedAt: task.DispatchedAt, Status: task.Status}, nil
}
