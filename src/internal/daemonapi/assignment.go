package daemonapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// TaskAssignment is the current owner-visible assignment, not a mutation fence.
// Task history emits dispatched_at at second precision even when the claim
// retains fractions; compare at the observable precision. The fail endpoint
// does not accept a runtime or dispatch-generation precondition.
type TaskAssignment struct {
	RuntimeID    string `json:"runtime_id"`
	DispatchedAt string `json:"dispatched_at"`
	Status       string `json:"status"`
}

// TaskAssignment reads task history with controller authority to validate a
// backend assignment. Business proxy requests retain their own task token.
func (c *Client) TaskAssignment(ctx context.Context, workspaceID, agentID, taskID string) (TaskAssignment, error) {
	if !segment(workspaceID) || !segment(agentID) || !segment(taskID) {
		return TaskAssignment{}, errors.New("invalid task assignment scope")
	}
	raw, err := c.send(ctx, http.MethodGet, "/api/agents/"+agentID+"/tasks", c.token, workspaceID, nil)
	if err != nil {
		return TaskAssignment{}, err
	}
	var tasks []struct {
		ID          string `json:"id"`
		AgentID     string `json:"agent_id"`
		WorkspaceID string `json:"workspace_id"`
		TaskAssignment
	}
	if json.Unmarshal(raw, &tasks) != nil {
		return TaskAssignment{}, errors.New("invalid task assignment response")
	}
	var result TaskAssignment
	found := false
	for _, task := range tasks {
		if task.ID != taskID {
			continue
		}
		if found || task.AgentID != agentID || task.WorkspaceID != workspaceID || !segment(task.RuntimeID) || task.Status == "" {
			return TaskAssignment{}, errors.New("task assignment identity unavailable")
		}
		if _, err := time.Parse(time.RFC3339, task.DispatchedAt); err != nil {
			return TaskAssignment{}, errors.New("task dispatch generation unavailable")
		}
		result = task.TaskAssignment
		found = true
	}
	if !found {
		return TaskAssignment{}, errors.New("task assignment unavailable")
	}
	return result, nil
}
