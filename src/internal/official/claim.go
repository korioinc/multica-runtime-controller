package official

import (
	"encoding/json"
	"errors"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

// The wire remains opaque except for authority fields and the explicitly
// supported continuity rewrite. Prompts and credentials are never journaled.
func (b *bridge) claim(raw []byte) ([]byte, error) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil || envelope == nil {
		return nil, errors.New("invalid claim envelope")
	}
	batch, ok := envelope["tasks"]
	if !ok || string(batch) == "null" {
		return nil, errors.New("official claim requires the batch envelope")
	}
	var tasks []json.RawMessage
	if err := json.Unmarshal(batch, &tasks); err != nil {
		return nil, err
	}
	observations := make([]workspace.Observation, len(tasks))
	fields := make([]map[string]json.RawMessage, len(tasks))
	for i, rawTask := range tasks {
		var task struct {
			ID             string          `json:"id"`
			WorkspaceID    string          `json:"workspace_id"`
			AgentID        string          `json:"agent_id"`
			IssueID        string          `json:"issue_id"`
			ChatID         string          `json:"chat_session_id"`
			ProjectID      string          `json:"project_id"`
			AuthToken      string          `json:"auth_token"`
			PriorWorkDir   string          `json:"prior_work_dir"`
			PriorSession   string          `json:"prior_session_id"`
			LocalDirectory json.RawMessage `json:"local_directory"`
			ExecutionMode  string          `json:"execution_mode"`
			Agent          *struct {
				CustomEnv map[string]string `json:"custom_env"`
			} `json:"agent"`
			Repos []struct {
				URL string `json:"url"`
			} `json:"repos"`
			Resources []struct {
				Type string `json:"resource_type"`
			} `json:"project_resources"`
		}
		if json.Unmarshal(rawTask, &task) != nil || json.Unmarshal(rawTask, &fields[i]) != nil || fields[i] == nil {
			return nil, errors.New("invalid official task")
		}
		observation := workspace.Observation{ID: task.ID, WorkspaceID: task.WorkspaceID, AgentID: task.AgentID, IssueID: task.IssueID, ChatID: task.ChatID, ProjectID: task.ProjectID, AuthToken: task.AuthToken, PriorWorkDir: task.PriorWorkDir, PriorSession: task.PriorSession, ExecutionMode: task.ExecutionMode, Environment: b.environment}
		if task.Agent != nil {
			for key := range task.Agent.CustomEnv {
				observation.TaskEnvKeys = append(observation.TaskEnvKeys, key)
			}
		}
		if len(task.LocalDirectory) > 0 && string(task.LocalDirectory) != "null" && string(task.LocalDirectory) != `""` {
			observation.LocalDirectory = "unsupported"
		}
		for _, resource := range task.Resources {
			if resource.Type == "local_directory" {
				observation.LocalDirectory = "unsupported"
			}
		}
		for _, repo := range task.Repos {
			observation.RepositoryURLs = append(observation.RepositoryURLs, repo.URL)
		}
		observations[i] = observation
	}
	decisions, err := b.store.ObserveBatch(observations)
	if err != nil {
		return nil, err
	}
	changed := false
	for i, decision := range decisions {
		if decision.ResetWorkDir {
			fields[i]["prior_work_dir"] = json.RawMessage(`""`)
		}
		if decision.ResetSession {
			fields[i]["prior_session_id"] = json.RawMessage(`""`)
			fields[i]["prior_session_resume_unavailable"] = json.RawMessage(`true`)
		}
		if decision.ResetSession || decision.ResetWorkDir {
			tasks[i], err = json.Marshal(fields[i])
			if err != nil {
				return nil, err
			}
			changed = true
		}
	}
	if !changed {
		return raw, nil
	}
	envelope["tasks"], err = json.Marshal(tasks)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope)
}
