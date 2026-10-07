package daemonapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

type AttributionUser struct {
	ID string `json:"id"`
}

// TaskAttribution preserves existing backend lineage. Display names have no
// role in either source selection or compatibility authority.
type TaskAttribution struct {
	Source              string           `json:"source"`
	Precise             bool             `json:"precise"`
	Initiator           *AttributionUser `json:"initiator,omitempty"`
	Originator          *AttributionUser `json:"originator,omitempty"`
	Evidence            *TaskEvidence    `json:"evidence,omitempty"`
	RuleVersionID       string           `json:"rule_version_id,omitempty"`
	DelegatedFromTaskID string           `json:"delegated_from_task_id,omitempty"`
	RetryOfTaskID       string           `json:"retry_of_task_id,omitempty"`
	RerunOfTaskID       string           `json:"rerun_of_task_id,omitempty"`
}

type TaskEvidence struct {
	Kind  string `json:"kind"`
	RefID string `json:"ref_id"`
}

// TaskCompleteRequest is the complete accepted request stored by the existing
// backend. Omitted optional fields retain the backend's declared zero defaults.
type TaskCompleteRequest struct {
	PRURL                 string `json:"pr_url"`
	Output                string `json:"output"`
	SessionID             string `json:"session_id"`
	WorkDir               string `json:"work_dir"`
	DurableWorkDir        string `json:"durable_work_dir,omitempty"`
	BranchName            string `json:"branch_name,omitempty"`
	SessionRolloutMissing bool   `json:"session_rollout_missing,omitempty"`
	RetiredSessionID      string `json:"retired_session_id,omitempty"`
}

// TaskObservation is an owner-visible row, not proof that all producers are
// visible. Result is populated only for a complete, supported stored request.
type TaskObservation struct {
	ID             string               `json:"id"`
	WorkspaceID    string               `json:"workspace_id"`
	AgentID        string               `json:"agent_id"`
	RuntimeID      string               `json:"runtime_id"`
	IssueID        string               `json:"issue_id"`
	ChatSessionID  string               `json:"chat_session_id"`
	Kind           string               `json:"kind"`
	Status         string               `json:"status"`
	Error          string               `json:"error"`
	FailureReason  string               `json:"failure_reason"`
	CreatedAt      string               `json:"created_at"`
	DispatchedAt   string               `json:"dispatched_at"`
	StartedAt      string               `json:"started_at"`
	CompletedAt    string               `json:"completed_at"`
	WorkDir        string               `json:"work_dir"`
	DurableWorkDir string               `json:"durable_work_dir"`
	BranchName     string               `json:"branch_name"`
	Attribution    *TaskAttribution     `json:"attribution"`
	Result         *TaskCompleteRequest `json:"result,omitempty"`
}

func (t *TaskObservation) UnmarshalJSON(raw []byte) error {
	type row TaskObservation
	var decoded struct {
		row
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*t = TaskObservation(decoded.row)
	if t.Status == "completed" {
		result, err := parseCompletion(decoded.Result, true)
		if err == nil {
			t.Result = &result
		}
	}
	return nil
}

func parseCompletion(raw []byte, stored bool) (TaskCompleteRequest, error) {
	var fields map[string]json.RawMessage
	if len(raw) == 0 || len(raw) > MaxPayload || json.Unmarshal(raw, &fields) != nil || fields == nil {
		return TaskCompleteRequest{}, errors.New("completion witness unavailable")
	}
	if stored {
		// These fields always appear when the supported backend marshals a
		// TaskCompleteRequest, even when the daemon omitted them on input.
		for _, key := range []string{"pr_url", "output", "session_id", "work_dir"} {
			if _, ok := fields[key]; !ok {
				return TaskCompleteRequest{}, errors.New("completion witness incomplete")
			}
		}
		for _, value := range fields {
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return TaskCompleteRequest{}, errors.New("completion witness unsupported")
			}
		}
	}
	var result TaskCompleteRequest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return TaskCompleteRequest{}, errors.New("completion witness unsupported")
	}
	if !stored {
		for _, value := range []*string{&result.PRURL, &result.Output, &result.SessionID, &result.WorkDir, &result.DurableWorkDir, &result.BranchName, &result.RetiredSessionID} {
			*value = strings.ReplaceAll(strings.ToValidUTF8(*value, "\uFFFD"), "\x00", "")
		}
	}
	return result, nil
}

func observationID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

// AcceptsCompletion proves the backend stored this exact normalized request.
// It does not establish writer quiescence or authorize any workspace selection.
func (t TaskObservation) AcceptsCompletion(claim Claim, sent []byte) bool {
	expected, err := parseCompletion(sent, false)
	if err != nil || t.Result == nil || t.Status != "completed" || !t.matchesStartedClaim(claim) {
		return false
	}
	return observedWorkDir(expected.WorkDir) && t.WorkDir == expected.WorkDir && t.DurableWorkDir == expected.DurableWorkDir && *t.Result == expected
}

// AcceptsTransientFailure matches the supported failed-row columns. The caller
// must separately prove the signed native result rejected resume before start.
// A failed row does not store a complete request in result JSON.
func (t TaskObservation) AcceptsTransientFailure(claim Claim, sent []byte) bool {
	var expected struct {
		Error   string `json:"error"`
		WorkDir string `json:"work_dir"`
	}
	var fields map[string]json.RawMessage
	if len(sent) == 0 || len(sent) > MaxPayload || json.Unmarshal(sent, &fields) != nil || len(fields) != 2 || fields["error"] == nil || fields["work_dir"] == nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(sent))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&expected) != nil {
		return false
	}
	for _, value := range []*string{&expected.Error, &expected.WorkDir} {
		*value = strings.ReplaceAll(strings.ToValidUTF8(*value, "\uFFFD"), "\x00", "")
	}
	reason := taskfailure.NormalizeDaemonReason(taskfailure.Classify(expected.Error).String(), expected.Error).String()
	return t.Status == "failed" && t.matchesStartedClaim(claim) && expected.Error != "" && observedWorkDir(expected.WorkDir) &&
		t.Error == expected.Error && t.FailureReason == reason && t.WorkDir == expected.WorkDir && t.DurableWorkDir == ""
}

// AcceptsFailure proves the accepted failure and its workspace, not native
// session adoption or retirement. The backend does not retain the fail request.
func (t TaskObservation) AcceptsFailure(claim Claim, sent []byte) bool {
	var expected struct {
		Error                 string `json:"error"`
		SessionID             string `json:"session_id"`
		WorkDir               string `json:"work_dir"`
		DurableWorkDir        string `json:"durable_work_dir"`
		FailureReason         string `json:"failure_reason"`
		BranchName            string `json:"branch_name"`
		SessionRolloutMissing bool   `json:"session_rollout_missing"`
		RetiredSessionID      string `json:"retired_session_id"`
	}
	if len(sent) == 0 || len(sent) > MaxPayload || !json.Valid(sent) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(sent))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&expected) != nil {
		return false
	}
	for _, value := range []*string{&expected.Error, &expected.WorkDir, &expected.DurableWorkDir, &expected.FailureReason, &expected.BranchName} {
		*value = strings.ReplaceAll(strings.ToValidUTF8(*value, "\uFFFD"), "\x00", "")
	}
	reason := expected.FailureReason
	if reason == "" {
		reason = taskfailure.Classify(expected.Error).String()
	}
	reason = taskfailure.NormalizeDaemonReason(reason, expected.Error).String()
	return t.Status == "failed" && t.matchesStartedClaim(claim) && expected.Error != "" && observedWorkDir(expected.WorkDir) &&
		t.Error == expected.Error && t.FailureReason == reason && t.WorkDir == expected.WorkDir &&
		(expected.DurableWorkDir == "" || t.DurableWorkDir == expected.DurableWorkDir) &&
		(expected.BranchName == "" || t.BranchName == expected.BranchName)
}

func observedWorkDir(directory string) bool {
	return directory != "" && path.IsAbs(directory) && path.Clean(directory) == directory && !strings.ContainsAny(directory, "\x00\\")
}

// MatchesResultClaim binds a result observation to its claimed generation.
// Quick Create can attach its new issue after claim without becoming another task.
// This observation does not authorize execution or expand the task's API scope.
func (t TaskObservation) MatchesResultClaim(claim Claim) bool {
	issueMatches := t.IssueID == claim.IssueID || claim.Kind == "quick_create" && claim.IssueID == "" && observationID(t.IssueID)
	if t.ID != claim.ID || t.WorkspaceID != claim.WorkspaceID || t.AgentID != claim.AgentID || t.RuntimeID != claim.RuntimeID ||
		!issueMatches || t.ChatSessionID != claim.ChatSessionID || t.Kind != claim.Kind ||
		!observationID(t.ID) || !observationID(t.WorkspaceID) || !observationID(t.AgentID) || !observationID(t.RuntimeID) {
		return false
	}
	claimedGeneration, claimErr := time.Parse(time.RFC3339Nano, claim.DispatchedAt)
	observedGeneration, observedErr := time.Parse(time.RFC3339Nano, t.DispatchedAt)
	return claimErr == nil && observedErr == nil && claimedGeneration.Truncate(time.Second).Equal(observedGeneration.Truncate(time.Second))
}

func (t TaskObservation) matchesStartedClaim(claim Claim) bool {
	_, startErr := time.Parse(time.RFC3339Nano, t.StartedAt)
	_, completeErr := time.Parse(time.RFC3339Nano, t.CompletedAt)
	// taskToResponse exposes second precision. This is corroborating evidence;
	// the controller separately requires its generation-bound start receipt.
	return claim.StartClaimSupported && t.MatchesResultClaim(claim) && startErr == nil && completeErr == nil
}

// TerminalObservation sends once and retains the row returned by that request.
// A successful transport alone is never an accepted-completion witness.
func (c *Client) TerminalObservation(ctx context.Context, taskID, kind string, body []byte) (TaskObservation, error) {
	if !segment(taskID) || !terminalKind(kind) || !json.Valid(body) {
		return TaskObservation{}, errors.New("invalid terminal callback")
	}
	raw, err := c.request(ctx, http.MethodPost, "/api/daemon/tasks/"+taskID+"/"+kind, c.token, body)
	if err != nil {
		return TaskObservation{}, err
	}
	var task TaskObservation
	if json.Unmarshal(raw, &task) != nil || task.ID != taskID {
		return TaskObservation{}, errors.New("invalid terminal observation")
	}
	return task, nil
}

var ErrStartClaimUnsupported = errors.New("generation-bound task start unavailable")

// StartClaim uses the exact dispatched generation advertised by the backend.
// Persisted legacy grants retain the separate Start contract.
func (c *Client) StartClaim(ctx context.Context, claim Claim) error {
	if !claim.StartClaimSupported {
		return ErrStartClaimUnsupported
	}
	dispatched, err := time.Parse(time.RFC3339Nano, claim.DispatchedAt)
	if !observationID(claim.ID) || !observationID(claim.RuntimeID) || err != nil || dispatched.Nanosecond()%1000 != 0 {
		return errors.New("invalid task start generation")
	}
	return c.json(ctx, http.MethodPost, "/api/daemon/tasks/"+claim.ID+"/start", struct {
		RuntimeID    string `json:"runtime_id"`
		DispatchedAt string `json:"dispatched_at"`
	}{claim.RuntimeID, claim.DispatchedAt}, nil)
}
