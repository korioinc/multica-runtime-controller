package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

func TestEveryContextRetainsPodWithoutNativeHistory(t *testing.T) {
	for _, kind := range []workspace.ConversationKind{workspace.ConversationIssue, workspace.ConversationAgentDM, workspace.ConversationTask} {
		t.Run(string(kind), func(t *testing.T) {
			f := newSessionControllerContextFixture(t, "codex", kind, false)
			terminal := receiveFixtureTurnResult(t, f, true)
			quiesceFixtureTurn(t, f, terminal)
			if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			if f.Session.State != workspace.SessionIdle || !f.Grant.TurnComplete || f.Session.Retention != DefaultConversationIdleTimeout ||
				f.Session.IdleDeadline.Sub(f.Session.IdleSince) != DefaultConversationIdleTimeout || f.Grant.Selection.ReuseEligible {
				t.Fatal("accepted quiescent context lost uniform retention or gained native history")
			}
		})
	}
}

func TestWorkspaceSelectionReusesPodWithFreshNativeInputs(t *testing.T) {
	for _, kind := range []workspace.ConversationKind{workspace.ConversationIssue, workspace.ConversationAgentDM} {
		t.Run(string(kind), func(t *testing.T) {
			f := newSessionControllerContextFixture(t, "codex", kind, false)
			terminal := receiveFixtureTurnResult(t, f, true)
			quiesceFixtureTurn(t, f, terminal)
			if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			first, session := f.Grant, f.Session
			installWorkspaceAuthorityFixture(f, first.OwnerID)
			queued := queueWorkspaceFollowup(t, f, func(fields map[string]any) {
				fields["agent"].(map[string]any)["model"] = "another-model"
				fields["agent"].(map[string]any)["custom_env"] = map[string]string{"ANTHROPIC_AUTH_TOKEN": "fixture-only"}
				fields["connected_apps"] = []map[string]string{{"name": "opaque fixture integration"}}
			})
			if err := f.C.selectTurn(t.Context(), queued); err != nil {
				t.Fatal(err)
			}
			selected, err := f.C.Store.Get(queued.AttemptID)
			if err != nil || selected.Selection.Mode != workspace.SelectionFreshSession || selected.Selection.SessionID != "" ||
				selected.Selection.ReuseEligible || !selected.Selection.WorkspaceReuseEligible || selected.Selection.StorageID != first.StorageID ||
				selected.BackendSelection.Mode != workspace.SelectionFreshWorkspace || selected.Compatibility.Model != "another-model" {
				t.Fatal("fresh native execution lost the independently proved context workspace", err)
			}
			next, retained, err := f.C.Store.ReserveTurn(selected.AttemptID, *selected.Selection, selected.CompatibilityDigest,
				f.C.ConversationIdleTimeout, f.C.MaxResidentPods)
			if err != nil || retained.ID != session.ID || retained.PodUID != session.PodUID || next.TaskRoot != first.TaskRoot ||
				next.TurnSequence != first.TurnSequence+1 || !bytes.Equal(retained.Bootstrap, session.Bootstrap) ||
				next.CompatibilityDigest == first.CompatibilityDigest || retained.CompatibilityDigest != session.CompatibilityDigest {
				t.Fatal("fresh native turn replaced its Pod, bootstrap, or anchored files", err)
			}
		})
	}
}

func TestWorkspaceFallbackCannotReplaceExplicitSourceOrAuthority(t *testing.T) {
	for _, boundary := range []string{"principal", "named source", "explicit directory"} {
		t.Run(boundary, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			terminal := receiveFixtureTurnResult(t, f, true)
			quiesceFixtureTurn(t, f, terminal)
			if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			principal := f.Grant.OwnerID
			if boundary == "principal" {
				principal = uuid.NewString()
			}
			installWorkspaceAuthorityFixture(f, principal)
			queued := queueWorkspaceFollowup(t, f, func(fields map[string]any) {
				fields["agent"].(map[string]any)["custom_env"] = map[string]string{"ANTHROPIC_AUTH_TOKEN": "fixture-only"}
				if boundary == "named source" {
					fields["attribution"] = daemonapi.TaskAttribution{Source: "unattributed", RerunOfTaskID: uuid.NewString()}
				}
				if boundary == "explicit directory" {
					fields["prior_work_dir"] = "/workspace/another-context/workdir"
				}
			})
			if err := f.C.selectTurn(t.Context(), queued); err != nil {
				t.Fatal(err)
			}
			selected, err := f.C.Store.Get(queued.AttemptID)
			if err != nil || selected.Selection.Mode != workspace.SelectionFreshWorkspace || selected.Selection.StorageID != "" {
				t.Fatal("workspace fallback ignored a source or authority boundary", err)
			}
			_, _, err = f.C.Store.ReserveTurn(selected.AttemptID, *selected.Selection, selected.CompatibilityDigest,
				f.C.ConversationIdleTimeout, f.C.MaxResidentPods)
			if !errors.Is(err, workspace.ErrStorageBusy) {
				t.Fatal("replacement bypassed old Pod termination", err)
			}
		})
	}
}

func TestAcceptedOrdinaryFailureRetainsPodWithoutNativeCheckpoint(t *testing.T) {
	f := newSessionControllerContextFixture(t, "codex", workspace.ConversationAgentDM, false)
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/fail") {
			var result struct {
				Error   string `json:"error"`
				WorkDir string `json:"work_dir"`
			}
			if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
				t.Error(err)
				return
			}
			f.mu.Lock()
			row := f.Rows[f.Grant.TaskID]
			row.Status, row.Error, row.WorkDir = "failed", result.Error, result.WorkDir
			row.FailureReason = taskfailure.NormalizeDaemonReason(taskfailure.Classify(result.Error).String(), result.Error).String()
			row.CompletedAt = time.Now().UTC().Format(time.RFC3339)
			f.Rows[row.ID] = row
			f.mu.Unlock()
			writeJSON(w, row)
			return
		}
		f.serveBackend(w, r)
	}))
	terminal := receiveFixtureProviderResult(t, f, agent.Result{Status: "failed", Error: "provider quota exhausted"}, true)
	quiesceFixtureTurn(t, f, terminal)
	if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
	if err != nil || f.Session.State != workspace.SessionIdle || f.Grant.CompletionWitness == nil ||
		f.Grant.CompletionWitness.Status != "failed" || storage.Checkpoint != nil || !storage.Dirty || storage.WriterSessionID != f.Session.ID {
		t.Fatal("accepted failure lost reusable compute or gained native history", err)
	}
}

func queueWorkspaceFollowup(t *testing.T, f *sessionControllerFixture, change func(map[string]any)) workspace.TaskGrant {
	t.Helper()
	prior := f.Grant
	var fields map[string]any
	if err := json.Unmarshal(prior.Envelope, &fields); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	fields["id"], fields["auth_token"], fields["dispatched_at"] = id, "mat_"+id, time.Now().UTC().Format(time.RFC3339Nano)
	change(fields)
	raw, _ := json.Marshal(fields)
	queued, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, WorkspaceID: prior.WorkspaceID,
		AgentID: prior.AgentID, RuntimeID: prior.RuntimeID, RuntimeRef: prior.RuntimeRef, Metadata: prior.Metadata, Envelope: raw,
		Repositories: prior.Repositories, ResourceScope: prior.ResourceScope})
	if err != nil {
		t.Fatal(err)
	}
	return queued
}

func installWorkspaceAuthorityFixture(f *sessionControllerFixture, principal string) {
	prior := f.Grant
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/me":
			writeJSON(w, map[string]string{"id": principal})
		case "/api/agents/" + prior.AgentID:
			writeJSON(w, map[string]string{"id": prior.AgentID, "workspace_id": prior.WorkspaceID, "runtime_id": prior.RuntimeID, "owner_id": prior.OwnerID})
		case "/api/workspaces/" + prior.WorkspaceID + "/members":
			members := []map[string]string{{"id": uuid.NewString(), "workspace_id": prior.WorkspaceID, "user_id": prior.OwnerID, "role": "owner"}}
			if principal != prior.OwnerID {
				members = append(members, map[string]string{"id": uuid.NewString(), "workspace_id": prior.WorkspaceID, "user_id": principal, "role": "member"})
			}
			writeJSON(w, members)
		case "/api/agents/" + prior.AgentID + "/mcp-servers":
			writeJSON(w, []any{})
		case "/api/workspaces/" + prior.WorkspaceID + "/plugins":
			writeJSON(w, map[string]any{"plugins": []any{}})
		default:
			f.serveBackend(w, r)
		}
	}))
}
