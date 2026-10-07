//go:build linux

package controller

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestClaudeSignedResultRetainsPodWithoutNativeReuse(t *testing.T) {
	if os.Getenv("MULTICA_CONVERSATION_FS_TEST") != "1" {
		t.Skip("requires an explicitly owned Linux /workspace fixture")
	}
	f := newSessionControllerFixture(t, "claude")
	project := filepath.Join(f.Grant.TaskRoot, workspace.ClaudeSessionsDir, workspace.ClaudeProjectDir)
	for _, directory := range []string{project, f.Grant.TaskRoot + "/workdir"} {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	root := f.Grant.TaskRoot
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	id := uuid.NewString()
	record, _ := json.Marshal(map[string]any{"type": "user", "sessionId": id, "cwd": f.Grant.TaskRoot + "/workdir",
		"message": map[string]string{"role": "user", "content": "the current task's conversation"}})
	transcript := filepath.Join(project, id+".jsonl")
	if err := os.WriteFile(transcript, append(record, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	terminal := receiveFixtureProviderResult(t, f, agent.Result{Status: "completed", Output: "actual Claude result", SessionID: id}, true)
	quiesceNativeFixtureTurn(t, f, terminal)
	if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if f.Grant.PendingResume == nil || f.Grant.PendingResume.SessionID != id || f.Grant.CompletionWitness == nil || !f.Grant.TurnComplete ||
		f.Grant.Selection.ReuseEligible || f.Session.State != workspace.SessionIdle || f.Session.Retention != f.C.ConversationIdleTimeout {
		t.Fatal("Claude lost its retained Pod or gained unproven native continuity")
	}
	if err := workspace.ValidateSession(*f.Grant.Prepared, id); err != nil {
		t.Fatal("writer settlement lost the actual task-local transcript", err)
	}
	first := f.Grant
	installWorkspaceAuthorityFixture(f, first.OwnerID)
	queued := queueWorkspaceFollowup(t, f, func(map[string]any) {})
	if err := f.C.selectTurn(t.Context(), queued); err != nil {
		t.Fatal(err)
	}
	selected, err := f.C.Store.Get(queued.AttemptID)
	if err != nil || selected.Selection.Mode != workspace.SelectionFreshSession || selected.Selection.ReuseEligible {
		t.Fatal("Claude follow-up could not retain its workspace with fresh native history", err)
	}
	f.Grant, f.Session, err = f.C.Store.ReserveTurn(selected.AttemptID, *selected.Selection, selected.CompatibilityDigest,
		f.C.ConversationIdleTimeout, f.C.MaxResidentPods)
	if err != nil || f.Session.ID != first.WorkerSessionID || f.Session.PodUID != first.PodUID || f.Grant.TaskRoot != first.TaskRoot {
		t.Fatal("Claude fresh-history follow-up replaced its Pod or files", err)
	}
	prepareConversationFixtureTurn(t, f.C, f.Grant)
	f.refresh(t)
	var run wire.Run
	if json.Unmarshal(f.Grant.Execution, &run) != nil || run.Options.ResumeExpected || run.Options.ResumeSessionID != "" {
		t.Fatal("Claude follow-up adopted unproven native history")
	}
	if actual, err := os.ReadFile(transcript); err != nil || !bytes.Contains(actual, record) {
		t.Fatal("fresh-history follow-up changed the retained native transcript", err)
	}
}

func TestNativeResultPreservesOutcomeAndExactSessionMetadata(t *testing.T) {
	if os.Getenv("MULTICA_CONVERSATION_FS_TEST") != "1" {
		t.Skip("requires an explicitly owned Linux /workspace fixture")
	}
	for _, scenario := range []string{"completed_fallback", "missing_returned_rollout", "no_returned_session", "explicit_resume_rejection"} {
		t.Run(scenario, func(t *testing.T) {
			f, previousID := nativeResultFixture(t)
			var run wire.Run
			if json.Unmarshal(f.Grant.Execution, &run) != nil || run.Options.ResumeSessionID != previousID {
				t.Fatal("fixture lost the actual attempted resume pointer")
			}
			returnedID := uuid.NewString()
			result := agent.Result{Status: "completed", Output: "the actual completed native outcome", SessionID: returnedID}
			switch scenario {
			case "completed_fallback":
				writeNativeResultRollout(t, f, returnedID)
			case "no_returned_session":
				result.SessionID = ""
			case "explicit_resume_rejection":
				result = agent.Result{Status: "failed", Error: "codex thread/resume failed: bufio.Scanner: token too long", ResumeRejected: true}
			}
			terminal := receiveFixtureProviderResult(t, f, result, true)
			f.refresh(t)
			resume := f.Grant.PendingResume
			storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || resume == nil {
				t.Fatal("native outcome lost its persisted metadata", err)
			}
			var body daemonapi.TaskCompleteRequest
			if json.Unmarshal(terminal.Body, &body) != nil {
				t.Fatal("terminal JSON is invalid")
			}
			if scenario == "explicit_resume_rejection" {
				if terminal.Kind != "fail" || resume.RetiredSessionID != previousID || body.RetiredSessionID != previousID ||
					!slices.Contains(storage.RetiredSessions, previousID) || resume.MissingSessionID != "" || resume.SessionRolloutMissing {
					t.Fatal("explicit rejection did not retire precisely the attempted session")
				}
				var failed struct {
					Error string `json:"error"`
				}
				if json.Unmarshal(terminal.Body, &failed) != nil || failed.Error != result.Error {
					t.Fatal("session retirement changed the actual failed result")
				}
			} else {
				if terminal.Kind != "complete" || body.Output != result.Output || resume.RetiredSessionID != "" || slices.Contains(storage.RetiredSessions, previousID) {
					t.Fatal("a new or absent returned ID relabeled success or guessed retirement")
				}
				switch scenario {
				case "completed_fallback":
					if resume.SessionID != returnedID || body.SessionID != returnedID || resume.MissingSessionID != "" || resume.SessionRolloutMissing {
						t.Fatal("a completed fallback lost its validated new native session")
					}
				case "missing_returned_rollout":
					if resume.SessionID != "" || body.SessionID != "" || resume.MissingSessionID != returnedID || !resume.SessionRolloutMissing ||
						!body.SessionRolloutMissing || !slices.Contains(storage.RetiredSessions, returnedID) {
						t.Fatal("missing rollout evidence named the attempted session instead of the actual returned ID")
					}
				case "no_returned_session":
					if resume.SessionID != "" || body.SessionID != "" || resume.MissingSessionID != "" || resume.SessionRolloutMissing {
						t.Fatal("an absent returned ID manufactured missing-rollout evidence")
					}
				}
			}
			retained, err := os.ReadFile(filepath.Join(f.Grant.TaskRoot, "codex-home/sessions/rollout-fixture-"+previousID+".jsonl"))
			if err != nil || !bytes.Contains(retained, []byte(previousID)) {
				t.Fatal("result metadata removed the retained original native file", err)
			}
			f.mu.Lock()
			starts := f.Starts[f.Grant.TaskID]
			f.mu.Unlock()
			if starts != 1 {
				t.Fatal("returned native metadata repeated start authorization")
			}
		})
	}
}

func nativeResultFixture(t *testing.T) (*sessionControllerFixture, string) {
	t.Helper()
	f := newSessionControllerFixture(t)
	if _, err := os.Lstat(f.Grant.TaskRoot); !os.IsNotExist(err) {
		t.Fatal("fixture native root already exists", err)
	}
	for _, directory := range []string{"workdir", "codex-home/sessions"} {
		if err := os.MkdirAll(filepath.Join(f.Grant.TaskRoot, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	root := f.Grant.TaskRoot
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	previousID := uuid.NewString()
	writeNativeResultRollout(t, f, previousID)
	terminal := receiveFixtureProviderResult(t, f, agent.Result{Status: "completed", Output: "original native outcome", SessionID: previousID}, true)
	quiesceNativeFixtureTurn(t, f, terminal)
	if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	// This shared helper uses the fixture's provider metadata for every turn.
	startPiFixtureFollowup(t, f)
	return f, previousID
}

func writeNativeResultRollout(t *testing.T, f *sessionControllerFixture, id string) {
	t.Helper()
	header, _ := json.Marshal(map[string]any{"type": "session_meta", "payload": map[string]string{"id": id, "cwd": f.Grant.TaskRoot + "/workdir"}})
	path := filepath.Join(f.Grant.TaskRoot, "codex-home/sessions/rollout-fixture-"+id+".jsonl")
	if err := os.WriteFile(path, append(header, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
