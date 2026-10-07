//go:build linux

package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

// This journal race uses an owned native session file. It does not prove NFS.
func TestModernLateStopObservationCannotDrainTheNextTurn(t *testing.T) {
	if os.Getenv("MULTICA_CONVERSATION_FS_TEST") != "1" {
		t.Skip("requires an explicitly owned Linux /workspace fixture")
	}
	for _, boundary := range []string{"daemon status", "assignment"} {
		for _, status := range []string{"completed", "failed", "cancelled"} {
			t.Run(boundary+"/"+status, func(t *testing.T) {
				f := newSessionControllerFixture(t)
				first := f.Grant
				if _, err := os.Lstat(first.TaskRoot); !os.IsNotExist(err) {
					t.Fatal("fixture native root already exists", err)
				}
				for _, directory := range []string{"workdir", "codex-home/sessions"} {
					if err := os.MkdirAll(filepath.Join(first.TaskRoot, directory), 0700); err != nil {
						t.Fatal(err)
					}
				}
				t.Cleanup(func() { _ = os.RemoveAll(first.TaskRoot) })
				nativeID := uuid.NewString()
				header, _ := json.Marshal(map[string]any{"type": "session_meta", "payload": map[string]string{"id": nativeID, "cwd": first.TaskRoot + "/workdir"}})
				path := filepath.Join(first.TaskRoot, "codex-home/sessions/rollout-fixture-"+nativeID+".jsonl")
				if err := os.WriteFile(path, append(header, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
				finish := holdFixtureStopObservation(t, f, boundary, status)
				terminal := receiveFixtureProviderResult(t, f, agent.Result{Status: "completed", Output: "accepted native outcome", SessionID: nativeID}, true)
				quiesceFixtureTurn(t, f, terminal)
				if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
					t.Fatal(err)
				}
				f.refresh(t)
				if !f.Grant.TurnComplete || f.Grant.CompletionWitness == nil || f.Session.State != workspace.SessionIdle {
					t.Fatal("first turn did not reach accepted, quiescent idle")
				}
				var envelope map[string]any
				if err := json.Unmarshal(first.Envelope, &envelope); err != nil {
					t.Fatal(err)
				}
				nextID := uuid.NewString()
				envelope["id"], envelope["auth_token"] = nextID, "mat_"+nextID
				envelope["dispatched_at"] = time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
				envelope["prior_session_id"], envelope["prior_work_dir"], envelope["new_comments_delta_known"] = nativeID, first.TaskRoot+"/workdir", true
				raw, _ := json.Marshal(envelope)
				claim, err := daemonapi.ParseClaim(raw)
				if err != nil {
					t.Fatal(err)
				}
				queued, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: nextID, WorkspaceID: claim.WorkspaceID,
					AgentID: claim.AgentID, RuntimeID: claim.RuntimeID, RuntimeRef: first.RuntimeRef, Envelope: raw,
					Metadata: first.Metadata, Repositories: first.Repositories, ResourceScope: first.ResourceScope})
				if err != nil {
					t.Fatal(err)
				}
				source := workspace.CheckpointSource{TaskID: first.TaskID, AttemptID: first.AttemptID}
				selection := workspace.Selection{Conversation: first.Conversation, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: workspace.SelectionResume,
					StorageID: first.StorageID, SessionID: nativeID, WorkDir: first.TaskRoot + "/workdir",
					SessionSource: source, WorkspaceSource: source, LatestWriter: source}
				if err := f.C.Store.RecordBackendSelection(queued.AttemptID, selection); err != nil {
					t.Fatal(err)
				}
				if err := f.C.Store.RecordCompatibility(queued.AttemptID, *first.Compatibility); err != nil {
					t.Fatal(err)
				}
				if err := f.C.Store.RecordSelection(queued.AttemptID, selection, first.CompatibilityDigest); err != nil {
					t.Fatal(err)
				}
				second, session, err := f.C.Store.ReserveTurn(queued.AttemptID, selection, first.CompatibilityDigest, f.C.ConversationIdleTimeout, f.C.MaxResidentPods)
				if err != nil || session.ID != first.WorkerSessionID || session.State != workspace.SessionReserved || second.TurnSequence != first.TurnSequence+1 {
					t.Fatal("next turn did not reserve the original session", err)
				}
				observed := finish()
				if observed.stopped || observed.err != nil {
					t.Fatal("late observation still acted on the completed turn", observed)
				}
				current, err := f.C.Store.GetSession(session.ID)
				if err != nil || current.Stop != nil || current.State != workspace.SessionReserved || current.ActiveAttempt != second.AttemptID || current.TurnSequence != second.TurnSequence {
					t.Fatal("the old backend response drained or replaced the reserved turn", err)
				}
				storage, err := f.C.Store.GetStorage(first.StorageID)
				if err != nil || storage.WriterSessionID != current.ID || storage.ActiveAttempt != second.AttemptID {
					t.Fatal("the old backend response changed the new writer lease", err)
				}
				second, err = f.C.Store.Get(second.AttemptID)
				if err != nil || second.Stop != nil || second.ExecutionRevoked {
					t.Fatal("the old backend response revoked the new turn", err)
				}
			})
		}
	}
}
