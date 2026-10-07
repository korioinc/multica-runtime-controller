//go:build linux

package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

// This fixture proves the controller HTTP and durable native-file boundaries.
// The pinned Pi adapter's lock behavior has separate provider protocol coverage.
func TestTurnPiTransientHTTPPreservesOriginalNativeSession(t *testing.T) {
	if os.Getenv("MULTICA_CONVERSATION_FS_TEST") != "1" {
		t.Skip("requires an explicitly owned Linux /workspace fixture")
	}
	f := newSessionControllerFixture(t, "pi")
	first := f.Grant
	if _, err := os.Lstat(first.TaskRoot); !os.IsNotExist(err) {
		t.Fatal("fixture native root already exists", err)
	}
	for _, directory := range []string{"workdir", "pi-sessions"} {
		if err := os.MkdirAll(filepath.Join(first.TaskRoot, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = os.RemoveAll(first.TaskRoot) })
	nativeID := filepath.Join(first.TaskRoot, "pi-sessions/original.jsonl")
	header, _ := json.Marshal(map[string]string{"type": "session", "id": uuid.NewString(), "cwd": first.TaskRoot + "/workdir"})
	if err := os.WriteFile(nativeID, append(header, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	terminal := receiveFixtureProviderResult(t, f, agent.Result{Status: "completed", Output: "original Pi session", SessionID: nativeID}, true)
	quiesceNativeFixtureTurn(t, f, terminal)
	if err := f.C.reconcileDelivery(t.Context(), first.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if f.Session.State != workspace.SessionIdle {
		t.Fatal("first native session did not become idle")
	}
	startPiFixtureFollowup(t, f)
	second := f.Grant
	errorText := "Pi session file " + nativeID + " is already in use by another execution"
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/fail") {
			f.serveBackend(w, r)
			return
		}
		var failure struct {
			Error   string `json:"error"`
			WorkDir string `json:"work_dir"`
		}
		if json.NewDecoder(r.Body).Decode(&failure) != nil {
			http.Error(w, "invalid failure", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		row := f.Rows[second.TaskID]
		row.Status, row.Error, row.WorkDir, row.CompletedAt = "failed", failure.Error, failure.WorkDir, time.Now().UTC().Format(time.RFC3339)
		row.FailureReason = taskfailure.NormalizeDaemonReason(taskfailure.Classify(failure.Error).String(), failure.Error).String()
		f.Rows[row.ID] = row
		f.mu.Unlock()
		writeJSON(w, row)
	}))
	terminal = receiveFixtureProviderResult(t, f, agent.Result{Status: "failed", Error: errorText, ResumeRejectedTransient: true}, true)
	if f.Grant.PendingResume == nil || !f.Grant.PendingResume.ResumeRejectedTransient || f.Grant.PendingResume.RetiredSessionID != "" {
		t.Fatal("transient provider result was dropped or permanently retired")
	}
	quiesceNativeFixtureTurn(t, f, terminal)
	if err := f.C.reconcileDelivery(t.Context(), second.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	storage, _ := f.C.Store.GetStorage(second.StorageID)
	actual, _ := f.C.Store.Terminal(second.AttemptID)
	if f.Grant.CompletionWitness == nil || f.Grant.CompletionWitness.Status != "failed" || f.Grant.CompletionWitness.SessionID != "" ||
		storage.Checkpoint != nil || !storage.Dirty || storage.WriterSessionID != f.Session.ID || storage.LatestWriter.AttemptID != second.AttemptID ||
		f.Session.State != workspace.SessionIdle || f.Session.ID != first.WorkerSessionID || f.Grant.TaskRoot != first.TaskRoot ||
		actual.Kind != "fail" || actual.State != "delivered" || !bytes.Equal(actual.Body, terminal.Body) {
		t.Fatal("transient HTTP settlement changed result, producer, storage, or compute identity")
	}
	if err := workspace.ValidateSession(*f.Grant.Prepared, nativeID); err != nil {
		t.Fatal("transient failure lost the actual original native file", err)
	}
	startPiFixtureFollowup(t, f)
	if f.Grant.ResumeSession != nativeID || f.Grant.Selection.SessionSource.AttemptID != first.AttemptID ||
		f.Grant.Selection.WorkspaceSource.AttemptID != second.AttemptID || f.Session.ID != first.WorkerSessionID {
		t.Fatal("warm transient continuation lost its accepted native producer or live owner")
	}
}

func quiesceNativeFixtureTurn(t *testing.T, f *sessionControllerFixture, terminal workspace.Terminal) {
	t.Helper()
	stop, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, "stop")
	if err != nil {
		t.Fatal(err)
	}
	f.request(http.MethodPost, "/internal/attempts/"+f.Grant.AttemptID+"/checkout-stop", stop, struct{}{})
	if err := workspace.SyncTaskFilesystem(f.Grant.TaskRoot); err != nil {
		t.Fatal(err)
	}
	quiesceFixtureTurn(t, f, terminal)
}

func startPiFixtureFollowup(t *testing.T, f *sessionControllerFixture) {
	t.Helper()
	previous := f.Grant
	var fields map[string]any
	if err := json.Unmarshal(previous.Envelope, &fields); err != nil {
		t.Fatal(err)
	}
	storage, err := f.C.Store.GetStorage(previous.StorageID)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	nativeID := previous.CompletionWitness.SessionID
	nativeSource := storage.LatestWriter
	if nativeID == "" && previous.PendingResume.ResumeRejectedTransient {
		nativeID, nativeSource = previous.Selection.SessionID, previous.Selection.SessionSource
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	fields["id"], fields["auth_token"], fields["dispatched_at"] = id, "mat_"+id, at.Format(time.RFC3339Nano)
	fields["prior_session_id"], fields["prior_work_dir"] = nativeID, storage.TaskRoot+"/workdir"
	fields["new_comments_delta_known"] = true
	envelope, _ := json.Marshal(fields)
	claim, err := daemonapi.ParseClaim(envelope)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.Rows[id] = daemonapi.TaskObservation{ID: id, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID, RuntimeID: claim.RuntimeID,
		Kind: claim.Kind, IssueID: claim.IssueID, Status: "dispatched", CreatedAt: at.Format(time.RFC3339), DispatchedAt: at.Format(time.RFC3339)}
	f.mu.Unlock()
	queued, err := f.C.Store.QueueClaim(workspace.TaskGrant{TaskID: id, SessionProtocol: true, WorkspaceID: previous.WorkspaceID,
		AgentID: previous.AgentID, RuntimeID: previous.RuntimeID, RuntimeRef: previous.RuntimeRef, Metadata: previous.Metadata,
		Envelope: envelope, Repositories: previous.Repositories, ResourceScope: previous.ResourceScope})
	if err != nil {
		t.Fatal(err)
	}
	selection := workspace.Selection{Conversation: previous.Conversation, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: workspace.SelectionResume,
		StorageID: storage.ID, SessionSource: nativeSource, WorkspaceSource: storage.LatestWriter,
		LatestWriter: storage.LatestWriter, SessionID: nativeID, WorkDir: storage.TaskRoot + "/workdir"}
	if err := f.C.Store.RecordBackendSelection(queued.AttemptID, selection); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.RecordCompatibility(queued.AttemptID, *previous.Compatibility); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.RecordSelection(queued.AttemptID, selection, previous.CompatibilityDigest); err != nil {
		t.Fatal(err)
	}
	f.Grant, f.Session, err = f.C.Store.ReserveTurn(queued.AttemptID, selection, previous.CompatibilityDigest, f.C.ConversationIdleTimeout, f.C.MaxResidentPods)
	if err != nil {
		t.Fatal(err)
	}
	prepareConversationFixtureTurn(t, f.C, f.Grant)
	f.refresh(t)
	if err := f.C.provisionTurn(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	f.acceptAndStart(t)
	if !f.Grant.StartConfirmed {
		t.Fatal("follow-up lacked durable start authorization")
	}
}
