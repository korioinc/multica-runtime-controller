package controller

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func receiveFixtureTurnResult(t *testing.T, f *sessionControllerFixture, authenticated bool) workspace.Terminal {
	return receiveFixtureProviderResult(t, f, agent.Result{Status: "completed", Output: "the exact successful provider result"}, authenticated)
}

func receiveFixtureProviderResult(t *testing.T, f *sessionControllerFixture, result agent.Result, authenticated bool) workspace.Terminal {
	t.Helper()
	token, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	response := f.request(http.MethodPost, "/internal/attempts/"+f.Grant.AttemptID+"/result", token,
		wire.ProviderResult{Result: result})
	if response.Code != http.StatusOK {
		t.Fatalf("provider result: %d %s", response.Code, response.Body.String())
	}
	terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if authenticated {
		receipt := workspace.ResultReceipt{WorkerSessionID: f.Session.ID, TurnSequence: f.Grant.TurnSequence,
			TaskID: f.Grant.TaskID, AttemptID: f.Grant.AttemptID, PodUID: f.Grant.PodUID, PVCUID: f.Grant.PVCUID,
			RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}
		receipt.Signature = ed25519.Sign(f.Key, workspace.ResultReceiptMessage(receipt))
		response = f.request(http.MethodPost, "/internal/attempts/"+f.Grant.AttemptID+"/result-receipt", token, receipt)
		if response.Code != http.StatusOK {
			t.Fatalf("result signature: %d %s", response.Code, response.Body.String())
		}
	}
	f.refresh(t)
	return terminal
}

func quiesceFixtureTurn(t *testing.T, f *sessionControllerFixture, terminal workspace.Terminal) {
	t.Helper()
	// The controller's public journal boundary receives positive local writer
	// and flush observations. Linux/NFS proofs cover those observations separately.
	if err := f.C.Store.CloseCheckouts(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.CheckoutsFlushed(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	receipt := workspace.TurnExecutionReceipt{WorkerSessionID: f.Session.ID, Conversation: f.Grant.Conversation, StorageID: f.Grant.StorageID,
		TaskID: f.Grant.TaskID, AttemptID: f.Grant.AttemptID,
		Generation: f.Grant.Generation, TurnSequence: f.Grant.TurnSequence, InputDigest: f.Grant.InputDigest,
		PodUID: f.Grant.PodUID, PVCUID: f.Grant.PVCUID, ResultDigest: terminal.ResultDigest, RequestDigest: terminal.RequestDigest,
		Nonce: terminal.Nonce, TaskProcessesStopped: true, LocalRequestsClosed: true, PrivateStateCleared: true}
	receipt.Signature = ed25519.Sign(f.Key, workspace.TurnExecutionReceiptMessage(receipt))
	f.signed(t, workspace.SessionOperationTurnExecutionReceipt, receipt)
	f.refresh(t)
	if f.Grant.TurnExecutionReceipt == nil {
		t.Fatal("task execution stop was not committed")
	}
}

func TestTurnDeliveryJoinsAcceptanceAndQuiescenceInEitherOrder(t *testing.T) {
	for _, first := range []string{"acceptance", "quiescence"} {
		t.Run(first, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			terminal := receiveFixtureTurnResult(t, f, true)
			if first == "quiescence" {
				quiesceFixtureTurn(t, f, terminal)
			} else if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
				t.Fatal(err)
			}
			reopenStopRegressionStore(t, f.C, f.Options)
			f.refresh(t)
			storage, _ := f.C.Store.GetStorage(f.Grant.StorageID)
			if storage.Checkpoint != nil {
				t.Fatal("one independent proof published a reusable checkpoint")
			}
			if first == "acceptance" {
				quiesceFixtureTurn(t, f, terminal)
			}
			if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			storage, _ = f.C.Store.GetStorage(f.Grant.StorageID)
			accepted, _ := f.C.Store.Terminal(f.Grant.AttemptID)
			if f.Session.State != workspace.SessionIdle || !f.Grant.TurnComplete || f.Grant.CompletionWitness == nil ||
				storage.Checkpoint != nil || !storage.Dirty || storage.WriterSessionID != f.Session.ID || accepted.State != "delivered" || !bytes.Equal(accepted.Body, terminal.Body) {
				t.Fatal("matching independent proofs did not retain the actual successful result")
			}
			deadline := f.Session.IdleDeadline
			reopenStopRegressionStore(t, f.C, f.Options)
			f.refresh(t)
			if f.Session.State != workspace.SessionIdle || f.Session.IdleDeadline != deadline {
				t.Fatal("recovery changed the completed turn or extended its idle interval")
			}
		})
	}
}

func TestCurrentTurnAssignmentAndDeliveryIgnoreUnneededHistoryPages(t *testing.T) {
	f := newSessionControllerFixture(t)
	receiveFixtureTurnResult(t, f, true)
	var tailReads atomic.Int32
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/agents/") && strings.HasSuffix(r.URL.Path, "/tasks") {
			if r.URL.Query().Get("before") != "" {
				tailReads.Add(1)
				http.Error(w, "unneeded history unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("X-Agent-Tasks-Next-Cursor", "older-history")
		}
		f.serveBackend(w, r)
	}))
	if !f.C.sameAssignment(t.Context(), f.Grant) {
		t.Fatal("current assignment required unrelated history pages")
	}
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal("exact completion observation required unrelated history pages", err)
	}
	f.refresh(t)
	terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil || terminal.State != "delivered" || f.Grant.CompletionWitness == nil || tailReads.Load() != 0 {
		t.Fatal("current task did not settle independently of the history tail", err, tailReads.Load())
	}
}

func TestTurnDeliveryLostAcknowledgementRecoversFromExactBackendHistory(t *testing.T) {
	f := newSessionControllerFixture(t)
	var callbacks atomic.Int32
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/complete") {
			f.serveBackend(w, r)
			return
		}
		callbacks.Add(1)
		f.serveBackend(httptest.NewRecorder(), r)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	terminal := receiveFixtureTurnResult(t, f, true)
	quiesceFixtureTurn(t, f, terminal)
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	pending, _ := f.C.Store.Terminal(f.Grant.AttemptID)
	storage, _ := f.C.Store.GetStorage(f.Grant.StorageID)
	if pending.State != "uncertain" || f.Grant.CompletionWitness != nil || storage.Checkpoint != nil {
		t.Fatal("lost response became assumed backend acceptance")
	}
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	accepted, _ := f.C.Store.Terminal(f.Grant.AttemptID)
	if callbacks.Load() != 1 || f.Grant.CompletionWitness == nil || !f.Grant.TurnComplete || accepted.State != "delivered" ||
		f.Session.State != workspace.SessionIdle || !bytes.Equal(accepted.Body, terminal.Body) {
		t.Fatal("exact historical acknowledgement did not settle the original result once")
	}
	f.mu.Lock()
	starts := f.Starts[f.Grant.TaskID]
	f.mu.Unlock()
	if starts != 1 {
		t.Fatal("recovery repeated provider start")
	}
}

func TestTurnDeliveryRecoversWithLaterClaimAfterOriginalTokenRevocation(t *testing.T) {
	f := newSessionControllerFixture(t)
	terminal := receiveFixtureTurnResult(t, f, true)
	quiesceFixtureTurn(t, f, terminal)
	if err := f.C.Store.BeginForward(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/daemon/tasks/"+f.Grant.TaskID+"/complete", bytes.NewReader(terminal.Body))
	f.serveBackend(httptest.NewRecorder(), request)
	if err := f.C.Store.FinishForward(f.Grant.AttemptID, "uncertain"); err != nil {
		t.Fatal(err)
	}
	original, err := daemonapi.ParseClaim(f.Grant.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(f.Grant.Envelope, &fields); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	fields["id"], _ = json.Marshal(id)
	fields["auth_token"], _ = json.Marshal("mat_" + id)
	envelope, _ := json.Marshal(fields)
	if _, err := f.C.Store.QueueClaim(workspace.TaskGrant{TaskID: id, WorkspaceID: f.Grant.WorkspaceID, AgentID: f.Grant.AgentID,
		RuntimeID: f.Grant.RuntimeID, RuntimeRef: f.Grant.RuntimeRef, SessionProtocol: true, Envelope: envelope, Metadata: f.Grant.Metadata,
		Repositories: f.Grant.Repositories, ResourceScope: f.Grant.ResourceScope}); err != nil {
		t.Fatal(err)
	}
	var currentReads atomic.Int32
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer "+original.AuthToken {
			http.Error(w, "terminal task token revoked", http.StatusUnauthorized)
			return
		}
		if r.Header.Get("Authorization") == "Bearer mat_"+id {
			currentReads.Add(1)
		}
		f.serveBackend(w, r)
	}))
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if currentReads.Load() == 0 || f.Grant.CompletionWitness == nil || !f.Grant.TurnComplete {
		t.Fatal("later authorized task could not witness its predecessor's accepted result")
	}
}

func TestTurnLateAcceptanceRequiresFinalClosedWriterProof(t *testing.T) {
	f := newSessionControllerFixture(t)
	terminal := receiveFixtureTurnResult(t, f, true)
	if err := f.C.Store.BeginForward(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/daemon/tasks/"+f.Grant.TaskID+"/complete", bytes.NewReader(terminal.Body))
	f.serveBackend(httptest.NewRecorder(), request)
	if err := f.C.Store.FinishForward(f.Grant.AttemptID, "uncertain"); err != nil {
		t.Fatal(err)
	}
	session, err := f.C.Store.RequestSessionStop(f.Session.ID, "delivery_unknown_at_deadline")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.CloseCheckouts(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.CheckoutsFlushed(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.ObserveSessionStop(session.ID, workspace.StopEvidence{Kind: "terminated", PodUID: session.PodUID,
		PVCUID: session.PVCUID, ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.CloseSession(session.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.MarkSessionCleaned(session.ID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	closed := f.Session
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	storage, _ := f.C.Store.GetStorage(f.Grant.StorageID)
	if f.Grant.CompletionWitness == nil || storage.Checkpoint != nil || !storage.Dirty {
		t.Fatal("late accepted result substituted for final writer flush proof")
	}
	if err := f.C.Store.RecordSessionControllerFlush(session.ID); err != nil {
		t.Fatal(err)
	}
	receipt := workspace.SessionStopReceipt{WorkerSessionID: session.ID, AttemptID: session.LastAttemptID, InputDigest: session.InputDigest,
		TurnSequence: session.TurnSequence, PodUID: session.PodUID, PVCUID: session.PVCUID, Revision: session.Stop.Revision,
		Nonce: session.Stop.Nonce, WritersStopped: true, FlushOK: true}
	receipt.Signature = ed25519.Sign(f.Key, workspace.SessionStopReceiptMessage(receipt))
	response := f.signed(t, workspace.SessionOperationStopReceipt, receipt)
	if response.Code != http.StatusOK {
		t.Fatalf("late session proof: %d %s", response.Code, response.Body.String())
	}
	if err := f.C.finishTurn(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	storage, _ = f.C.Store.GetStorage(f.Grant.StorageID)
	if storage.Checkpoint == nil || storage.Dirty || f.Session.State != workspace.SessionClosed || f.Session.Revision != closed.Revision ||
		!f.Session.ResourcesCleaned || f.Session.IdleDeadline != closed.IdleDeadline {
		t.Fatal("late final proof lost the checkpoint or reopened old compute")
	}
}

func TestTurnDeliveryRejectsSuccessfulResponseWithDifferentResult(t *testing.T) {
	for _, mismatch := range []string{"output", "dispatch"} {
		t.Run(mismatch, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/complete") {
					f.serveBackend(w, r)
					return
				}
				response := httptest.NewRecorder()
				f.serveBackend(response, r)
				var row daemonapi.TaskObservation
				_ = json.Unmarshal(response.Body.Bytes(), &row)
				if mismatch == "output" {
					row.Result.Output = "another successful result"
				} else {
					row.DispatchedAt = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
				}
				writeJSON(w, row)
			}))
			terminal := receiveFixtureTurnResult(t, f, true)
			quiesceFixtureTurn(t, f, terminal)
			if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			actual, _ := f.C.Store.Terminal(f.Grant.AttemptID)
			storage, _ := f.C.Store.GetStorage(f.Grant.StorageID)
			if actual.State != "rejected" || f.Grant.CompletionWitness != nil || storage.Checkpoint != nil ||
				f.Session.State != workspace.SessionDraining || actual.Kind != terminal.Kind || !bytes.Equal(actual.Body, terminal.Body) {
				t.Fatal("HTTP success replaced exact acceptance or changed the native outcome")
			}
		})
	}
}

func TestTurnDeliveryWaitsForResultSignature(t *testing.T) {
	f := newSessionControllerFixture(t)
	receiveFixtureTurnResult(t, f, false)
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	row := f.Rows[f.Grant.TaskID]
	f.mu.Unlock()
	if row.Status != "running" {
		t.Fatal("unsigned outcome reached the backend terminal endpoint")
	}
}

func TestTurnUncertainEventDrainsWithoutReplacingAcceptedSuccess(t *testing.T) {
	f := newSessionControllerFixture(t)
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/messages") {
			http.Error(w, "delivery outcome unknown", http.StatusServiceUnavailable)
			return
		}
		f.serveBackend(w, r)
	}))
	token, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	event := wire.ProviderEvent{Sequence: 1, Message: &agent.Message{Type: agent.MessageText, Content: "observed provider progress"}}
	response := f.request(http.MethodPost, "/internal/attempts/"+f.Grant.AttemptID+"/event", token, event)
	if response.Code != http.StatusOK {
		t.Fatalf("event journal: %d %s", response.Code, response.Body.String())
	}
	f.refresh(t)
	if len(f.Grant.Events) != 1 || f.Grant.Events[0].State != "uncertain" {
		t.Fatal("ambiguous event delivery was not preserved")
	}
	terminal := receiveFixtureTurnResult(t, f, true)
	quiesceFixtureTurn(t, f, terminal)
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	actual, _ := f.C.Store.Terminal(f.Grant.AttemptID)
	storage, _ := f.C.Store.GetStorage(f.Grant.StorageID)
	if actual.State != "delivered" || f.Grant.CompletionWitness == nil || actual.Kind != "complete" || !bytes.Equal(actual.Body, terminal.Body) ||
		storage.Checkpoint != nil || f.Session.State != workspace.SessionDraining || f.Session.Stop.Reason != "event_delivery_uncertain" {
		t.Fatal("event uncertainty changed success or authorized reuse")
	}
}
