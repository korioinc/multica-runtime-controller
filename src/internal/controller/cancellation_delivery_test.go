package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestCancelledModernDeliverySurvivesRevokedTaskToken(t *testing.T) {
	for _, kind := range []string{"cancel-ack", "worker-failure", "worker-completion", "controller-failure"} {
		t.Run(kind, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			if kind == "controller-failure" {
				if err := f.C.recordFailure(f.Grant, errors.New("worker stopped")); err != nil {
					t.Fatal(err)
				}
			} else {
				status := "failed"
				if kind == "worker-completion" {
					status = "completed"
				} else if kind == "cancel-ack" {
					if _, err := f.C.requestStop(f.Grant, "backend_cancelled"); err != nil {
						t.Fatal(err)
					}
					f.refresh(t)
					status = "cancelled"
				}
				receiveFixtureProviderResult(t, f, agent.Result{Status: status, Output: "actual result", Error: "actual cancellation or failure"}, true)
			}
			before, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			f.Options.MaxPendingResults = 1
			reopenStopRegressionStore(t, f.C, f.Options)
			var envelope map[string]any
			if err := json.Unmarshal(f.Grant.Envelope, &envelope); err != nil {
				t.Fatal(err)
			}
			nextID := uuid.NewString()
			envelope["id"], envelope["auth_token"] = nextID, "mat_"+nextID
			raw, _ := json.Marshal(envelope)
			next, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: nextID,
				WorkspaceID: f.Grant.WorkspaceID, AgentID: f.Grant.AgentID, RuntimeID: f.Grant.RuntimeID,
				RuntimeRef: f.Grant.RuntimeRef, Envelope: raw, Metadata: f.Grant.Metadata,
				Repositories: f.Grant.Repositories, ResourceScope: f.Grant.ResourceScope})
			if err != nil {
				t.Fatal(err)
			}
			failureBody, _ := json.Marshal(map[string]string{"error": "next pending result"})
			if _, err := f.C.Store.ReceiveFailure(next.AttemptID, failureBody); !errors.Is(err, workspace.ErrAdmissionBudget) {
				t.Fatal("unsettled cancellation did not occupy the pending-result budget", err)
			}
			var acknowledgements atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/daemon/tasks/"+f.Grant.TaskID+"/status" {
					if r.Header.Get("Authorization") != "Bearer local-owner-token" {
						t.Error("terminal observation used revoked task authority")
					}
					writeJSON(w, map[string]string{"status": "cancelled"})
					return
				}
				if r.URL.Path == "/api/daemon/tasks/"+f.Grant.TaskID+"/cancel-ack" {
					if r.Header.Get("Authorization") != "Bearer local-owner-token" {
						t.Error("cancel acknowledgement used revoked task authority")
					}
					acknowledgements.Add(1)
					writeJSON(w, struct{}{})
					return
				}
				if strings.HasSuffix(r.URL.Path, "/complete") || strings.HasSuffix(r.URL.Path, "/fail") {
					t.Error("cancelled task received a replacement completion or failure")
				}
				http.Error(w, "task token revoked", http.StatusUnauthorized)
			}))
			f.refresh(t)
			if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
				t.Fatal("revoked task token blocked terminal settlement", err)
			}
			reopenStopRegressionStore(t, f.C, f.Options)
			f.refresh(t)
			if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
				t.Fatal("settled delivery changed after restart", err)
			}
			after, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			wantState, wantACKs := "rejected", int32(0)
			if kind == "cancel-ack" {
				wantState, wantACKs = "delivered", 1
			}
			if after.State != wantState || acknowledgements.Load() != wantACKs || !bytes.Equal(after.Body, before.Body) ||
				after.ResultDigest != before.ResultDigest || after.RequestDigest != before.RequestDigest ||
				(before.ResultReceipt != nil && after.ResultReceipt == nil) || f.Grant.CompletionWitness != nil {
				t.Fatal("terminal did not settle exactly once without replacing the native outcome", after.State, acknowledgements.Load())
			}
			storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || storage.Checkpoint != nil || storage.WriterSessionID != f.Session.ID {
				t.Fatal("terminal status released the unproven writer or published continuity", err)
			}
			if f.C.sameAssignment(t.Context(), f.Grant) {
				t.Fatal("terminal status restored task execution authority")
			}
			if _, err := f.C.Store.ReceiveFailure(next.AttemptID, failureBody); err != nil {
				t.Fatal("settled cancellation retained pending-result budget", err)
			}
		})
	}
}

func TestCancelledModernDeliveryRetriesLostAcknowledgementAfterReopen(t *testing.T) {
	f := newSessionControllerFixture(t)
	if _, err := f.C.requestStop(f.Grant, "backend_cancelled"); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	receiveFixtureProviderResult(t, f, agent.Result{Status: "cancelled", Error: "preserved cancellation"}, true)
	var acknowledgements atomic.Int32
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/daemon/tasks/" + f.Grant.TaskID + "/status":
			writeJSON(w, map[string]string{"status": "cancelled"})
		case "/api/daemon/tasks/" + f.Grant.TaskID + "/cancel-ack":
			if acknowledgements.Add(1) == 1 {
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				_ = connection.Close()
				return
			}
			writeJSON(w, struct{}{})
		default:
			http.Error(w, "task token revoked", http.StatusUnauthorized)
		}
	}))
	if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	before, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil || before.State != "uncertain" || acknowledgements.Load() != 1 {
		t.Fatal("lost cancellation acknowledgement was not retained", err)
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	f.refresh(t)
	if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	after, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil || after.State != "delivered" || acknowledgements.Load() != 2 || !bytes.Equal(before.Body, after.Body) ||
		after.RequestDigest != before.RequestDigest || after.ResultReceipt == nil {
		t.Fatal("reopened cancellation did not replay its exact signed result", err)
	}
}

func TestCancelledModernDeliveryWaitsForCancellationEvidence(t *testing.T) {
	for _, status := range []string{"running", "unavailable"} {
		t.Run(status, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			if _, err := f.C.requestStop(f.Grant, "task_deadline_exceeded"); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			receiveFixtureProviderResult(t, f, agent.Result{Status: "cancelled"}, true)
			var acknowledgements atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/status") {
					if status == "unavailable" {
						http.Error(w, "temporary failure", http.StatusServiceUnavailable)
					} else {
						writeJSON(w, map[string]string{"status": status})
					}
					return
				}
				if strings.HasSuffix(r.URL.Path, "/cancel-ack") {
					acknowledgements.Add(1)
				}
				http.Error(w, "task token unavailable", http.StatusUnauthorized)
			}))
			if err := f.C.deliverResult(t.Context(), f.Grant); err == nil {
				t.Fatal("nonterminal or unavailable status replaced assignment authority")
			}
			terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil || terminal.State != "received" || acknowledgements.Load() != 0 {
				t.Fatal("uncertain cancellation observation settled or forwarded a result", err)
			}
		})
	}
}

func TestCancelledModernDeliveryRequiresAuthenticatedResult(t *testing.T) {
	f := newSessionControllerFixture(t)
	if _, err := f.C.requestStop(f.Grant, "backend_cancelled"); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	receiveFixtureProviderResult(t, f, agent.Result{Status: "cancelled"}, false)
	var requests atomic.Int32
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		writeJSON(w, map[string]string{"status": "cancelled"})
	}))
	if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil || terminal.State != "received" || terminal.ResultReceipt != nil || requests.Load() != 0 {
		t.Fatal("unsigned cancellation acquired terminal delivery authority", err)
	}
	token, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, "daemon")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.C.Store.Authorize(token, "daemon"); !errors.Is(err, workspace.ErrUnauthorized) {
		t.Fatal("cancelled turn regained business API authority", err)
	}
}
