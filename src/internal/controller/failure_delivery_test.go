package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestFailureDeliverySettlesLostAcknowledgementAfterTokenRevocation(t *testing.T) {
	for _, source := range []string{"controller", "worker", "recovery"} {
		t.Run(source, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			before := recordFailureDeliveryFixture(t, f, source)
			taskID := f.Grant.TaskID
			body := before.Body
			if before.RecoveryFailure != nil {
				body = before.RecoveryFailure.Body
			}
			var accepted atomic.Bool
			var callbacks, deniedReads atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/daemon/tasks/" + taskID + "/status":
					if r.Header.Get("Authorization") != "Bearer local-owner-token" {
						t.Error("terminal observation used revoked task credentials")
					}
					status := "running"
					if accepted.Load() {
						status = "failed"
					}
					writeJSON(w, map[string]string{"status": status})
				case "/api/daemon/tasks/" + taskID + "/fail":
					callbacks.Add(1)
					raw, err := io.ReadAll(r.Body)
					if err != nil || !bytes.Equal(raw, body) {
						t.Error("failure delivery changed the recorded payload", err)
					}
					// Model the backend commit and token revocation before the
					// accepted callback's response is lost on the connection.
					accepted.Store(true)
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
				default:
					if accepted.Load() {
						deniedReads.Add(1)
						http.Error(w, "task token revoked", http.StatusUnauthorized)
						return
					}
					f.serveBackend(w, r)
				}
			}))
			if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
				t.Fatal(err)
			}
			uncertain, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil || failureDeliveryState(uncertain) != "uncertain" || callbacks.Load() != 1 || !accepted.Load() {
				t.Fatal("accepted failure with a lost response did not remain uncertain", err)
			}
			if f.C.sameAssignment(t.Context(), f.Grant) || deniedReads.Load() == 0 {
				t.Fatal("accepted failure left its task token usable")
			}
			tryNextFailure := pendingFailureBudgetFixture(t, f)
			if err := tryNextFailure(); !errors.Is(err, workspace.ErrAdmissionBudget) {
				t.Fatal("uncertain failure did not occupy the pending-result budget", err)
			}
			reopenStopRegressionStore(t, f.C, f.Options)
			f.refresh(t)
			if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
				t.Fatal("terminal failure remained blocked by its revoked task token", err)
			}
			reopenStopRegressionStore(t, f.C, f.Options)
			f.refresh(t)
			if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
				t.Fatal("settled failure changed after restart", err)
			}
			after, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil || failureDeliveryState(after) != "rejected" || callbacks.Load() != 1 || f.Grant.CompletionWitness != nil {
				t.Fatal("terminal observation inferred payload acceptance or repeated failure delivery", err)
			}
			assertFailureDeliveryEvidence(t, before, after)
			storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || storage.Checkpoint != nil || storage.WriterSessionID != f.Session.ID || storage.ActiveAttempt != f.Grant.AttemptID {
				t.Fatal("terminal settlement published continuity or released the writer", err)
			}
			if err := tryNextFailure(); err != nil {
				t.Fatal("settled failure retained the pending-result budget", err)
			}
		})
	}
}

func TestFailureDeliveryRejectsAnAlreadyCompletedTask(t *testing.T) {
	for _, source := range []string{"controller", "worker", "recovery"} {
		t.Run(source, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			before := recordFailureDeliveryFixture(t, f, source)
			var callbacks atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/status") {
					writeJSON(w, map[string]string{"status": "completed"})
					return
				}
				if r.Method == http.MethodPost {
					callbacks.Add(1)
				}
				http.Error(w, "task token revoked", http.StatusUnauthorized)
			}))
			if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			after, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil || failureDeliveryState(after) != "rejected" || callbacks.Load() != 0 || f.Grant.CompletionWitness != nil {
				t.Fatal("the existing completion authorized a failure callback or payload witness", err)
			}
			assertFailureDeliveryEvidence(t, before, after)
		})
	}
}

func TestFailureDeliveryRetainsActiveAssignmentChecks(t *testing.T) {
	for _, scenario := range []string{"running", "unknown", "unavailable", "runtime changed", "dispatch changed"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			before := recordFailureDeliveryFixture(t, f, "worker")
			if err := f.C.Store.BeginForward(f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := f.C.Store.FinishForward(f.Grant.AttemptID, "uncertain"); err != nil {
				t.Fatal(err)
			}
			reassigned := scenario == "runtime changed" || scenario == "dispatch changed"
			if reassigned {
				f.mu.Lock()
				row := f.Rows[f.Grant.TaskID]
				if scenario == "runtime changed" {
					row.RuntimeID = uuid.NewString()
				} else {
					row.DispatchedAt = time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
				}
				f.Rows[row.ID] = row
				f.mu.Unlock()
			}
			var callbacks atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/status") {
					switch scenario {
					case "unavailable":
						http.Error(w, "temporary failure", http.StatusServiceUnavailable)
					case "unknown":
						writeJSON(w, map[string]string{"status": "unknown"})
					default:
						writeJSON(w, map[string]string{"status": "running"})
					}
					return
				}
				if r.Method == http.MethodPost {
					callbacks.Add(1)
				}
				if reassigned {
					f.serveBackend(w, r)
				} else {
					http.Error(w, "task token unavailable", http.StatusUnauthorized)
				}
			}))
			err := f.C.deliverResult(t.Context(), f.Grant)
			if reassigned && err != nil || !reassigned && err == nil {
				t.Fatal("nonterminal control status displaced assignment validation", err)
			}
			after, err := f.C.Store.Terminal(f.Grant.AttemptID)
			wantState := "uncertain"
			if reassigned {
				wantState = "rejected"
			}
			if err != nil || after.State != wantState || callbacks.Load() != 0 {
				t.Fatal("failure delivery replayed without current task authority", err)
			}
			assertFailureDeliveryEvidence(t, before, after)
		})
	}
}

func TestTerminalStatusCannotSettleAnUnwitnessedNativeCompletion(t *testing.T) {
	for _, status := range []string{"failed", "completed"} {
		t.Run(status, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			receiveFixtureTurnResult(t, f, true)
			if err := f.C.Store.BeginForward(f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := f.C.Store.FinishForward(f.Grant.AttemptID, "uncertain"); err != nil {
				t.Fatal(err)
			}
			before, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			var callbacks atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/status") {
					writeJSON(w, map[string]string{"status": status})
					return
				}
				if r.Method == http.MethodPost {
					callbacks.Add(1)
				}
				http.Error(w, "task token revoked", http.StatusUnauthorized)
			}))
			if err := f.C.deliverResult(t.Context(), f.Grant); err == nil {
				t.Fatal("terminal status substituted for exact accepted completion evidence")
			}
			f.refresh(t)
			after, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil || after.State != "uncertain" || callbacks.Load() != 0 || f.Grant.CompletionWitness != nil {
				t.Fatal("unwitnessed native completion was settled or replayed", err)
			}
			assertFailureDeliveryEvidence(t, before, after)
			storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || storage.Checkpoint != nil || storage.WriterSessionID != f.Session.ID || storage.ActiveAttempt != f.Grant.AttemptID {
				t.Fatal("unwitnessed completion published continuity or released its writer", err)
			}
		})
	}
}

func recordFailureDeliveryFixture(t *testing.T, f *sessionControllerFixture, source string) workspace.Terminal {
	t.Helper()
	switch source {
	case "controller":
		if err := f.C.recordFailure(f.Grant, errors.New("observed worker failure")); err != nil {
			t.Fatal(err)
		}
	case "worker":
		receiveFixtureProviderResult(t, f, agent.Result{Status: "failed", Error: "actual native failure"}, true)
	case "recovery":
		receiveFixtureProviderResult(t, f, agent.Result{Status: "completed", Output: "actual unsigned native outcome"}, false)
		if _, err := f.C.requestStop(f.Grant, "worker_unsealed"); err != nil {
			t.Fatal(err)
		}
		if err := f.C.Store.CloseCheckouts(f.Grant.AttemptID); err != nil {
			t.Fatal(err)
		}
		if err := f.C.Store.ObserveStop(f.Grant.AttemptID, workspace.StopEvidence{Kind: "terminated", PodUID: f.Grant.PodUID,
			PVCUID: f.Grant.PVCUID, ObservedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		if err := f.C.Store.RecordRecoveryFailure(f.Grant.AttemptID, []byte(`{"error":"shutdown proof missing; actual result retained"}`)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("unsupported failure fixture")
	}
	f.refresh(t)
	terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return terminal
}

func failureDeliveryState(terminal workspace.Terminal) string {
	if terminal.RecoveryFailure != nil {
		return terminal.RecoveryFailure.State
	}
	return terminal.State
}

func assertFailureDeliveryEvidence(t *testing.T, before, after workspace.Terminal) {
	t.Helper()
	if after.Source != before.Source || after.Kind != before.Kind || !bytes.Equal(after.Body, before.Body) ||
		after.RequestDigest != before.RequestDigest || after.ResultDigest != before.ResultDigest || after.Nonce != before.Nonce ||
		!reflect.DeepEqual(after.ResultReceipt, before.ResultReceipt) || !reflect.DeepEqual(after.Seal, before.Seal) {
		t.Fatal("delivery settlement changed the retained native outcome or signature")
	}
	if before.RecoveryFailure != nil && (after.RecoveryFailure == nil || after.State != before.State ||
		!bytes.Equal(after.RecoveryFailure.Body, before.RecoveryFailure.Body)) {
		t.Fatal("recovery settlement changed the original result or recovery payload")
	}
}

func pendingFailureBudgetFixture(t *testing.T, f *sessionControllerFixture) func() error {
	t.Helper()
	f.Options.MaxPendingResults = 1
	reopenStopRegressionStore(t, f.C, f.Options)
	var envelope map[string]any
	if err := json.Unmarshal(f.Grant.Envelope, &envelope); err != nil {
		t.Fatal(err)
	}
	nextID := uuid.NewString()
	envelope["id"], envelope["auth_token"] = nextID, "mat_"+nextID
	raw, _ := json.Marshal(envelope)
	next, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: nextID, WorkspaceID: f.Grant.WorkspaceID,
		AgentID: f.Grant.AgentID, RuntimeID: f.Grant.RuntimeID, RuntimeRef: f.Grant.RuntimeRef, Envelope: raw,
		Metadata: f.Grant.Metadata, Repositories: f.Grant.Repositories, ResourceScope: f.Grant.ResourceScope})
	if err != nil {
		t.Fatal(err)
	}
	return func() error {
		_, err := f.C.Store.ReceiveFailure(next.AttemptID, []byte(`{"error":"next pending result"}`))
		return err
	}
}
