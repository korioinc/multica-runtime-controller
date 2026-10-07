package controller

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestTurnDeliveryUsesControllerObservationAfterTaskTokenExpiry(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		name := "acknowledged"
		if lostReply {
			name = "lost reply and journal reopen"
		}
		t.Run(name, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			terminal := receiveFixtureTurnResult(t, f, true)
			quiesceFixtureTurn(t, f, terminal)
			before, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer mat_") {
					http.Error(w, "task token expired", http.StatusUnauthorized)
					return
				}
				if r.Header.Get("Authorization") != "Bearer local-owner-token" {
					http.Error(w, "controller authority required", http.StatusUnauthorized)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/complete") {
					if lostReply {
						f.serveBackend(httptest.NewRecorder(), r)
						connection, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = connection.Close()
						return
					}
				}
				f.serveBackend(w, r)
			}))
			if f.C.sameAssignment(t.Context(), f.Grant) {
				t.Fatal("expired task token retained execution authority")
			}
			if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
				t.Fatal("expired task token blocked its authenticated native result", err)
			}
			if lostReply {
				f.refresh(t)
				pending, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil || pending.State != "uncertain" || f.Grant.CompletionWitness != nil {
					t.Fatal("lost response became an inferred acceptance witness", err)
				}
				reopenStopRegressionStore(t, f.C, f.Options)
				f.refresh(t)
				if err := f.C.deliverResult(t.Context(), f.Grant); err != nil {
					t.Fatal("controller could not recover the exact accepted result after restart", err)
				}
			}
			f.refresh(t)
			after, err := f.C.Store.Terminal(f.Grant.AttemptID)
			storage, storageErr := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || storageErr != nil || after.State != "delivered" || f.Grant.CompletionWitness == nil ||
				!f.Grant.TurnComplete || f.Session.State != workspace.SessionIdle || storage.Checkpoint != nil || !storage.Dirty || storage.WriterSessionID != f.Session.ID {
				t.Fatal("controller observation did not settle exactly one authenticated completion", err, storageErr)
			}
			if !bytes.Equal(after.Body, before.Body) || after.ResultDigest != before.ResultDigest || after.RequestDigest != before.RequestDigest ||
				!reflect.DeepEqual(after.ResultReceipt, before.ResultReceipt) {
				t.Fatal("token-expiry recovery changed the signed native result")
			}
			if f.C.sameAssignment(t.Context(), f.Grant) {
				t.Fatal("result-delivery fallback restored task execution authority")
			}
			f.mu.Lock()
			starts := f.Starts[f.Grant.TaskID]
			f.mu.Unlock()
			if starts != 1 {
				t.Fatal("result recovery repeated backend start authorization")
			}
		})
	}
}

func TestControllerResultObservationCannotInventAcceptanceOrAssignment(t *testing.T) {
	for _, scenario := range []string{
		"controller denied", "controller unavailable", "status only", "missing task", "foreign workspace", "foreign agent",
		"other runtime", "other dispatch", "other result", "missing result", "unsigned result",
	} {
		t.Run(scenario, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			receiveFixtureTurnResult(t, f, scenario != "unsigned result")
			before, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "other result" || scenario == "missing result" {
				request := httptest.NewRequest(http.MethodPost, "/api/daemon/tasks/"+f.Grant.TaskID+"/complete", bytes.NewReader(before.Body))
				f.serveBackend(httptest.NewRecorder(), request)
			}
			f.mu.Lock()
			row := f.Rows[f.Grant.TaskID]
			switch scenario {
			case "foreign workspace":
				row.WorkspaceID = uuid.NewString()
			case "foreign agent":
				row.AgentID = uuid.NewString()
			case "other runtime":
				row.RuntimeID = uuid.NewString()
			case "other dispatch":
				row.DispatchedAt = time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
			case "other result":
				row.Result.Output = "another execution's result"
			case "missing result":
				row.Result = nil
			}
			f.Rows[row.ID] = row
			f.mu.Unlock()
			var reads, callbacks atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				if r.Method == http.MethodPost {
					callbacks.Add(1)
				}
				if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer mat_") {
					http.Error(w, "task token expired", http.StatusUnauthorized)
					return
				}
				if strings.HasPrefix(r.URL.Path, "/api/agents/") && strings.HasSuffix(r.URL.Path, "/tasks") {
					switch scenario {
					case "controller denied":
						http.Error(w, "controller read denied", http.StatusForbidden)
						return
					case "controller unavailable":
						http.Error(w, "controller read unavailable", http.StatusServiceUnavailable)
						return
					case "status only":
						_, _ = w.Write([]byte(`[{"status":"running"}]`))
						return
					case "missing task":
						_, _ = w.Write([]byte(`[]`))
						return
					}
				}
				f.serveBackend(w, r)
			}))
			_ = f.C.deliverResult(t.Context(), f.Grant)
			f.refresh(t)
			after, err := f.C.Store.Terminal(f.Grant.AttemptID)
			storage, storageErr := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || storageErr != nil || callbacks.Load() != 0 || f.Grant.CompletionWitness != nil || storage.Checkpoint != nil ||
				storage.WriterSessionID != f.Session.ID || storage.ActiveAttempt != f.Grant.AttemptID {
				t.Fatal("unproven controller observation authorized delivery, continuity, or writer release", err, storageErr)
			}
			if !bytes.Equal(after.Body, before.Body) || after.ResultDigest != before.ResultDigest || after.RequestDigest != before.RequestDigest ||
				!reflect.DeepEqual(after.ResultReceipt, before.ResultReceipt) {
				t.Fatal("rejected observation replaced native evidence")
			}
			if scenario == "unsigned result" && reads.Load() != 0 {
				t.Fatal("unsigned native result acquired controller observation authority")
			}
		})
	}
}
