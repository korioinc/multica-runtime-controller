package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestModernCancellationSurvivesTaskTokenRevocation(t *testing.T) {
	testModernCancellationSurvivesTaskTokenRevocation(t, "codex")
}

func TestClaudeCancellationSurvivesTaskTokenRevocation(t *testing.T) {
	testModernCancellationSurvivesTaskTokenRevocation(t, "claude")
}

func testModernCancellationSurvivesTaskTokenRevocation(t *testing.T, provider string) {
	t.Helper()
	for _, denied := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(denied), func(t *testing.T) {
			f := newSessionControllerFixture(t, provider)
			apiToken, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, "daemon")
			if err != nil {
				t.Fatal(err)
			}
			var statusReads atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/daemon/tasks/"+f.Grant.TaskID+"/status" {
					statusReads.Add(1)
					if r.Header.Get("Authorization") != "Bearer local-owner-token" {
						t.Error("cancellation observation used revoked task authority")
						http.Error(w, "denied", http.StatusUnauthorized)
						return
					}
					writeJSON(w, map[string]string{"status": "cancelled"})
					return
				}
				if strings.HasPrefix(r.URL.Path, "/api/agents/") && strings.HasSuffix(r.URL.Path, "/tasks") {
					http.Error(w, "task token revoked", denied)
					return
				}
				f.serveBackend(w, r)
			}))
			if err := f.C.reconcileAttempt(t.Context(), f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			reopenStopRegressionStore(t, f.C, f.Options)
			f.refresh(t)
			if statusReads.Load() == 0 || !f.Grant.ExecutionRevoked || f.Grant.Stop == nil || f.Grant.Stop.Reason != "backend_cancelled" ||
				f.Session.Stop == nil || f.Session.State != workspace.SessionDraining || f.Session.ActiveAttempt != f.Grant.AttemptID {
				t.Fatal("cancelled backend task retained grant or session execution authority after restart")
			}
			if _, err := f.C.Store.Authorize(apiToken, "daemon"); err == nil {
				t.Fatal("backend cancellation left the task API capability usable")
			}
			if _, err := f.C.Store.Terminal(f.Grant.AttemptID); !errors.Is(err, workspace.ErrUnauthorized) {
				t.Fatal("cancellation invented a provider result", err)
			}
			response := f.signed(t, workspace.SessionOperationStopControl, struct{}{})
			var command wire.SessionStopCommand
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &command) != nil || command.Revision == 0 || command.AttemptID != f.Grant.AttemptID {
				t.Fatal("the incarnation watchdog cannot observe the durable cancellation")
			}
			storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || storage.WriterSessionID != f.Session.ID || storage.ActiveAttempt != f.Grant.AttemptID || f.Grant.TurnComplete {
				t.Fatal("cancellation released an unproven writer", err)
			}
		})
	}
}

func TestModernStopObservationErrorsDoNotCancelOrRestoreTaskAuthority(t *testing.T) {
	for _, scenario := range []string{"running", "temporary failure", "daemon denied", "transport failure"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			var statusReads atomic.Int32
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/daemon/tasks/"+f.Grant.TaskID+"/status" {
					statusReads.Add(1)
					switch scenario {
					case "running":
						writeJSON(w, map[string]string{"status": "running"})
					case "temporary failure":
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
					case "daemon denied":
						http.Error(w, "denied", http.StatusForbidden)
					case "transport failure":
						connection, _, err := w.(http.Hijacker).Hijack()
						if err == nil {
							_ = connection.Close()
						}
					}
					return
				}
				if strings.HasPrefix(r.URL.Path, "/api/agents/") && strings.HasSuffix(r.URL.Path, "/tasks") {
					http.Error(w, "task token unavailable", http.StatusUnauthorized)
					return
				}
				f.serveBackend(w, r)
			}))
			if err := f.C.reconcileAttempt(t.Context(), f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			if statusReads.Load() == 0 || f.Grant.Stop != nil || f.Grant.ExecutionRevoked || f.Session.Stop != nil || f.Session.State != workspace.SessionRunning {
				t.Fatal("a nonterminal or unavailable observation fabricated cancellation")
			}
			if f.C.sameAssignment(t.Context(), f.Grant) {
				t.Fatal("daemon status substituted for task assignment or admission authority")
			}
		})
	}
}

func TestModernStopStatusKeepsGenerationChecks(t *testing.T) {
	for _, statusCode := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			f := newSessionControllerFixture(t)
			f.mu.Lock()
			row := f.Rows[f.Grant.TaskID]
			row.RuntimeID = uuid.NewString()
			f.Rows[row.ID] = row
			f.mu.Unlock()
			f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/daemon/tasks/"+f.Grant.TaskID+"/status" {
					if statusCode != http.StatusOK {
						http.Error(w, "unavailable", statusCode)
					} else {
						writeJSON(w, map[string]string{"status": "running"})
					}
					return
				}
				f.serveBackend(w, r)
			}))
			if err := f.C.reconcileAttempt(t.Context(), f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			if !f.Grant.ExecutionRevoked || f.Grant.Stop == nil || f.Grant.Stop.Reason != "assignment_changed" {
				t.Fatal("daemon status displaced the generation-bound assignment observation")
			}
		})
	}
}

func TestModernTerminalStatusPreservesAuthenticatedProviderResult(t *testing.T) {
	for _, backendStatus := range []string{"completed", "failed"} {
		for _, providerStatus := range []string{"completed", "failed"} {
			t.Run(backendStatus+"/"+providerStatus, func(t *testing.T) {
				f := newSessionControllerFixture(t)
				receiveFixtureProviderResult(t, f, agent.Result{Status: providerStatus, Output: "actual provider completion", Error: "actual provider failure"}, true)
				before, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/api/daemon/tasks/"+f.Grant.TaskID+"/status" {
						writeJSON(w, map[string]string{"status": backendStatus})
						return
					}
					http.Error(w, "task token revoked", http.StatusUnauthorized)
				}))
				if stopped, err := f.C.observeBackendStop(t.Context(), f.Grant); stopped || err != nil {
					t.Fatal("terminal status displaced the observed native result owner", err)
				}
				f.refresh(t)
				after, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil || f.Grant.Stop != nil || after.Source != "worker" || after.Kind != before.Kind || after.State != before.State ||
					after.ResultReceipt == nil || after.RecoveryFailure != nil || !bytes.Equal(after.Body, before.Body) || after.RequestDigest != before.RequestDigest {
					t.Fatal("control-plane status changed or replaced the authenticated native outcome", err)
				}
			})
		}
	}
}

func TestModernStopObservationRechecksResultsAfterBackendIO(t *testing.T) {
	for _, boundary := range []string{"daemon status", "assignment"} {
		for _, backendStatus := range []string{"completed", "failed", "cancelled"} {
			t.Run(boundary+"/"+backendStatus, func(t *testing.T) {
				f := newSessionControllerFixture(t)
				finish := holdFixtureStopObservation(t, f, boundary, backendStatus)
				receiveFixtureTurnResult(t, f, true)
				before, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				row := f.Rows[f.Grant.TaskID]
				row.Status = backendStatus
				f.Rows[row.ID] = row
				f.mu.Unlock()
				observed := finish()
				wantStop := backendStatus == "cancelled"
				if observed.stopped != wantStop || observed.err != nil {
					t.Fatal("backend observation did not use the result committed during I/O", observed)
				}
				f.refresh(t)
				after, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil || after.Source != "worker" || after.Kind != before.Kind || after.State != before.State ||
					after.ResultReceipt == nil || after.RecoveryFailure != nil || !bytes.Equal(after.Body, before.Body) || after.RequestDigest != before.RequestDigest {
					t.Fatal("late backend observation replaced the authenticated native result", err)
				}
				if wantStop {
					if f.Grant.Stop == nil || f.Grant.Stop.Reason != "backend_cancelled" || !f.Grant.ExecutionRevoked {
						t.Fatal("concurrent cancellation failed to revoke execution")
					}
				} else if f.Grant.Stop != nil || f.Session.Stop != nil {
					t.Fatal("late terminal status incorrectly drained a native result")
				}
			})
		}
	}
}

type stopObservationResult struct {
	stopped bool
	err     error
}

// Hold one external observation while the real result and journal handlers run.
// The snapshot and task ID stay immutable when the caller advances the fixture.
func holdFixtureStopObservation(t *testing.T, f *sessionControllerFixture, boundary, status string) func() stopObservationResult {
	t.Helper()
	grant := f.Grant
	taskID := grant.TaskID
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	var releaseOnce sync.Once
	f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isStatus := r.URL.Path == "/api/daemon/tasks/"+taskID+"/status"
		isAssignment := strings.HasPrefix(r.URL.Path, "/api/agents/") && strings.HasSuffix(r.URL.Path, "/tasks")
		if (boundary == "daemon status" && isStatus || boundary == "assignment" && isAssignment) && held.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		if isStatus {
			observed := status
			if boundary == "assignment" {
				observed = "running"
			}
			writeJSON(w, map[string]string{"status": observed})
			return
		}
		f.serveBackend(w, r)
	}))
	var result stopObservationResult
	go func() {
		result.stopped, result.err = f.C.observeBackendStop(t.Context(), grant)
		close(done)
	}()
	finish := func() stopObservationResult {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
			return result
		case <-time.After(10 * time.Second):
			t.Fatal("backend stop observation did not finish")
			return stopObservationResult{}
		}
	}
	t.Cleanup(func() { _ = finish() })
	select {
	case <-entered:
	case <-done:
		t.Fatal("backend stop observation did not reach the held boundary", result)
	case <-time.After(10 * time.Second):
		t.Fatal("backend stop observation did not reach the held boundary")
	}
	return finish
}
