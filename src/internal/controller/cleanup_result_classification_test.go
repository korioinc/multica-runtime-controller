package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

func TestCleanupFenceDoesNotTurnProviderInterruptionIntoCancellation(t *testing.T) {
	for _, status := range []string{"aborted", "cancelled"} {
		for _, localStop := range []bool{false, true} {
			name := status + "/cleanup only"
			if localStop {
				name = status + "/local deadline"
			}
			t.Run(name, func(t *testing.T) {
				f := newSessionControllerFixture(t)
				taskID := f.Grant.TaskID
				daemonToken, err := f.C.Store.CapabilityToken(f.Grant.AttemptID, "daemon")
				if err != nil {
					t.Fatal(err)
				}
				f.Handler.Store(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/api/daemon/tasks/" + taskID + "/status":
						f.mu.Lock()
						current := f.Rows[taskID].Status
						f.mu.Unlock()
						writeJSON(w, map[string]string{"status": current})
					case "/api/daemon/tasks/" + taskID + "/fail":
						var failure struct {
							Error   string `json:"error"`
							WorkDir string `json:"work_dir"`
						}
						if json.NewDecoder(r.Body).Decode(&failure) != nil || failure.Error == "" {
							http.Error(w, "invalid failure", http.StatusBadRequest)
							return
						}
						f.mu.Lock()
						row := f.Rows[taskID]
						if row.Status == "running" {
							row.Status, row.Error, row.WorkDir = "failed", failure.Error, failure.WorkDir
							row.FailureReason = taskfailure.NormalizeDaemonReason(taskfailure.Classify(failure.Error).String(), failure.Error).String()
							row.CompletedAt = time.Now().UTC().Format(time.RFC3339)
							f.Rows[taskID] = row
						}
						f.mu.Unlock()
						writeJSON(w, row)
					default:
						f.serveBackend(w, r)
					}
				}))
				// Force the cleanup side of the worker's parallel shutdown to
				// commit its execution fence before the result arrives.
				if err := f.C.stopCheckoutWrites(t.Context(), f.Grant.AttemptID); err != nil {
					t.Fatal(err)
				}
				if localStop {
					if _, err := f.C.requestStop(f.Grant, "task_deadline_exceeded"); err != nil {
						t.Fatal(err)
					}
				}
				f.refresh(t)
				if !f.Grant.CheckoutClosed || !f.Grant.ExecutionRevoked || !localStop && f.Grant.Stop != nil {
					t.Fatal("cleanup did not retain its independent execution fence")
				}
				if _, err := f.C.Store.Authorize(daemonToken, "daemon"); !errors.Is(err, workspace.ErrUnauthorized) {
					t.Fatal("cleanup left task API authority usable", err)
				}
				native := agent.Result{Status: status, Error: "actual provider interruption"}
				receiveFixtureProviderResult(t, f, native, true)
				before, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				backend := f.Rows[taskID]
				f.mu.Unlock()
				if backend.Status != "failed" || backend.Error == "" {
					t.Fatal("cleanup revocation left the interrupted backend task running", backend.Status)
				}
				reopenStopRegressionStore(t, f.C, f.Options)
				f.refresh(t)
				after, err := f.C.Store.Terminal(f.Grant.AttemptID)
				raw, _ := json.Marshal(wire.ProviderResult{Result: native})
				if err != nil || after.Source != "worker" || after.Kind != "fail" || after.State != "delivered" ||
					after.ResultReceipt == nil || after.ResultDigest != wire.Digest(raw) || !bytes.Equal(after.Body, before.Body) ||
					after.RequestDigest != before.RequestDigest || !reflect.DeepEqual(after.ResultReceipt, before.ResultReceipt) {
					t.Fatal("delivery changed or lost the authenticated provider outcome", err)
				}
				if !f.Grant.CheckoutClosed || !f.Grant.ExecutionRevoked || f.Grant.CompletionWitness == nil || f.Grant.CompletionWitness.Status != "failed" {
					t.Fatal("accepted failure lost its workspace witness or reopened execution")
				}
				if _, err := f.C.Store.Authorize(daemonToken, "daemon"); !errors.Is(err, workspace.ErrUnauthorized) {
					t.Fatal("failure settlement restored task API authority", err)
				}
				storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
				if err != nil || storage.Checkpoint != nil || storage.ActiveAttempt != f.Grant.AttemptID || storage.WriterSessionID != f.Session.ID {
					t.Fatal("failure delivery released storage without writer proof", err)
				}
			})
		}
	}
}
