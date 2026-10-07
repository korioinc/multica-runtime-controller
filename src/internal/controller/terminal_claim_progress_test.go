package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestTerminalSettlementClaimsEligibleWorkWithoutNotification(t *testing.T) {
	for _, kind := range []string{"complete", "cancel-ack", "controller-failure", "recovery-failure"} {
		for _, observedFinal := range []bool{false, true} {
			settlement := "delivered"
			if observedFinal {
				settlement = "uncertain-then-reconciled"
			}
			t.Run(kind+"/"+settlement, func(t *testing.T) {
				c, g, _, _, _ := controllerFixture(t)
				q := newDispatchQueues()
				c.dispatch = q
				t.Cleanup(func() { q.attempts.ShutDown(); q.deliveries.ShutDown() })
				closeClaimPredecessor(t, c, g, kind)

				var next map[string]any
				if err := json.Unmarshal(g.Envelope, &next); err != nil {
					t.Fatal(err)
				}
				nextID := uuid.NewString()
				next["id"] = nextID
				var mu sync.Mutex
				status, settledStatus := "running", "failed"
				if kind == "complete" {
					settledStatus = "completed"
				}
				pending := true
				if kind == "cancel-ack" {
					status, settledStatus = "cancelled", "cancelled"
					// Cancellation already committed upstream. A new queued task
					// arrives after the empty claim while its notification is lost.
					pending = false
				}
				settle := false
				emptyClaim := make(chan struct{})
				var emptyOnce sync.Once
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					switch r.URL.Path {
					case "/api/daemon/tasks/claim":
						batch := []any{}
						// One backend agent slot keeps its next task ineligible while
						// the predecessor runs, even after local compute is released.
						if status != "running" && pending {
							batch, pending = []any{next}, false
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"tasks": batch})
						if len(batch) == 0 {
							emptyOnce.Do(func() { close(emptyClaim) })
						}
					case "/api/agents/" + g.AgentID + "/tasks":
						if !settle && !observedFinal {
							http.Error(w, "assignment temporarily unavailable", http.StatusServiceUnavailable)
							return
						}
						_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": status}})
					case "/api/daemon/tasks/" + g.TaskID + "/complete", "/api/daemon/tasks/" + g.TaskID + "/fail", "/api/daemon/tasks/" + g.TaskID + "/cancel-ack":
						if !settle {
							http.Error(w, "terminal outcome unavailable", http.StatusServiceUnavailable)
							return
						}
						status = settledStatus
						_, _ = w.Write([]byte(`{}`))
					default:
						http.NotFound(w, r)
					}
				}))
				defer backend.Close()
				var err error
				c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, backend.Client())
				if err != nil {
					t.Fatal(err)
				}
				// Cleanup can finish while assignment/result delivery is unavailable.
				// Its earlier capacity wake must not stand in for settlement progress.
				_ = c.reconcileDelivery(t.Context(), g.AttemptID)
				closed, err := c.Store.Get(g.AttemptID)
				if err != nil || closed.State != "closed" || !closed.CleanupComplete {
					t.Fatal("predecessor did not release stopped compute", err)
				}
				c.Capacity = 1
				c.runtimes = map[string]daemonapi.Bootstrap{g.RuntimeID: {Workspace: daemonapi.Workspace{ID: g.WorkspaceID}}}
				c.PollInterval = time.Hour // No fallback tick or backend notification.
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				done := make(chan struct{})
				go func() { defer close(done); c.runClaims(ctx, q) }()
				defer func() { cancel(); <-done }()
				select {
				case <-emptyClaim:
				case <-ctx.Done():
					t.Fatal("initial capacity recovery did not reach claim")
				}
				if claimTaskAccepted(t, c, nextID) {
					t.Fatal("ineligible queued task was accepted before settlement")
				}
				mu.Lock()
				settle, pending = true, true
				if observedFinal {
					// Observe a late terminal commit, or the cancellation whose
					// acknowledgement still needs delivery after an uncertain reply.
					status = settledStatus
				}
				mu.Unlock()
				if err := c.reconcileDelivery(ctx, g.AttemptID); err != nil {
					t.Fatal(err)
				}
				for !claimTaskAccepted(t, c, nextID) {
					select {
					case <-ctx.Done():
						t.Fatal("confirmed terminal settlement left eligible queued work unclaimed")
					case <-time.After(time.Millisecond):
					}
				}
			})
		}
	}
}

func closeClaimPredecessor(t *testing.T, c *Controller, g workspace.TaskGrant, kind string) {
	t.Helper()
	key := admitStopRegressionWorker(t, c, g)
	var err error
	if kind == "cancel-ack" {
		g, err = c.requestStop(g, "backend_cancelled")
		if err != nil {
			t.Fatal(err)
		}
	}
	if kind == "controller-failure" {
		if err := c.recordFailure(g, errors.New("worker stopped")); err != nil {
			t.Fatal(err)
		}
	} else {
		status := "completed"
		if kind == "cancel-ack" {
			status = "cancelled"
		}
		command, err := c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: status}})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
			t.Fatal(err)
		}
		if kind != "recovery-failure" {
			if err := c.Store.Seal(g.AttemptID, nativeStopRegressionReceipt(g, command, key)); err != nil {
				t.Fatal(err)
			}
		}
	}
	finishTaskPod(t, c, g)
	g, err = c.requestStop(g, "execution_finished")
	if err != nil {
		t.Fatal(err)
	}
	if kind == "recovery-failure" {
		if err := c.Store.ObserveStop(g.AttemptID, workspace.StopEvidence{Kind: "terminated", PodUID: g.PodUID, PVCUID: g.PVCUID, ObservedAt: time.Now().Add(-time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.reconcileStop(t.Context(), g); err != nil {
		t.Fatal(err)
	}
}

func claimTaskAccepted(t *testing.T, c *Controller, id string) bool {
	t.Helper()
	grants, err := c.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range grants {
		if g.TaskID == id {
			return true
		}
	}
	return false
}
