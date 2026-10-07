package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestUncertainCompletionRecoversWithoutRepeatingCompletedWork(t *testing.T) {
	c, g, _, options, _ := controllerFixture(t)
	key := admitStopRegressionWorker(t, c, g)
	command, err := c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "retained result"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.Seal(g.AttemptID, nativeStopRegressionReceipt(g, command, key)); err != nil {
		t.Fatal(err)
	}
	finishTaskPod(t, c, g)
	var unavailable atomic.Bool
	unavailable.Store(true)
	var backendMu sync.Mutex
	backendState, completedOutput := "running", ""
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/"+g.AgentID+"/tasks" {
			backendMu.Lock()
			status := backendState
			backendMu.Unlock()
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": status}})
			return
		}
		if unavailable.Load() {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		var completion struct {
			Output string `json:"output"`
		}
		if json.NewDecoder(r.Body).Decode(&completion) != nil {
			http.Error(w, "invalid completion", http.StatusBadRequest)
			return
		}
		backendMu.Lock()
		if backendState == "running" {
			backendState, completedOutput = "completed", completion.Output
		}
		backendMu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = reconcileFixture(t.Context(), c)
	reopenStopRegressionStore(t, c, options)
	unavailable.Store(false)
	for range 3 {
		_ = reconcileFixture(t.Context(), c)
	}
	backendMu.Lock()
	state, output := backendState, completedOutput
	backendMu.Unlock()
	if state != "completed" || output != "retained result" {
		t.Fatal("backend recovery did not preserve completed work", state, output)
	}
	if _, err := c.Store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}); err != nil {
		t.Fatal("confirmed completion kept clean storage locked", err)
	}
}

func TestMissingSealEndsBackendTaskAndRetainsDirtyStorage(t *testing.T) {
	c, g, _, options, _ := controllerFixture(t)
	admitStopRegressionWorker(t, c, g)
	if _, err := c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "unsealed work"}}); err != nil {
		t.Fatal(err)
	}
	original, err := c.Store.Terminal(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	finishTaskPod(t, c, g)
	if _, err := c.requestStop(g, "worker_unsealed"); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	// Model a known terminated writer whose seal window has already elapsed.
	if err := c.Store.ObserveStop(g.AttemptID, workspace.StopEvidence{Kind: "terminated", PodUID: g.PodUID, PVCUID: g.PVCUID, ObservedAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	var failed atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/"+g.AgentID+"/tasks" {
			status := "running"
			if failed.Load() {
				status = "failed"
			}
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": status}})
			return
		}
		if r.URL.Path == "/api/daemon/tasks/"+g.TaskID+"/fail" {
			failed.Store(true)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = reconcileFixture(context.Background(), c)
	reopenStopRegressionStore(t, c, options)
	_ = reconcileFixture(context.Background(), c)
	if !failed.Load() {
		t.Fatal("terminated unsealed execution left its backend task running")
	}
	retained, err := c.Store.Terminal(g.AttemptID)
	if err != nil || retained.ResultDigest != original.ResultDigest || retained.RequestDigest != original.RequestDigest || !bytes.Equal(retained.Body, original.Body) {
		t.Fatal("recovery replaced the observed native result", err)
	}
	if _, err := c.Store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}); !errors.Is(err, workspace.ErrStorageBusy) {
		t.Fatal("reporting recovery failure authorized reuse of unflushed data", err)
	}
}

func TestSupersededCancelAcknowledgementReleasesCleanStorage(t *testing.T) {
	for _, finalStatus := range []string{"completed", "failed"} {
		t.Run(finalStatus, func(t *testing.T) {
			c, g, _, _, assignment := controllerFixture(t)
			key := admitStopRegressionWorker(t, c, g)
			var err error
			g, err = c.requestStop(g, "backend_cancelled")
			if err != nil {
				t.Fatal(err)
			}
			command, err := c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: "cancelled"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Store.CloseCheckouts(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := c.Store.Seal(g.AttemptID, nativeStopRegressionReceipt(g, command, key)); err != nil {
				t.Fatal(err)
			}
			if err := c.Store.BeginForward(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := c.Store.FinishForward(g.AttemptID, "uncertain"); err != nil {
				t.Fatal(err)
			}
			current := *assignment.Load()
			current.Status = finalStatus
			assignment.Store(&current)
			finishTaskPod(t, c, g)
			if err := reconcileFixture(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}); err != nil {
				t.Fatal("obsolete cancellation acknowledgement kept clean task storage locked", err)
			}
		})
	}
}
