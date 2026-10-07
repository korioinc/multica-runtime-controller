package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
)

func TestPreparationAndAdmissionCanFinishDuringProvision(t *testing.T) {
	for _, progress := range []string{"preparation", "admission", "started"} {
		t.Run(progress, func(t *testing.T) {
			c, g, _, _, _, prepared := controllerFixturePreparing(t)
			if progress != "preparation" {
				if err := c.Store.CompletePreparation(g.AttemptID, &prepared, json.RawMessage(`{}`)); err != nil {
					t.Fatal(err)
				}
				if err := c.provision(t.Context(), g); err != nil {
					t.Fatal(err)
				}
			} else {
				c.preparing = map[string]bool{g.AttemptID: true}
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var leaseOnce, releaseOnce sync.Once
			var started atomic.Bool
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/api/agents/"+g.AgentID+"/tasks" {
					status := "dispatched"
					if started.Load() {
						status = "running"
					}
					_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": status}})
					return
				}
				leaseOnce.Do(func() { close(entered); <-release })
				if started.Load() {
					// The backend refuses preparation leases once execution starts.
					http.Error(w, "task is no longer preparing", http.StatusBadRequest)
					return
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(backend.Close)
			var err error
			c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, nil)
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan struct{})
			var reconcileErr error
			go func() {
				reconcileErr = c.reconcileAttempt(t.Context(), g.AttemptID)
				close(finished)
			}()
			t.Cleanup(func() { unblock(); <-finished })
			select {
			case <-entered:
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			key, _, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			if progress == "preparation" {
				if err := c.Store.CompletePreparation(g.AttemptID, &prepared, json.RawMessage(`{}`)); err != nil {
					t.Fatal(err)
				}
				c.mu.Lock()
				c.preparing[g.AttemptID] = false
				c.mu.Unlock()
			} else {
				current, err := c.Store.Get(g.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Store.BindPod(g.AttemptID, current.PodName, current.PodUID, "worker-node"); err != nil {
					t.Fatal(err)
				}
				if err := c.Store.Admit(g.AttemptID, key); err != nil {
					t.Fatal(err)
				}
				if progress == "started" {
					if _, offered, err := c.Store.Offer(g.AttemptID); err != nil || !offered {
						t.Fatal(err)
					}
					if err := c.Store.BeginStart(g.AttemptID); err != nil {
						t.Fatal(err)
					}
					if err := c.Store.MarkStarted(g.AttemptID); err != nil {
						t.Fatal(err)
					}
					started.Store(true)
				}
			}
			unblock()
			<-finished
			if reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			if progress == "started" {
				if _, err := c.Store.ReceiveEvent(g.AttemptID, 1, json.RawMessage(`{"type":"message","content":"running"}`)); err != nil {
					t.Fatal("obsolete preparation lease revoked an executing worker", err)
				}
				return
			}
			if progress == "preparation" {
				if err := c.reconcileAttempt(context.Background(), g.AttemptID); err != nil {
					t.Fatal(err)
				}
				current, err := c.Store.Get(g.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Store.BindPod(g.AttemptID, current.PodName, current.PodUID, "worker-node"); err != nil {
					t.Fatal("completed preparation lost worker authority", err)
				}
				if err := c.Store.Admit(g.AttemptID, key); err != nil {
					t.Fatal("completed preparation could not admit its worker", err)
				}
			}
			if _, offered, err := c.Store.Offer(g.AttemptID); err != nil || !offered {
				t.Fatal("concurrent progress revoked a valid execution", err)
			}
			if err := c.Store.BeginStart(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := c.Store.MarkStarted(g.AttemptID); err != nil {
				t.Fatal("concurrent progress prevented execution", err)
			}
		})
	}
}
