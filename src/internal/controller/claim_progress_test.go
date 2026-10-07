package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
)

func TestClaimWakeFillsCapacityForIndependentTasksOfOneAgent(t *testing.T) {
	c, existing, _, _, _ := controllerFixtureBeforeProvision(t)
	var input map[string]any
	if err := json.Unmarshal(existing.Envelope, &input); err != nil {
		t.Fatal(err)
	}
	var pending []json.RawMessage
	wanted := make(map[string]bool)
	for range 6 {
		id := uuid.NewString()
		input["id"], input["chat_session_id"] = id, uuid.NewString()
		raw, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		pending = append(pending, raw)
		wanted[id] = false
	}
	// The backend selects at most one task per agent in a claim response.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		batch := []json.RawMessage{}
		if len(pending) > 0 {
			batch, pending = pending[:1], pending[1:]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tasks": batch})
	}))
	defer backend.Close()
	var err error
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	c.Capacity = len(wanted) + 1 // The fixture already owns one slot.
	c.runtimes = map[string]daemonapi.Bootstrap{existing.RuntimeID: {Workspace: daemonapi.Workspace{ID: existing.WorkspaceID}}}
	c.PollInterval = time.Hour
	q := newDispatchQueues()
	c.dispatch = q
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); c.runClaims(ctx, q) }()
	defer func() { cancel(); <-done; q.attempts.ShutDown(); q.deliveries.ShutDown() }()
	c.wakeClaim()
	for {
		grants, err := c.Store.List()
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range grants {
			if _, ok := wanted[g.TaskID]; ok {
				wanted[g.TaskID] = true
			}
		}
		complete := true
		for _, accepted := range wanted {
			complete = complete && accepted
		}
		if complete {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("available agent capacity remained idle after a successful partial claim")
		case <-time.After(time.Millisecond):
		}
	}
}
