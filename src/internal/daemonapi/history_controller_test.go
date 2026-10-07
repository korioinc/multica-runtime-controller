package daemonapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestControllerTaskObservationKeepsTaskCredentialsSeparate(t *testing.T) {
	claim := observationFixture(t)
	originalToken := claim.AuthToken
	const controllerToken = "controller-observation-token"
	completion := TaskCompleteRequest{Output: "stored completion", WorkDir: "/workspace/retained/workdir"}
	sent, err := json.Marshal(completion)
	if err != nil {
		t.Fatal(err)
	}
	var controllerReads, taskReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agents/"+claim.AgentID+"/tasks" || r.Header.Get("X-Workspace-ID") != claim.WorkspaceID {
			http.Error(w, "wrong observation scope", http.StatusForbidden)
			return
		}
		if r.Header.Get("Authorization") == "Bearer "+controllerToken {
			controllerReads.Add(1)
			_ = json.NewEncoder(w).Encode([]TaskObservation{observedCompletion(claim, completion)})
			return
		}
		if r.Header.Get("Authorization") == "Bearer "+originalToken {
			taskReads.Add(1)
		}
		http.Error(w, "task token expired", http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, controllerToken, "test", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	observed, err := client.ControllerTask(t.Context(), claim)
	if err != nil || !observed.AcceptsCompletion(claim, sent) || controllerReads.Load() != 1 || claim.AuthToken != originalToken {
		t.Fatal("controller read lost the exact original result or changed task credentials", err)
	}
	if _, err := client.AgentTask(t.Context(), claim, claim.ID); err == nil {
		t.Fatal("ordinary task observation inherited controller authority")
	}
	if _, err := client.ClaimTaskAssignment(t.Context(), claim); err == nil {
		t.Fatal("current assignment inherited controller authority")
	}
	if _, err := client.AgentTaskHistory(t.Context(), claim); err == nil {
		t.Fatal("continuity selection inherited controller authority")
	}
	if taskReads.Load() != 3 || controllerReads.Load() != 1 {
		t.Fatal("task credential failures silently used controller authority")
	}
}
