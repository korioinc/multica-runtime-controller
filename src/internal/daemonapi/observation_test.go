package daemonapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

func observationFixture(t *testing.T) Claim {
	t.Helper()
	claim, _, _, _ := executionFixture(t, map[string]any{
		"kind": "direct", "start_claim_supported": true,
		"created_at": "2026-10-04T01:00:00Z", "dispatched_at": "2026-10-04T01:00:01.123456Z",
	})
	return claim
}

func observedCompletion(claim Claim, completion TaskCompleteRequest) TaskObservation {
	return TaskObservation{
		ID: claim.ID, AgentID: claim.AgentID, RuntimeID: claim.RuntimeID,
		WorkspaceID: claim.WorkspaceID, IssueID: claim.IssueID, ChatSessionID: claim.ChatSessionID,
		Kind: claim.Kind, Status: "completed", CreatedAt: claim.CreatedAt,
		DispatchedAt: "2026-10-04T01:00:01Z", StartedAt: "2026-10-04T01:00:02Z", CompletedAt: "2026-10-04T01:00:03Z",
		WorkDir: completion.WorkDir, Result: &completion,
	}
}

func observationClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := NewClient(server.URL, "mdt_controller", "test", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// The evidence boundary must not adopt files or native sessions merely because
// a callback returned success. The server stores its normalized full request.
func TestCompletionObservationRequiresAcceptedProducingRequest(t *testing.T) {
	claim := observationFixture(t)
	sent := []byte(`{"output":"finished\u0000","session_id":"native-session","work_dir":"/workspace/retained/workdir"}`)
	completion := TaskCompleteRequest{Output: "finished", SessionID: "native-session", WorkDir: "/workspace/retained/workdir"}
	for _, test := range []struct {
		name   string
		change func(map[string]any)
		accept bool
	}{
		{name: "accepted normalized request", accept: true},
		{name: "cancelled before completion", change: func(row map[string]any) { row["status"], row["result"] = "cancelled", nil }},
		{name: "different accepted output", change: func(row map[string]any) { row["result"].(map[string]any)["output"] = "a different execution" }},
		{name: "another producing agent", change: func(row map[string]any) { row["agent_id"] = uuid.NewString() }},
		{name: "different native root", change: func(row map[string]any) { row["work_dir"] = "/workspace/foreign/workdir" }},
		{name: "incomplete historical result", change: func(row map[string]any) { delete(row["result"].(map[string]any), "output") }},
		{name: "different dispatch", change: func(row map[string]any) { row["dispatched_at"] = "2026-10-04T02:00:01Z" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(observedCompletion(claim, completion))
			if err != nil {
				t.Fatal(err)
			}
			var row map[string]any
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			if test.change != nil {
				test.change(row)
			}
			client := observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(row) }))
			observed, err := client.TerminalObservation(context.Background(), claim.ID, "complete", sent)
			if err != nil {
				t.Fatal(err)
			}
			if observed.AcceptsCompletion(claim, sent) != test.accept {
				t.Fatal("backend acceptance authorized the wrong producing execution")
			}
		})
	}
}

func TestTransientFailureObservationRequiresExactAcceptedFailureColumns(t *testing.T) {
	claim := observationFixture(t)
	workdir := "/workspace/retained/workdir"
	errorText := `Pi session file "/workspace/retained/pi-sessions/session.jsonl" is already in use by another execution`
	sent, err := json.Marshal(map[string]string{"error": errorText + "\x00", "work_dir": workdir})
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []struct {
		name   string
		change func(map[string]any)
		accept bool
	}{
		{name: "exact normalized failure", accept: true},
		{name: "different task", change: func(row map[string]any) { row["id"] = uuid.NewString() }},
		{name: "different scope", change: func(row map[string]any) { row["workspace_id"] = uuid.NewString() }},
		{name: "different runtime", change: func(row map[string]any) { row["runtime_id"] = uuid.NewString() }},
		{name: "different kind", change: func(row map[string]any) { row["kind"] = "comment" }},
		{name: "different accepted error", change: func(row map[string]any) { row["error"] = "another execution failed" }},
		{name: "different accepted reason", change: func(row map[string]any) { row["failure_reason"] = "local_directory_error" }},
		{name: "different directory", change: func(row map[string]any) { row["work_dir"] = "/workspace/foreign/workdir" }},
		{name: "unexpected durable directory", change: func(row map[string]any) { row["durable_work_dir"] = workdir }},
		{name: "different dispatch", change: func(row map[string]any) { row["dispatched_at"] = "2026-10-04T01:00:00Z" }},
		{name: "unstarted failure", change: func(row map[string]any) { row["started_at"] = nil }},
		{name: "unfinished failure", change: func(row map[string]any) { row["completed_at"] = nil }},
		{name: "cancelled task", change: func(row map[string]any) { row["status"] = "cancelled" }},
		{name: "invented failure result without columns", change: func(row map[string]any) {
			row["error"], row["failure_reason"] = nil, ""
			row["result"] = map[string]string{"error": errorText, "work_dir": workdir}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			observed := observedCompletion(claim, TaskCompleteRequest{WorkDir: workdir})
			observed.Status, observed.Result, observed.Error = "failed", nil, errorText
			observed.FailureReason = taskfailure.NormalizeDaemonReason(taskfailure.Classify(errorText).String(), errorText).String()
			raw, _ := json.Marshal(observed)
			var fields map[string]any
			if json.Unmarshal(raw, &fields) != nil {
				t.Fatal("invalid fixture")
			}
			if scenario.change != nil {
				scenario.change(fields)
			}
			raw, _ = json.Marshal(fields)
			if json.Unmarshal(raw, &observed) != nil {
				t.Fatal("invalid observation")
			}
			if observed.AcceptsTransientFailure(claim, sent) != scenario.accept {
				t.Fatal("failure columns authorized the wrong accepted execution")
			}
			if scenario.accept {
				withSession, _ := json.Marshal(map[string]string{"error": errorText, "work_dir": workdir, "session_id": "invented-session"})
				if observed.AcceptsTransientFailure(claim, withSession) {
					t.Fatal("a generic failed-session publication entered the narrow transient contract")
				}
			}
		})
	}
}

func TestStartClaimCannotGrantAReplacedDispatch(t *testing.T) {
	claim := observationFixture(t)
	current, err := time.Parse(time.RFC3339Nano, claim.DispatchedAt)
	if err != nil {
		t.Fatal(err)
	}
	current = current.Add(time.Microsecond)
	started := false
	client := observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RuntimeID    string `json:"runtime_id"`
			DispatchedAt string `json:"dispatched_at"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			http.Error(w, "invalid", http.StatusBadRequest)
			return
		}
		generation, err := time.Parse(time.RFC3339Nano, request.DispatchedAt)
		legacy := request.RuntimeID == "" && request.DispatchedAt == ""
		if !legacy && (err != nil || request.RuntimeID != claim.RuntimeID || !generation.Equal(current)) {
			http.Error(w, "stale", http.StatusConflict)
			return
		}
		started = true
		_, _ = w.Write([]byte(`{}`))
	}))
	if client.StartClaim(context.Background(), claim) == nil || started {
		t.Fatal("a superseded claim acquired execution authority")
	}
	claim.DispatchedAt = current.Format(time.RFC3339Nano)
	if err := client.StartClaim(context.Background(), claim); err != nil || !started {
		t.Fatal("the current claim could not acquire execution authority")
	}
}

func TestHistoryRequiresAllPagesBeforeSourceAuthority(t *testing.T) {
	claim := observationFixture(t)
	completion := TaskCompleteRequest{Output: "finished", SessionID: "native-session", WorkDir: "/workspace/retained/workdir"}
	sent, err := json.Marshal(completion)
	if err != nil {
		t.Fatal(err)
	}
	source := observedCompletion(claim, completion)
	newer := source
	newer.ID = uuid.NewString()
	for _, failure := range []string{"", "read failure", "duplicate task", "foreign scope"} {
		t.Run(failure, func(t *testing.T) {
			client := observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer "+claim.AuthToken {
					http.Error(w, "task authority required", http.StatusUnauthorized)
					return
				}
				if r.URL.Query().Get("before") == "" {
					w.Header().Set(agentTasksNextCursor, "older-page")
					_ = json.NewEncoder(w).Encode([]TaskObservation{newer})
					return
				}
				row := source
				switch failure {
				case "read failure":
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				case "duplicate task":
					row.ID = newer.ID
				case "foreign scope":
					row.WorkspaceID = uuid.NewString()
				}
				_ = json.NewEncoder(w).Encode([]TaskObservation{row})
			}))
			history, err := client.AgentTaskHistory(context.Background(), claim)
			if failure != "" {
				if err == nil {
					t.Fatal("incomplete or conflicting history supplied source authority")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			accepted := false
			for _, row := range history {
				accepted = accepted || row.AcceptsCompletion(claim, sent)
			}
			if !accepted {
				t.Fatal("pagination lost the selected producing execution")
			}
		})
	}
}

type authorityFixture struct {
	claim           Claim
	principal       string
	agentOwner      string
	bindings        any
	plugins         any
	pluginDisabled  bool
	pluginForbidden bool
}

func newAuthorityFixture(t *testing.T) *authorityFixture {
	t.Helper()
	claim := observationFixture(t)
	principal := uuid.NewString()
	claim.Attribution = &TaskAttribution{Source: "direct_human", Precise: true, Originator: &AttributionUser{ID: principal}}
	return &authorityFixture{claim: claim, principal: principal, agentOwner: principal, bindings: []any{}, plugins: map[string]any{"plugins": []any{}}}
}

func (f *authorityFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer mat_") {
		http.Error(w, "task authority required", http.StatusUnauthorized)
		return
	}
	var response any
	switch r.URL.Path {
	case "/api/me":
		response = map[string]string{"id": f.principal}
	case "/api/agents/" + f.claim.AgentID:
		response = map[string]string{"id": f.claim.AgentID, "workspace_id": f.claim.WorkspaceID, "runtime_id": f.claim.RuntimeID, "owner_id": f.agentOwner}
	case "/api/workspaces/" + f.claim.WorkspaceID + "/members":
		users := map[string]bool{f.principal: true, f.agentOwner: true}
		if f.claim.Attribution != nil && f.claim.Attribution.Originator != nil {
			users[f.claim.Attribution.Originator.ID] = true
		}
		var members []map[string]string
		for user := range users {
			members = append(members, map[string]string{"id": uuid.NewString(), "workspace_id": f.claim.WorkspaceID, "user_id": user, "role": "member"})
		}
		response = members
	case "/api/agents/" + f.claim.AgentID + "/mcp-servers":
		response = f.bindings
	case "/api/workspaces/" + f.claim.WorkspaceID + "/plugins":
		if f.pluginDisabled || f.pluginForbidden {
			w.WriteHeader(http.StatusForbidden)
			code := "forbidden"
			if f.pluginDisabled {
				code = "plugin_api_disabled"
			}
			response = map[string]string{"code": code}
		} else {
			response = f.plugins
		}
	default:
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(response)
}

func TestAuthorityTracksStablePrincipalsAcrossCredentialRotation(t *testing.T) {
	fixture := newAuthorityFixture(t)
	client := observationClient(t, fixture)
	first, err := client.ObserveAuthority(context.Background(), fixture.claim)
	if err != nil || !first.ValidForReuse() {
		t.Fatal("ordinary task authority could not seed continuity", err)
	}
	fixture.claim.AuthToken = "mat_rotated"
	rotated, err := client.ObserveAuthority(context.Background(), fixture.claim)
	if err != nil || !rotated.ValidForReuse() || !reflect.DeepEqual(first, rotated) {
		t.Fatal("token rotation changed otherwise identical authority", err)
	}
	fixture.principal = uuid.NewString()
	changed, err := client.ObserveAuthority(context.Background(), fixture.claim)
	if err != nil || !changed.ValidForReuse() || reflect.DeepEqual(rotated, changed) {
		t.Fatal("a different principal inherited the previous authority identity", err)
	}
}

func TestUnknownIntegrationOrDelegationCannotAuthorizeReuse(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*authorityFixture)
		known  bool
	}{
		{"disabled plugins", func(f *authorityFixture) { f.pluginDisabled = true }, true},
		{"known no delegation", func(f *authorityFixture) { f.claim.Attribution = &TaskAttribution{Source: "unattributed"} }, true},
		{"explicit empty MCP", func(f *authorityFixture) { f.claim.Agent.MCPConfig = json.RawMessage(`{"mcpServers":{}}`) }, true},
		{"missing delegation", func(f *authorityFixture) { f.claim.Attribution = nil }, false},
		{"conflicting delegation", func(f *authorityFixture) { f.claim.InitiatorID = uuid.NewString() }, false},
		{"unavailable bindings", func(f *authorityFixture) { f.bindings = nil }, false},
		{"opaque bindings", func(f *authorityFixture) { f.bindings = []any{map[string]string{"id": uuid.NewString()}} }, false},
		{"unavailable plugin inventory", func(f *authorityFixture) { f.pluginForbidden = true }, false},
		{"missing plugin inventory", func(f *authorityFixture) { f.plugins = map[string]any{} }, false},
		{"opaque account", func(f *authorityFixture) { f.claim.ConnectedApps = json.RawMessage(`[{"name":"Connected app"}]`) }, false},
		{"malformed MCP", func(f *authorityFixture) { f.claim.Agent.MCPConfig = json.RawMessage(`{"mcpServers":[]}`) }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAuthorityFixture(t)
			test.change(fixture)
			client := observationClient(t, fixture)
			authority, err := client.ObserveAuthority(context.Background(), fixture.claim)
			if err != nil {
				t.Fatal("unknown continuity evidence rejected ordinary execution", err)
			}
			if authority.ValidForReuse() != test.known {
				t.Fatal("reuse authority did not follow the available principal and integration evidence")
			}
		})
	}
}

func TestWorkspaceAuthorityRequiresCompleteMembershipEvidence(t *testing.T) {
	fixture := newAuthorityFixture(t)
	fixture.claim.ConnectedApps = json.RawMessage(`[{"name":"Opaque account"}]`)
	client := observationClient(t, fixture)
	observed, err := client.ObserveAuthority(t.Context(), fixture.claim)
	if err != nil || !observed.ValidForWorkspaceReuse() || observed.ValidForReuse() {
		t.Fatal("opaque integration erased proven core authority or granted native history", err)
	}
	client = observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workspaces/"+fixture.claim.WorkspaceID+"/members" {
			member := map[string]string{"id": uuid.NewString(), "workspace_id": fixture.claim.WorkspaceID, "user_id": fixture.principal, "role": "owner"}
			_ = json.NewEncoder(w).Encode([]any{member, member})
			return
		}
		fixture.ServeHTTP(w, r)
	}))
	observed, err = client.ObserveAuthority(t.Context(), fixture.claim)
	if err != nil || observed.ValidForWorkspaceReuse() || observed.ValidForReuse() {
		t.Fatal("partially valid membership inventory authorized retained files", err)
	}
}
