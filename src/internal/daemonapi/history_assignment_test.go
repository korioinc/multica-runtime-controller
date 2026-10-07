package daemonapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

func TestCurrentAssignmentDoesNotRequireUnrelatedHistory(t *testing.T) {
	for _, limit := range []string{"pages", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			claim := observationFixture(t)
			pageCount, padding := maxHistoryPages+1, ""
			if limit == "bytes" {
				padding = strings.Repeat("x", MaxPayload/2)
				pageCount = maxHistoryBytes/len(padding) + 2
			}
			pages := make([][]byte, pageCount)
			for page := range pages {
				row := observedCompletion(claim, TaskCompleteRequest{})
				row.Status, row.Result = "running", nil
				if page != 0 {
					row.ID, row.Error = uuid.NewString(), padding
				}
				var err error
				pages[page], err = json.Marshal([]TaskObservation{row})
				if err != nil {
					t.Fatal(err)
				}
			}
			var reads atomic.Int32
			client := observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				if r.Header.Get("Authorization") != "Bearer "+claim.AuthToken || r.Header.Get("X-Workspace-ID") != claim.WorkspaceID ||
					r.URL.Path != "/api/agents/"+claim.AgentID+"/tasks" {
					http.Error(w, "task authority required", http.StatusForbidden)
					return
				}
				page := 0
				if cursor := r.URL.Query().Get("before"); cursor != "" {
					var err error
					page, err = strconv.Atoi(cursor)
					if err != nil || page < 0 || page >= len(pages) {
						http.Error(w, "invalid cursor", http.StatusBadRequest)
						return
					}
				}
				if page+1 < len(pages) {
					w.Header().Set(agentTasksNextCursor, strconv.Itoa(page+1))
				}
				_, _ = w.Write(pages[page])
			}))
			assignment, err := client.ClaimTaskAssignment(t.Context(), claim)
			if err != nil || assignment.RuntimeID != claim.RuntimeID || assignment.DispatchedAt == "" || reads.Load() != 1 {
				t.Fatal("unrelated historical volume blocked the current execution assignment", err, reads.Load())
			}
			reads.Store(0)
			history, err := client.AgentTaskHistory(t.Context(), claim)
			if err == nil || history != nil || reads.Load() <= 1 || reads.Load() > maxHistoryPages {
				t.Fatal("incomplete history became usable continuity evidence", err, reads.Load())
			}
			reads.Store(0)
			missing := claim
			missing.ID = uuid.NewString()
			assignment, err = client.ClaimTaskAssignment(t.Context(), missing)
			if err == nil || assignment != (TaskAssignment{}) || reads.Load() <= 1 || reads.Load() > maxHistoryPages {
				t.Fatal("bounded search invented an unavailable execution assignment", err, reads.Load())
			}
		})
	}
}

func TestTargetTaskObservationUsesLaterClaimAuthority(t *testing.T) {
	original := observationFixture(t)
	current := original
	current.ID, current.AuthToken = uuid.NewString(), "mat_current_task"
	completion := TaskCompleteRequest{Output: "accepted provider result", WorkDir: "/workspace/retained/workdir", SessionID: "native-session"}
	sent, err := json.Marshal(completion)
	if err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int32
	client := observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+current.AuthToken || r.Header.Get("X-Workspace-ID") != current.WorkspaceID {
			http.Error(w, "current task authority required", http.StatusForbidden)
			return
		}
		switch r.URL.Query().Get("before") {
		case "":
			w.Header().Set(agentTasksNextCursor, "earlier-task")
			_ = json.NewEncoder(w).Encode([]TaskObservation{observedCompletion(current, TaskCompleteRequest{})})
		case "earlier-task":
			w.Header().Set(agentTasksNextCursor, "unrelated-history")
			_ = json.NewEncoder(w).Encode([]TaskObservation{observedCompletion(original, completion)})
		default:
			http.Error(w, "unrelated history unavailable", http.StatusServiceUnavailable)
		}
	}))
	observed, err := client.AgentTask(t.Context(), current, original.ID)
	if err != nil || !observed.AcceptsCompletion(original, sent) || reads.Load() != 2 {
		t.Fatal("later task authority could not observe the exact earlier completion", err, reads.Load())
	}
}

func TestCurrentAssignmentRejectsInvalidVisitedEvidence(t *testing.T) {
	for _, scenario := range []string{
		"missing task", "malformed response", "null response", "foreign workspace", "foreign agent", "invalid task identity",
		"missing runtime", "invalid runtime", "missing dispatch", "invalid dispatch", "duplicate target", "malformed sibling",
		"ambiguous cursor", "oversized cursor", "cursor cycle", "empty page cursor", "duplicate earlier task",
	} {
		t.Run(scenario, func(t *testing.T) {
			claim := observationFixture(t)
			earlierID := uuid.NewString()
			var reads atomic.Int32
			client := observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := reads.Add(1)
				if page > 2 {
					http.Error(w, "unexpected page", http.StatusBadRequest)
					return
				}
				row := observedCompletion(claim, TaskCompleteRequest{})
				tasks := []TaskObservation{row}
				switch scenario {
				case "missing task":
					tasks = []TaskObservation{}
				case "malformed response":
					_, _ = w.Write([]byte(`{"incomplete":`))
					return
				case "null response":
					_, _ = w.Write([]byte(`null`))
					return
				case "foreign workspace":
					tasks[0].WorkspaceID = uuid.NewString()
				case "foreign agent":
					tasks[0].AgentID = uuid.NewString()
				case "invalid task identity":
					tasks[0].ID = "invalid-task"
				case "missing runtime":
					tasks[0].RuntimeID = ""
				case "invalid runtime":
					tasks[0].RuntimeID = "invalid-runtime"
				case "missing dispatch":
					tasks[0].DispatchedAt = ""
				case "invalid dispatch":
					tasks[0].DispatchedAt = "invalid-dispatch"
				case "duplicate target":
					tasks = append(tasks, row)
				case "malformed sibling":
					row.ID, row.CreatedAt = earlierID, "invalid-time"
					tasks = append(tasks, row)
				case "ambiguous cursor":
					w.Header().Add(agentTasksNextCursor, "one")
					w.Header().Add(agentTasksNextCursor, "two")
				case "oversized cursor":
					w.Header().Set(agentTasksNextCursor, strings.Repeat("x", maxHistoryCursorBytes+1))
				case "cursor cycle":
					tasks[0].ID = uuid.NewString()
					w.Header().Set(agentTasksNextCursor, "same-page")
				case "empty page cursor":
					tasks = []TaskObservation{}
					w.Header().Set(agentTasksNextCursor, "older-page")
				case "duplicate earlier task":
					tasks[0].ID = earlierID
					if page == 1 {
						w.Header().Set(agentTasksNextCursor, "older-page")
					} else {
						tasks = append(tasks, row)
					}
				}
				_ = json.NewEncoder(w).Encode(tasks)
			}))
			assignment, err := client.ClaimTaskAssignment(t.Context(), claim)
			if err == nil || assignment != (TaskAssignment{}) || reads.Load() > 2 {
				t.Fatal("invalid or ambiguous evidence granted a current execution assignment", err, reads.Load())
			}
		})
	}
}

func TestTargetTaskObservationRejectsInvalidScopeBeforeRequest(t *testing.T) {
	for _, scenario := range []string{"target identity", "claim agent"} {
		t.Run(scenario, func(t *testing.T) {
			claim := observationFixture(t)
			target := claim.ID
			if scenario == "target identity" {
				target = "invalid-target"
			} else {
				claim.Agent.ID = uuid.NewString()
			}
			var reads atomic.Int32
			client := observationClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				_ = json.NewEncoder(w).Encode([]TaskObservation{observedCompletion(claim, TaskCompleteRequest{})})
			}))
			observed, err := client.AgentTask(t.Context(), claim, target)
			if err == nil || observed.ID != "" || reads.Load() != 0 {
				t.Fatal("invalid target or claim scope reached task observation authority", err, reads.Load())
			}
		})
	}
}
