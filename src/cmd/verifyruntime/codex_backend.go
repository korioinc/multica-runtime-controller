package main

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

type taskEvent struct {
	TranscriptMarker string    `json:"transcriptMarker,omitempty"`
	Phase            string    `json:"phase"`
	At               time.Time `json:"at"`
	Transport        string    `json:"transport,omitempty"`
	AttemptID        string    `json:"attemptID,omitempty"`
}

type codexEvidence struct {
	TranscriptMarker string    `json:"transcriptMarker,omitempty"`
	TaskID           string    `json:"taskID"`
	AttemptID        string    `json:"attemptID"`
	Phase            string    `json:"phase"`
	Prompt           string    `json:"prompt,omitempty"`
	PromptDigest     string    `json:"promptDigest,omitempty"`
	ThreadMethod     string    `json:"threadMethod,omitempty"`
	InitializeAt     time.Time `json:"initializeAt,omitempty"`
	InterruptAt      time.Time `json:"interruptAt,omitempty"`
	EOFAt            time.Time `json:"eofAt,omitempty"`
}

// Called with the fixture mutex held. This boundary supplies the state already
// committed by Multica's queue/cancel handlers; it is not a replacement UI or SQL
// transaction verifier. In particular, claim is deliberately independent of ack.
func (f *runtimeBackend) codexControl(w http.ResponseWriter, r *http.Request) bool {
	switch {
	case r.URL.Path == "/fixture/codex" && r.Method == http.MethodPost:
		var evidence codexEvidence
		if decodeRequest(w, r, &evidence) != nil {
			http.Error(w, "invalid Codex evidence", 400)
			return true
		}
		task := f.state.Tasks[evidence.TaskID]
		if task == nil || task.Input.Provider != "codex" {
			http.Error(w, "unassigned Codex evidence", 403)
			return true
		}
		task.Codex = &evidence
		task.Events = append(task.Events, taskEvent{Phase: evidence.Phase, At: time.Now().UTC(), AttemptID: evidence.AttemptID})
		if err := f.persist(); err != nil {
			http.Error(w, err.Error(), 500)
			return true
		}
		writeJSON(w, map[string]bool{"recorded": true})
		return true
	case strings.HasPrefix(r.URL.Path, "/fixture/cancel/") && r.Method == http.MethodPost:
		task := f.state.Tasks[strings.TrimPrefix(r.URL.Path, "/fixture/cancel/")]
		if task == nil {
			http.Error(w, "unknown cancellation", 404)
			return true
		}
		if task.Status == "completed" || task.Status == "failed" {
			http.Error(w, "task already terminal", 409)
			return true
		}
		task.Status = "cancelled"
		task.Events = append(task.Events, taskEvent{Phase: "cancel_committed", At: time.Now().UTC()})
		if f.state.Pending == task.Input.TaskID {
			f.state.Pending = ""
		}
		_ = f.persist()
		writeJSON(w, task)
		return true
	}
	return false
}

// Explicit task routes prevent a generic {} response from hiding a missing
// cancellation/status/session/transcript contract in this fixture.
func (f *runtimeBackend) codexOfficial(w http.ResponseWriter, r *http.Request, body map[string]any) bool {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 5 || parts[0] != "api" || parts[1] != "daemon" || parts[2] != "tasks" {
		return false
	}
	task := f.state.Tasks[parts[3]]
	if task == nil || task.Input.Provider != "codex" {
		return false
	}
	operation := strings.Join(parts[4:], "/")
	output := any(map[string]any{})
	event := taskEvent{Phase: "official_" + operation, At: time.Now().UTC(), Transport: "http"}
	switch operation {
	case "status":
		output = map[string]any{"status": task.Status}
		if task.Status != "cancelled" {
			writeJSON(w, output)
			return true
		}
	case "start":
		if task.Status != "cancelled" {
			task.Status = "running"
		}
	case "cancel-ack":
		if task.Status != "cancelled" {
			f.fail(errors.New("official daemon acknowledged a task without committed cancellation"))
			http.Error(w, "not cancelled", 409)
			return true
		}
		task.CancelAck = body
	case "session":
		// Session is an upstream observation. The controller still decides whether
		// that provider-native session is registered and authorized for reuse.
	case "complete":
		if task.Status == "cancelled" {
			http.Error(w, "cancelled task cannot complete", 409)
			return true
		}
		task.Status = "completed"
		task.Completion = body
	case "fail":
		if task.Status == "cancelled" {
			http.Error(w, "cancelled task cannot fail", 409)
			return true
		}
		task.Status = "failed"
		task.Failure = body
	case "progress", "messages":
		task.Messages = append(task.Messages, body)
		if operation == "messages" && task.Codex != nil && codexBatchContains(body, task.Codex.TranscriptMarker) {
			event.TranscriptMarker = task.Codex.TranscriptMarker
		}
	case "usage", "wait-local-directory":
	case "plugin-hooks":
		output = map[string]any{"hooks": []any{}}
	case "gc-check":
		output = map[string]any{"safe_to_delete": false}
	default:
		// Provider repo checkout also reaches scoped credential routes, which the
		// existing backend handles for the public disposable Git repository.
		if strings.HasSuffix(operation, "/credential") {
			return false
		}
		f.fail(errors.New("unhandled Codex task operation: " + operation))
		http.NotFound(w, r)
		return true
	}
	task.Events = append(task.Events, event)
	if err := f.persist(); err != nil {
		http.Error(w, err.Error(), 500)
		return true
	}
	writeJSON(w, output)
	return true
}

// Observe actual persisted transcript content, independently of provider reports
// or progress summaries that never entered the daemon's transcript flush.
func codexBatchContains(batch map[string]any, marker string) bool {
	if marker == "" {
		return false
	}
	messages, _ := batch["messages"].([]any)
	for _, raw := range messages {
		message, _ := raw.(map[string]any)
		content, _ := message["content"].(string)
		if strings.Contains(content, marker) {
			return true
		}
	}
	return false
}
func codexTranscriptRetained(task *taskRecord) bool {
	if task.Codex == nil {
		return false
	}
	for _, batch := range task.Messages {
		if codexBatchContains(batch, task.Codex.TranscriptMarker) {
			return true
		}
	}
	return false
}
