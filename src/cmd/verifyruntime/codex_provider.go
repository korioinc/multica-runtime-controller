package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type codexRPC struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type codexFixture struct {
	ctx                                     context.Context
	client                                  fixtureClient
	request                                 wire.Request
	report                                  providerResult
	evidence                                codexEvidence
	encoder                                 *json.Encoder
	initialized, notified, active, finished bool
	threadID, turnID, mode                  string
	pendingInitialize                       json.RawMessage
}

// This is a stateful external-provider fixture. The real pinned daemon owns
// cancellation, draining and process shutdown; production shims never synthesize
// a successful app-server response. No model service is called.
func runCodexProvider(ctx context.Context, args []string) (returnErr error) {
	if slices.Contains(args, "--version") {
		fmt.Println("codex-cli 0.154.0")
		return nil
	}
	if !slices.Contains(args, "app-server") {
		return errors.New("Codex fixture requires app-server")
	}
	fixture := &codexFixture{ctx: ctx, encoder: json.NewEncoder(os.Stdout), mode: os.Getenv("VERIFYRUNTIME_CODEX_MODE")}
	if os.Getenv("MULTICA_TASK_ID") != "" {
		if err := fixtureOnly(); err != nil {
			return err
		}
		raw, err := os.ReadFile(wire.RequestPath)
		if err != nil {
			return err
		}
		fixture.request, err = wire.Decode(raw)
		if err != nil {
			return err
		}
		request := fixture.request
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		if request.Provider != "codex" || request.TaskID != os.Getenv("MULTICA_TASK_ID") || cwd != request.WorkDir {
			return errors.New("Codex worker task identity mismatch")
		}
		backend := strings.TrimRight(os.Getenv("VERIFYRUNTIME_BACKEND"), "/")
		if !strings.HasPrefix(backend, "http://") {
			return errors.New("Codex fixture requires disposable backend")
		}
		fixture.client = fixtureClient{origin: backend, http: &http.Client{Timeout: 10 * time.Second}}
		fixture.report = providerResult{TaskID: request.TaskID, Case: os.Getenv("VERIFYRUNTIME_CASE"), Stage: "started", WorkDir: cwd, Storage: request.WorkerSubPath, RuntimeRef: request.RuntimeRef, Request: raw}
		fixture.evidence = codexEvidence{TaskID: request.TaskID, AttemptID: request.AttemptID}
		// Keep prior Pi capture/request fixtures intact; Codex execution has its own
		// immutable mounted-request evidence in the task record.
		if err := postProvider(ctx, backend, fixture.report); err != nil {
			return err
		}
		defer func() {
			if returnErr != nil && !errors.Is(returnErr, context.Canceled) {
				returnErr = finishProvider(backend, fixture.report, returnErr)
			}
		}()
	}
	incoming := make(chan codexRPC)
	readDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 4096), 16<<20)
		for scanner.Scan() {
			var message codexRPC
			if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
				readDone <- err
				return
			}
			select {
			case incoming <- message:
			case <-ctx.Done():
				return
			}
		}
		readDone <- scanner.Err()
	}()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readDone:
			if err != nil {
				return err
			}
			if fixture.mode == "ignore-interrupt" {
				readDone = nil
				continue
			}
			fixture.evidence.EOFAt = time.Now().UTC()
			return fixture.record("stdin_eof")
		case message := <-incoming:
			if err := fixture.handle(message); err != nil {
				return err
			}
		case <-ticker.C:
			if len(fixture.pendingInitialize) == 0 && (!fixture.active || fixture.finished) {
				continue
			}
			if fixture.request.TaskID == "" {
				continue
			}
			var release struct {
				Released bool `json:"released"`
			}
			if err := fixture.client.get(ctx, "/fixture/release/"+fixture.request.TaskID, &release); err != nil {
				return err
			}
			if !release.Released {
				continue
			}
			if len(fixture.pendingInitialize) > 0 {
				id := fixture.pendingInitialize
				fixture.pendingInitialize = nil
				if err := fixture.initialize(id); err != nil {
					return err
				}
			}
			if fixture.active && !fixture.finished {
				if err := fixture.complete("completed"); err != nil {
					return err
				}
			}
		}
	}
}

func (f *codexFixture) record(phase string) error {
	if f.request.TaskID == "" {
		return nil
	}
	f.evidence.Phase = phase
	var result json.RawMessage
	return f.client.post(f.ctx, "/fixture/codex", f.evidence, &result)
}
func (f *codexFixture) reply(id json.RawMessage, result any) error {
	return f.encoder.Encode(map[string]any{"id": id, "result": result})
}
func (f *codexFixture) notify(method string, params any) error {
	return f.encoder.Encode(map[string]any{"method": method, "params": params})
}
func (f *codexFixture) initialize(id json.RawMessage) error {
	f.initialized = true
	f.evidence.InitializeAt = time.Now().UTC()
	if err := f.reply(id, map[string]any{"userAgent": "disposable-codex-fixture"}); err != nil {
		return err
	}
	return f.record("initialize_response")
}
func (f *codexFixture) handle(m codexRPC) error {
	if m.Method == "initialize" {
		if f.initialized || len(f.pendingInitialize) > 0 {
			return errors.New("duplicate initialize in one app-server")
		}
		if err := f.record("initialize_received"); err != nil {
			return err
		}
		if f.mode == "initialize-hold" {
			f.pendingInitialize = append(json.RawMessage(nil), m.ID...)
			return nil
		}
		return f.initialize(m.ID)
	}
	if !f.initialized {
		return errors.New("Codex request before initialize completed")
	}
	if m.Method == "initialized" {
		f.notified = true
		return f.record("initialized")
	}
	if !f.notified {
		return errors.New("Codex request before initialized notification")
	}
	switch m.Method {
	case "model/list":
		return f.reply(m.ID, map[string]any{"data": []any{map[string]any{"id": "fixture-model", "model": "fixture-model", "displayName": "Disposable Codex fixture", "description": "Local protocol fixture", "isDefault": true, "supportedReasoningEfforts": []any{}, "defaultReasoningEffort": "medium", "inputModalities": []string{"text"}}}, "nextCursor": nil})
	case "account/read":
		return f.reply(m.ID, map[string]any{"account": nil, "requiresOpenaiAuth": false})
	case "thread/start", "thread/resume":
		if f.threadID != "" {
			return errors.New("duplicate thread setup")
		}
		f.evidence.ThreadMethod = m.Method
		if m.Method == "thread/resume" {
			// No Codex native session is registered by this controller today. Refuse an
			// unexpected resume honestly; daemon may create a fresh session.
			if err := f.record("resume_rejected"); err != nil {
				return err
			}
			return f.encoder.Encode(map[string]any{"id": m.ID, "error": map[string]any{"code": -32602, "message": "thread not found"}})
		}
		f.threadID = uuid.NewString()
		f.report.Session = f.threadID
		return f.reply(m.ID, map[string]any{"thread": map[string]any{"id": f.threadID}})
	case "thread/name/set":
		return f.reply(m.ID, map[string]any{})
	case "turn/start":
		if f.active || f.threadID == "" {
			return errors.New("turn/start without an idle thread")
		}
		var params struct {
			ThreadID string `json:"threadId"`
			Input    []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"input"`
		}
		if err := json.Unmarshal(m.Params, &params); err != nil {
			return err
		}
		if params.ThreadID != f.threadID {
			return errors.New("turn input belongs to a different thread")
		}
		var text strings.Builder
		for _, part := range params.Input {
			if part.Type == "text" {
				text.WriteString(part.Text)
			}
		}
		f.evidence.Prompt = text.String()
		f.evidence.PromptDigest = core.Digest([]byte(text.String()))
		if f.request.TaskID == "" {
			return errors.New("discovery app-server received task execution")
		}
		if err := f.record("turn_input"); err != nil {
			return err
		}
		if err := verifyProviderCheckout(f.ctx, f.request, f.client.origin, &f.report); err != nil {
			return err
		}
		f.active = true
		f.turnID = uuid.NewString()
		f.report.Stage = "held"
		if err := postProvider(f.ctx, f.client.origin, f.report); err != nil {
			return err
		}
		if err := f.reply(m.ID, map[string]any{"turn": map[string]any{"id": f.turnID, "status": "inProgress"}}); err != nil {
			return err
		}
		if err := f.notify("turn/started", map[string]any{"threadId": f.threadID, "turn": map[string]any{"id": f.turnID, "status": "inProgress"}}); err != nil {
			return err
		}
		if f.mode == "hold" {
			f.evidence.TranscriptMarker = "partial-task=" + f.request.TaskID + " attempt=" + f.request.AttemptID
			if err := f.record("partial_output"); err != nil {
				return err
			}
			// The pinned Multica SDK forwards completed agent-message items,
			// including commentary, but does not consume agentMessage/delta.
			// A commentary item completes only this partial message, not the turn.
			if err := f.notify("item/completed", map[string]any{"threadId": f.threadID, "turnId": f.turnID, "item": map[string]any{"id": uuid.NewString(), "type": "agentMessage", "phase": "commentary", "text": f.evidence.TranscriptMarker}}); err != nil {
				return err
			}
		}
		if err := f.record("turn_active"); err != nil {
			return err
		}
		if f.mode == "" {
			return f.complete("completed")
		}
		return nil
	case "turn/interrupt":
		var params struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
		}
		if err := json.Unmarshal(m.Params, &params); err != nil {
			return err
		}
		if !f.active || params.ThreadID != f.threadID || params.TurnID != f.turnID {
			return errors.New("interrupt refers to another task turn")
		}
		f.evidence.InterruptAt = time.Now().UTC()
		if err := f.record("interrupt_received"); err != nil {
			return err
		}
		if f.mode == "ignore-interrupt" {
			return nil
		}
		if err := f.reply(m.ID, map[string]any{}); err != nil {
			return err
		}
		if !f.finished {
			return f.complete("interrupted")
		}
		return nil
	default:
		return fmt.Errorf("unsupported Codex request %q", m.Method)
	}
}

func (f *codexFixture) complete(status string) error {
	if !f.active || f.finished {
		return errors.New("no unfinished Codex turn")
	}
	f.finished = true
	if status == "completed" {
		// The answer is derived from the actual received turn and durable edit. A
		// crossed task prompt cannot pass by emitting a preselected canned answer.
		answer := "Verified task " + f.request.TaskID + " input " + f.evidence.PromptDigest + " work " + f.report.ModifiedDigest
		if err := f.notify("item/completed", map[string]any{"threadId": f.threadID, "turnId": f.turnID, "item": map[string]any{"id": uuid.NewString(), "type": "agentMessage", "phase": "final_answer", "text": answer}}); err != nil {
			return err
		}
		f.report.Stage = "completed"
	} else {
		f.report.Stage = "interrupted"
	}
	if err := postProvider(f.ctx, f.client.origin, f.report); err != nil {
		return err
	}
	if err := f.notify("turn/completed", map[string]any{"threadId": f.threadID, "turn": map[string]any{"id": f.turnID, "status": status}}); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		return err
	}
	return f.record("turn_" + status)
}
