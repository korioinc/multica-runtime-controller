package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (c fixtureClient) codexSchedule(ctx context.Context, input runRequest) (taskRecord, error) {
	input.Provider = "codex"
	if input.Prompt == "" {
		input.Prompt = "Preserve earlier edits and handle selected instruction " + uuid.NewString()
	}
	var task taskRecord
	err := c.post(ctx, "/fixture/run", input, &task)
	return task, err
}
func (c fixtureClient) codexWait(ctx context.Context, queued taskRecord, condition func(*taskRecord) bool, allowFailure bool) (taskRecord, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := c.state(ctx)
		if err != nil {
			return taskRecord{}, err
		}
		record := state.Tasks[queued.Input.TaskID]
		if record == nil || record.Epoch != queued.Epoch {
			return taskRecord{}, errors.New("Codex task changed while waiting")
		}
		if !allowFailure && record.Failure != nil {
			raw, _ := json.Marshal(record.Failure)
			return *record, fmt.Errorf("official Codex task %s failed: %s", record.Input.Case, raw)
		}
		if record.Provider != nil && record.Provider.Error != "" {
			return *record, errors.New(record.Provider.Error)
		}
		if condition(record) {
			return *record, nil
		}
		select {
		case <-ctx.Done():
			return *record, fmt.Errorf("waiting for %s: %w", record.Input.Case, ctx.Err())
		case <-ticker.C:
		}
	}
}
func codexPhase(phase string) func(*taskRecord) bool {
	return func(task *taskRecord) bool {
		for _, event := range task.Events {
			if event.Phase == phase {
				return true
			}
		}
		return false
	}
}
func (c fixtureClient) codexCompleted(ctx context.Context, task taskRecord) (taskRecord, error) {
	record, err := c.codexWait(ctx, task, func(t *taskRecord) bool { return t.Completion != nil && t.Codex != nil && !t.Codex.EOFAt.IsZero() }, false)
	if err != nil {
		return record, err
	}
	if record.Provider == nil || record.Provider.ModifiedDigest == "" || record.Codex.PromptDigest == "" {
		return record, errors.New("Codex task did not finish its actual provider work")
	}
	if !strings.Contains(record.Codex.Prompt, record.Input.Prompt) {
		return record, errors.New("selected queued instruction was not delivered to its task")
	}
	answer, _ := json.Marshal(record.Completion)
	if !strings.Contains(string(answer), record.Codex.PromptDigest) || !strings.Contains(string(answer), record.Provider.ModifiedDigest) {
		return record, errors.New("official answer does not belong to the received prompt and persisted work")
	}
	return record, nil
}
func codexContinuity(prior, next taskRecord) error {
	if prior.Provider == nil || next.Provider == nil || !next.Provider.PriorWork || next.Provider.PriorWorkDigest != prior.Provider.ModifiedDigest || next.Provider.Storage != prior.Provider.Storage || next.Provider.Branch != prior.Provider.Branch {
		return errors.New("Codex handoff lost authorized unfinished files or branch")
	}
	if next.Provider.Session == prior.Provider.Session {
		return errors.New("fixture fabricated a native session continuation")
	}
	return nil
}
func (c fixtureClient) codexCancel(ctx context.Context, task taskRecord) error {
	var result taskRecord
	return c.post(ctx, "/fixture/cancel/"+task.Input.TaskID, map[string]any{}, &result)
}
func (c fixtureClient) codexRelease(ctx context.Context, task taskRecord) error {
	var result json.RawMessage
	return c.post(ctx, "/fixture/release/"+task.Input.TaskID, map[string]any{}, &result)
}
func (c fixtureClient) codexAck(ctx context.Context, task taskRecord, normal bool) (taskRecord, error) {
	result, err := c.codexWait(ctx, task, func(t *taskRecord) bool {
		// The daemon acknowledges its own transcript drain. Its independent SDK
		// shutdown may observe remote interrupt/EOF before or after that ack.
		return t.CancelAck != nil && (!normal || t.Codex != nil && !t.Codex.InterruptAt.IsZero() && !t.Codex.EOFAt.IsZero())
	}, false)
	if err != nil {
		return result, err
	}
	if result.Completion != nil {
		return result, errors.New("cancelled task published a completion")
	}
	if normal {
		if !codexTranscriptRetained(&result) {
			return result, errors.New("cancellation lost the actual partial assistant transcript")
		}
		markerPersisted, acknowledged := false, false
		for _, event := range result.Events {
			if event.Phase == "official_messages" {
				if acknowledged {
					return result, errors.New("task transcript arrived after its cancellation acknowledgment")
				}
				if event.TranscriptMarker == result.Codex.TranscriptMarker {
					markerPersisted = true
				}
			}
			if event.Phase == "official_cancel-ack" {
				if !markerPersisted {
					return result, errors.New("cancel acknowledgment preceded the observed partial transcript")
				}
				acknowledged = true
			}
		}
	}
	return result, nil
}

func (c fixtureClient) verifyCodexHandoff(ctx context.Context, phase string) error {
	switch phase {
	case "handoff-default-1", "handoff-default-2":
		for _, scenario := range []struct{ mode, transport string }{{"hold", "http"}, {"ignore-interrupt", "ws"}, {"initialize-hold", "http"}} {
			if err := c.codexTransition(ctx, phase, scenario.mode, scenario.transport); err != nil {
				return err
			}
		}
		if err := c.codexCompletionRace(ctx, phase); err != nil {
			return err
		}
		if phase == "handoff-default-2" {
			return c.codexParallelStorage(ctx, phase)
		}
		return nil
	case "handoff-explicit":
		return c.codexExplicitTimeout(ctx)
	case "handoff-timeout":
		return c.codexExpiredInitialize(ctx)
	case "handoff-pending":
		return c.codexPendingWorker(ctx)
	case "handoff-repeat-start":
		task, err := c.codexSchedule(ctx, runRequest{Case: phase, Scope: "codex-repeat", Transport: "ws", CodexMode: "ignore-interrupt"})
		if err != nil {
			return err
		}
		task, err = c.codexWait(ctx, task, codexPhase("turn_active"), false)
		if err != nil {
			return err
		}
		fmt.Println("handoff-repeat active task:", task.Input.TaskID)
		return nil
	case "handoff-repeat-follow":
		return c.codexRepeatedInstruction(ctx)
	default:
		return errors.New("unknown Codex handoff phase")
	}
}

func (c fixtureClient) codexTransition(ctx context.Context, phase, mode, transport string) error {
	scope := "codex-" + uuid.NewString()
	a, err := c.codexSchedule(ctx, runRequest{Case: phase + "-" + mode + "-A", Scope: scope, Transport: transport, CodexMode: mode})
	if err != nil {
		return err
	}
	waiting := "turn_active"
	if mode == "initialize-hold" {
		waiting = "initialize_received"
	}
	a, err = c.codexWait(ctx, a, codexPhase(waiting), false)
	if err != nil {
		return err
	}
	if mode == "hold" {
		// Cancellation starts only after the real daemon's /messages flush has
		// persisted the task-specific partial output. Provider reports alone do
		// not establish transcript retention.
		a, err = c.codexWait(ctx, a, codexTranscriptRetained, false)
		if err != nil {
			return err
		}
	}
	if err := c.codexCancel(ctx, a); err != nil {
		return err
	}
	b, err := c.codexSchedule(ctx, runRequest{Case: phase + "-" + mode + "-B", Scope: scope, Transport: transport, PriorTaskID: a.Input.TaskID})
	if err != nil {
		return err
	}
	b, err = c.codexCompleted(ctx, b)
	if err != nil {
		return err
	}
	a, err = c.codexAck(ctx, a, mode == "hold")
	if err != nil {
		return err
	}
	if mode == "ignore-interrupt" && (a.Codex == nil || a.Codex.InterruptAt.IsZero() || !a.Codex.EOFAt.IsZero()) {
		return errors.New("unresponsive interrupt did not exercise forced app-server termination")
	}
	if mode == "initialize-hold" && (a.Codex == nil || !a.Codex.InitializeAt.IsZero() || a.Provider.ModifiedDigest != "") {
		return errors.New("cancelled initializing task reached task execution")
	}
	if mode != "initialize-hold" {
		if err := codexContinuity(a, b); err != nil {
			return err
		}
	}
	if strings.Contains(b.Codex.Prompt, a.Input.Prompt) {
		return errors.New("earlier task instruction replaced or contaminated selected task input")
	}
	// Evidence records claim, cancellation poll, ack, initialize and actual worker
	// attempt identity. Ack-before-claim is intentionally not an acceptance rule.
	fmt.Printf("Codex handoff verified: %s %s A=%s B=%s storage=%s\n", phase, mode, a.Input.TaskID, b.Input.TaskID, b.Provider.Storage)
	return nil
}

func (c fixtureClient) codexCompletionRace(ctx context.Context, phase string) error {
	scope := "codex-" + uuid.NewString()
	a, err := c.codexSchedule(ctx, runRequest{Case: phase + "-completion-A", Scope: scope, Transport: "ws", CodexMode: "hold"})
	if err != nil {
		return err
	}
	a, err = c.codexWait(ctx, a, codexPhase("turn_active"), false)
	if err != nil {
		return err
	}
	// The server's completion wins. No committed cancellation is issued. The
	// continuation must still use its own prompt and retain the finished files.
	if err := c.codexRelease(ctx, a); err != nil {
		return err
	}
	a, err = c.codexCompleted(ctx, a)
	if err != nil {
		return err
	}
	b, err := c.codexSchedule(ctx, runRequest{Case: phase + "-completion-B", Scope: scope, Transport: "ws", PriorTaskID: a.Input.TaskID})
	if err != nil {
		return err
	}
	b, err = c.codexCompleted(ctx, b)
	if err != nil {
		return err
	}
	if err := codexContinuity(a, b); err != nil {
		return err
	}
	if a.CancelAck != nil {
		return errors.New("controller synthesized cancellation after normal completion")
	}
	fmt.Println("Codex completion-winning handoff verified:", a.Input.TaskID, b.Input.TaskID)
	return nil
}

func (c fixtureClient) codexParallelStorage(ctx context.Context, phase string) error {
	a, err := c.codexSchedule(ctx, runRequest{Case: phase + "-parallel-A", Scope: "codex-" + uuid.NewString(), Transport: "http", CodexMode: "hold"})
	if err != nil {
		return err
	}
	a, err = c.codexWait(ctx, a, codexPhase("turn_active"), false)
	if err != nil {
		return err
	}
	b, err := c.codexSchedule(ctx, runRequest{Case: phase + "-parallel-B", Scope: "codex-" + uuid.NewString(), Transport: "http"})
	if err != nil {
		return err
	}
	b, err = c.codexCompleted(ctx, b)
	if err != nil {
		return err
	}
	if a.Provider.Storage == b.Provider.Storage || b.Provider.PriorWork {
		return errors.New("independent conversations shared writable repository storage")
	}
	if err := c.codexRelease(ctx, a); err != nil {
		return err
	}
	_, err = c.codexCompleted(ctx, a)
	return err
}

func (c fixtureClient) codexExplicitTimeout(ctx context.Context) error {
	a, err := c.codexSchedule(ctx, runRequest{Case: "handoff-explicit", Scope: "codex-" + uuid.NewString(), Transport: "http", CodexMode: "initialize-hold"})
	if err != nil {
		return err
	}
	a, err = c.codexWait(ctx, a, codexPhase("initialize_received"), false)
	if err != nil {
		return err
	}
	initialAttempt := a.Codex.AttemptID
	// An intentional integration timing probe, recorded with daemon lifecycle
	// timestamps: exceed the default initialize budget, then release under 45s.
	timer := time.NewTimer(32 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	if err := c.codexRelease(ctx, a); err != nil {
		return err
	}
	completed, err := c.codexCompleted(ctx, a)
	if err != nil {
		return err
	}
	if initialAttempt == "" || completed.Codex.AttemptID != initialAttempt {
		return errors.New("explicit handshake budget did not preserve the original initializing attempt")
	}
	fmt.Println("explicit handshake preserved initializing attempt:", initialAttempt)
	return nil
}

func (c fixtureClient) codexExpiredInitialize(ctx context.Context) error {
	a, err := c.codexSchedule(ctx, runRequest{Case: "handoff-timeout", Scope: "codex-" + uuid.NewString(), Transport: "http", CodexMode: "initialize-hold"})
	if err != nil {
		return err
	}
	a, err = c.codexWait(ctx, a, codexPhase("initialize_received"), false)
	if err != nil {
		return err
	}
	a, err = c.codexWait(ctx, a, func(t *taskRecord) bool { return t.Failure != nil }, true)
	if err != nil {
		return err
	}
	if a.Completion != nil || a.Provider == nil || a.Provider.ModifiedDigest != "" || a.Codex == nil || !a.Codex.InitializeAt.IsZero() {
		return errors.New("timed-out initialize performed or completed task work")
	}
	// Preserve the official failure and timestamps for budget diagnosis. Error
	// wording is not the oracle for preventing work after a silent initialize.
	fmt.Println("Codex silent initialize failed without task work:", a.Input.TaskID)
	return nil
}

// The shell holds a unique NoSchedule taint on the only eligible fixture node.
// The real daemon's initialize budget must end this task before provider work.
func (c fixtureClient) codexPendingWorker(ctx context.Context) error {
	task, err := c.codexSchedule(ctx, runRequest{Case: "handoff-pending", Scope: "codex-" + uuid.NewString(), Transport: "http"})
	if err != nil {
		return err
	}
	task, err = c.codexWait(ctx, task, codexPhase("official_start"), false)
	if err != nil {
		return err
	}
	task, err = c.codexWait(ctx, task, func(t *taskRecord) bool { return t.Failure != nil || t.Completion != nil }, true)
	if err != nil {
		return err
	}
	if task.Failure == nil || task.Provider != nil || task.Completion != nil {
		return errors.New("unschedulable worker reached provider work or completed its task")
	}
	fmt.Println("Codex task ended safely while worker scheduling was blocked:", task.Input.TaskID)
	return nil
}

// Shell first adds a finalizer to A's exact worker Pod, so its cleanup holds the
// storage lease. It removes the finalizer only after B cancellation and C claim.
func (c fixtureClient) codexRepeatedInstruction(ctx context.Context) error {
	state, err := c.state(ctx)
	if err != nil {
		return err
	}
	var a taskRecord
	for _, task := range state.Tasks {
		if task.Input.Case == "handoff-repeat-start" {
			a = *task
		}
	}
	if a.Input.TaskID == "" {
		return errors.New("repeat handoff requires its held worker")
	}
	if err := c.codexCancel(ctx, a); err != nil {
		return err
	}
	b, err := c.codexSchedule(ctx, runRequest{Case: "handoff-repeat-B", Scope: a.Input.Scope, Transport: "ws", PriorTaskID: a.Input.TaskID})
	if err != nil {
		return err
	}
	b, err = c.codexWait(ctx, b, codexPhase("official_start"), false)
	if err != nil {
		return err
	}
	// Shell releases B only after observing its actual blocked execution phase.
	if err := waitForRelease(ctx, c.origin, b.Input.TaskID); err != nil {
		return err
	}
	if err := c.codexCancel(ctx, b); err != nil {
		return err
	}
	next, err := c.codexSchedule(ctx, runRequest{Case: "handoff-repeat-C", Scope: a.Input.Scope, Transport: "ws", PriorTaskID: a.Input.TaskID})
	if err != nil {
		return err
	}
	next, err = c.codexCompleted(ctx, next)
	if err != nil {
		return err
	}
	b, err = c.codexAck(ctx, b, false)
	if err != nil {
		return err
	}
	if b.Provider != nil || b.Completion != nil {
		return errors.New("superseded waiting instruction performed task work")
	}
	if strings.Contains(next.Codex.Prompt, b.Input.Prompt) {
		return errors.New("superseded queued instruction leaked into the chosen task")
	}
	if err := codexContinuity(a, next); err != nil {
		return err
	}
	_, err = c.codexAck(ctx, a, false)
	return err
}
