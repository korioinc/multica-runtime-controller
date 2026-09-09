package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (c fixtureClient) verifyBaseline(ctx context.Context) error {
	first, err := c.run(ctx, runRequest{Case: "initial", Scope: "a", Transport: "http"})
	if err != nil {
		return err
	}
	if first.Provider.PriorWork || first.Provider.PriorSession {
		return errors.New("first task adopted unapproved history")
	}
	continued, err := c.run(ctx, runRequest{Case: "continuation", Scope: "a", Transport: "ws", CacheOverride: "/tmp/fixture-task-cache", PriorTaskID: first.Input.TaskID})
	if err != nil {
		return err
	}
	if err := sameWorkspace(first, continued, false); err != nil {
		return err
	}
	retried, err := c.run(ctx, runRequest{Case: "retry", Scope: "a", Transport: "failure", TaskID: continued.Input.TaskID, PriorTaskID: continued.Input.TaskID})
	if err != nil {
		return err
	}
	if err := sameWorkspace(continued, retried, false); err != nil {
		return err
	}
	var ack map[string]any
	if err := c.post(ctx, "/fixture/baseline", map[string]string{"taskID": retried.Input.TaskID}, &ack); err != nil {
		return err
	}
	isolated, err := c.run(ctx, runRequest{Case: "scope-isolation", Scope: "b", Transport: "uncertain", PriorTaskID: retried.Input.TaskID})
	if err != nil {
		return err
	}
	if isolated.Provider.Storage == retried.Provider.Storage || isolated.Provider.PriorWork || isolated.Provider.PriorSession {
		return errors.New("another scope reused protected files or session history")
	}
	fmt.Println("runtime baseline verified: HTTP, WS continuation, same-task retry, scoped checkout and storage isolation")
	return nil
}

func (c fixtureClient) verifyEnvironmentChange(ctx context.Context) error {
	state, err := c.state(ctx)
	if err != nil {
		return err
	}
	prior := state.Tasks[state.Baseline]
	if prior == nil || prior.Provider == nil {
		return errors.New("baseline evidence is unavailable")
	}
	current, err := c.run(ctx, runRequest{Case: "environment-change", Scope: "a", Transport: "http", PriorTaskID: prior.Input.TaskID})
	if err != nil {
		return err
	}
	if err := sameWorkspace(*prior, current, true); err != nil {
		return err
	}
	fmt.Println("runtime environment change verified: files and branch preserved, fresh Pi session and official continuity notice")
	return nil
}

func (c fixtureClient) holdEnvironment(ctx context.Context) error {
	var record taskRecord
	if err := c.post(ctx, "/fixture/run", runRequest{Case: "held-environment", Scope: "b", Transport: "http", Hold: true}, &record); err != nil {
		return err
	}
	if _, err := c.wait(ctx, record, true); err != nil {
		return err
	}
	fmt.Println("runtime task is held with its immutable environment:", record.Input.TaskID)
	return nil
}

func (c fixtureClient) releaseEnvironment(ctx context.Context) error {
	state, err := c.state(ctx)
	if err != nil {
		return err
	}
	held := state.Tasks[state.Held]
	if held == nil || held.Provider == nil {
		return errors.New("no verified held provider exists")
	}
	started := *held.Provider
	var ack map[string]any
	if err := c.post(ctx, "/fixture/release/"+held.Input.TaskID, map[string]any{}, &ack); err != nil {
		return err
	}
	done, err := c.wait(ctx, *held, false)
	if err != nil {
		return err
	}
	if !done.Provider.RuntimeRef.Equal(started.RuntimeRef) {
		return errors.New("running worker changed its environment after another generation was prepared")
	}
	fmt.Println("held task completed with its original environment")
	return nil
}

func (c fixtureClient) verifyCleanupFailure(ctx context.Context) error {
	completed, err := c.run(ctx, runRequest{Case: "cleanup-failure", Scope: "cleanup", Transport: "http"})
	if err != nil {
		return err
	}
	if completed.Provider.PriorWork || completed.Provider.PriorSession {
		return errors.New("cleanup fixture did not begin with fresh storage")
	}
	fmt.Println("provider completed with retained files while the orchestrator denied controller DELETE:", completed.Input.TaskID)
	return nil
}

func (c fixtureClient) verifyCleanupRecovery(ctx context.Context) error {
	state, err := c.state(ctx)
	if err != nil {
		return err
	}
	prior := state.Tasks[state.CleanupTask]
	if prior == nil || prior.Completion == nil || prior.Provider == nil {
		return errors.New("cleanup-failure task evidence is unavailable")
	}
	next, err := c.run(ctx, runRequest{Case: "cleanup-recovered", Scope: "cleanup", Transport: "http", PriorTaskID: prior.Input.TaskID})
	if err != nil {
		return err
	}
	if err := sameWorkspace(*prior, next, false); err != nil {
		return err
	}
	fmt.Println("cleanup recovery preserved the prior task's files, branch and authorized session")
	return nil
}

func (c fixtureClient) holdInterruptedWorker(ctx context.Context) error {
	var record taskRecord
	if err := c.post(ctx, "/fixture/run", runRequest{Case: "interrupted-start", Scope: "interrupted", Transport: "http", HoldAfterWork: true}, &record); err != nil {
		return err
	}
	held, err := c.wait(ctx, record, true)
	if err != nil {
		return err
	}
	if held.Checkpoint == nil || held.Checkpoint.ModifiedDigest == "" || held.Checkpoint.Branch == "" || !held.Checkpoint.RepeatCheckoutChecked {
		return errors.New("interruption fixture has not durably modified its real worker repository")
	}
	fmt.Println("real worker is held after checkout, edits and Pi transcript publication:", record.Input.TaskID)
	return nil
}

func (c fixtureClient) verifyInterruptedRecovery(ctx context.Context) error {
	state, err := c.state(ctx)
	if err != nil {
		return err
	}
	prior := state.Tasks[state.InterruptedTask]
	if prior == nil || prior.Checkpoint == nil {
		return errors.New("held worker checkpoint is unavailable")
	}
	checkpoint := *prior.Checkpoint
	retried, err := c.run(ctx, runRequest{Case: "interrupted-retry", Scope: "interrupted", Transport: "http", TaskID: prior.Input.TaskID})
	if err != nil {
		return err
	}
	report := retried.Provider
	if report.Storage != checkpoint.Storage || report.Branch != checkpoint.Branch || !report.PriorWork || report.PriorWorkDigest != checkpoint.ModifiedDigest || !report.RuntimeRef.Equal(checkpoint.RuntimeRef) {
		return errors.New("controller interruption recovery lost worker files, branch or pinned environment")
	}
	fmt.Println("controller interruption recovery preserved the actual held worker's files and branch")
	return nil
}

func (c fixtureClient) verifyJournalFailure(ctx context.Context) error {
	var queued taskRecord
	if err := c.post(ctx, "/fixture/run", runRequest{Case: "journal-failure", Scope: "journal", Transport: "http"}, &queued); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			state, err := c.state(ctx)
			if err != nil {
				return err
			}
			record := state.Tasks[queued.Input.TaskID]
			if record == nil || record.Epoch != queued.Epoch {
				return errors.New("journal fixture task identity changed")
			}
			if record.Provider != nil || record.Completion != nil {
				return errors.New("journal intent failure permitted provider execution or successful completion")
			}
			if record.Failure != nil {
				if record.Transport != "http" {
					return errors.New("journal rejection was not preceded by the actual official claim")
				}
				fmt.Println("journal intent failure rejected task before provider execution:", queued.Input.TaskID)
				return nil
			}
		}
	}
}
