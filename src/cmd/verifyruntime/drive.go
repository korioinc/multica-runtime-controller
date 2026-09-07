package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type fixtureClient struct {
	origin string
	http   *http.Client
}

func (c fixtureClient) get(ctx context.Context, path string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.origin+path, nil)
	if err != nil {
		return err
	}
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	return readBody(response, target)
}
func (c fixtureClient) post(ctx context.Context, path string, value, target any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	return readBody(response, target)
}

func drive(ctx context.Context, origin, phase string) error {
	if err := fixtureOnly(); err != nil {
		return err
	}
	client := fixtureClient{origin: origin, http: &http.Client{Timeout: 15 * time.Second}}
	switch phase {
	case "baseline":
		first, err := client.run(ctx, runRequest{Case: "initial", Scope: "a", Transport: "http"})
		if err != nil {
			return err
		}
		if first.Provider.PriorWork || first.Provider.PriorSession {
			return errors.New("first task adopted unapproved history")
		}
		continued, err := client.run(ctx, runRequest{Case: "continuation", Scope: "a", Transport: "ws", CacheOverride: "/tmp/fixture-task-cache", PriorTaskID: first.Input.TaskID})
		if err != nil {
			return err
		}
		if err := sameWorkspace(first, continued, false); err != nil {
			return err
		}
		retried, err := client.run(ctx, runRequest{Case: "retry", Scope: "a", Transport: "failure", TaskID: continued.Input.TaskID, PriorTaskID: continued.Input.TaskID})
		if err != nil {
			return err
		}
		if err := sameWorkspace(continued, retried, false); err != nil {
			return err
		}
		var ack map[string]any
		if err := client.post(ctx, "/fixture/baseline", map[string]string{"taskID": retried.Input.TaskID}, &ack); err != nil {
			return err
		}
		isolated, err := client.run(ctx, runRequest{Case: "scope-isolation", Scope: "b", Transport: "uncertain", PriorTaskID: retried.Input.TaskID})
		if err != nil {
			return err
		}
		if isolated.Provider.Storage == retried.Provider.Storage || isolated.Provider.PriorWork || isolated.Provider.PriorSession {
			return errors.New("another scope reused protected files or session history")
		}
		fmt.Println("runtime baseline verified: HTTP, WS continuation, same-task retry, scoped checkout and storage isolation")
		return nil
	case "changed":
		state, err := client.state(ctx)
		if err != nil {
			return err
		}
		prior := state.Tasks[state.Baseline]
		if prior == nil || prior.Provider == nil {
			return errors.New("baseline evidence is unavailable")
		}
		current, err := client.run(ctx, runRequest{Case: "environment-change", Scope: "a", Transport: "http", PriorTaskID: prior.Input.TaskID})
		if err != nil {
			return err
		}
		if err := sameWorkspace(*prior, current, true); err != nil {
			return err
		}
		fmt.Println("runtime environment change verified: files and branch preserved, fresh Pi session and official continuity notice")
		return nil
	case "hold":
		var record taskRecord
		if err := client.post(ctx, "/fixture/run", runRequest{Case: "held-environment", Scope: "b", Transport: "http", Hold: true}, &record); err != nil {
			return err
		}
		if _, err := client.wait(ctx, record, true); err != nil {
			return err
		}
		fmt.Println("runtime task is held with its immutable environment:", record.Input.TaskID)
		return nil
	case "release":
		state, err := client.state(ctx)
		if err != nil {
			return err
		}
		held := state.Tasks[state.Held]
		if held == nil || held.Provider == nil {
			return errors.New("no verified held provider exists")
		}
		started := *held.Provider
		var ack map[string]any
		if err := client.post(ctx, "/fixture/release/"+held.Input.TaskID, map[string]any{}, &ack); err != nil {
			return err
		}
		done, err := client.wait(ctx, *held, false)
		if err != nil {
			return err
		}
		if !done.Provider.Environment.Equal(started.Environment) {
			return errors.New("running worker changed its environment after another generation was prepared")
		}
		fmt.Println("held task completed with its original environment")
		return nil
	case "cleanup-failure":
		completed, err := client.run(ctx, runRequest{Case: "cleanup-failure", Scope: "cleanup", Transport: "http"})
		if err != nil {
			return err
		}
		if completed.Provider.PriorWork || completed.Provider.PriorSession {
			return errors.New("cleanup fixture did not begin with fresh storage")
		}
		fmt.Println("provider completed with retained files while the orchestrator denied controller DELETE:", completed.Input.TaskID)
		return nil
	case "recovered":
		state, err := client.state(ctx)
		if err != nil {
			return err
		}
		prior := state.Tasks[state.CleanupTask]
		if prior == nil || prior.Completion == nil || prior.Provider == nil {
			return errors.New("cleanup-failure task evidence is unavailable")
		}
		next, err := client.run(ctx, runRequest{Case: "cleanup-recovered", Scope: "cleanup", Transport: "http", PriorTaskID: prior.Input.TaskID})
		if err != nil {
			return err
		}
		if err := sameWorkspace(*prior, next, false); err != nil {
			return err
		}
		fmt.Println("cleanup recovery preserved the prior task's files, branch and authorized session")
		return nil
	case "interrupted-start":
		var record taskRecord
		if err := client.post(ctx, "/fixture/run", runRequest{Case: "interrupted-start", Scope: "interrupted", Transport: "http", HoldAfterWork: true}, &record); err != nil {
			return err
		}
		held, err := client.wait(ctx, record, true)
		if err != nil {
			return err
		}
		if held.Checkpoint == nil || held.Checkpoint.ModifiedDigest == "" || held.Checkpoint.Branch == "" || !held.Checkpoint.RepeatCheckoutChecked {
			return errors.New("interruption fixture has not durably modified its real worker repository")
		}
		fmt.Println("real worker is held after checkout, edits and Pi transcript publication:", record.Input.TaskID)
		return nil
	case "interrupted-resume":
		state, err := client.state(ctx)
		if err != nil {
			return err
		}
		prior := state.Tasks[state.InterruptedTask]
		if prior == nil || prior.Checkpoint == nil {
			return errors.New("held worker checkpoint is unavailable")
		}
		checkpoint := *prior.Checkpoint
		retried, err := client.run(ctx, runRequest{Case: "interrupted-retry", Scope: "interrupted", Transport: "http", TaskID: prior.Input.TaskID})
		if err != nil {
			return err
		}
		report := retried.Provider
		if report.Storage != checkpoint.Storage || report.Branch != checkpoint.Branch || !report.PriorWork || report.PriorWorkDigest != checkpoint.ModifiedDigest || !report.Environment.Equal(checkpoint.Environment) {
			return errors.New("controller interruption recovery lost worker files, branch or pinned environment")
		}
		fmt.Println("controller interruption recovery preserved the actual held worker's files and branch")
		return nil
	case "journal-failure":
		var queued taskRecord
		if err := client.post(ctx, "/fixture/run", runRequest{Case: "journal-failure", Scope: "journal", Transport: "http"}, &queued); err != nil {
			return err
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				state, err := client.state(ctx)
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
	default:
		return errors.New("unknown runtime fixture phase")
	}
}

func (c fixtureClient) state(ctx context.Context) (backendState, error) {
	var state backendState
	err := c.get(ctx, "/fixture/state", &state)
	if err == nil && state.Error != "" {
		err = errors.New(state.Error)
	}
	return state, err
}
func (c fixtureClient) run(ctx context.Context, input runRequest) (taskRecord, error) {
	var record taskRecord
	if err := c.post(ctx, "/fixture/run", input, &record); err != nil {
		return record, err
	}
	result, err := c.wait(ctx, record, false)
	if err != nil {
		return result, err
	}
	fmt.Println("runtime task verified:", input.Case)
	return result, nil
}
func (c fixtureClient) wait(ctx context.Context, queued taskRecord, startedOnly bool) (taskRecord, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return taskRecord{}, ctx.Err()
		case <-ticker.C:
			state, err := c.state(ctx)
			if err != nil {
				return taskRecord{}, err
			}
			record := state.Tasks[queued.Input.TaskID]
			if record == nil || record.Epoch != queued.Epoch {
				return taskRecord{}, errors.New("fixture task identity changed while waiting")
			}
			if record.Failure != nil {
				raw, _ := json.Marshal(record.Failure)
				return *record, fmt.Errorf("official task failed (%s): %s", record.Input.Case, raw)
			}
			if record.Provider != nil && record.Provider.Error != "" {
				return *record, errors.New(record.Provider.Error)
			}
			if startedOnly && record.Provider != nil && (record.Provider.Stage == "started" || record.Provider.Stage == "held") {
				return *record, nil
			}
			if record.Completion != nil && record.Provider != nil && record.Provider.Stage == "completed" {
				report := record.Provider
				if !report.ROChecked || !report.WritableChecked || !report.IsolationChecked || !report.RepeatCheckoutChecked || !report.ScopeChecked || !report.CacheChecked || report.CoreHash != report.Environment.Core.Files["runtime"] {
					return *record, errors.New("provider did not establish its required execution/storage checks")
				}
				if report.TaskID != record.Input.TaskID || record.Completion["work_dir"] != report.WorkDir || record.Completion["session_id"] != report.Session {
					return *record, errors.New("official completion and real provider result disagree")
				}
				if record.Input.Transport == "ws" && record.Transport != "ws" || record.Input.Transport == "http" && record.Transport != "http" || (record.Input.Transport == "failure" || record.Input.Transport == "uncertain") && (!record.Injected || record.Transport != "http") {
					return *record, errors.New("task did not use its intended official transport path")
				}
				return *record, nil
			}
		}
	}
}

func sameWorkspace(prior, next taskRecord, changed bool) error {
	a, b := prior.Provider, next.Provider
	if a == nil || b == nil || a.Storage != b.Storage || a.Branch != b.Branch || !b.PriorWork || b.PriorWorkDigest != a.ModifiedDigest {
		return errors.New("authorized continuation lost its worker files, branch or storage")
	}
	if changed {
		if a.Environment.Equal(b.Environment) || a.Session == b.Session || b.PriorSession || !b.ContinuityNotice {
			return errors.New("environment change did not start an honest new provider session")
		}
	} else if !a.Environment.Equal(b.Environment) || a.Session != b.Session || !b.PriorSession {
		return errors.New("same-environment continuation lost its authorized session")
	}
	return nil
}
