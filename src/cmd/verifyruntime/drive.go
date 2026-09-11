package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	if strings.HasPrefix(phase, "handoff-") {
		return client.verifyCodexHandoff(ctx, phase)
	}
	switch phase {
	case "baseline":
		return client.verifyBaseline(ctx)
	case "changed":
		return client.verifyEnvironmentChange(ctx)
	case "hold":
		return client.holdEnvironment(ctx)
	case "release":
		return client.releaseEnvironment(ctx)
	case "cleanup-failure":
		return client.verifyCleanupFailure(ctx)
	case "recovered":
		return client.verifyCleanupRecovery(ctx)
	case "interrupted-start":
		return client.holdInterruptedWorker(ctx)
	case "interrupted-resume":
		return client.verifyInterruptedRecovery(ctx)
	case "journal-failure":
		return client.verifyJournalFailure(ctx)
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
				if !report.ROChecked || !report.WritableChecked || !report.IsolationChecked || !report.RepeatCheckoutChecked || !report.ScopeChecked || !report.CacheChecked || report.CoreHash != report.RuntimeRef.Controller.RuntimeSHA256 {
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
		if a.RuntimeRef.Equal(b.RuntimeRef) || a.Session == b.Session || b.PriorSession || !b.ContinuityNotice {
			return errors.New("environment change did not start an honest new provider session")
		}
	} else if !a.RuntimeRef.Equal(b.RuntimeRef) || a.Session != b.Session || !b.PriorSession {
		return errors.New("same-environment continuation lost its authorized session")
	}
	return nil
}
