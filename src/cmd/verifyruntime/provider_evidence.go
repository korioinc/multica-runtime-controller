package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"
)

func finishProvider(backend string, report providerResult, runErr error) error {
	if runErr != nil {
		report.Error = runErr.Error()
		report.Stage = "failed"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := postProvider(ctx, backend, report); runErr == nil && err != nil {
		return err
	}
	return runErr
}

func holdProvider(ctx context.Context, backend string, report providerResult) error {
	if err := postProvider(ctx, backend, report); err != nil {
		return err
	}
	return waitForRelease(ctx, backend, report.TaskID)
}

func postProvider(ctx context.Context, backend string, report providerResult) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, backend+"/fixture/provider", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	var result map[string]any
	return readBody(response, &result)
}
func waitForRelease(ctx context.Context, backend, taskID string) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			request, _ := http.NewRequestWithContext(ctx, http.MethodGet, backend+"/fixture/release/"+taskID, nil)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return err
			}
			var result struct {
				Released bool `json:"released"`
			}
			if err := readBody(response, &result); err != nil {
				return err
			}
			if result.Released {
				return nil
			}
		}
	}
}
