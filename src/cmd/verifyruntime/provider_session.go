package main

import (
	"bytes"
	"encoding/json"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func inspectProviderSession(report *providerResult) (fresh bool, err error) {
	priorSession, err := os.ReadFile(report.Session)
	if err != nil {
		return false, err
	}
	report.PriorSession = bytes.Contains(priorSession, []byte("runtime fixture transcript"))
	return len(priorSession) == 0, nil
}

func recordProviderSession(report *providerResult, fresh bool) error {
	if err := writeSession(report.Session, report.WorkDir, fresh); err != nil {
		return err
	}
	sessionData, err := os.ReadFile(report.Session)
	if err != nil {
		return err
	}
	report.SessionDigest = core.Digest(sessionData)
	return nil
}

func writeSession(path, cwd string, fresh bool) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	if fresh {
		if err := encoder.Encode(map[string]any{"type": "session", "version": 3, "id": uuid.NewString(), "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "cwd": cwd}); err != nil {
			return err
		}
	}
	return encoder.Encode(map[string]any{"type": "message", "id": uuid.NewString(), "parentId": nil, "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "runtime fixture transcript"}}}})
}
