package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const workspaceID = "41000000-0000-4000-8000-000000000001"
const agentID = "41000000-0000-4000-8000-000000000002"
const builtinRuntimeID = "41000000-0000-4000-8000-000000000003"
const customProfileID = "41000000-0000-4000-8000-000000000004"

type taskFailure struct{ TaskID, Reason string }
type observedRegistration struct {
	Types    []string
	Injected bool
}

type backend struct {
	mutex                 sync.Mutex
	mode                  string
	pending               map[string]any
	delivered             string
	injected              bool
	injectProfile         bool
	registered            bool
	profileRequests       int
	bridgeProfileRequests int
	registrations         []observedRegistration
	failures              chan taskFailure
	errors                chan error
	models                chan json.RawMessage
	bridgeDone            chan string
	modelSent             bool
	connections           map[*websocket.Conn]bool
	registerRedirect      string
	claimRedirect         string
	redirectTask          map[string]any
	redirectIssued        bool
	redirectCycleFinished bool
	redirectRequests      []redirectRequest
}

type redirectRequest struct {
	Path              string `json:"path"`
	CredentialPresent bool   `json:"credentialPresent"`
}

func newBackend() *backend {
	return &backend{mode: "http", failures: make(chan taskFailure, 16), errors: make(chan error, 16), models: make(chan json.RawMessage, 16), bridgeDone: make(chan string, 64), connections: map[*websocket.Conn]bool{}}
}
func (f *backend) problem(err error) {
	select {
	case f.errors <- err:
	default:
	}
}
func (f *backend) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/daemon/ws" {
		f.websocket(w, r)
		return
	}
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	var body map[string]json.RawMessage
	_ = json.Unmarshal(raw, &body)
	var output any = map[string]any{}
	switch {
	case r.URL.Path == "/api/daemon/workspaces":
		output = []any{map[string]any{"id": workspaceID, "slug": "official-fixture", "name": "Official fixture"}}
	case strings.HasSuffix(r.URL.Path, "/runtime-profiles"):
		f.mutex.Lock()
		f.profileRequests++
		f.mutex.Unlock()
		output = map[string]any{"workspace_id": workspaceID, "runtime_profiles": []any{map[string]any{"id": customProfileID, "workspace_id": workspaceID, "command_name": "fixture-custom", "protocol_family": "pi", "enabled": true}}}
	case r.URL.Path == "/api/daemon/register":
		var entries []struct {
			Type    string `json:"type"`
			Profile string `json:"profile_id"`
		}
		if json.Unmarshal(body["runtimes"], &entries) != nil {
			f.problem(errors.New("official daemon emitted invalid registration"))
			http.Error(w, "bad registration", 400)
			return
		}
		runtimes := []any{}
		types := []string{}
		for _, entry := range entries {
			if entry.Type != "pi" || entry.Profile != "" {
				f.problem(errors.New("an unauthorized runtime reached registration"))
				http.Error(w, "unauthorized registration", 403)
				return
			}
			types = append(types, entry.Type)
			runtimes = append(runtimes, map[string]any{"id": builtinRuntimeID, "provider": entry.Type, "name": "Official fixture", "status": "online"})
		}
		f.mutex.Lock()
		inject := f.injectProfile
		f.registered = len(runtimes) > 0
		f.registrations = append(f.registrations, observedRegistration{types, inject})
		redirect := f.registerRedirect
		if redirect != "" {
			f.redirectIssued = true
		}
		f.mutex.Unlock()
		if redirect != "" {
			http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
			return
		}
		if inject {
			runtimes = append(runtimes, map[string]any{"id": uuid.NewString(), "provider": "pi", "profile_id": customProfileID, "status": "online"})
		}
		output = map[string]any{"runtimes": runtimes, "repos": []any{}}
	case strings.HasSuffix(r.URL.Path, "/repos"):
		output = map[string]any{"workspace_id": workspaceID, "repos": []any{}}
	case r.URL.Path == "/api/daemon/tasks/claim":
		f.mutex.Lock()
		redirect := f.claimRedirect
		configured := f.redirectTask != nil
		issued := f.redirectIssued
		if redirect != "" && configured {
			if issued {
				f.redirectCycleFinished = true
			} else {
				f.redirectIssued = true
			}
		}
		f.mutex.Unlock()
		if redirect != "" && configured {
			if !issued {
				http.Redirect(w, r, redirect, http.StatusTemporaryRedirect)
			} else {
				writeFixtureJSON(w, map[string]any{"tasks": []any{}})
			}
			return
		}
		output = f.claim("http")
	case strings.HasSuffix(r.URL.Path, "/fail"):
		var reason string
		_ = json.Unmarshal(body["failure_reason"], &reason)
		parts := strings.Split(r.URL.Path, "/")
		f.failures <- taskFailure{parts[4], reason}
	case strings.Contains(r.URL.Path, "/models/") && strings.HasSuffix(r.URL.Path, "/result"):
		f.models <- json.RawMessage(raw)
	case r.URL.Path == "/api/daemon/heartbeat":
		output = f.heartbeat(false)
	case r.URL.Path == "/api/tokens/current/renew":
		output = map[string]any{"token": "mul_disposable_official_fixture"}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(output)
}

func writeFixtureJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (f *backend) redirectTarget(w http.ResponseWriter, r *http.Request) {
	f.mutex.Lock()
	f.redirectRequests = append(f.redirectRequests, redirectRequest{r.URL.Path, r.Header.Get("Authorization") != ""})
	task := f.redirectTask
	f.mutex.Unlock()
	if r.URL.Path == "/claim" {
		writeFixtureJSON(w, map[string]any{"tasks": []any{task}})
		return
	}
	writeFixtureJSON(w, map[string]any{"runtimes": []any{map[string]any{"id": uuid.NewString(), "provider": "pi", "profile_id": customProfileID, "status": "online"}, map[string]any{"id": uuid.NewString(), "provider": "claude", "status": "online"}}, "repos": []any{}})
}

func (f *backend) heartbeat(ws bool) map[string]any {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	result := map[string]any{"runtime_id": builtinRuntimeID}
	if ws {
		result["server_capabilities"] = []string{"rpc-v1"}
	}
	if f.registered && !f.modelSent {
		result["pending_model_list"] = map[string]any{"id": "fixture-model-request"}
		f.modelSent = true
	}
	return result
}

func (f *backend) claim(transport string) any {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	empty := map[string]any{"tasks": []any{}}
	if f.pending == nil {
		return empty
	}
	if f.mode == "ws" && transport != "ws" {
		return empty
	}
	if f.mode == "http" && transport != "http" {
		return empty
	}
	if (f.mode == "failure" || f.mode == "uncertain") && !f.injected {
		return empty
	}
	task := f.pending
	f.pending = nil
	f.delivered = transport
	return map[string]any{"tasks": []any{task}, "opaque_extension": map[string]any{"retained": true}}
}

func (f *backend) websocket(w http.ResponseWriter, r *http.Request) {
	f.mutex.Lock()
	mode := f.mode
	f.mutex.Unlock()
	if mode == "http" {
		http.Error(w, "HTTP-only phase", 503)
		return
	}
	connection, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()
	f.mutex.Lock()
	f.connections[connection] = true
	f.mutex.Unlock()
	defer func() { f.mutex.Lock(); delete(f.connections, connection); f.mutex.Unlock() }()
	for {
		var message struct {
			Type    string `json:"type"`
			Payload struct {
				ID     string `json:"request_id"`
				Method string `json:"method"`
			} `json:"payload"`
		}
		if connection.ReadJSON(&message) != nil {
			return
		}
		switch message.Type {
		case "daemon:heartbeat":
			if connection.WriteJSON(map[string]any{"type": "daemon:heartbeat_ack", "payload": f.heartbeat(true)}) != nil {
				return
			}
			if connection.WriteJSON(map[string]any{"type": "daemon:task_available", "payload": map[string]any{"runtime_id": builtinRuntimeID}}) != nil {
				return
			}
			if connection.WriteJSON(map[string]any{"type": "daemon:runtime_profiles_changed", "payload": map[string]any{"workspace_id": workspaceID}}) != nil {
				return
			}
		case "daemon:rpc_request":
			if message.Payload.Method != "tasks.claim" {
				continue
			}
			f.mutex.Lock()
			mode = f.mode
			inject := f.pending != nil && !f.injected && (mode == "failure" || mode == "uncertain")
			if inject {
				f.injected = true
			}
			f.mutex.Unlock()
			if inject {
				if mode == "uncertain" {
					return
				}
				if connection.WriteJSON(map[string]any{"type": "daemon:rpc_response", "payload": map[string]any{"request_id": message.Payload.ID, "status": 503, "error": "injected transport refusal"}}) != nil {
					return
				}
				continue
			}
			if connection.WriteJSON(map[string]any{"type": "daemon:rpc_response", "payload": map[string]any{"request_id": message.Payload.ID, "status": 200, "body": f.claim("ws")}}) != nil {
				return
			}
		}
	}
}

func (f *backend) enqueue(mode string) map[string]any {
	task := map[string]any{"id": uuid.NewString(), "agent_id": agentID, "runtime_id": uuid.NewString(), "workspace_id": workspaceID, "auth_token": "mat_disposable_official_task", "repos": []any{}}
	f.queue(mode, task)
	return task
}

func (f *backend) queue(mode string, task map[string]any) {
	f.mutex.Lock()
	f.mode = mode
	f.pending = task
	f.injected = false
	f.delivered = ""
	connections := []*websocket.Conn{}
	if mode == "http" {
		for connection := range f.connections {
			connections = append(connections, connection)
		}
	}
	f.mutex.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}
