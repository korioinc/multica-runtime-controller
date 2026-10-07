package daemonapi

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestGatewayManagementKeepsTaskAuthority(t *testing.T) {
	workspaceID, definition := "", "original instructions"
	credentialExposed, identityForged := false, false
	backendAllows := true
	var mu sync.Mutex
	g, grant, capability := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer mat_task-a" {
			credentialExposed = true
			return
		}
		if r.Header.Get("X-Workspace-ID") != workspaceID || r.Header.Get("X-User-ID") != "" || r.Header.Get("X-Agent-ID") != "" || r.Header.Get("X-Task-ID") != "" || r.Header.Get("X-Multica-Attempt-Capability") != "" {
			identityForged = true
			return
		}
		if !backendAllows {
			http.Error(w, "backend permission denied", http.StatusForbidden)
			return
		}
		var input struct{ Instructions string }
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			return
		}
		definition = input.Instructions
		_, _ = io.WriteString(w, `{}`)
	}))
	workspaceID = grant.WorkspaceID
	current := func() string {
		mu.Lock()
		defer mu.Unlock()
		return definition
	}
	endpoint := "/api/agents/another-agent"
	update := func(token, cap, query string) {
		r := httptest.NewRequest(http.MethodPut, endpoint+query, strings.NewReader(`{"instructions":"updated instructions"}`))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Multica-Attempt-Capability", cap)
		r.Header.Set("X-Workspace-ID", "foreign-workspace")
		r.Header.Set("X-User-ID", "owner")
		r.Header.Set("X-Agent-ID", "other-agent")
		r.Header.Set("X-Task-ID", "other-task")
		g.ServeHTTP(httptest.NewRecorder(), r)
	}
	for _, invalid := range []struct{ token, capability, query string }{
		{"mat_other-task", capability, ""},
		{"owner-secret", capability, ""},
		{"mat_task-a", "foreign-capability", ""},
	} {
		update(invalid.token, invalid.capability, invalid.query)
		if current() != "original instructions" {
			t.Fatal("request without admitted task authority changed the agent")
		}
	}
	mu.Lock()
	backendAllows = false
	mu.Unlock()
	update("mat_task-a", capability, "")
	if current() != "original instructions" {
		t.Fatal("backend-denied update changed the agent")
	}
	mu.Lock()
	backendAllows = true
	mu.Unlock()
	update("mat_task-a", capability, "")
	if current() != "updated instructions" {
		t.Fatal("authorized task could not update the agent")
	}
	mu.Lock()
	unsafe := credentialExposed || identityForged
	definition = "original instructions"
	mu.Unlock()
	if unsafe {
		t.Fatal("management request exposed credentials or forwarded a forged identity")
	}
	if _, err := g.Store.RequestStop(grant.AttemptID, "cancelled"); err != nil {
		t.Fatal(err)
	}
	update("mat_task-a", capability, "")
	if current() != "original instructions" {
		t.Fatal("cancelled task changed the agent")
	}
}

func TestGatewaySkillImportKeepsTaskAuthority(t *testing.T) {
	workspaceID, imported := "", ""
	credentialExposed := false
	var mu sync.Mutex
	g, grant, capability := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer mat_task-a" || r.Header.Get("X-Workspace-ID") != workspaceID {
			credentialExposed = true
			return
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			return
		}
		defer r.MultipartForm.RemoveAll()
		file, _, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			return
		}
		defer file.Close()
		raw, err := io.ReadAll(file)
		if err != nil {
			t.Error(err)
			return
		}
		archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			t.Error(err)
			return
		}
		content, err := archive.Open("SKILL.md")
		if err != nil {
			t.Error(err)
			return
		}
		defer content.Close()
		raw, err = io.ReadAll(content)
		if err != nil {
			t.Error(err)
			return
		}
		imported = string(raw)
		_, _ = io.WriteString(w, `{}`)
	}))
	workspaceID = grant.WorkspaceID
	const skill = "---\nname: authorized-skill\ndescription: Test skill\n---\nAuthorized instructions\n"
	var archive bytes.Buffer
	zipWriter := zip.NewWriter(&archive)
	content, err := zipWriter.Create("SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(content, skill)
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("file", "skill.zip")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.Write(archive.Bytes())
	if err := writer.WriteField("on_conflict", "fail"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"mat_foreign-task", "mat_task-a"} {
		r := httptest.NewRequest(http.MethodPost, "/api/skills/import", bytes.NewReader(body.Bytes()))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Multica-Attempt-Capability", capability)
		r.Header.Set("Content-Type", writer.FormDataContentType())
		g.ServeHTTP(httptest.NewRecorder(), r)
		mu.Lock()
		unauthorizedImport := token == "mat_foreign-task" && imported != ""
		mu.Unlock()
		if unauthorizedImport {
			t.Fatal("another task imported a skill through this attempt")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if imported != skill || credentialExposed {
		t.Fatal("skill import did not retain the admitted task's authority and content")
	}
}

func TestGatewayAutopilotRetriesDoNotRepeatExecution(t *testing.T) {
	for _, path := range []string{"/api/autopilots/autopilot-a/trigger", "/api/autopilots/autopilot-a/deliveries/delivery-a/replay"} {
		t.Run(path, func(t *testing.T) {
			var mu sync.Mutex
			executions := 0
			seen := map[string]bool{}
			g, _, capability := fixtureGateway(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				// Native actions deduplicate a supplied key; an absent key starts
				// a new execution for every request.
				key := r.Header.Get("Idempotency-Key")
				if key == "" || !seen[key] {
					executions++
					seen[key] = true
				}
				_, _ = io.WriteString(w, `{}`)
			}))
			for _, key := range []string{"first-execution", "first-execution", "second-execution"} {
				r := httptest.NewRequest(http.MethodPost, path, strings.NewReader("null"))
				r.Header.Set("Authorization", "Bearer mat_task-a")
				r.Header.Set("X-Multica-Attempt-Capability", capability)
				r.Header.Set("Idempotency-Key", key)
				g.ServeHTTP(httptest.NewRecorder(), r)
			}
			mu.Lock()
			defer mu.Unlock()
			if executions != 2 {
				t.Fatalf("one retried action and one new action started %d executions", executions)
			}
		})
	}
}
