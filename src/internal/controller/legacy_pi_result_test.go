package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestImportedLegacyPiTransientFailureKeepsItsResultAndDelivery(t *testing.T) {
	c, g, options := importedLegacyPiResultFixture(t)
	key := admitStopRegressionWorker(t, c, g)
	var err error
	g, err = c.Store.Get(g.AttemptID)
	if err != nil || !g.Legacy || g.SessionProtocol || g.WorkerSessionID != "" || !g.StartConfirmed {
		t.Fatal("fixture did not import and start a schema-13 legacy attempt", err)
	}
	var mu sync.Mutex
	upstreamStatus := "running"
	var delivered []byte
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/agents/" + g.AgentID + "/tasks":
			_ = json.NewEncoder(w).Encode([]any{map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID,
				"runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": upstreamStatus}})
		case "/api/daemon/tasks/" + g.TaskID + "/fail":
			var readErr error
			delivered, readErr = io.ReadAll(r.Body)
			if readErr != nil || !json.Valid(delivered) {
				http.Error(w, "invalid failure", http.StatusBadRequest)
				return
			}
			upstreamStatus = "failed"
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "legacy-controller-token", c.RuntimeRef.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	failure := wire.ProviderResult{Result: agent.Result{Status: "failed", ResumeRejectedTransient: true,
		Error: fmt.Sprintf("Pi session file %q is already in use by another execution", g.ResumeSession)}}
	command, err := c.receiveResult(g, failure)
	if err != nil {
		t.Fatal("actual legacy Pi failure was rejected instead of entering its outbox", err)
	}
	if err := c.Store.RecordResultReceipt(g.AttemptID, signResultReceipt(g, command, key)); err != nil {
		t.Fatal("legacy failed result could not retain its worker authentication", err)
	}
	reopenStopRegressionStore(t, c, options)
	terminal, err := c.Store.Terminal(g.AttemptID)
	rawResult, _ := json.Marshal(failure)
	if err != nil || terminal.Source != "worker" || terminal.Kind != "fail" || terminal.State != "received" ||
		terminal.ResultDigest != wire.Digest(rawResult) || terminal.ResultReceipt == nil || terminal.RecoveryFailure != nil {
		t.Fatal("reopen lost or replaced the authenticated legacy failure", err)
	}
	if err := c.reconcileDelivery(t.Context(), g.AttemptID); err != nil {
		t.Fatal("legacy transient provider failure could not be delivered", err)
	}
	mu.Lock()
	status, body := upstreamStatus, bytes.Clone(delivered)
	mu.Unlock()
	var observed struct {
		Error   string `json:"error"`
		WorkDir string `json:"work_dir"`
	}
	if status != "failed" || !bytes.Equal(body, terminal.Body) || json.Unmarshal(body, &observed) != nil ||
		observed.Error != failure.Result.Error || observed.WorkDir != g.TaskRoot+"/workdir" {
		t.Fatal("the backend did not commit the provider's actual failed outcome")
	}
	reopenStopRegressionStore(t, c, options)
	retained, err := c.Store.Terminal(g.AttemptID)
	current, grantErr := c.Store.Get(g.AttemptID)
	storage, storageErr := c.Store.GetStorage(g.StorageID)
	if err != nil || grantErr != nil || storageErr != nil || retained.State != "delivered" || !bytes.Equal(retained.Body, terminal.Body) ||
		retained.ResultDigest != terminal.ResultDigest || retained.ResultReceipt == nil || retained.RecoveryFailure != nil ||
		current.ResumeSession != g.ResumeSession || storage.SessionID != g.ResumeSession || current.PendingResume != nil || current.CompletionWitness != nil {
		t.Fatal("delivery changed the legacy result or introduced modern continuity authority", err, grantErr, storageErr)
	}
}

// Import the supported schema-13 Pi shape before provisioning. No live grant,
// Pod, signed result, or runtime binding is changed after admission.
func importedLegacyPiResultFixture(t *testing.T) (*Controller, workspace.TaskGrant, workspace.Options) {
	t.Helper()
	c, g, _, options, _ := controllerFixtureBeforeProvision(t)
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(options.Directory, "journal.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]json.RawMessage
	var grants map[string]workspace.TaskGrant
	var storages map[string]workspace.Storage
	if json.Unmarshal(raw, &journal) != nil || json.Unmarshal(journal["grants"], &grants) != nil || json.Unmarshal(journal["storages"], &storages) != nil {
		t.Fatal("legacy fixture journal is unavailable")
	}
	g = grants[g.AttemptID]
	g.RuntimeRef.Providers = map[string]runtimeimage.Executable{"pi": g.RuntimeRef.Providers["codex"]}
	g.Fingerprint = workspace.RuntimeFingerprint(g.RuntimeRef)
	g.Reuse = true
	g.ResumeSession, g.ResumeWorkDir = g.TaskRoot+"/pi-sessions/original.jsonl", g.TaskRoot+"/workdir"
	prepared := *g.Prepared
	prepared.Provider, prepared.Environment.CodexHome, prepared.RuntimeDigest, prepared.Digest = "pi", "", g.Fingerprint, ""
	raw, err = json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared.Digest = core.Digest(raw)
	g.Prepared = &prepared
	g.Execution, err = json.Marshal(wire.Run{Provider: "pi", Options: agent.ExecOptions{Cwd: g.ResumeWorkDir, Timeout: time.Hour,
		ResumeSessionID: g.ResumeSession, ResumeExpected: true}})
	if err != nil {
		t.Fatal(err)
	}
	storage := storages[g.StorageID]
	storage.Fingerprint, storage.Prepared, storage.SessionID = g.Fingerprint, &prepared, g.ResumeSession
	grants[g.AttemptID], storages[storage.ID] = g, storage
	encode := func(value any) json.RawMessage {
		t.Helper()
		var output bytes.Buffer
		encoder := json.NewEncoder(&output)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			t.Fatal(err)
		}
		return output.Bytes()
	}
	journal["schemaVersion"], journal["grants"], journal["storages"] = json.RawMessage(`13`), encode(grants), encode(storages)
	if err := os.WriteFile(path, encode(journal), 0600); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal("schema-13 Pi fixture failed real migration", err)
	}
	c.RuntimeRef = g.RuntimeRef
	c.Descriptor.Providers = g.RuntimeRef.Providers
	g, err = c.Store.Get(g.AttemptID)
	if err != nil || !g.Legacy || g.Prepared.Provider != "pi" || g.ResumeSession == "" {
		t.Fatal("Pi result fixture did not retain its imported legacy resume state", err)
	}
	if err := c.provision(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	g, err = c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return c, g, options
}
