//go:build linux

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// The driver puts this controller, its local model, and the real worker in
// separate containers. Only the worker's anchored task directory is mounted.
func TestResidentDesktopGraphicalController(t *testing.T) {
	control := os.Getenv("MULTICA_RESIDENT_DESKTOP_PROOF")
	if control == "" {
		t.Skip("run scripts/test-resident-desktop.sh with the candidate runtime")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	d, descriptorDigest, err := runtimeimage.Check(ctx, runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	c, legacy, _, _, _ := controllerFixtureBeforeProvision(t)
	for _, step := range []func() error{
		func() error { _, err := c.Store.RequestStop(legacy.AttemptID, "unused_fixture_seed"); return err },
		func() error { return c.Store.CloseCheckouts(legacy.AttemptID) },
		func() error {
			return c.Store.ObserveStop(legacy.AttemptID, workspace.StopEvidence{PVCUID: legacy.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()})
		},
		func() error { return c.Store.CloseStopped(legacy.AttemptID) },
		func() error { return c.Store.MarkCleaned(legacy.AttemptID) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	model := &residentLocalModel{control: control}
	residentFixtureServer(t, 8081, model)
	config := []byte(`model_provider = "fixture"
[model_providers.fixture]
name = "Local proof"
base_url = "http://resident-controller:8081"
env_key = "OPENAI_API_KEY"
wire_api = "responses"
requires_openai_auth = false
`)
	groups := []configuration.Group{{Name: "local-proof", Directories: []string{".codex"}, Files: []configuration.File{{Target: ".codex/config.toml", Mode: 0600, Content: config, SHA256: core.Digest(config)}}}}
	c.Configuration = configuration.Bundle{Groups: groups, Digest: configuration.Digest(groups)}
	if err := configuration.ApplyHomeConfiguration(wire.Home, c.Configuration); err != nil {
		t.Fatal(err)
	}
	image := "registry.fixture/resident@" + os.Getenv("MULTICA_RESIDENT_IMAGE_ID")
	c.RuntimeRef, err = d.Reference(image, descriptorDigest, configuration.ExecutionDigest(c.Configuration, nil))
	if err != nil {
		t.Fatal(err)
	}
	c.Descriptor, c.GatewayURL, c.NFSServer = d, "http://resident-controller:8080", "127.0.0.1"
	if server := os.Getenv("MULTICA_RESIDENT_NFS_SERVER"); server != "" {
		c.NFSServer = server
	}
	c.Policy.Platform, c.Policy.Storage.MaxBytes = d.Platform, 1<<30
	c.Policy.Worker.PreparationTimeoutSeconds, c.Policy.Worker.TaskDeadlineSeconds = 120, 180
	c.Policy.Worker.TerminationGraceSeconds = 2
	c.Capacity, c.MaxResidentPods, c.ConversationIdleTimeout = 2, 2, 3*time.Minute
	controllerPod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, c.Owner.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	controllerPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "controller", ContainerID: "docker://" + os.Getenv("HOSTNAME"), State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}
	_, err = c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(ctx, controllerPod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	f := &sessionControllerFixture{C: c, Rows: map[string]daemonapi.TaskObservation{}, Starts: map[string]int{}}
	agentID, issueID := uuid.NewString(), uuid.NewString()
	backend := residentFixtureServer(t, 8082, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/me":
			writeJSON(w, map[string]string{"id": c.OwnerID})
		case "/api/agents/" + agentID:
			writeJSON(w, map[string]string{"id": agentID, "workspace_id": legacy.WorkspaceID, "runtime_id": legacy.RuntimeID, "owner_id": c.OwnerID})
		case "/api/workspaces/" + legacy.WorkspaceID + "/members":
			writeJSON(w, []any{map[string]string{"id": c.OwnerID, "workspace_id": legacy.WorkspaceID, "user_id": c.OwnerID, "role": "owner"}})
		case "/api/agents/" + agentID + "/mcp-servers":
			writeJSON(w, []any{})
		case "/api/workspaces/" + legacy.WorkspaceID + "/plugins":
			writeJSON(w, map[string]any{"plugins": []any{}})
		default:
			f.serveBackend(w, r)
		}
	}))
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", d.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	residentFixtureServer(t, 8080, c.Handler())
	var first workspace.TaskGrant
	var session workspace.WorkerSession
	for turn := 1; turn <= 2; turn++ {
		if turn == 2 {
			residentFixtureWait(t, ctx, control, "next-turn")
		}
		model.setTurn(turn)
		id, now := uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond)
		row := daemonapi.TaskObservation{ID: id, WorkspaceID: legacy.WorkspaceID, AgentID: agentID, RuntimeID: legacy.RuntimeID, Kind: "direct", IssueID: issueID,
			Status: "dispatched", CreatedAt: now.Add(-time.Second).Format(time.RFC3339Nano), DispatchedAt: now.Format(time.RFC3339Nano)}
		f.mu.Lock()
		f.Rows[id] = row
		f.mu.Unlock()
		fields := map[string]any{"id": id, "kind": "direct", "workspace_id": row.WorkspaceID, "agent_id": agentID, "runtime_id": row.RuntimeID,
			"issue_id": issueID, "auth_token": "mat_" + id, "start_claim_supported": true, "dispatched_at": row.DispatchedAt, "created_at": row.CreatedAt,
			"attribution": daemonapi.TaskAttribution{Source: "unattributed"},
			"agent":       map[string]any{"id": agentID, "custom_env": map[string]string{"OPENAI_API_KEY": "local-fixture-only", "OPENAI_BASE_URL": "http://resident-controller:8081"}},
			"issue":       map[string]string{"id": issueID, "title": fmt.Sprintf("Graphical proof turn %d", turn), "description": "Use the current local proof command."}}
		if turn == 2 {
			fields["prior_work_dir"], fields["prior_session_id"] = first.TaskRoot+"/workdir", first.CompletionWitness.SessionID
			fields["new_comments_delta_known"] = true
		}
		envelope, _ := json.Marshal(fields)
		metadata, _ := json.Marshal(daemonapi.Bootstrap{Workspace: daemonapi.Workspace{ID: row.WorkspaceID}, Runtime: daemonapi.Runtime{ID: row.RuntimeID, Provider: "codex"}})
		g, err := c.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, AgentID: agentID, WorkspaceID: row.WorkspaceID,
			RuntimeID: row.RuntimeID, RuntimeRef: c.RuntimeRef, Envelope: envelope, Metadata: metadata, ResourceScope: []string{"issue:" + issueID}})
		if err != nil {
			t.Fatal(err)
		}
		for g.WorkerSessionID == "" {
			if err := c.activateTurn(ctx, g); err != nil {
				t.Fatal(err)
			}
			g, err = c.Store.Get(g.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			time.Sleep(20 * time.Millisecond)
		}
		session, err = c.Store.GetSession(g.WorkerSessionID)
		if err != nil {
			t.Fatal(err)
		}
		model.setRoot(g.TaskRoot)
		if turn == 2 && (session.ID != first.WorkerSessionID || g.ResumeSession != first.CompletionWitness.SessionID || g.ResumeSession == "") {
			t.Fatal("compatible native follow-up lost its exact live owner or accepted session")
		}
		published := false
		bootstrapPublished := false
		for !g.TurnComplete {
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			if !published {
				err := c.provisionTurn(ctx, g)
				if err != nil && !errors.Is(err, errPreparePending) {
					t.Fatal(err)
				}
				session, err = c.Store.GetSession(g.WorkerSessionID)
				if err != nil {
					t.Fatal(err)
				}
				if session.PodUID != "" {
					pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, session.PodName, metav1.GetOptions{})
					if err != nil {
						t.Fatal(err)
					}
					pod.Spec.NodeName = "worker-node"
					if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
					pod.Status.Phase, pod.Status.PodIP = corev1.PodRunning, "127.0.0.1"
					pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "task-layout", ImageID: c.RuntimeRef.Image}}
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", ImageID: c.RuntimeRef.Image, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
					if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
						t.Fatal(err)
					}
					if turn == 1 && !bootstrapPublished {
						residentFixtureWrite(t, filepath.Join(control, "request.json"), session.Bootstrap)
						residentFixtureJSON(t, filepath.Join(control, "bootstrap.json"), map[string]any{"taskRoot": g.TaskRoot, "podUID": session.PodUID, "pvcUID": session.PVCUID, "sessionID": session.ID, "requestDigest": session.BootstrapDigest, "desktopHome": wire.DesktopHome, "chromeUserDataDir": wire.ChromeProfileRoot, "chromeExecutable": filepath.Join(filepath.Dir(wire.ChromeOriginal), "chrome"), "descriptorDigest": descriptorDigest, "controllerDigest": d.Controller.RuntimeSHA256, "terminationGraceSeconds": c.Policy.Worker.TerminationGraceSeconds})
						bootstrapPublished = true
					}
				}
			}
			if _, err := c.Store.Terminal(g.AttemptID); err == nil {
				if err := c.reconcileDelivery(ctx, g.AttemptID); err != nil {
					t.Fatal(err)
				}
			}
			g, err = c.Store.Get(g.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			published = len(g.Assignment) != 0
			time.Sleep(25 * time.Millisecond)
		}
		storage, err := c.Store.GetStorage(g.StorageID)
		if err != nil {
			t.Fatal(err)
		}
		if !storage.Dirty || storage.WriterSessionID != session.ID || storage.Checkpoint != nil || g.CompletionWitness == nil || g.CompletionWitness.SessionID == "" {
			t.Fatal("live graphical completion released storage or lost native producer evidence")
		}
		if turn == 1 {
			first = g
		}
		podCreates := 0
		for _, action := range c.Kube.API.(*fake.Clientset).Actions() {
			if action.GetVerb() == "create" && action.GetResource().Resource == "pods" {
				podCreates++
			}
		}
		residentFixtureJSON(t, filepath.Join(control, fmt.Sprintf("idle-%d.json", turn)), map[string]any{"taskRoot": g.TaskRoot, "sessionID": session.ID, "nativeSessionID": g.CompletionWitness.SessionID, "taskID": g.TaskID, "turn": turn, "podUID": session.PodUID, "podCreates": podCreates})
	}
	residentFixtureWait(t, ctx, control, "stop")
	if _, err := c.Store.RequestSessionStop(session.ID, "graphical_proof_complete"); err != nil {
		t.Fatal(err)
	}
	residentFixtureWait(t, ctx, control, "termination.json")
	var termination struct {
		PodUID     string
		ExitCode   int
		FinishedAt time.Time
	}
	raw, err := os.ReadFile(filepath.Join(control, "termination.json"))
	if err != nil || json.Unmarshal(raw, &termination) != nil || termination.PodUID != session.PodUID || termination.FinishedAt.IsZero() {
		t.Fatal("driver did not prove the exact worker container exited", err)
	}
	session, err = c.Store.GetSession(session.ID)
	if err != nil {
		t.Fatal(err)
	}
	rootLoss := os.Getenv("MULTICA_RESIDENT_NFS_ROOT_LOSS") == "1"
	if rootLoss {
		if session.Stop == nil || session.Stop.ControllerWritersStopped || session.Stop.Receipt != nil && session.Stop.Receipt.FlushOK {
			t.Fatal("lost root acquired clean flush approval")
		}
	} else if session.Stop == nil || session.Stop.Receipt == nil || !session.Stop.Receipt.WritersStopped || !session.Stop.Receipt.FlushOK {
		t.Fatal("final worker receipt did not prove coordinated storage shutdown")
	}
	if err := c.Store.ObserveSessionStop(session.ID, workspace.StopEvidence{Kind: "terminated", PodUID: session.PodUID, PVCUID: session.PVCUID, ObservedAt: termination.FinishedAt}); err != nil {
		t.Fatal(err)
	}
	if rootLoss && session.Stop.Receipt == nil {
		// The actual worker used its bounded final-stop budget without certifying
		// this filesystem. Retain the existing post-termination recovery grace.
		grace, err := c.sessionTerminationGrace(session)
		if err != nil {
			t.Fatal(err)
		}
		timer := time.NewTimer(max(0, grace-time.Since(termination.FinishedAt)))
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := c.Store.CloseSession(session.ID); err != nil {
		t.Fatal(err)
	}
	storage, err := c.Store.GetStorage(first.StorageID)
	if err != nil {
		t.Fatal(err)
	}
	if rootLoss {
		if !storage.Dirty || storage.WriterSessionID != "" || storage.Checkpoint != nil {
			t.Fatal("lost root did not close dirty without granting file reuse")
		}
	} else if storage.Dirty || storage.WriterSessionID != "" || storage.Checkpoint == nil {
		t.Fatal("clean final shutdown did not publish a selectable final checkpoint")
	}
	residentFixtureJSON(t, filepath.Join(control, "complete.json"), map[string]any{"sessionID": session.ID, "sealed": !rootLoss, "latestSource": storage.LatestWriter})
}

func residentFixtureServer(t *testing.T, port int, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func residentFixtureWait(t *testing.T, ctx context.Context, root, name string) {
	t.Helper()
	for {
		if _, err := os.Stat(filepath.Join(root, name)); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func residentFixtureWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path+".tmp", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

func residentFixtureJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	residentFixtureWrite(t, path, raw)
}

type residentLocalModel struct {
	mu            sync.Mutex
	control, root string
	turn          int
	offered       bool
}

func (m *residentLocalModel) setTurn(turn int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.turn, m.offered = turn, false
}
func (m *residentLocalModel) setRoot(root string) { m.mu.Lock(); defer m.mu.Unlock(); m.root = root }

func (m *residentLocalModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/page":
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<!doctype html><title>Resident desktop local proof</title><input id="note"><a id="download" href="/download" download="resident-proof.txt">Download</a><script>fetch('/slow').then(r=>r.text()).then(t=>document.body.dataset.progress=t)</script>`)
		case "/slow", "/idle", "/download":
			if r.URL.Path == "/download" {
				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Content-Disposition", `attachment; filename="resident-proof.txt"`)
				fmt.Fprint(w, "accepted-before-idle\n")
				w.(http.Flusher).Flush()
			}
			if err := os.WriteFile(filepath.Join(m.control, filepath.Base(r.URL.Path)+"-accepted"), []byte("accepted"), 0600); err != nil {
				http.Error(w, "local activity marker unavailable", http.StatusInternalServerError)
				return
			}
			deadline := time.NewTimer(2 * time.Minute)
			defer deadline.Stop()
			for {
				if _, err := os.Stat(filepath.Join(m.control, "idle-1.json")); err == nil {
					break
				}
				select {
				case <-r.Context().Done():
					return
				case <-deadline.C:
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
			fmt.Fprint(w, "progress-during-idle\n")
		default:
			http.NotFound(w, r)
		}
		return
	}
	var input map[string]any
	if json.NewDecoder(r.Body).Decode(&input) != nil {
		http.Error(w, "invalid local model request", 400)
		return
	}
	m.mu.Lock()
	turn, root, offered := m.turn, m.root, m.offered
	m.offered = true
	m.mu.Unlock()
	var item map[string]any
	if !offered {
		command := fmt.Sprintf("printf ready > %s/workdir/proof-turn-%d", root, turn)
		if turn == 1 {
			command = "command -v google-chrome-stable; google-chrome-stable --version; xdpyinfo -display \"$DISPLAY\" >/dev/null; OBU_CHROME_STARTUP_LOG=$(mktemp /tmp/obu-chrome-startup.XXXXXX); nohup google-chrome-stable > \"$OBU_CHROME_STARTUP_LOG\" 2>&1 < /dev/null &\n" +
				"cua-driver call launch_app '{\"name\":\"mousepad\",\"session\":\"resident-acceptance\"}' > " + root + "/workdir/proof-editor.json\n" +
				"cua-driver call launch_app '" + residentIdleApplication(root) + "' >/dev/null\n" +
				"setsid /bin/sh -c 'echo $$ > \"$1/lingering.pid\"; trap \"\" TERM; while :; do echo task >> \"$1/task-writer\"; sleep .05; done' task " + root + "/workdir >/dev/null 2>&1 < /dev/null &\n" +
				"printf ready > " + root + "/workdir/proof-turn-1"
		}
		arguments, _ := json.Marshal(map[string]any{"cmd": command, "yield_time_ms": 1000})
		item = map[string]any{"id": fmt.Sprintf("fc_turn_%d", turn), "type": "function_call", "call_id": fmt.Sprintf("call_turn_%d", turn), "name": "exec_command", "arguments": string(arguments), "status": "completed"}
	} else {
		deadline := time.NewTimer(2 * time.Minute)
		defer deadline.Stop()
		for {
			if _, err := os.Stat(filepath.Join(m.control, fmt.Sprintf("finish-%d", turn))); err == nil {
				break
			}
			select {
			case <-r.Context().Done():
				return
			case <-deadline.C:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		item = map[string]any{"id": fmt.Sprintf("msg_turn_%d", turn), "type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": fmt.Sprintf("Accepted local turn %d", turn), "annotations": []any{}}}}
	}
	result := map[string]any{"id": fmt.Sprintf("resp_turn_%d", turn), "object": "response", "created_at": time.Now().Unix(), "status": "completed", "error": nil, "incomplete_details": nil, "model": input["model"], "output": []any{item}, "usage": map[string]int{"input_tokens": 1, "output_tokens": 3, "total_tokens": 4}}
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range []struct {
		name string
		body any
	}{
		{"response.created", map[string]any{"type": "response.created", "response": map[string]any{"id": result["id"], "object": "response", "status": "in_progress", "output": []any{}}}},
		{"response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": 0, "item": item}},
		{"response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item}},
		{"response.completed", map[string]any{"type": "response.completed", "response": result}},
	} {
		raw, _ := json.Marshal(event.body)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.name, raw)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func residentIdleApplication(root string) string {
	code := "import urllib.request; urllib.request.urlopen(\"http://resident-controller:8081/idle\",timeout=120).read(); open(\"" + root + "/workdir/application-idle-write\",\"w\").write(\"progress-during-idle\")"
	raw, _ := json.Marshal(map[string]any{"name": "python3", "additional_arguments": []string{"-c", code}, "session": "resident-acceptance"})
	return string(raw)
}
