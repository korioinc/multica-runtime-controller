package worker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestProviderCompletionRetainsRedactedTranscript(t *testing.T) {
	options := workspace.Options{Directory: filepath.Join(t.TempDir(), "journal"), OwnerID: uuid.NewString()}
	store, err := workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	grant := startedEventGrant(t, store)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			io.WriteString(w, `{}`)
			return
		}
		raw, err := io.ReadAll(r.Body)
		var event wire.ProviderEvent
		if err == nil {
			err = json.Unmarshal(raw, &event)
		}
		if err == nil {
			_, err = store.ReceiveEvent(grant.AttemptID, event.Sequence, raw)
		}
		if err != nil {
			t.Error(err)
			http.Error(w, "journal refused event", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	// Official adapters close a buffered transcript before delivering Result;
	// both channels can therefore be readable before the consumer begins.
	messages := make(chan agent.Message, 32)
	secret := "sk-" + uuid.NewString()
	for i := range cap(messages) {
		messages <- agent.Message{Type: agent.MessageToolUse, Input: map[string]any{
			"changes": []any{map[string]any{"content": fmt.Sprintf("user edit %d", i), "diff": secret}},
		}}
	}
	close(messages)
	results := make(chan agent.Result, 1)
	results <- agent.Result{Status: "completed"}
	close(results)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = drainProvider(ctx, cancel, &agent.Session{Messages: messages, Result: results}, server.Client(), wire.Bootstrap{GatewayURL: server.URL, AttemptID: grant.AttemptID, TerminationGraceSeconds: 1}, wire.Run{}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := store.Get(grant.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	transcript := make(map[string]bool)
	for _, record := range retained.Events {
		var event wire.ProviderEvent
		if err := json.Unmarshal(record.Body, &event); err != nil {
			t.Fatal(err)
		}
		if event.Message != nil {
			for _, change := range event.Message.Input["changes"].([]any) {
				transcript[change.(map[string]any)["content"].(string)] = true
			}
		}
	}
	for i := range cap(messages) {
		if !transcript[fmt.Sprintf("user edit %d", i)] {
			t.Fatal("completion lost a buffered user edit from the durable transcript")
		}
	}
	journal, err := os.ReadFile(filepath.Join(options.Directory, "journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(journal, []byte(secret)) {
		t.Fatal("nested provider credential reached durable journal")
	}
}

func TestProviderCancellationRequiresObservedTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	messages := make(chan agent.Message)
	close(messages)
	// The provider has stopped emitting events but has not proven its outcome.
	session := &agent.Session{Messages: messages, Result: make(chan agent.Result)}
	if _, err := drainProvider(ctx, cancel, session, http.DefaultClient, wire.Bootstrap{TerminationGraceSeconds: 1}, wire.Run{}, time.Now()); err == nil {
		t.Fatal("cancellation invented terminal authority for an unproven provider")
	}
}

func TestPiTierKeepsEnvironmentCredentialInMemory(t *testing.T) {
	raw, err := os.ReadFile(runtimeimage.DescriptorPath)
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("requires the installed complete runtime model catalog")
	}
	if err != nil {
		t.Fatal(err)
	}
	var descriptor runtimeimage.Descriptor
	if err := json.Unmarshal(raw, &descriptor); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	extension := ".pi/agent/npm/node_modules/pi-openai-service-tier/index.ts"
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(extension)), 0700); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(wire.Home, extension))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, extension), raw, 0600); err != nil {
		t.Fatal(err)
	}
	home, err := os.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer home.Close()
	secret := "sk-" + uuid.NewString()
	environment := map[string]string{"OPENAI_API_KEY": secret}
	run := wire.Run{Options: agent.ExecOptions{Model: "openai/gpt-5.4", ServiceTier: "priority"}}
	if err := configurePiServiceTier(home, descriptor, &run, environment); err != nil {
		t.Fatal(err)
	}
	raw, err = home.ReadFile(".pi/agent/models.json")
	if err != nil || bytes.Contains(raw, []byte(secret)) {
		t.Fatal("worker credential was copied into native model files", err)
	}
	for _, reference := range []string{"$LOCAL_PROVIDER_KEY", "${LOCAL_PROVIDER_KEY}"} {
		auth, _ := json.Marshal(map[string]any{"openai": map[string]string{"type": "api_key", "key": reference}})
		if err := os.WriteFile(filepath.Join(root, ".pi/agent/auth.json"), auth, 0600); err != nil {
			t.Fatal(err)
		}
		values := map[string]string{"LOCAL_PROVIDER_KEY": secret}
		selected := wire.Run{Options: agent.ExecOptions{Model: "openai/gpt-5.4", ServiceTier: "priority"}}
		if err := configurePiServiceTier(home, descriptor, &selected, values); err != nil {
			t.Fatal("native auth reference could not configure the requested tier", err)
		}
		if values["MULTICA_PI_OPENAI_API_KEY"] != secret {
			t.Fatal("native auth reference changed its credential")
		}
		for _, name := range []string{".pi/agent/models.json", ".pi/agent/auth.json"} {
			content, err := home.ReadFile(name)
			if err != nil || bytes.Contains(content, []byte(secret)) {
				t.Fatal("resolved native auth credential was persisted", name, err)
			}
		}
	}
	if err := home.Remove(".pi/agent/auth.json"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "$MISSING_KEY", "!touch " + filepath.Join(root, "executed")} {
		raw, _ := json.Marshal(map[string]any{"providers": map[string]any{"openai": map[string]any{"apiKey": key}}})
		if err := os.WriteFile(filepath.Join(root, ".pi/agent/models.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		run.Options.Model = "openai/gpt-5.4"
		if err := configurePiServiceTier(home, descriptor, &run, environment); err == nil {
			t.Fatal("unresolved credential authorized native provider configuration")
		}
		retained, err := home.ReadFile(".pi/agent/models.json")
		if err != nil || !bytes.Equal(retained, raw) {
			t.Fatal("rejected credential changed existing native configuration", err)
		}
		if strings.HasPrefix(key, "!") {
			if _, err := home.Stat("executed"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("credential configuration executed a command", err)
			}
		}
	}
}

func startedEventGrant(t *testing.T, store *workspace.Store) workspace.TaskGrant {
	t.Helper()
	g := preparedEventGrant(t, store)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Admit(g.AttemptID, pub); err != nil {
		t.Fatal(err)
	}
	if _, offered, err := store.Offer(g.AttemptID); err != nil || !offered {
		t.Fatal("fixture task was not offered", err)
	}
	if err := store.BeginStart(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkStarted(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	return g
}

func preparedEventGrant(t *testing.T, store *workspace.Store) workspace.TaskGrant {
	t.Helper()
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check(store.BindWorkspace("workspace-new", uuid.NewString(), "127.0.0.1"))
	sha := core.Digest([]byte("fixture executable"))
	bundle := configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)}
	configurationDigest := configuration.ExecutionDigest(bundle, nil)
	executable := runtimeimage.Executable{Path: "/opt/tools/provider", Version: "1.0.0", SHA256: sha}
	ref := runtimeimage.Ref{Image: "example.invalid/runtime@sha256:" + sha, Platform: "linux/arm64", ImageBuildID: uuid.NewString(), DescriptorDigest: sha, ConfigurationDigest: configurationDigest,
		Controller: core.Contract{BuildID: sha, Platform: "linux/arm64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.6"},
		Daemon:     runtimeimage.Daemon{Executable: executable, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"codex": executable}}
	g, err := store.Create(workspace.TaskGrant{TaskID: uuid.NewString(), RuntimeID: uuid.NewString(), WorkspaceID: uuid.NewString(), AgentID: uuid.NewString(), Envelope: json.RawMessage(`{}`), RuntimeRef: ref})
	check(err)
	check(store.BeginPreparation(g.AttemptID, workspace.PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://fixture"}))
	p := workspace.Prepared{OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID, Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, Provider: "codex", Executable: executable.Path, RuntimeDigest: g.Fingerprint, ConfigurationDigest: configurationDigest, CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{}, Environment: workspace.NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}}
	raw, err := json.Marshal(p)
	check(err)
	p.Digest = core.Digest(raw)
	check(store.CompletePreparation(g.AttemptID, &p, json.RawMessage(`{}`)))
	check(store.BindPod(g.AttemptID, g.PodName, uuid.NewString(), "node-a"))
	g, err = store.Get(g.AttemptID)
	check(err)
	return g
}
