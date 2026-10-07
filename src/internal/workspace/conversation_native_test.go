package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func TestInstalledCodexAliasesCanReusePreparedWorkspace(t *testing.T) {
	if runtime.GOOS != "linux" || os.Getenv("MULTICA_NATIVE_CODEX_REUSE") != "1" || os.Getenv("MULTICA_CONVERSATION_FS_TEST") != "1" {
		t.Skip("requires an installed runtime and an owned Linux /workspace fixture")
	}
	d, _, err := runtimeimage.ReadInstalled(runtimeimage.Root, core.Root, core.HostPlatform())
	if err != nil {
		t.Fatal(err)
	}
	helper, err := runtimeimage.CodexHelperExecutable(d)
	if err != nil {
		t.Fatal(err)
	}
	input := preparationFixture(t, "codex")
	input.Executable, input.CodexVersion = d.Providers["codex"].Path, d.Providers["codex"].Version
	input.CodexHelper = helper
	input.WorkspacesRoot = filepath.Join("/workspace", "codex-reuse-"+uuid.NewString())
	if err := os.Mkdir(input.WorkspacesRoot, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(input.WorkspacesRoot) })
	input.TaskRoot, err = TaskRoot(input.WorkspacesRoot, input.WorkspaceID, input.TaskID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	input.Conversation = ConversationKey{OwnerID: input.OwnerID, WorkspaceID: input.WorkspaceID, Kind: ConversationIssue,
		SubjectID: uuid.NewString(), AgentID: input.AgentID}
	input.WorkspaceAnchorTaskID = input.TaskID
	prepared, err := PrepareTask(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(prepared.Environment.WorkDir, "retained-work.txt")
	if err := os.WriteFile(work, []byte("preserve checkout edits"), 0600); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	header, _ := json.Marshal(map[string]any{"type": "session_meta", "payload": map[string]string{"id": id, "cwd": prepared.Environment.WorkDir}})
	session := filepath.Join(prepared.Environment.CodexHome, "sessions", "rollout-fixture-"+id+".jsonl")
	if err := os.WriteFile(session, append(header, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	var processes []*exec.Cmd
	for range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		t.Cleanup(cancel)
		command := exec.CommandContext(ctx, helper.Path, "app-server", "--listen", "stdio://", "--disable", "hooks", "--disable", "plugins", "--disable", "remote_plugin")
		command.WaitDelay = time.Second
		command.Env = append(slices.Clone(input.Environment), "CODEX_HOME="+prepared.Environment.CodexHome, "OPENAI_API_KEY=sk-local-fixture")
		command.Dir, command.Stderr = prepared.Environment.WorkDir, io.Discard
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdin, err := command.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		stdout, err := command.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		processes = append(processes, command)
		t.Cleanup(func() { _ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL); _ = command.Wait() })
		if _, err := io.WriteString(stdin, `{"id":1,"method":"initialize","params":{"clientInfo":{"name":"codex-reuse-proof","version":"1"}}}`+"\n"); err != nil {
			t.Fatal(err)
		}
		var response struct {
			ID    int
			Error json.RawMessage
		}
		if err := json.NewDecoder(stdout).Decode(&response); err != nil || response.ID != 1 || len(response.Error) != 0 {
			t.Fatal("native Codex metadata startup did not complete", err)
		}
	}
	for _, command := range processes {
		if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		_ = command.Wait()
	}
	aliases, err := os.ReadDir(filepath.Join(prepared.Environment.CodexHome, "tmp/arg0"))
	if err != nil || len(aliases) != 2 {
		t.Fatal("native startup did not leave the two observed helper groups", err)
	}
	if err := VerifyTaskTree(prepared.TaskRoot, prepared.AllowedLinks); err == nil || !strings.Contains(err.Error(), "untrusted symbolic link") {
		t.Fatal("baseline scanner did not reproduce the retained-helper rejection", err)
	} else {
		t.Logf("baseline retained-tree rejection: %s", err)
	}
	retry := retryPreparation(input, prepared)
	retry.TaskID = uuid.NewString()
	reused, err := PrepareTask(t.Context(), retry)
	if err != nil {
		t.Fatal("stopped native Codex helpers blocked the fresh-history follow-up", err)
	}
	if err := VerifyTaskTree(reused.TaskRoot, reused.AllowedLinks); err != nil {
		t.Fatal("helper cleanup weakened or did not satisfy retained-tree validation", err)
	}
	if err := ValidateSession(reused, id); err != nil {
		t.Fatal("helper cleanup lost the native session", err)
	}
	if raw, err := os.ReadFile(work); err != nil || string(raw) != "preserve checkout edits" {
		t.Fatal("helper cleanup lost checkout edits", err)
	}
	t.Logf("native=%s sha256=%s groups=%d currentTask=%s anchor=%s", helper.Path, helper.SHA256, len(aliases), reused.TaskID, reused.WorkspaceAnchorTaskID)
}

func nativeConversationFixture(t *testing.T) (Prepared, string) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	workspaces := filepath.Join(base, "workspaces")
	workspaceID, anchor, owner, agent := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	root, err := TaskRoot(workspaces, workspaceID, anchor, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{"workdir/.multica", "multica-config", "pi-sessions"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0700); err != nil {
			t.Fatal(err)
		}
	}
	key := ConversationKey{OwnerID: owner, WorkspaceID: workspaceID, Kind: ConversationIssue, SubjectID: uuid.NewString(), AgentID: agent}
	p := Prepared{Conversation: &key, WorkspaceAnchorTaskID: anchor, OwnerID: owner, WorkspaceID: workspaceID, TaskID: uuid.NewString(), AgentID: agent,
		AttemptID: uuid.NewString(), Generation: 1, PVCUID: uuid.NewString(), TaskRoot: root, Provider: "pi", Executable: "/opt/providers/pi",
		RuntimeDigest: digest([]byte("runtime")), ConfigurationDigest: digest([]byte("configuration")), CreatedAt: time.Now().UTC(),
		CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{}, Environment: NativeEnvironment{RootDir: root, WorkDir: root + "/workdir", MulticaConfigRoot: root + "/multica-config"}}
	p.Digest = preparedDigest(p)
	ownerRaw, _ := json.Marshal(map[string]string{"workspace_id": workspaceID, "task_id": anchor})
	contextRaw, _ := json.Marshal(map[string]string{"agent_id": agent, "managed_by": "multica-daemon-task"})
	if err := os.WriteFile(filepath.Join(root, ".task_owner"), ownerRaw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "workdir/.multica/daemon_task_context.json"), contextRaw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePrepared(p); err != nil {
		t.Fatal(err)
	}
	return p, workspaces
}

func TestConversationPreparationKeepsNativeAnchorAndCurrentTaskDistinct(t *testing.T) {
	p, workspaces := nativeConversationFixture(t)
	base, err := os.OpenRoot(workspaces)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	input := Preparation{WorkspaceID: p.WorkspaceID, TaskID: p.WorkspaceAnchorTaskID, WorkspaceAnchorTaskID: p.WorkspaceAnchorTaskID, TaskRoot: p.TaskRoot, WorkspacesRoot: workspaces}
	if err := pinNativeRoot(base, input); err != nil {
		t.Fatal(err)
	}
	input.TaskID, input.IsReuse = p.TaskID, true
	if err := pinNativeRoot(base, input); err != nil {
		t.Fatal("new task lost the anchor's native index", err)
	}
	entries, err := os.ReadDir(filepath.Join(workspaces, ".task_roots"))
	if err != nil || len(entries) != 1 {
		t.Fatal("follow-up created another native owner index", err)
	}
	root, err := os.OpenRoot(p.TaskRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := verifyNativeFiles(root, p); err != nil {
		t.Fatal("anchor owner rejected current task", err)
	}
	wrong := p
	wrong.WorkspaceAnchorTaskID = p.TaskID
	wrong.Digest = preparedDigest(wrong)
	if err := ValidatePrepared(wrong); err == nil {
		t.Fatal("current task identity replaced the frozen root owner")
	}
	replaced, _ := json.Marshal(map[string]string{"workspace_id": p.WorkspaceID, "task_id": p.TaskID})
	if err := os.WriteFile(filepath.Join(p.TaskRoot, ".task_owner"), replaced, 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyNativeFiles(root, p); err == nil {
		t.Fatal("rewritten native owner was accepted")
	}
}

func TestConversationNamedRerunValidatesHistoricalNativeSessionAfterDifferentLatestSession(t *testing.T) {
	p, _ := nativeConversationFixture(t)
	oldSession, newSession := filepath.Join(p.TaskRoot, "pi-sessions/old.jsonl"), filepath.Join(p.TaskRoot, "pi-sessions/new.jsonl")
	for _, path := range []string{oldSession, newSession} {
		header, _ := json.Marshal(map[string]string{"type": "session", "id": uuid.NewString(), "cwd": p.Environment.WorkDir})
		if err := os.WriteFile(path, append(header, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	storageID := uuid.NewString()
	oldSource := CheckpointSource{TaskID: p.WorkspaceAnchorTaskID, AttemptID: uuid.NewString()}
	latest := CheckpointSource{TaskID: p.TaskID, AttemptID: p.AttemptID}
	compatibility := digest([]byte("unchanged authority and provider"))
	storage := Storage{ID: storageID, Conversation: *p.Conversation, TaskRoot: p.TaskRoot, Prepared: &p, CompatibilityDigest: compatibility, WorkspaceCompatibilityDigest: compatibility, LatestWriter: latest,
		Checkpoint: &SessionCheckpoint{Source: latest, SessionSource: latest, WorkspaceSource: latest, SessionID: newSession, WorkDir: p.Environment.WorkDir}}
	// These records are the validated journal boundary. This check owns the
	// additional native-file proof needed to select A after B produced another ID.
	st := registry{Storages: map[string]Storage{storageID: storage}, Grants: map[string]TaskGrant{}, Terminals: map[string]Terminal{}}
	for source, sessionID := range map[CheckpointSource]string{oldSource: oldSession, latest: newSession} {
		st.Grants[source.AttemptID] = TaskGrant{TaskID: source.TaskID, AttemptID: source.AttemptID, WorkerSessionID: uuid.NewString(), StorageID: storageID, Conversation: *p.Conversation, TurnComplete: true,
			CompatibilityDigest: compatibility, Selection: &Selection{ReuseEligible: true, WorkspaceReuseEligible: true},
			CompletionWitness: &CompletionWitness{SessionID: sessionID, WorkDir: p.Environment.WorkDir}}
		st.Terminals[source.AttemptID] = Terminal{Kind: "complete", State: "delivered"}
	}
	selection := Selection{Conversation: *p.Conversation, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionResume, StorageID: storageID, SessionSource: oldSource,
		WorkspaceSource: oldSource, LatestWriter: latest, SessionID: oldSession, WorkDir: p.Environment.WorkDir}
	if err := selectedSourceWitnesses(&st, selection, storage, compatibility); err != nil {
		t.Fatal("valid historical native session was replaced by latest-session inference", err)
	}
	if err := ValidateSession(p, selection.SessionID); err != nil {
		t.Fatal("valid historical native session was unavailable", err)
	}
	if err := os.Remove(oldSession); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSession(p, selection.SessionID); err == nil {
		t.Fatal("missing historical session gained resume authority", err)
	}
	header, _ := json.Marshal(map[string]string{"type": "session", "id": uuid.NewString(), "cwd": p.Environment.WorkDir})
	if err := os.WriteFile(oldSession, append(header, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	storage.RetiredSessions = []string{oldSession}
	st.Storages[storageID] = storage
	if err := selectedSourceWitnesses(&st, selection, storage, compatibility); !errors.Is(err, ErrConflict) {
		t.Fatal("restored file resurrected a durably retired source", err)
	}
}

func TestConversationMissingRolloutExcludesOnlyTheObservedMissingIdentity(t *testing.T) {
	p, _ := nativeConversationFixture(t)
	oldSession, newSession := filepath.Join(p.TaskRoot, "pi-sessions/old.jsonl"), filepath.Join(p.TaskRoot, "pi-sessions/new.jsonl")
	storageID := uuid.NewString()
	st := registry{Storages: map[string]Storage{storageID: {ID: storageID, SessionID: oldSession, Checkpoint: &SessionCheckpoint{SessionID: oldSession}}}}
	g := TaskGrant{WorkerSessionID: uuid.NewString(), StorageID: storageID, TaskRoot: p.TaskRoot, ResumeSession: oldSession, Prepared: &p}
	if err := terminalResume(&st, &g, ResumePointers{WorkDir: p.Environment.WorkDir, SessionRolloutMissing: true, MissingSessionID: newSession}); err != nil {
		t.Fatal(err)
	}
	storage := st.Storages[storageID]
	if !slices.Contains(storage.RetiredSessions, newSession) || slices.Contains(storage.RetiredSessions, oldSession) || storage.Checkpoint.SessionID != oldSession {
		t.Fatal("a missing new rollout retired the separate older selected pointer")
	}
	if err := terminalResume(&st, &g, ResumePointers{WorkDir: p.Environment.WorkDir, SessionRolloutMissing: true, MissingSessionID: oldSession}); err != nil {
		t.Fatal(err)
	}
	storage = st.Storages[storageID]
	if !slices.Contains(storage.RetiredSessions, oldSession) || storage.Checkpoint.SessionID != "" {
		t.Fatal("exact missing prior rollout was not durably excluded")
	}
}

func TestConversationTransientFailureRetainsTheNativeProducingSource(t *testing.T) {
	p, _ := nativeConversationFixture(t)
	nativeID := filepath.Join(p.TaskRoot, "pi-sessions/original.jsonl")
	header, _ := json.Marshal(map[string]string{"type": "session", "id": uuid.NewString(), "cwd": p.Environment.WorkDir})
	if err := os.WriteFile(nativeID, append(header, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSession(p, nativeID); err != nil {
		t.Fatal(err)
	}
	storage := Storage{ID: uuid.NewString(), TaskRoot: p.TaskRoot, Prepared: &p}
	original := CheckpointSource{TaskID: p.WorkspaceAnchorTaskID, AttemptID: uuid.NewString()}
	latest := CheckpointSource{TaskID: p.TaskID, AttemptID: p.AttemptID}
	st := registry{Grants: map[string]TaskGrant{original.AttemptID: {TaskID: original.TaskID, AttemptID: original.AttemptID,
		WorkerSessionID: uuid.NewString(), StorageID: storage.ID, Conversation: *p.Conversation, TurnComplete: true,
		Selection:         &Selection{ReuseEligible: true, WorkspaceReuseEligible: true},
		CompletionWitness: &CompletionWitness{Status: "completed", SessionID: nativeID, WorkDir: p.Environment.WorkDir}}},
		Terminals: map[string]Terminal{original.AttemptID: {Kind: "complete", State: "delivered"}, latest.AttemptID: {Kind: "fail", State: "delivered"}}}
	g := TaskGrant{TaskID: latest.TaskID, AttemptID: latest.AttemptID, StorageID: storage.ID, TaskRoot: p.TaskRoot,
		WorkerSessionID: uuid.NewString(), TurnSequence: 2, Prepared: &p, ResumeSession: nativeID,
		Selection:         &Selection{Mode: SelectionResume, ReuseEligible: true, WorkspaceReuseEligible: true, SessionSource: original, WorkspaceSource: original, SessionID: nativeID},
		PendingResume:     &ResumePointers{WorkDir: p.Environment.WorkDir, ResumeRejectedTransient: true},
		CompletionWitness: &CompletionWitness{Status: "failed", WorkDir: p.Environment.WorkDir}}
	for _, late := range []bool{false, true} {
		checkpoint := completedCheckpoint(&st, g, storage, time.Now().UTC(), late)
		if checkpoint.Source != latest || checkpoint.SessionSource != original || checkpoint.SessionID != nativeID || checkpoint.WorkspaceSource != latest {
			t.Fatal("transient failure relabeled the session producer or lost independent workspace evidence")
		}
	}
	if err := os.Remove(nativeID); err != nil {
		t.Fatal(err)
	}
	if checkpoint := completedCheckpoint(&st, g, storage, time.Now().UTC(), true); checkpoint.SessionID != "" || checkpoint.SessionSource != (CheckpointSource{}) {
		t.Fatal("late transient settlement resurrected a missing native rollout")
	}
}
