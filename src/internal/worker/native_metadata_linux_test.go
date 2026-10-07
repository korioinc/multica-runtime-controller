//go:build linux

package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

// This prerequisite runs the installed helper and providers without model
// credentials. The driver pins the image, platform, and test binary.
func TestResidentInstalledHelperPrivateProjection(t *testing.T) {
	if os.Getenv("MULTICA_RESIDENT_HELPER_TEST") != "1" {
		t.Skip("run scripts/test-resident-helper.sh with a candidate runtime image")
	}
	raw, err := os.ReadFile(runtimeimage.DescriptorPath)
	if err != nil {
		t.Fatal(err)
	}
	var descriptor runtimeimage.Descriptor
	if err := json.Unmarshal(raw, &descriptor); err != nil {
		t.Fatal(err)
	}
	t.Logf("descriptor_sha256=%s controller_sha256=%s helper_sha256=%s", core.Digest(raw), descriptor.Controller.RuntimeSHA256, descriptor.Daemon.SHA256)
	for _, provider := range []string{"codex", "pi", "claude"} {
		t.Run(provider, func(t *testing.T) {
			input, task := residentHelperInput(t, descriptor, provider)
			baseline, err := captureManagedHome(wire.Home, configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)})
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := workspace.PrepareTask(t.Context(), input)
			if err != nil {
				t.Fatal("installed helper rejected scratch preparation", err)
			}
			assertResidentHelperPaths(t, prepared)
			history := writeScratchNativeHistory(t, prepared)
			if err := workspace.ValidateSession(prepared, history.session); err != nil {
				t.Fatal("offline native history is not eligible", err)
			}
			realRoot := prepared.TaskRoot
			if err := os.MkdirAll(filepath.Join(realRoot, "workdir"), 0700); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(realRoot) })
			// Project resources and the guard marker are fixed projections too.
			first := materializeResidentHelper(t, prepared, baseline)
			commands := discoverResidentSkills(t, descriptor.Providers[provider].Path, provider, first, realRoot)
			assertResidentSkill(t, commands, "bound-skill", "first bound", true)
			assertResidentSkill(t, commands, "removed-skill", "removed bound", true)
			assertResidentSkill(t, commands, "operator-skill", "operator default", true)
			if provider != "pi" {
				assertResidentSkill(t, commands, "disabled-skill", "", false)
			}
			if provider == "codex" {
				assertSDKCodexPrivateWrite(t, descriptor, first, realRoot)
				if bytes.Contains(prepared.NativeMetadata.CodexConfig, []byte("synthetic-private-token")) {
					t.Fatal("SDK configuration changed the bound private input")
				}
			} else if provider == "pi" {
				assertResidentPiDisableRejected(t, input, descriptor)
			}
			// Only the scratch tree receives the helper's path-based refresh.
			task["AgentSkills"] = []map[string]any{residentBoundSkill("bound-skill", "current bound"), residentBoundSkill("added-skill", "added bound")}
			task["ProjectResources"] = nil
			task["ProjectID"], task["ProjectTitle"] = "", ""
			input.Task, _ = json.Marshal(task)
			input.IsReuse, input.Prior = true, &prepared
			input.Generation++
			input.AttemptID = uuid.NewString()
			input.TaskID = uuid.NewString()
			input.TurnSequence++
			input.LiveReuse = true
			refreshed, err := workspace.PrepareTask(t.Context(), input)
			if err != nil {
				t.Fatal("installed helper rejected scratch refresh", err)
			}
			assertResidentHelperPaths(t, refreshed)
			if current, err := os.ReadFile(history.path); err != nil || !bytes.Equal(current, history.raw) {
				t.Fatal("scratch refresh changed native history", err)
			}
			if err := workspace.ValidateSession(refreshed, history.session); err != nil {
				t.Fatal("scratch refresh lost compatible native resume", err)
			}
			if provider == "codex" {
				if raw, err := os.ReadFile(filepath.Join(refreshed.TaskRoot, "codex-home/opaque-native.db")); err != nil || string(raw) != "opaque native state" {
					t.Fatal("scratch refresh lost opaque native state", err)
				}
			}
			second := materializeResidentHelper(t, refreshed, baseline)
			commands = discoverResidentSkills(t, descriptor.Providers[provider].Path, provider, second, realRoot)
			assertResidentSkill(t, commands, "bound-skill", "current bound", true)
			assertResidentSkill(t, commands, "added-skill", "added bound", true)
			assertResidentSkill(t, commands, "removed-skill", "", false)
			assertResidentSkill(t, commands, "operator-skill", "operator default", true)
			if provider != "pi" {
				assertResidentSkill(t, commands, "disabled-skill", "", false)
			}
			if _, err := os.ReadFile(filepath.Join(realRoot, "workdir/.multica/project/resources.json")); !os.IsNotExist(err) {
				t.Fatal("an omitted current resource exposed the preceding turn", err)
			}
			// Inspect native precedence and preserve user project bytes.
			projectSkills := map[string]string{"codex": ".agents/skills", "pi": ".pi/skills", "claude": ".claude/skills"}[provider]
			projectPath := filepath.Join(realRoot, "workdir", projectSkills, "bound-skill/SKILL.md")
			projectBody := []byte(residentSkillBody("bound-skill", "user project"))
			residentWrite(t, projectPath, projectBody)
			residentWrite(t, filepath.Join(realRoot, "workdir", projectSkills, "user-project-skill/SKILL.md"), []byte(residentSkillBody("user-project-skill", "user project")))
			commands = discoverResidentSkills(t, descriptor.Providers[provider].Path, provider, second, realRoot)
			if provider == "pi" {
				assertResidentSkill(t, commands, "user-project-skill", "", false)
				// Inspect Pi's supported trust rule; production keeps native trust.
				second["HELPER_PI_APPROVE"] = "1"
				commands = discoverResidentSkills(t, descriptor.Providers[provider].Path, provider, second, realRoot)
			}
			assertResidentSkill(t, commands, "user-project-skill", "user project", true)
			for _, skill := range commands {
				if skill.Name == "bound-skill" {
					t.Logf("native precedence: %s", skill.Description)
				}
			}
			if raw, err := os.ReadFile(projectPath); err != nil || !bytes.Equal(raw, projectBody) {
				t.Fatal("private selection changed a user-owned project skill", err)
			}
			home, err := os.OpenRoot(wire.Home)
			if err != nil {
				t.Fatal(err)
			}
			err = clearNativeTurnMetadata()
			home.Close()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.ReadFile(filepath.Join(realRoot, "workdir/.multica/daemon_task_context.json")); err != nil {
				t.Fatal("clearing private turn state removed the session guard", err)
			}
			assertResidentIdleGuardRejectsAmbientToken(t, descriptor.Daemon.Path, realRoot)
		})
	}
}

func assertResidentIdleGuardRejectsAmbientToken(t *testing.T, binary, taskRoot string) {
	t.Helper()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusUnauthorized) }))
	defer server.Close()
	config, _ := json.Marshal(map[string]string{"server_url": server.URL, "workspace_id": uuid.NewString(), "token": "synthetic-ambient-pat"})
	path := filepath.Join(wire.Home, ".multica/config.json")
	residentWrite(t, path, config)
	defer os.Remove(path)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "issue", "get", uuid.NewString())
	command.Dir = t.TempDir()
	command.Env = []string{"HOME=" + wire.Home, "PATH=" + os.Getenv("PATH")}
	if output, err := command.CombinedOutput(); calls.Load() != 1 {
		t.Fatalf("ambient-token calibration did not exercise the local API: %v %s", err, output)
	}
	command = exec.CommandContext(ctx, binary, "issue", "get", uuid.NewString())
	command.Dir = filepath.Join(taskRoot, "workdir")
	command.Env = []string{"HOME=" + wire.Home, "PATH=" + os.Getenv("PATH")}
	_, err := command.CombinedOutput()
	if err == nil || calls.Load() != 1 {
		t.Fatal("idle native guard permitted ambient token fallback")
	}
}

func TestResidentInstalledHelperRacesAndCollisions(t *testing.T) {
	if os.Getenv("MULTICA_RESIDENT_HELPER_TEST") != "1" {
		t.Skip("run scripts/test-resident-helper.sh with a candidate runtime image")
	}
	raw, err := os.ReadFile(runtimeimage.DescriptorPath)
	if err != nil {
		t.Fatal(err)
	}
	var descriptor runtimeimage.Descriptor
	if err := json.Unmarshal(raw, &descriptor); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"ordinary-renames", "artifact-link", "artifact-collision", "artifact-parent-link", "marker-collision", "optional-resource-collision", "optional-parent-link", "materialization-mutation"} {
		t.Run(scenario, func(t *testing.T) {
			input, _ := residentHelperInput(t, descriptor, "pi")
			baseline, err := captureManagedHome(wire.Home, configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)})
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := workspace.PrepareTask(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(prepared.TaskRoot) })
			materializeResidentHelper(t, prepared, baseline)
			if err := clearNativeTurnMetadata(); err != nil {
				t.Fatal(err)
			}
			if err := baseline.restore(); err != nil {
				t.Fatal(err)
			}
			other := filepath.Join(wire.WorkspaceRoot, "other-"+uuid.NewString())
			residentWrite(t, filepath.Join(other, "sentinel"), []byte("another task remains intact"))
			t.Cleanup(func() { _ = os.RemoveAll(other) })
			input.IsReuse, input.Prior, input.LiveReuse = true, &prepared, true
			input.TaskID, input.AttemptID = uuid.NewString(), uuid.NewString()
			input.Generation++
			input.TurnSequence++
			nextRoot := workspace.NativeArtifactRoot(input.TaskRoot, input.WorkerSessionID, input.TurnSequence)
			var preservedPath string
			preserved := []byte("user-owned data remains intact")
			var stopRenames chan struct{}
			var renamesDone chan struct{}
			var renameCount atomic.Int64
			switch scenario {
			case "artifact-link":
				if err := os.Symlink(other, nextRoot); err != nil {
					t.Fatal(err)
				}
			case "artifact-collision":
				preservedPath = filepath.Join(nextRoot, "user-file")
				residentWrite(t, preservedPath, preserved)
			case "artifact-parent-link":
				parent := filepath.Join(input.TaskRoot, workspace.RuntimeArtifactsDir)
				if err := os.Rename(parent, parent+"-retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, parent); err != nil {
					t.Fatal(err)
				}
			case "marker-collision", "optional-resource-collision":
				path := workspace.TaskMarkerRelative
				if scenario == "optional-resource-collision" {
					path = workspace.TaskResourcesRelative
				}
				preservedPath = filepath.Join(input.TaskRoot, path)
				if err := os.Remove(preservedPath); err != nil {
					t.Fatal(err)
				}
				residentWrite(t, preservedPath, preserved)
			case "optional-parent-link":
				parent := filepath.Join(input.TaskRoot, "workdir/.multica/project")
				moved := filepath.Join(filepath.Dir(parent), "user-project")
				if err := os.Rename(parent, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("user-project", parent); err != nil {
					t.Fatal(err)
				}
				preservedPath = filepath.Join(moved, "user-file")
				residentWrite(t, preservedPath, preserved)
			case "ordinary-renames":
				first := filepath.Join(input.TaskRoot, "workdir/user-file")
				second := first + "-moving"
				residentWrite(t, first, preserved)
				stopRenames, renamesDone = make(chan struct{}), make(chan struct{})
				go func() {
					defer close(renamesDone)
					for {
						select {
						case <-stopRenames:
							return
						default:
						}
						if os.Rename(first, second) == nil {
							renameCount.Add(1)
						}
						if os.Rename(second, first) == nil {
							renameCount.Add(1)
						}
					}
				}()
				preservedPath = first
			}
			refreshed, prepareErr := workspace.PrepareTask(t.Context(), input)
			if stopRenames != nil {
				close(stopRenames)
				<-renamesDone
				if renameCount.Load() == 0 {
					t.Fatal("ordinary rename race was not exercised")
				}
			}
			allowed := scenario == "ordinary-renames" || scenario == "optional-resource-collision" || scenario == "optional-parent-link" || scenario == "materialization-mutation"
			if allowed && prepareErr != nil {
				t.Fatal("safe warm preparation was refused", prepareErr)
			}
			if !allowed && prepareErr == nil {
				t.Fatal("unowned required publication was admitted")
			}
			if preservedPath != "" {
				current, err := os.ReadFile(preservedPath)
				if scenario == "ordinary-renames" && os.IsNotExist(err) {
					current, err = os.ReadFile(preservedPath + "-moving")
				}
				if err != nil || !bytes.Equal(current, preserved) {
					t.Fatal("preparation changed user content", err)
				}
			}
			if current, err := os.ReadFile(filepath.Join(other, "sentinel")); err != nil || string(current) != "another task remains intact" {
				t.Fatal("preparation changed another task", err)
			}
			if scenario == "optional-resource-collision" || scenario == "optional-parent-link" {
				if refreshed.AllowedLinks[workspace.TaskResourcesRelative] != "" || len(refreshed.NativeMetadata.ProjectResources) != 0 {
					t.Fatal("unowned optional resources acquired runtime authority")
				}
				materializeResidentHelper(t, refreshed, baseline)
				if _, err := os.ReadFile(workspace.NativeMetadataView + "/resources.json"); !os.IsNotExist(err) {
					t.Fatal("omitted current resources retained earlier private bytes", err)
				}
			}
			if scenario == "materialization-mutation" {
				artifact := refreshed.NativeMetadata.Artifacts[0]
				path := filepath.Join(refreshed.NativeMetadata.ArtifactRoot, artifact.Path)
				changed := bytes.Repeat([]byte("x"), int(artifact.Size))
				if err := os.WriteFile(path, changed, 0600); err != nil {
					t.Fatal(err)
				}
				home, err := os.OpenRoot(wire.Home)
				if err != nil {
					t.Fatal(err)
				}
				b := wire.Bootstrap{TaskRoot: refreshed.TaskRoot, Provider: refreshed.Provider, WorkerSessionID: input.WorkerSessionID, TurnSequence: input.TurnSequence, AllowedLinks: refreshed.AllowedLinks, NativeMetadataDigest: refreshed.NativeMetadata.Digest()}
				err = materializeNativeMetadata(t.Context(), b, wire.Run{NativeMetadata: refreshed.NativeMetadata}, home)
				home.Close()
				if err == nil {
					t.Fatal("provider metadata admitted bytes changed after publication")
				}
				if _, err := os.Lstat(workspace.NativeMetadataView); !os.IsNotExist(err) {
					t.Fatal("failed copy activated a private metadata view", err)
				}
			}
			if err := clearNativeTurnMetadata(); err != nil {
				t.Fatal(err)
			}
			if err := baseline.restore(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func residentHelperInput(t *testing.T, d runtimeimage.Descriptor, provider string) (workspace.Preparation, map[string]any) {
	t.Helper()
	base := t.TempDir()
	home, scratch := wire.Home, filepath.Join(base, "scratch")
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	providerHome := map[string]string{"codex": ".codex", "pi": ".pi/agent", "claude": ".claude"}[provider]
	for _, skill := range []string{"operator-skill", "disabled-skill", "bound-skill"} {
		residentWrite(t, filepath.Join(home, providerHome, "skills", skill, "SKILL.md"), []byte(residentSkillBody(skill, "operator default")))
	}
	if provider == "codex" {
		residentWrite(t, filepath.Join(home, ".codex/config.toml"), []byte("model = \"offline-fixture\"\n"))
	}
	if provider == "claude" {
		residentWrite(t, filepath.Join(home, ".claude/settings.json"), []byte(`{"permissions":{"deny":["Bash(operator-deny)"]}}`))
	}
	input := workspace.Preparation{OwnerID: uuid.NewString(), WorkspaceID: uuid.NewString(), TaskID: uuid.NewString(), AgentID: uuid.NewString(),
		AttemptID: uuid.NewString(), Generation: 1, PVCUID: uuid.NewString(), Provider: provider, Executable: d.Providers[provider].Path,
		RuntimeDigest: core.Digest([]byte("runtime")), ConfigurationDigest: core.Digest([]byte("configuration")), Command: d.Daemon.Path,
		WorkspacesRoot: wire.WorkspaceRoot, ScratchRoot: scratch, WorkerSessionID: uuid.NewString(), TurnSequence: 1, CodexVersion: d.Providers["codex"].Version,
		Environment: []string{"HOME=" + home, "PATH=" + os.Getenv("PATH")}}
	var err error
	input.TaskRoot, err = workspace.TaskRoot(wire.WorkspaceRoot, input.WorkspaceID, input.TaskID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	task := map[string]any{"AgentID": input.AgentID, "IssueID": uuid.NewString(), "ChatSessionID": uuid.NewString(),
		"ProjectID": uuid.NewString(), "ProjectTitle": "helper fixture", "ProjectResources": []map[string]any{{"id": uuid.NewString(), "resource_type": "github_repo", "resource_ref": map[string]string{"url": "https://github.com/example/offline"}}},
		"AgentSkills": []map[string]any{residentBoundSkill("bound-skill", "first bound"), residentBoundSkill("removed-skill", "removed bound")}}
	if provider != "pi" {
		task["DisabledRuntimeSkills"] = []map[string]string{{"Root": "provider", "Key": "disabled-skill", "Name": "disabled-skill"}}
	}
	input.WorkspaceAnchorTaskID = input.TaskID
	input.Conversation = workspace.ConversationKey{OwnerID: input.OwnerID, WorkspaceID: input.WorkspaceID, AgentID: input.AgentID, Kind: workspace.ConversationIssue, SubjectID: task["IssueID"].(string)}
	input.Task, _ = json.Marshal(task)
	return input, task
}

func residentBoundSkill(name, description string) map[string]any {
	return map[string]any{"Name": name, "Content": residentSkillBody(name, description), "Files": []map[string]string{{"Path": "support.txt", "Content": "opaque /tmp/scratch-literal must remain unchanged"}}}
}

func residentSkillBody(name, description string) string {
	return "---\nname: " + name + "\ndescription: " + description + "\n---\nBound instructions.\n"
}

func assertResidentHelperPaths(t *testing.T, p workspace.Prepared) {
	t.Helper()
	if p.Environment.RootDir != p.TaskRoot || p.Environment.WorkDir != filepath.Join(p.TaskRoot, "workdir") || p.Environment.MulticaConfigRoot != filepath.Join(p.TaskRoot, "multica-config") {
		t.Fatal("helper environment escaped scratch")
	}
	var manifest struct{ Files, Dirs []string }
	if err := json.Unmarshal(p.CleanupManifest, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, path := range append(manifest.Files, manifest.Dirs...) {
		relative, err := filepath.Rel(p.TaskRoot, path)
		if err != nil || !fs.ValidPath(relative) || filepath.IsAbs(relative) {
			t.Fatal("helper cleanup escaped its private scratch root", path)
		}
	}
}

type residentHistory struct {
	path, session string
	raw           []byte
}

func writeScratchNativeHistory(t *testing.T, p workspace.Prepared) residentHistory {
	t.Helper()
	id := uuid.NewString()
	h := residentHistory{session: id}
	var record any
	switch p.Provider {
	case "codex":
		h.path = filepath.Join(p.TaskRoot, "codex-home/sessions", "rollout-fixture-"+id+".jsonl")
		record = map[string]any{"type": "session_meta", "payload": map[string]string{"id": id, "cwd": p.Environment.WorkDir}}
		residentWrite(t, filepath.Join(p.TaskRoot, "codex-home/opaque-native.db"), []byte("opaque native state"))
	case "pi":
		h.path = filepath.Join(p.TaskRoot, "pi-sessions", id+".jsonl")
		h.session = h.path
		record = map[string]string{"type": "session", "id": id, "cwd": p.Environment.WorkDir}
	case "claude":
		h.path = filepath.Join(p.TaskRoot, workspace.ClaudeSessionsDir, workspace.ClaudeProjectDir, id+".jsonl")
		record = map[string]any{"type": "user", "sessionId": id, "cwd": p.Environment.WorkDir, "message": map[string]string{"role": "user", "content": "Offline native history."}}
	}
	h.raw, _ = json.Marshal(record)
	h.raw = append(h.raw, '\n')
	residentWrite(t, h.path, h.raw)
	return h
}

func materializeResidentHelper(t *testing.T, p workspace.Prepared, baseline *managedHome) map[string]string {
	if err := baseline.restore(); err != nil {
		t.Fatal(err)
	}
	t.Helper()
	home, err := os.OpenRoot(wire.Home)
	if err != nil {
		t.Fatal(err)
	}
	defer home.Close()
	b := wire.Bootstrap{TaskRoot: p.TaskRoot, Provider: p.Provider, WorkerSessionID: p.NativeMetadata.WorkerSessionID, TurnSequence: p.NativeMetadata.TurnSequence, AllowedLinks: p.AllowedLinks, NativeMetadataDigest: p.NativeMetadata.Digest()}
	run := wire.Run{NativeMetadata: p.NativeMetadata}
	if err := materializeNativeMetadata(t.Context(), b, run, home); err != nil {
		t.Fatal("actual private materialization failed", err)
	}
	env := map[string]string{"HOME": wire.Home, "PATH": os.Getenv("PATH"), "DISABLE_AUTOUPDATER": "1", "PI_TELEMETRY": "0"}
	if p.Provider == "codex" {
		env["CODEX_HOME"] = p.Environment.CodexHome
	}
	if p.Provider == "claude" {
		env["CLAUDE_CONFIG_DIR"] = filepath.Join(wire.Home, ".claude")
		env["HELPER_SETTINGS"] = p.Environment.ClaudeSettingsPath
	}
	encoded, err := json.Marshal(wire.TurnAssignment{Bootstrap: b, Run: run})
	if err != nil || len(encoded) > wire.MaxAssignmentBytes {
		t.Fatal("actual private metadata exceeds assignment budget", err)
	}
	t.Logf("actual_private_assignment_bytes=%d limit=%d", len(encoded), wire.MaxAssignmentBytes)
	return env
}

func assertResidentPiDisableRejected(t *testing.T, input workspace.Preparation, d runtimeimage.Descriptor) {
	t.Helper()
	runtimeID := uuid.NewString()
	raw, _ := json.Marshal(map[string]any{"id": input.TaskID, "agent_id": input.AgentID, "workspace_id": input.WorkspaceID, "runtime_id": runtimeID, "issue_id": uuid.NewString(), "auth_token": "mat_fixture",
		"agent": map[string]any{"id": input.AgentID, "disabled_runtime_skills": []map[string]string{{"runtime_id": runtimeID, "provider": "pi", "root": "provider", "key": "disabled-skill", "name": "disabled-skill"}}}})
	claim, err := daemonapi.ParseClaim(raw)
	if err != nil {
		t.Fatal(err)
	}
	metadata := wire.Bootstrap{TaskID: input.TaskID, AgentID: input.AgentID, WorkspaceID: input.WorkspaceID, RuntimeID: runtimeID}
	metadata.RuntimeRef.Providers = d.Providers
	root, err := workspace.TaskRoot(wire.WorkspaceRoot, input.WorkspaceID, input.TaskID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = daemonapi.ExecutionInput(claim, metadata, d.Providers["pi"], daemonapi.ExecutionSettings{Provider: "pi", TaskRoot: root})
	if err == nil || !strings.Contains(err.Error(), "cannot enforce") {
		t.Fatal("unsupported Pi disable policy was not rejected before preparation", err)
	}
}

func residentWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

type residentSkill struct {
	Name, Description, Path string
	Enabled                 *bool
}

func discoverResidentSkills(t *testing.T, binary, provider string, env map[string]string, realRoot string) []residentSkill {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var args []string
	var requests []map[string]any
	switch provider {
	case "codex":
		args = []string{"app-server"}
		requests = []map[string]any{{"id": "init", "method": "initialize", "params": map[string]any{"clientInfo": map[string]string{"name": "resident-helper-fixture", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}},
			{"method": "initialized"}, {"id": "skills", "method": "skills/list", "params": map[string]any{"cwds": []string{filepath.Join(realRoot, "workdir")}, "forceReload": true}}}
	case "pi":
		args = []string{"--mode", "rpc", "--no-session"}
		if env["HELPER_PI_APPROVE"] == "1" {
			args = append(args, "--approve")
		}
		requests = []map[string]any{{"id": "skills", "type": "get_commands"}}
	case "claude":
		args = []string{"--print", "--verbose", "--input-format", "stream-json", "--output-format", "stream-json", "--strict-mcp-config", "--settings", env["HELPER_SETTINGS"]}
		requests = []map[string]any{{"type": "control_request", "request_id": "skills", "request": map[string]any{"subtype": "initialize", "hooks": map[string]any{}}}}
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = filepath.Join(realRoot, "workdir")
	cmd.Env = wire.Environment(env)
	cmd.WaitDelay = time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdin.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	for _, request := range requests {
		if err := json.NewEncoder(stdin).Encode(request); err != nil {
			t.Fatal(err)
		}
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 4096), wire.MaxAssignmentBytes)
	for scanner.Scan() {
		var response struct {
			ID     string
			Type   string
			Result struct {
				Data []struct{ Skills []residentSkill }
			}
			Data     struct{ Commands []residentSkill }
			Response struct {
				RequestID string `json:"request_id"`
				Response  struct{ Commands []residentSkill }
			}
		}
		if json.Unmarshal(scanner.Bytes(), &response) != nil {
			continue
		}
		if provider == "codex" && response.ID == "skills" && len(response.Result.Data) == 1 {
			return response.Result.Data[0].Skills
		}
		if provider == "pi" && response.ID == "skills" {
			for i := range response.Data.Commands {
				response.Data.Commands[i].Name = strings.TrimPrefix(response.Data.Commands[i].Name, "skill:")
			}
			return response.Data.Commands
		}
		if provider == "claude" && response.Response.RequestID == "skills" {
			return response.Response.Response.Commands
		}
	}
	t.Fatal("installed provider did not report native skill discovery", provider, scanner.Err(), ctx.Err())
	return nil
}

func assertResidentSkill(t *testing.T, skills []residentSkill, name, description string, enabled bool) {
	t.Helper()
	for _, skill := range skills {
		if skill.Name == name && (description == "" || strings.Contains(skill.Description, description)) {
			actual := skill.Enabled == nil || *skill.Enabled
			if actual == enabled {
				return
			}
			t.Fatalf("native skill %s enabled=%v; want %v", name, actual, enabled)
		}
	}
	if enabled {
		t.Fatalf("native provider did not discover current %s (%s)", name, description)
	}
}

func assertSDKCodexPrivateWrite(t *testing.T, d runtimeimage.Descriptor, env map[string]string, realRoot string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	backend, err := agent.New("codex", agent.Config{ExecutablePath: d.Providers["codex"].Path, BuiltinRuntime: true, Env: env, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	session, err := backend.Execute(ctx, "offline configuration probe", agent.ExecOptions{Cwd: filepath.Join(realRoot, "workdir"), Timeout: 3 * time.Second,
		HandshakeTimeout: time.Second, ThreadHandshakeTimeout: time.Second, SemanticInactivityTimeout: time.Second,
		McpConfig: json.RawMessage(`{"mcpServers":{"fixture":{"command":"/usr/bin/true","env":{"FIXTURE_TOKEN":"synthetic-private-token"}}}}`)})
	if err == nil {
		for range session.Messages {
		}
		<-session.Result
	}
	path := workspace.NativeMetadataView + "/codex/config.toml"
	raw, readErr := os.ReadFile(path)
	info, statErr := os.Stat(path)
	if readErr != nil || statErr != nil || !bytes.Contains(raw, []byte("synthetic-private-token")) || info.Mode().Perm() != 0600 {
		t.Fatal("pinned SDK did not write and chmod the private Codex config projection", readErr, statErr)
	}
	if info, err := os.Lstat(filepath.Join(realRoot, "codex-home/config.toml")); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("SDK replaced the fixed native config projection", err)
	}
	t.Logf("SDK config projection: private mode=%s, model credentials absent", fmt.Sprint(info.Mode().Perm()))
}
