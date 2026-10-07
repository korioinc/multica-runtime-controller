package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func preparationFixture(t *testing.T, provider string) Preparation {
	t.Helper()
	binary := os.Getenv("MULTICA_PREPARATION_BINARY")
	if binary == "" {
		t.Skip("set MULTICA_PREPARATION_BINARY to the original pinned multica CLI")
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"workspace", "home"} {
		if err := os.Mkdir(filepath.Join(base, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	input := Preparation{OwnerID: uuid.NewString(), WorkspaceID: uuid.NewString(), TaskID: uuid.NewString(), AgentID: uuid.NewString(), AttemptID: uuid.NewString(), Generation: 1, PVCUID: uuid.NewString(), Provider: provider, Executable: "/opt/providers/" + provider, RuntimeDigest: digest([]byte("runtime")), ConfigurationDigest: digest([]byte("configuration")), Command: binary, WorkspacesRoot: filepath.Join(base, "workspace"), CodexVersion: "0.154.0", Environment: []string{"PATH=" + os.Getenv("PATH"), "HOME=" + filepath.Join(base, "home")}}
	input.TaskRoot, err = TaskRoot(input.WorkspacesRoot, input.WorkspaceID, input.TaskID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	input.Task, err = json.Marshal(map[string]any{"AgentID": input.AgentID, "AgentSkills": []map[string]string{{"Name": "fixture-skill", "Content": "# Skill\nfixture content"}}})
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func retryPreparation(input Preparation, prior Prepared) Preparation {
	input.IsReuse, input.Prior = true, &prior
	input.AttemptID, input.Generation = uuid.NewString(), input.Generation+1
	return input
}

func TestCodexHelperCleanupRejectsForgedGroupsBeforeAnyDeletion(t *testing.T) {
	for _, attack := range []string{"none", "target", "extra-file", "incomplete", "lock-hardlink", "lock-symlink", "tmp-link", "arg0-link", "group-link", "empty-group-link", "writable-directory", "empty-writable-directory", "wrong-helper-hash"} {
		t.Run(attack, func(t *testing.T) {
			path, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			helperPath, err := filepath.EvalSymlinks("/usr/bin/true")
			if err != nil {
				t.Fatal(err)
			}
			sha, err := core.HashFile(helperPath)
			if err != nil {
				t.Fatal(err)
			}
			helper := runtimeimage.Executable{Path: helperPath, SHA256: sha}
			base := filepath.Join(path, "codex-home/tmp/arg0")
			emptyGroup := filepath.Join(base, "codex-arg0MMMMMM")
			if err := os.MkdirAll(emptyGroup, 0700); err != nil {
				t.Fatal(err)
			}
			groups := []string{filepath.Join(base, "codex-arg0AAAAAA"), filepath.Join(base, "codex-arg0ZZZZZZ")}
			for _, group := range groups {
				if err := os.MkdirAll(group, 0700); err != nil {
					t.Fatal(err)
				}
				for _, name := range codexHelperNames {
					if name == ".lock" {
						err = os.WriteFile(filepath.Join(group, name), nil, 0600)
					} else {
						err = os.Symlink(helper.Path, filepath.Join(group, name))
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			victim := filepath.Join(path, "outside-user-file")
			if err := os.WriteFile(victim, []byte("preserve this file"), 0600); err != nil {
				t.Fatal(err)
			}
			switch attack {
			case "target":
				if err := os.Remove(filepath.Join(groups[1], "apply_patch")); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(victim, filepath.Join(groups[1], "apply_patch"))
			case "extra-file":
				err = os.WriteFile(filepath.Join(groups[1], "user-data"), []byte("do not delete"), 0600)
			case "incomplete":
				err = os.Remove(filepath.Join(groups[1], codexHelperNames[0]))
			case "lock-hardlink", "lock-symlink":
				if err := os.Remove(filepath.Join(groups[1], ".lock")); err != nil {
					t.Fatal(err)
				}
				if attack == "lock-hardlink" {
					err = os.Link(victim, filepath.Join(groups[1], ".lock"))
				} else {
					err = os.Symlink(victim, filepath.Join(groups[1], ".lock"))
				}
			case "tmp-link", "arg0-link", "group-link", "empty-group-link":
				target := map[string]string{"tmp-link": filepath.Dir(base), "arg0-link": base, "group-link": groups[1], "empty-group-link": emptyGroup}[attack]
				moved := target + "-retained"
				if err := os.Rename(target, moved); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(moved, target)
			case "writable-directory":
				err = os.Chmod(groups[1], 0777)
			case "empty-writable-directory":
				err = os.Chmod(emptyGroup, 0777)
			case "wrong-helper-hash":
				helper.SHA256 = strings.Repeat("0", 64)
			}
			if err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(path)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			err = cleanCodexHelpers(root, helper)
			if attack == "none" {
				if err != nil {
					t.Fatal("exact stopped native helper groups were rejected", err)
				}
				if err := VerifyTaskTree(path, nil); err != nil {
					t.Fatal("stopped native helper cleanup did not permit safe workspace reuse", err)
				}
			} else {
				if err == nil {
					t.Fatal("forged native helper group was accepted")
				}
				if _, err := os.Lstat(filepath.Join(groups[0], "apply_patch")); err != nil {
					t.Fatal("a valid group was deleted before the later forged group was rejected", err)
				}
			}
			if raw, err := os.ReadFile(victim); err != nil || string(raw) != "preserve this file" {
				t.Fatal("cleanup changed a file outside its validated groups", err)
			}
		})
	}
}

func TestPreparationRetainsNamedTaskDataAfterLabelChange(t *testing.T) {
	for _, provider := range []string{"codex", "pi"} {
		t.Run(provider, func(t *testing.T) {
			input := preparationFixture(t, provider)
			input.WorkspaceSlug = "kor-io"
			input.IssueIdentifier = "KOR-219"
			var err error
			input.TaskRoot, err = TaskRoot(input.WorkspacesRoot, input.WorkspaceID, input.TaskID, input.WorkspaceSlug, input.IssueIdentifier)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := PrepareTask(context.Background(), input)
			if err != nil {
				t.Fatal("native helper rejected the admitted named task", err)
			}
			work := filepath.Join(prepared.Environment.WorkDir, "user-work.txt")
			body := []byte("retained task work")
			if err := os.WriteFile(work, body, 0600); err != nil {
				t.Fatal(err)
			}
			retry := retryPreparation(input, prepared)
			retry.WorkspaceSlug = "renamed-workspace"
			retry.IssueIdentifier = "KOR-220"
			reused, err := PrepareTask(context.Background(), retry)
			if err != nil {
				t.Fatal("label change prevented task reuse", err)
			}
			if reused.Environment != prepared.Environment {
				t.Fatal("label change relocated the task environment")
			}
			actual, err := os.ReadFile(work)
			if err != nil || !bytes.Equal(actual, body) {
				t.Fatal("label change lost task work", err)
			}
			other := input
			other.TaskID = uuid.NewString()
			if _, err := PrepareTask(context.Background(), other); err == nil {
				t.Fatal("another task acquired the named workspace")
			}
			other = retry
			other.WorkspaceID = uuid.NewString()
			if _, err := PrepareTask(context.Background(), other); err == nil {
				t.Fatal("another workspace acquired the retained task data")
			}
		})
	}
}

func TestPreparationKeepsHomeSkillAuthorityOnReuse(t *testing.T) {
	input := preparationFixture(t, "codex")
	home := environmentValue(input.Environment, "HOME")
	skill := filepath.Join(home, ".codex", "skills", "operator-skill")
	if err := os.MkdirAll(skill, 0700); err != nil {
		t.Fatal(err)
	}
	body := []byte("# Operator skill\nAuthorized shared instructions\n")
	if err := os.WriteFile(filepath.Join(skill, "SKILL.md"), body, 0600); err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareTask(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(prepared.Environment.CodexHome, "skills", "operator-skill")
	raw, err := os.ReadFile(filepath.Join(link, "SKILL.md"))
	if err != nil || !bytes.Equal(raw, body) {
		t.Fatal("authorized operator skill is unavailable", err)
	}
	if _, err := PrepareTask(context.Background(), retryPreparation(input, prepared)); err != nil {
		t.Fatal("authorized operator skill prevented task reuse", err)
	}
	outside := t.TempDir()
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareTask(context.Background(), retryPreparation(input, prepared)); err == nil {
		t.Fatal("retargeted operator skill gained preparation authority")
	}
}

func TestPreparationRejectsIndirectHomeSkills(t *testing.T) {
	for _, path := range []string{".codex", ".codex/skills", ".codex/skills/operator-skill"} {
		t.Run(path, func(t *testing.T) {
			input := preparationFixture(t, "codex")
			home := environmentValue(input.Environment, "HOME")
			link := filepath.Join(home, path)
			if err := os.MkdirAll(filepath.Dir(link), 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(home, "indirect-source")
			if err := os.MkdirAll(filepath.Join(target, "skills"), 0700); err != nil {
				t.Fatal(err)
			}
			relative, err := filepath.Rel(filepath.Dir(link), target)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(relative, link); err != nil {
				t.Fatal(err)
			}
			if _, err := PrepareTask(context.Background(), input); err == nil {
				t.Fatal("indirect HOME skills gained preparation authority")
			}
		})
	}
}

func TestPreparationPreservesSameTaskFilesAndSessions(t *testing.T) {
	for _, provider := range []string{"codex", "pi", "claude"} {
		t.Run(provider, func(t *testing.T) {
			input := preparationFixture(t, provider)
			prepared, err := PrepareTask(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			work := filepath.Join(prepared.Environment.WorkDir, "uncommitted.txt")
			sessionID := uuid.NewString()
			session := filepath.Join(prepared.TaskRoot, "pi-sessions", "fixture.jsonl")
			if provider == "codex" {
				session = filepath.Join(prepared.Environment.CodexHome, "sessions", "rollout-fixture-"+sessionID+".jsonl")
			} else if provider == "claude" {
				session = filepath.Join(prepared.TaskRoot, ClaudeSessionsDir, ClaudeProjectDir, sessionID+".jsonl")
				if err := os.MkdirAll(filepath.Dir(session), 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				sessionID = session
			}
			header := map[string]any{"type": "session", "id": uuid.NewString(), "cwd": prepared.Environment.WorkDir}
			if provider == "codex" {
				header = map[string]any{"type": "session_meta", "payload": map[string]string{"id": sessionID, "cwd": prepared.Environment.WorkDir}}
			} else if provider == "claude" {
				header = map[string]any{"type": "user", "sessionId": sessionID, "cwd": prepared.Environment.WorkDir, "message": map[string]string{"role": "user", "content": "Preserve this task conversation."}}
			}
			sessionBytes, _ := json.Marshal(header)
			sessionBytes = append(sessionBytes, '\n')
			contents := map[string][]byte{work: []byte("preserved user bytes"), session: sessionBytes}
			for path, raw := range contents {
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			// No session binding was supplied: ownership alone must retain data.
			reused, err := PrepareTask(context.Background(), retryPreparation(input, prepared))
			if err != nil {
				t.Fatal(err)
			}
			for path, expected := range contents {
				raw, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(raw, expected) {
					t.Fatalf("retained data lost: %s: %v", path, err)
				}
			}
			if err := ValidateSession(reused, sessionID); err != nil {
				t.Fatal(err)
			}
			other := reused
			other.TaskID = uuid.NewString()
			other.TaskRoot, _ = TaskRoot(input.WorkspacesRoot, input.WorkspaceID, other.TaskID, "", "")
			other.Environment = NativeEnvironment{RootDir: other.TaskRoot, WorkDir: filepath.Join(other.TaskRoot, "workdir"), MulticaConfigRoot: filepath.Join(other.TaskRoot, "multica-config")}
			if provider == "codex" {
				other.Environment.CodexHome = filepath.Join(other.TaskRoot, "codex-home")
			}
			other.CleanupManifest = json.RawMessage(`{}`)
			other.Digest = preparedDigest(other)
			if err := ValidateSession(other, sessionID); err == nil {
				t.Fatal("another task adopted this session")
			}
		})
	}
}

func TestClaudeSessionRequiresTaskOwnedConversation(t *testing.T) {
	for _, attack := range []string{"other-cwd", "other-session", "missing-message", "malformed-prefix", "metadata-only", "file-symlink", "file-hardlink", "directory-symlink"} {
		t.Run(attack, func(t *testing.T) {
			base, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			p := Prepared{OwnerID: uuid.NewString(), WorkspaceID: uuid.NewString(), TaskID: uuid.NewString(), AgentID: uuid.NewString(), AttemptID: uuid.NewString(), Generation: 1, PVCUID: uuid.NewString(), Provider: "claude", Executable: "/opt/providers/claude", RuntimeDigest: digest([]byte("runtime")), ConfigurationDigest: digest([]byte("configuration")), CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`)}
			p.TaskRoot, err = TaskRoot(base, p.WorkspaceID, p.TaskID, "", "")
			if err != nil {
				t.Fatal(err)
			}
			p.Environment = NativeEnvironment{RootDir: p.TaskRoot, WorkDir: filepath.Join(p.TaskRoot, "workdir"), MulticaConfigRoot: filepath.Join(p.TaskRoot, "multica-config")}
			p.Digest = preparedDigest(p)
			sessionID := uuid.NewString()
			project := filepath.Join(p.TaskRoot, ClaudeSessionsDir, ClaudeProjectDir)
			if err := os.MkdirAll(project, 0700); err != nil {
				t.Fatal(err)
			}
			session := filepath.Join(project, sessionID+".jsonl")
			conversation := map[string]any{"type": "user", "sessionId": sessionID, "cwd": p.Environment.WorkDir, "message": map[string]string{"role": "user", "content": "Retain my task's private work."}}
			metadata, _ := json.Marshal(map[string]string{"type": "queue-operation", "sessionId": sessionID})
			message, _ := json.Marshal(conversation)
			prefix := append(metadata, '\n')
			original := append(append([]byte{}, prefix...), append(message, '\n')...)
			if err := os.WriteFile(session, original, 0600); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSession(p, sessionID); err != nil {
				t.Fatal("task-owned conversation could not resume after leading metadata", err)
			}
			switch attack {
			case "other-cwd":
				conversation["cwd"] = filepath.Join(base, "another-task", "workdir")
			case "other-session":
				conversation["sessionId"] = uuid.NewString()
			case "missing-message":
				delete(conversation, "message")
			case "malformed-prefix":
				prefix = []byte("not a transcript record\n")
			case "metadata-only":
				conversation = map[string]any{"type": "queue-operation", "sessionId": sessionID}
			case "file-symlink", "file-hardlink", "directory-symlink":
				other := filepath.Join(base, "another-task")
				if err := os.Mkdir(other, 0700); err != nil {
					t.Fatal(err)
				}
				outside := filepath.Join(other, filepath.Base(session))
				if err := os.WriteFile(outside, original, 0600); err != nil {
					t.Fatal(err)
				}
				if attack == "directory-symlink" {
					if err := os.RemoveAll(project); err != nil {
						t.Fatal(err)
					}
					err = os.Symlink(other, project)
				} else {
					if err := os.Remove(session); err != nil {
						t.Fatal(err)
					}
					if attack == "file-symlink" {
						err = os.Symlink(outside, session)
					} else {
						err = os.Link(outside, session)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if !strings.HasSuffix(attack, "link") {
				message, _ = json.Marshal(conversation)
				if err := os.WriteFile(session, append(prefix, append(message, '\n')...), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := ValidateSession(p, sessionID); err == nil {
				t.Fatal("unowned or incomplete transcript gained resume authority")
			}
		})
	}
}

func TestClaudePreparationEnforcesUnshadowedSkillPolicy(t *testing.T) {
	for _, scenario := range []string{"bound-skill-shadows-inherited", "unshadowed-skill-policy", "policy-write-fails"} {
		t.Run(scenario, func(t *testing.T) {
			input := preparationFixture(t, "claude")
			prepared, err := PrepareTask(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			work := filepath.Join(prepared.Environment.WorkDir, "user-work.txt")
			original := []byte("Retain existing task work during policy changes.")
			if err := os.WriteFile(work, original, 0600); err != nil {
				t.Fatal(err)
			}
			retry := retryPreparation(input, prepared)
			var task map[string]any
			if err := json.Unmarshal(retry.Task, &task); err != nil {
				t.Fatal(err)
			}
			name := "Fixture Skill"
			if scenario != "bound-skill-shadows-inherited" {
				name = "Protected operator action"
			}
			if scenario == "policy-write-fails" {
				// Reuse reports policy-write failures only as warnings. Obstruct the
				// real helper's policy output without affecting other task writes.
				if err := os.Mkdir(filepath.Join(prepared.TaskRoot, claudeSkillSettingsFile), 0700); err != nil {
					t.Fatal(err)
				}
			}
			task["DisabledRuntimeSkills"] = []map[string]string{{"Root": "provider", "Key": "inherited-skill", "Name": name}}
			retry.Task, _ = json.Marshal(task)
			reused, err := PrepareTask(context.Background(), retry)
			if scenario != "policy-write-fails" && err != nil {
				t.Fatal("valid inherited skill policy could not prepare task", err)
			}
			if scenario == "policy-write-fails" && err == nil {
				t.Fatal("missing required skill denial gained execution authority")
			}
			if scenario == "unshadowed-skill-policy" && reused.Environment.ClaudeSettingsPath != filepath.Join(prepared.TaskRoot, claudeSkillSettingsFile) {
				t.Fatal("the pinned helper's skill policy was not retained in the prepared environment")
			}
			if retained, err := os.ReadFile(work); err != nil || !bytes.Equal(retained, original) {
				t.Fatal("skill policy preparation lost existing task work", err)
			}
		})
	}
}

func TestFailedTaskConfigurationCanReuseNativeInitializationWithoutLosingFiles(t *testing.T) {
	input := preparationFixture(t, "pi")
	var checkpoint Prepared
	contents := []byte("retained task data")
	input.Instructions = []byte("controller instructions")
	input.Checkpoint = func(prepared Prepared) error {
		checkpoint = prepared
		if err := os.WriteFile(filepath.Join(prepared.Environment.WorkDir, "partial.txt"), contents, 0600); err != nil {
			return err
		}
		// An obstructed output makes the real atomic configuration write fail.
		return os.Mkdir(filepath.Join(prepared.Environment.WorkDir, "AGENTS.md"), 0700)
	}
	if _, err := PrepareTask(context.Background(), input); err == nil {
		t.Fatal("failed configuration unexpectedly gained execution authority")
	}
	if err := os.Remove(filepath.Join(input.TaskRoot, "workdir", "AGENTS.md")); err != nil {
		t.Fatal(err)
	}
	retry := retryPreparation(input, checkpoint)
	retry.Checkpoint = nil
	if _, err := PrepareTask(context.Background(), retry); err != nil {
		t.Fatal("failed configuration stranded an initialized task root", err)
	}
	raw, err := os.ReadFile(filepath.Join(input.TaskRoot, "workdir", "partial.txt"))
	if err != nil || !bytes.Equal(raw, contents) {
		t.Fatal("retry discarded task data", err)
	}
}

func TestPreparationRejectsForgedCleanupBeforeDeletion(t *testing.T) {
	input := preparationFixture(t, "pi")
	prepared, err := PrepareTask(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(filepath.Dir(input.WorkspacesRoot), "outside.txt")
	if err := os.WriteFile(victim, []byte("outside user data"), 0600); err != nil {
		t.Fatal(err)
	}
	forged, _ := json.Marshal(cleanupTargets{Files: []string{victim}})
	if err := os.WriteFile(filepath.Join(prepared.TaskRoot, cleanupManifestName), forged, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareTask(context.Background(), retryPreparation(input, prepared)); err == nil {
		t.Fatal("untrusted cleanup authorized")
	}
	raw, err := os.ReadFile(victim)
	if err != nil || string(raw) != "outside user data" {
		t.Fatal("helper deleted an external file")
	}
}

func TestBoundSkillsRequireEachCompleteMaterialization(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.MkdirAll("skills/00-unrelated", 0700); err != nil {
		t.Fatal(err)
	}
	oversized, err := root.OpenFile("skills/00-unrelated/SKILL.md", os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = oversized.Truncate(nativeTaskBytes + 1)
	if err := errors.Join(err, oversized.Close()); err != nil {
		t.Fatal(err)
	}
	skills := []nativeSkill{
		{Name: "first", Content: "Shared instructions", Files: []struct{ Path, Content string }{{"guide.txt", "first skill guidance"}}},
		{Name: "second", Content: "Shared instructions", Files: []struct{ Path, Content string }{{"guide.txt", "second skill guidance"}}},
	}
	for _, skill := range skills {
		directory := filepath.Join("skills", skill.Name)
		if err := root.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		if err := root.WriteFile(filepath.Join(directory, "SKILL.md"), []byte(skill.Content), 0600); err != nil {
			t.Fatal(err)
		}
		if err := root.WriteFile(filepath.Join(directory, "guide.txt"), []byte(skill.Files[0].Content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := verifyBoundSkills(root, "skills", skills); err != nil {
		t.Fatal("complete bound skills were rejected", err)
	}
	if err := root.WriteFile("skills/second/guide.txt", []byte("incomplete or replaced guidance"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyBoundSkills(root, "skills", skills); err == nil {
		t.Fatal("a complete sibling skill concealed an incomplete bound skill")
	}
}

func TestPreparationRejectsExistingUnownedRoot(t *testing.T) {
	input := preparationFixture(t, "pi")
	if err := os.MkdirAll(input.TaskRoot, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(input.TaskRoot, "unowned.txt")
	if err := os.WriteFile(file, []byte("unowned data"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareTask(context.Background(), input); err == nil {
		t.Fatal("existing root initialized")
	}
	input.IsReuse = true
	if _, err := PrepareTask(context.Background(), input); err == nil {
		t.Fatal("unrecorded reuse initialized")
	}
	raw, err := os.ReadFile(file)
	if err != nil || string(raw) != "unowned data" {
		t.Fatal("unowned data lost")
	}
}

func TestPreparationRejectsSharedConfigBeforeOverwrite(t *testing.T) {
	for _, link := range []string{"symlink", "hardlink"} {
		t.Run(link, func(t *testing.T) {
			input := preparationFixture(t, "codex")
			prepared, err := PrepareTask(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			victim := filepath.Join(filepath.Dir(input.WorkspacesRoot), "outside.toml")
			original := []byte("model = 'private outside configuration'\n")
			if err := os.WriteFile(victim, original, 0600); err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(prepared.Environment.CodexHome, "config.toml")
			if err := os.Remove(config); err != nil {
				t.Fatal(err)
			}
			if link == "symlink" {
				err = os.Symlink(victim, config)
			} else {
				err = os.Link(victim, config)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := PrepareTask(context.Background(), retryPreparation(input, prepared)); err == nil {
				t.Fatal("shared configuration accepted")
			}
			raw, err := os.ReadFile(victim)
			if err != nil || string(raw) != string(original) {
				t.Fatal("helper overwrote shared configuration")
			}
		})
	}
}

func TestPreparationRejectsAlternateNativeRootBeforeReset(t *testing.T) {
	input := preparationFixture(t, "pi")
	payload, _ := json.Marshal(map[string]any{"action": "prepare", "prepare": map[string]any{"WorkspacesRoot": input.WorkspacesRoot, "WorkspaceID": input.WorkspaceID, "TaskID": input.TaskID, "WorkspaceSlug": "alternate", "IssueIdentifier": "issue", "Provider": "pi", "Task": input.Task}})
	raw, err := runPreparation(context.Background(), input.Command, input.Environment, payload)
	if err != nil {
		t.Fatal(err)
	}
	var native struct {
		Environment NativeEnvironment `json:"environment"`
	}
	if err := json.Unmarshal(raw, &native); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(native.Environment.WorkDir, "unowned.txt")
	if err := os.WriteFile(victim, []byte("unowned existing workspace"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareTask(context.Background(), input); err == nil {
		t.Fatal("alternate root adopted")
	}
	after, err := os.ReadFile(victim)
	if err != nil || string(after) != "unowned existing workspace" {
		t.Fatal("helper reset a different root before validation")
	}
}

func TestPreparationCancellationStopsActualWriter(t *testing.T) {
	base := t.TempDir()
	binary, pidfile := filepath.Join(base, "helper"), filepath.Join(base, "pid")
	quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
	script := "#!/bin/sh\necho $$ > " + quote(pidfile) + "\nwhile :; do printf x >> " + quote(filepath.Join(base, "writes")) + "; done\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := runPreparation(ctx, binary, []string{"PATH=/usr/bin:/bin"}, nil); done <- err }()
	var pid int
	for pid == 0 {
		if raw, err := os.ReadFile(pidfile); err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		}
		select {
		case err := <-done:
			t.Fatalf("writer did not start: %v", err)
		default:
		}
		if ctx.Err() != nil {
			t.Fatal("writer start did not become observable")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		t.Fatal("preparation returned while writer still exists")
	}
}
