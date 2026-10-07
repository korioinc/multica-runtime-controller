package daemonapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/skillbundle"
)

func executionFixture(t *testing.T, fields map[string]any) (Claim, wire.Bootstrap, runtimeimage.Executable, ExecutionSettings) {
	t.Helper()
	metadata := wire.Bootstrap{TaskID: uuid.NewString(), AgentID: uuid.NewString(), WorkspaceID: uuid.NewString(), RuntimeID: uuid.NewString()}
	executable := runtimeimage.Executable{Path: "/opt/providers/codex", Version: "fixture", SHA256: strings.Repeat("a", 64)}
	metadata.RuntimeRef.Providers = map[string]runtimeimage.Executable{"codex": executable}
	input := map[string]any{"id": metadata.TaskID, "agent_id": metadata.AgentID, "workspace_id": metadata.WorkspaceID, "runtime_id": metadata.RuntimeID, "issue_id": uuid.NewString(), "auth_token": "mat_fixture", "agent": map[string]any{"id": metadata.AgentID}}
	for key, value := range fields {
		input[key] = value
	}
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := ParseClaim(raw)
	if err != nil {
		t.Fatal(err)
	}
	root, err := workspace.TaskRoot(wire.WorkspaceRoot, metadata.WorkspaceID, metadata.TaskID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	return claim, metadata, executable, ExecutionSettings{Provider: "codex", TaskRoot: root}
}

func TestResourceOmissionPreservesCurrentContextAndOriginalClaim(t *testing.T) {
	claim, _, _, _ := executionFixture(t, map[string]any{
		"workspace_context": "mandatory workspace context",
		"project_resources": []map[string]any{{"id": uuid.NewString(), "resource_type": "fixture", "resource_ref": map[string]string{"value": "optional resource sentinel"}}},
	})
	var envelope map[string]any
	if err := json.Unmarshal(claim.Envelope, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["agent"].(map[string]any)["instructions"] = "mandatory agent instructions"
	claim.Envelope, _ = json.Marshal(envelope)
	original := string(claim.Envelope)
	brief, err := RuntimeBriefWithoutResources(claim, []SkillBundle{{Name: "current bound skill", Description: "current skill description"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range []string{claim.ID, claim.AgentID, claim.WorkspaceID, "mandatory workspace context", "mandatory agent instructions", "current bound skill"} {
		if !strings.Contains(brief, current) {
			t.Fatal("optional omission removed current mandatory context", current)
		}
	}
	if strings.Contains(brief, "optional resource sentinel") || string(claim.Envelope) != original {
		t.Fatal("optional omission retained resource bytes or changed the authoritative claim")
	}
}

func TestExecutionPreservesTaskMeaningForInstalledProviders(t *testing.T) {
	for _, provider := range []string{"codex", "pi", "claude"} {
		for _, test := range []struct {
			name       string
			fields     map[string]any
			want       []string
			readsIssue bool
		}{
			{"issue", map[string]any{"issue_id": "assigned-issue"}, []string{"assigned-issue"}, true},
			{"comment", map[string]any{"trigger_comment_id": "comment-A", "trigger_thread_id": "thread-A", "trigger_comment_content": "comment-instruction", "coalesced_comments": []any{map[string]string{"id": "comment-B", "thread_id": "thread-B", "content": "second-instruction"}}}, []string{"comment-instruction", "second-instruction", "thread-A", "thread-B"}, true},
			{"chat", map[string]any{"chat_session_id": "chat-A", "chat_message": "chat-instruction", "chat_channel_type": "slack", "chat_in_thread": true, "chat_message_attachments": []any{map[string]string{"id": "chat-attachment"}}}, []string{"chat-instruction", "chat-attachment", "multica chat thread"}, false},
			{"quick-create", map[string]any{"quick_create_prompt": "create-instruction", "quick_create_priority": "urgent", "quick_create_attachment_ids": []string{"create-attachment"}, "quick_create_source_context": map[string]string{"text": "quoted-history"}}, []string{"create-instruction", "create-attachment", "quoted-history", "urgent", "multica issue create"}, false},
			{"autopilot", map[string]any{"autopilot_run_id": "run-A", "autopilot_id": "automation-A", "autopilot_description": "automation-instruction", "autopilot_trigger_payload": map[string]string{"text": "trigger-context"}}, []string{"run-A", "automation-A", "automation-instruction", "trigger-context"}, false},
		} {
			t.Run(provider+"/"+test.name, func(t *testing.T) {
				model := "gpt-5.4"
				if provider == "pi" {
					model = "openai/" + model
				}
				claim, metadata, executable, settings := executionFixture(t, test.fields)
				var input map[string]any
				if err := json.Unmarshal(claim.Envelope, &input); err != nil {
					t.Fatal(err)
				}
				input["agent"].(map[string]any)["model"] = model
				input["agent"].(map[string]any)["thinking_level"] = "high"
				input["agent"].(map[string]any)["service_tier"] = "priority"
				input["agent"].(map[string]any)["instructions"] = "agent-instruction"
				claim.Envelope, _ = json.Marshal(input)
				metadata.RuntimeRef.Providers = map[string]runtimeimage.Executable{provider: executable}
				metadata.Environment = []string{"MULTICA_AGENT_TIMEOUT=45", "MULTICA_AGENT_IDLE_WATCHDOG=20s", "MULTICA_AGENT_TOOL_WATCHDOG=30s"}
				settings.Provider = provider
				settings.Skills = []SkillBundle{{ID: "skill-A", Name: "Task skill", Content: "skill-instruction"}}
				execution, err := ExecutionInput(claim, metadata, executable, settings)
				if err != nil {
					t.Fatal(err)
				}
				for _, value := range test.want {
					if !strings.Contains(execution.Run.Prompt, value) {
						t.Fatalf("task meaning lost %q", value)
					}
				}
				if strings.Contains(execution.Run.Prompt, "multica issue get") != test.readsIssue {
					t.Fatal("task routed to the wrong issue workflow")
				}
				options := execution.Run.Options
				if options.Model != model || options.ThinkingLevel != "high" || options.ServiceTier != "priority" || options.Timeout != 45*time.Second || options.IdleWatchdogTimeout != 20*time.Second {
					t.Fatal("task model or execution options were lost")
				}
				if !strings.Contains(execution.Run.RuntimeBrief, "agent-instruction") || !strings.Contains(string(execution.HelperTask), "skill-instruction") || execution.Run.Environment["MULTICA_AGENT_TOOL_WATCHDOG"] != "30s" {
					t.Fatal("instructions, skill or tool deadline were lost")
				}
			})
		}
	}
}

// The execution adapter is an authorization boundary: a claim must not select
// another task's workspace, executable or controller credentials.
func TestExecutionRejectsBorrowedTaskAuthority(t *testing.T) {
	claim, metadata, executable, settings := executionFixture(t, nil)
	if _, err := ExecutionInput(claim, metadata, executable, settings); err != nil {
		t.Fatal(err)
	}
	otherRoot, err := workspace.TaskRoot(wire.WorkspaceRoot, metadata.WorkspaceID, uuid.NewString(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	settings.TaskRoot = otherRoot
	if _, err := ExecutionInput(claim, metadata, executable, settings); err == nil {
		t.Fatal("claim borrowed another task's writable files")
	}
	for _, key := range []string{"MULTICA_TOKEN", "CODEX_HOME", "HOME", "LD_PRELOAD"} {
		claim, metadata, executable, settings = executionFixture(t, nil)
		var input map[string]any
		if err := json.Unmarshal(claim.Envelope, &input); err != nil {
			t.Fatal(err)
		}
		input["agent"].(map[string]any)["custom_env"] = map[string]string{key: "borrowed-authority"}
		claim.Envelope, _ = json.Marshal(input)
		if _, err := ExecutionInput(claim, metadata, executable, settings); err == nil {
			t.Fatalf("custom environment replaced protected authority through %s", key)
		}
	}
}

func TestExecutionCannotDisableTaskSessionRecording(t *testing.T) {
	claim, metadata, executable, settings := executionFixture(t, nil)
	metadata.RuntimeRef.Providers = map[string]runtimeimage.Executable{"pi": executable}
	settings.Provider = "pi"
	var input map[string]any
	if err := json.Unmarshal(claim.Envelope, &input); err != nil {
		t.Fatal(err)
	}
	input["agent"].(map[string]any)["custom_args"] = []string{"'--no-session'"}
	claim.Envelope, _ = json.Marshal(input)
	if _, err := ExecutionInput(claim, metadata, executable, settings); err == nil {
		t.Fatal("task acquired execution with durable sessions disabled")
	}
}

func TestClaudeExecutionCannotReplaceManagedConversation(t *testing.T) {
	for _, attack := range []string{"config-root", "project-root", "resume-argument", "compact-resume", "combined-resume", "combined-continue", "quoted-combined-resume", "quoted-combined-continue", "disabled-recording", "disabled-recording-env", "settings-env", "default-resume", "default-combined-resume", "default-disabled-recording"} {
		t.Run(attack, func(t *testing.T) {
			claim, metadata, executable, settings := executionFixture(t, nil)
			settings.Provider = "claude"
			metadata.RuntimeRef.Providers = map[string]runtimeimage.Executable{settings.Provider: executable}
			var input map[string]any
			if err := json.Unmarshal(claim.Envelope, &input); err != nil {
				t.Fatal(err)
			}
			agent := input["agent"].(map[string]any)
			agent["custom_args"] = []string{"--max-turns=1", "-d"}
			claim.Envelope, _ = json.Marshal(input)
			if _, err := ExecutionInput(claim, metadata, executable, settings); err != nil {
				t.Fatal("authorized Claude task with ordinary options was rejected", err)
			}
			switch attack {
			case "config-root":
				agent["custom_env"] = map[string]string{"CLAUDE_CONFIG_DIR": "/workspace/another-task"}
			case "project-root":
				agent["custom_env"] = map[string]string{"CLAUDE_CODE_PROJECT_DIR_NAME": "../../another-task"}
			case "resume-argument":
				agent["custom_args"] = []string{"'--resume=" + uuid.NewString() + "'"}
			case "compact-resume":
				agent["custom_args"] = []string{"-r" + uuid.NewString()}
			case "combined-resume":
				agent["custom_args"] = []string{"-pr" + uuid.NewString()}
			case "combined-continue":
				agent["custom_args"] = []string{"-pc"}
			case "quoted-combined-resume":
				agent["custom_args"] = []string{`"-pr` + uuid.NewString() + `"`}
			case "quoted-combined-continue":
				agent["custom_args"] = []string{"'-pc'"}
			case "disabled-recording":
				agent["custom_args"] = []string{"--no-session-persistence"}
			case "disabled-recording-env":
				agent["custom_env"] = map[string]string{"CLAUDE_CODE_SKIP_PROMPT_HISTORY": "1"}
			case "settings-env":
				agent["custom_args"] = []string{"--settings", `{"env":{"CLAUDE_CODE_PROJECT_DIR_NAME":"../../another-task"}}`}
			case "default-resume":
				metadata.Environment = []string{"MULTICA_CLAUDE_ARGS=--resume " + uuid.NewString()}
			case "default-combined-resume":
				metadata.Environment = []string{"MULTICA_CLAUDE_ARGS=-pr" + uuid.NewString()}
			case "default-disabled-recording":
				metadata.Environment = []string{"MULTICA_CLAUDE_ARGS=--no-session-persistence"}
			}
			claim.Envelope, _ = json.Marshal(input)
			if _, err := ExecutionInput(claim, metadata, executable, settings); err == nil {
				t.Fatal("task options replaced or disabled the managed conversation")
			}
		})
	}
}

// Bundle bytes arrive from a remote service and are later materialized by the
// native helper. Even a hash-consistent response cannot authorize path traversal.
func TestExecutionSkillsRejectTraversalAndTampering(t *testing.T) {
	for _, traversal := range []bool{false, true} {
		bundle := SkillBundle{ID: "skill-fixture", Source: skillbundle.SourcePlugin, Name: "Review", Content: "Read the repository."}
		file := skillbundle.File{Path: "guide.txt", Content: "original"}
		if traversal {
			file.Path = "../../controller-secret"
		}
		manifest := skillbundle.BuildManifest(skillbundle.Skill{ID: bundle.ID, Source: bundle.Source, Name: bundle.Name, Content: bundle.Content, Files: []skillbundle.File{file}})
		bundle.Hash, bundle.SizeBytes = manifest.Hash, manifest.SizeBytes
		bundle.Files = []SkillFile{{Path: file.Path, Content: file.Content}}
		if !traversal {
			bundle.Files[0].Content = "tampered"
		}
		valid := SkillBundle{ID: "other-skill", Source: skillbundle.SourcePlugin, Name: "Plan", Content: "Plan the task."}
		valid.Hash = skillbundle.BuildManifest(skillbundle.Skill{ID: valid.ID, Source: valid.Source, Name: valid.Name, Content: valid.Content}).Hash
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"bundles": []SkillBundle{valid, bundle}})
		}))
		client, err := NewClient(upstream.URL, "controller-fixture", fixtureRef().Daemon.Version, upstream.Client())
		if err != nil {
			upstream.Close()
			t.Fatal(err)
		}
		claim, _, _, _ := executionFixture(t, nil)
		var input map[string]any
		if err := json.Unmarshal(claim.Envelope, &input); err != nil {
			upstream.Close()
			t.Fatal(err)
		}
		input["agent"].(map[string]any)["skill_refs"] = []any{
			map[string]any{"id": valid.ID, "source": valid.Source, "hash": valid.Hash},
			map[string]any{"id": bundle.ID, "source": bundle.Source, "hash": bundle.Hash},
		}
		claim.Envelope, _ = json.Marshal(input)
		claim, err = ParseClaim(claim.Envelope)
		if err != nil {
			upstream.Close()
			t.Fatal(err)
		}
		_, err = client.ExecutionSkills(context.Background(), claim)
		upstream.Close()
		if err == nil {
			t.Fatal("untrusted bundle was admitted for filesystem preparation")
		}
	}
}
