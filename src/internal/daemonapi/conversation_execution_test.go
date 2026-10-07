package daemonapi

import (
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestConversationExecutionBindsCurrentTaskToAdmittedResume(t *testing.T) {
	for _, provider := range []string{"codex", "pi"} {
		t.Run(provider, func(t *testing.T) {
			claim, bootstrap, executable, settings := executionFixture(t, map[string]any{
				"kind": "comment", "trigger_comment_id": uuid.NewString(), "trigger_comment_content": "new request exactly once",
				"new_comments_delta_known": true, "new_comment_count": 0, "prior_session_resume_unavailable": true,
			})
			bootstrap.WorkerSessionID, bootstrap.WorkspaceAnchorTaskID = uuid.NewString(), uuid.NewString()
			root, err := workspace.TaskRoot(wire.WorkspaceRoot, claim.WorkspaceID, bootstrap.WorkspaceAnchorTaskID, "", "")
			if err != nil {
				t.Fatal(err)
			}
			bootstrap.TaskRoot = root
			bootstrap.RuntimeRef.Providers = map[string]runtimeimage.Executable{provider: executable}
			settings.Provider, settings.TaskRoot, settings.SourceProven = provider, root, true
			settings.SessionID = uuid.NewString()
			if provider == "pi" {
				settings.SessionID = root + "/pi-sessions/retained.jsonl"
			}
			execution, err := ExecutionInput(claim, bootstrap, executable, settings)
			if err != nil {
				t.Fatal(err)
			}
			if execution.Run.Environment["MULTICA_TASK_ID"] != claim.ID ||
				execution.Run.Options.Cwd != root+"/workdir" || execution.Run.Options.ResumeSessionID != settings.SessionID ||
				!execution.Run.Options.ResumeExpected {
				t.Fatal("resume lost the current task identity or admitted workspace")
			}
		})
	}
}
