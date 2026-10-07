package controller

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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
	"github.com/multica-ai/multica/server/pkg/skillbundle"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

// Run only through scripts/verify-backend-contract.py. Every backend identity,
// claim, authority observation and task result comes from its unmodified server.
func TestUnchangedBackendConversationContract(t *testing.T) {
	origin := os.Getenv("MULTICA_BACKEND_CONTRACT_URL")
	if origin == "" {
		t.Skip("requires a disposable unchanged backend")
	}
	endpoint, err := url.Parse(origin)
	if err != nil || endpoint.Scheme != "http" || net.ParseIP(endpoint.Hostname()) == nil || !net.ParseIP(endpoint.Hostname()).IsLoopback() {
		t.Fatal("contract backend must use an explicit loopback HTTP origin")
	}
	proof := os.Getenv("MULTICA_BACKEND_CONTRACT_PROOF")
	if !filepath.IsAbs(proof) || os.Getenv("MULTICA_BACKEND_CONTRACT_CODE") == "" {
		t.Fatal("isolated contract proof configuration is missing")
	}
	trace, err := os.OpenFile(filepath.Join(proof, "backend-http.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trace.Close() })
	f := &backendContract{t: t, origin: origin, proof: proof, prepared: make(map[string]bool), http: &http.Client{Timeout: 10 * time.Second,
		Transport: &backendContractTransport{base: http.DefaultTransport, output: trace}}}
	for attempt := range 60 {
		response, err := f.http.Get(origin + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if attempt == 59 {
			t.Fatal("unchanged backend readiness timed out")
		}
		time.Sleep(100 * time.Millisecond)
	}
	email := "contract-" + uuid.NewString() + "@example.invalid"
	f.request(http.MethodPost, "/auth/send-code", map[string]string{"email": email}, nil)
	var login struct {
		Token string `json:"token"`
		User  struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	f.request(http.MethodPost, "/auth/verify-code", map[string]string{"email": email, "code": os.Getenv("MULTICA_BACKEND_CONTRACT_CODE")}, &login)
	f.token = login.Token
	var pat struct{ Token string }
	f.request(http.MethodPost, "/api/tokens", map[string]any{"name": "isolated controller contract", "expires_in_days": 1}, &pat)
	f.token = pat.Token
	var created struct{ ID string }
	f.request(http.MethodPost, "/api/workspaces", map[string]string{"name": "Controller contract", "slug": "contract-" + uuid.NewString()[:8]}, &created)
	f.workspaceID = created.ID
	f.api, err = daemonapi.NewClient(origin, f.token, "0.6.1", f.http)
	if err != nil {
		t.Fatal(err)
	}
	f.daemonID = "contract-" + uuid.NewString()
	registration, err := f.api.Register(t.Context(), daemonapi.Registration{WorkspaceID: f.workspaceID, DaemonID: f.daemonID,
		DeviceName: "isolated contract proof", Runtimes: []daemonapi.Inventory{{Name: "contract-pi", Type: "pi", Version: "0.6.1", Status: "available"}}})
	if err != nil || len(registration.Runtimes) != 1 {
		t.Fatal("official runtime registration failed", err)
	}
	f.runtimeID = registration.Runtimes[0].ID
	f.request(http.MethodPost, "/api/agents", map[string]any{"name": "Contract agent", "runtime_id": f.runtimeID,
		"instructions": "Validate local runtime contracts.", "model": "contract-model", "mcp_config": map[string]any{}, "max_concurrent_tasks": 1}, &created)
	f.agentID = created.ID
	if !wire.UUID(login.User.ID) || !wire.UUID(f.workspaceID) || !wire.UUID(f.agentID) || !strings.HasPrefix(f.token, "mul_") {
		t.Fatal("public auth and creation APIs did not return supported identities")
	}

	t.Run("issue_producer_and_named_rerun", func(t *testing.T) {
		f.t = t
		f.request(http.MethodPost, "/api/issues", map[string]any{"title": "Unchanged issue contracts", "status": "todo",
			"assignee_type": "agent", "assignee_id": f.agentID}, &created)
		issueID := created.ID
		f.businessIssueID = issueID
		first := f.claim("", issueID, "")
		authority := f.assertAuthority(first, login.User.ID)
		journal := newBackendJournal(t, first)
		f.prepareAdapterBridge(first, journal, authority)
		firstSession, secondSession := uuid.NewString(), uuid.NewString()
		firstGrant := f.complete(journal, first, firstSession, false)
		f.request(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]string{"content": "Continue this same issue in a new task."}, nil)
		second := f.claim("", issueID, "")
		if second.PriorSessionID != firstSession || second.PriorWorkDir != journal.storage.TaskRoot+"/workdir" ||
			!second.IssueStateDeltaKnown && !second.NewCommentsDeltaKnown {
			t.Fatal("ordinary issue claim did not expose its completed native producer")
		}
		f.selectSource(journal, second, workspace.SelectionResume, firstGrant, firstGrant)
		secondGrant := f.complete(journal, second, secondSession, false)
		f.request(http.MethodPost, "/api/issues/"+issueID+"/rerun", map[string]string{"task_id": first.ID}, &created)
		rerun := f.claim(created.ID, issueID, "")
		if rerun.Attribution == nil || rerun.Attribution.RerunOfTaskID != first.ID || rerun.PriorSessionID != firstSession {
			t.Fatal("named rerun selected a different producer or treated an internal reset flag as the effective mode")
		}
		selection := f.selectSource(journal, rerun, workspace.SelectionResume, firstGrant, firstGrant)
		if selection.LatestWriter != grantSource(secondGrant) {
			t.Fatal("named source erased the later local writer")
		}
		missingGrant := f.complete(journal, rerun, "", true)
		f.request(http.MethodPost, "/api/issues/"+issueID+"/rerun", map[string]string{"task_id": rerun.ID}, &created)
		fresh := f.claim(created.ID, issueID, "")
		if fresh.PriorSessionID != "" || fresh.PriorWorkDir != journal.storage.TaskRoot+"/workdir" || !fresh.PriorSessionResumeUnavailable {
			t.Fatal("missing rollout did not expose the supported workdir-only continuation")
		}
		f.selectSource(journal, fresh, workspace.SelectionFreshSession, workspace.TaskGrant{}, missingGrant)
		f.complete(journal, fresh, uuid.NewString(), false)
	})
	t.Run("native_dm_and_transient_pi_failure", func(t *testing.T) {
		f.t = t
		f.request(http.MethodPost, "/api/chat/sessions", map[string]string{"agent_id": f.agentID, "title": "Native contract chat"}, &created)
		chatID := created.ID
		f.request(http.MethodPost, "/api/chat/sessions/"+chatID+"/messages", map[string]string{"content": "First private native turn."}, nil)
		first := f.claim("", "", chatID)
		if first.ChatChannelType != "" || first.ChatType != "" || daemonapi.ConversationIdentity(uuid.NewString(), first).Kind != workspace.ConversationAgentDM {
			t.Fatal("native DM claim has an unsupported external or group discriminator")
		}
		authority := f.assertAuthority(first, login.User.ID)
		journal := newBackendJournal(t, first)
		f.prepareAdapterBridge(first, journal, authority)
		nativeSession := journal.storage.TaskRoot + "/pi-sessions/native.jsonl"
		firstGrant := f.complete(journal, first, nativeSession, false)
		f.request(http.MethodPost, "/api/chat/sessions/"+chatID+"/messages", map[string]string{"content": "Second private turn."}, nil)
		second := f.claim("", "", chatID)
		if second.PriorSessionID != nativeSession || second.PriorWorkDir != journal.storage.TaskRoot+"/workdir" {
			t.Fatal("native DM did not retain its session and workdir independently")
		}
		selection := f.selectSource(journal, second, workspace.SelectionResume, firstGrant, firstGrant)
		if err := f.api.StartClaim(t.Context(), second); err != nil {
			t.Fatal(err)
		}
		failure, _ := json.Marshal(map[string]string{"error": fmt.Sprintf("Pi session file %q is already in use by another execution", nativeSession),
			"work_dir": journal.storage.TaskRoot + "/workdir"})
		observed, err := f.api.TerminalObservation(t.Context(), second.ID, "fail", failure)
		if err != nil || !observed.AcceptsTransientFailure(second, failure) {
			t.Fatal("real backend failed-row classification differs from the pinned adapter", err, observed.FailureReason)
		}
		failedGrant := journal.record(t, second, "fail", failure, "", &selection, true)
		f.request(http.MethodPost, "/api/chat/sessions/"+chatID+"/messages", map[string]string{"content": "Continue after the temporary lock."}, nil)
		third := f.claim("", "", chatID)
		if third.PriorSessionID != nativeSession || third.PriorWorkDir != journal.storage.TaskRoot+"/workdir" {
			t.Fatal("transient Pi rejection dropped the original native DM pointers")
		}
		f.selectSource(journal, third, workspace.SelectionResume, firstGrant, failedGrant)
		f.complete(journal, third, nativeSession, false)
	})
	t.Run("ordinary_failure_proves_workspace_but_not_native_history", func(t *testing.T) {
		f.t = t
		f.request(http.MethodPost, "/api/issues", map[string]any{"title": "Accepted failed workspace contract", "status": "todo",
			"assignee_type": "agent", "assignee_id": f.agentID}, &created)
		issueID := created.ID
		claim := f.claim("", issueID, "")
		f.assertAuthority(claim, login.User.ID)
		journal := newBackendJournal(t, claim)
		if err := f.api.StartClaim(t.Context(), claim); err != nil {
			t.Fatal("ordinary failure task could not start", err)
		}
		reportedSession := uuid.NewString()
		failure := map[string]any{"error": "ordinary provider failure\x00 after partial work", "failure_reason": taskfailure.ReasonAgentUnknown.String(),
			"work_dir": journal.storage.TaskRoot + "/workdir", "durable_work_dir": journal.storage.TaskRoot + "/retained-output",
			"branch_name": "contract-partial-work", "session_id": reportedSession, "retired_session_id": uuid.NewString()}
		body, _ := json.Marshal(failure)
		accepted, err := f.api.TerminalObservation(t.Context(), claim.ID, "fail", body)
		if err != nil || !accepted.AcceptsFailure(claim, body) {
			t.Fatal("accepted ordinary failure did not prove the requested workspace and normalized outcome", err)
		}
		observed, err := f.api.ControllerTask(t.Context(), claim)
		if err != nil || !observed.AcceptsFailure(claim, body) {
			t.Fatal("controller read could not recover the accepted ordinary failure after task-token revocation", err)
		}
		for _, field := range []string{"error", "work_dir", "durable_work_dir", "branch_name"} {
			different := make(map[string]any, len(failure))
			for key, value := range failure {
				different[key] = value
			}
			different[field] = "another-accepted-value"
			if field == "work_dir" || field == "durable_work_dir" {
				different[field] = journal.storage.TaskRoot + "/another-directory"
			}
			otherBody, _ := json.Marshal(different)
			if observed.AcceptsFailure(claim, otherBody) {
				t.Fatal("the actual failed row accepted a different recorded result field", field)
			}
		}
		journal.record(t, claim, "fail", body, "", nil, false)
		f.request(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]string{"content": "Continue the accepted partial work."}, nil)
		next := f.claim("", issueID, "")
		if next.PriorSessionID != reportedSession || next.PriorWorkDir != journal.storage.TaskRoot+"/workdir" {
			t.Fatal("backend did not expose the failed turn's native hint and workspace independently")
		}
		// The backend retains the failed native hint, but its public failed row
		// cannot prove native adoption. A workspace witness must not promote it.
		rows, err := f.api.IssueTaskHistory(t.Context(), next)
		if err != nil {
			t.Fatal("actual issue history could not observe its accepted failed predecessor", err)
		}
		journal.evidence.History = rows
		selection := selectConversationSource(next, journal.key, journal.compatibility, true, journal.evidence)
		if selection.Mode == workspace.SelectionResume || selection.SessionID != "" || selection.SessionSource != (workspace.CheckpointSource{}) {
			t.Fatal("accepted failed workspace evidence promoted an unproven native producer")
		}
		f.complete(journal, next, uuid.NewString(), false)
	})
	t.Run("quick_create_issue_link_preserves_result_generation", func(t *testing.T) {
		f.t = t
		var queued struct {
			TaskID string `json:"task_id"`
		}
		f.request(http.MethodPost, "/api/issues/quick-create", map[string]string{"agent_id": f.agentID,
			"prompt": "Create the isolated contract issue."}, &queued)
		claim := f.claim(queued.TaskID, "", "")
		if claim.Kind != "quick_create" {
			t.Fatal("public quick-create request did not dispatch its own task context")
		}
		if err := f.api.StartClaim(t.Context(), claim); err != nil {
			t.Fatal("quick-create task could not start", err)
		}
		var issue struct{ ID string }
		f.requestWithToken(claim.AuthToken, http.MethodPost, "/api/issues", map[string]string{
			"title": "Issue created by the actual quick-create task", "status": "todo", "origin_type": "quick_create", "origin_id": claim.ID}, &issue)
		root, err := workspace.TaskRoot(wire.WorkspaceRoot, claim.WorkspaceID, claim.ID, claim.WorkspaceSlug, claim.IssueIdentifier)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(daemonapi.TaskCompleteRequest{Output: "Created the task's own issue.", WorkDir: root + "/workdir"})
		accepted, err := f.api.TerminalObservation(t.Context(), claim.ID, "complete", body)
		if err != nil || !accepted.AcceptsCompletion(claim, body) {
			t.Fatal("quick-create completion did not preserve its claimed result generation", err)
		}
		observed, err := f.api.ControllerTask(t.Context(), claim)
		if err != nil || observed.IssueID != issue.ID || !wire.UUID(issue.ID) || !observed.MatchesResultClaim(claim) || !observed.AcceptsCompletion(claim, body) {
			t.Fatal("linking the actual created issue prevented exact result recovery for the original quick-create claim", err)
		}
		otherGeneration := claim
		dispatchedAt, err := time.Parse(time.RFC3339Nano, claim.DispatchedAt)
		if err != nil {
			t.Fatal(err)
		}
		otherGeneration.DispatchedAt = dispatchedAt.Add(time.Second).Format(time.RFC3339Nano)
		if observed.MatchesResultClaim(otherGeneration) || observed.AcceptsCompletion(otherGeneration, body) {
			t.Fatal("the created issue link admitted another dispatch generation")
		}
		ordinary := claim
		ordinary.Kind = "direct"
		if observed.MatchesResultClaim(ordinary) {
			t.Fatal("the quick-create issue transition expanded ordinary task matching")
		}
	})
	t.Run("cancelled_task_status_survives_task_token_revocation", func(t *testing.T) {
		f.t = t
		f.request(http.MethodPost, "/api/issues", map[string]any{"title": "Cancellation observation contract", "status": "todo",
			"assignee_type": "agent", "assignee_id": f.agentID}, &created)
		claim := f.claim("", created.ID, "")
		if err := f.api.StartClaim(t.Context(), claim); err != nil {
			t.Fatal("actual claimed task could not start", err)
		}
		assignment, err := f.api.ClaimTaskAssignment(t.Context(), claim)
		if err != nil || assignment.Status != "running" || assignment.RuntimeID != claim.RuntimeID {
			t.Fatal("task-token observation did not establish the running assignment", err)
		}
		f.request(http.MethodPost, "/api/tasks/"+claim.ID+"/cancel", struct{}{}, nil)
		_, err = f.api.ClaimTaskAssignment(t.Context(), claim)
		var denied *daemonapi.ResponseError
		if !errors.As(err, &denied) || denied.StatusCode != http.StatusUnauthorized && denied.StatusCode != http.StatusForbidden {
			t.Fatal("cancelled task did not revoke ordinary task-token authority", err)
		}
		status, err := f.api.TaskStatus(t.Context(), claim.ID)
		if err != nil || status != "cancelled" {
			t.Fatal("controller daemon-route authority could not observe the committed cancellation", err, status)
		}
		acknowledgement, _ := json.Marshal(map[string]string{"error_message": "controller cancellation delivery proof"})
		for range 2 {
			if err := f.api.Terminal(t.Context(), claim.ID, "cancel-ack", acknowledgement); err != nil {
				t.Fatal("revoked task token prevented daemon cancellation acknowledgement or replay", err)
			}
		}
		if status, err := f.api.TaskStatus(t.Context(), claim.ID); err != nil || status != "cancelled" {
			t.Fatal("cancellation acknowledgement changed the committed task outcome", err, status)
		}
		t.Logf("cancelled task=%s: task token HTTP %d; daemon status=%s; cancel-ack and replay accepted", claim.ID, denied.StatusCode, status)
	})
	t.Run("failed_task_status_survives_task_token_revocation", func(t *testing.T) {
		f.t = t
		f.request(http.MethodPost, "/api/issues", map[string]any{"title": "Failure delivery contract", "status": "todo",
			"assignee_type": "agent", "assignee_id": f.agentID}, &created)
		claim := f.claim("", created.ID, "")
		if err := f.api.StartClaim(t.Context(), claim); err != nil {
			t.Fatal("actual failure task could not start", err)
		}
		body, _ := json.Marshal(map[string]string{"error": "observed worker failure"})
		if err := f.api.Terminal(t.Context(), claim.ID, "fail", body); err != nil {
			t.Fatal("actual backend did not accept the failure", err)
		}
		_, err := f.api.ClaimTaskAssignment(t.Context(), claim)
		var denied *daemonapi.ResponseError
		if !errors.As(err, &denied) || denied.StatusCode != http.StatusUnauthorized && denied.StatusCode != http.StatusForbidden {
			t.Fatal("accepted failure did not revoke ordinary task-token authority", err)
		}
		status, err := f.api.TaskStatus(t.Context(), claim.ID)
		if err != nil || status != "failed" {
			t.Fatal("controller could not observe the committed failure with daemon authority", err, status)
		}
		t.Logf("failed task=%s: task token HTTP %d; daemon status=%s", claim.ID, denied.StatusCode, status)
	})
	t.Run("controller_observation_survives_unusable_task_credentials", func(t *testing.T) {
		f.t = t
		f.request(http.MethodPost, "/api/issues", map[string]any{"title": "Controller result observation contract", "status": "todo",
			"assignee_type": "agent", "assignee_id": f.agentID}, &created)
		claim := f.claim("", created.ID, "")
		if err := f.api.StartClaim(t.Context(), claim); err != nil {
			t.Fatal("actual observation task could not start", err)
		}
		// Exercise a real authentication refusal without changing backend clocks
		// or stored credentials. Controller tests model the 24-hour expiry case.
		unavailable := claim
		unavailable.AuthToken = "mat_" + uuid.NewString()
		_, err := f.api.ClaimTaskAssignment(t.Context(), unavailable)
		var denied *daemonapi.ResponseError
		if !errors.As(err, &denied) || denied.StatusCode != http.StatusUnauthorized && denied.StatusCode != http.StatusForbidden {
			t.Fatal("unusable task credential did not fail ordinary assignment authentication", err)
		}
		observed, err := f.api.ControllerTask(t.Context(), unavailable)
		if err != nil || observed.Status != "running" || !sameObservedTurn(claim, observed) {
			t.Fatal("controller authority could not establish the original running assignment", err)
		}
		root, err := workspace.TaskRoot(wire.WorkspaceRoot, claim.WorkspaceID, claim.ID, claim.WorkspaceSlug, claim.IssueIdentifier)
		if err != nil {
			t.Fatal(err)
		}
		completion := daemonapi.TaskCompleteRequest{Output: "exact result observed with controller credentials", WorkDir: root + "/workdir"}
		body, _ := json.Marshal(completion)
		accepted, err := f.api.TerminalObservation(t.Context(), claim.ID, "complete", body)
		if err != nil || !accepted.AcceptsCompletion(claim, body) {
			t.Fatal("daemon completion did not return the exact accepted result", err)
		}
		observed, err = f.api.ControllerTask(t.Context(), unavailable)
		if err != nil || !observed.AcceptsCompletion(claim, body) {
			t.Fatal("controller read could not recover the exact stored result", err)
		}
		completion.Output = "different result"
		different, _ := json.Marshal(completion)
		if observed.AcceptsCompletion(claim, different) {
			t.Fatal("controller authority replaced exact result matching")
		}
		t.Logf("task=%s: unusable MAT HTTP %d; controller assignment and exact stored completion observed", claim.ID, denied.StatusCode)
	})
}

type backendContract struct {
	t                            *testing.T
	origin, token, workspaceID   string
	daemonID, runtimeID, agentID string
	http                         *http.Client
	api                          *daemonapi.Client
	proof, businessIssueID       string
	prepared                     map[string]bool
}

func (f *backendContract) request(method, path string, input, output any) {
	f.t.Helper()
	f.requestWithToken(f.token, method, path, input, output)
}

func (f *backendContract) requestWithToken(token, method, path string, input, output any) {
	f.t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := http.NewRequestWithContext(f.t.Context(), method, f.origin+path, bytes.NewReader(raw))
	if err != nil {
		f.t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if f.workspaceID != "" {
		r.Header.Set("X-Workspace-ID", f.workspaceID)
	}
	response, err := f.http.Do(r)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, daemonapi.MaxPayload))
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 {
		f.t.Fatalf("%s %s: HTTP %d %s (%v)", method, path, response.StatusCode, scrubBackendProof(body), err)
	}
	if output != nil && json.Unmarshal(body, output) != nil {
		f.t.Fatalf("%s %s returned an unsupported JSON shape", method, path)
	}
}

func (f *backendContract) claim(taskID, issueID, chatID string) daemonapi.Claim {
	f.t.Helper()
	for range 30 {
		if err := f.api.Heartbeat(f.t.Context(), f.runtimeID); err != nil {
			f.t.Fatal(err)
		}
		tasks, err := f.api.Claim(f.t.Context(), f.daemonID, []string{f.runtimeID}, 1)
		if err != nil {
			f.t.Fatal(err)
		}
		if len(tasks) == 1 {
			claim, err := daemonapi.ParseClaim(tasks[0])
			if err != nil || taskID != "" && claim.ID != taskID || claim.IssueID != issueID || claim.ChatSessionID != chatID || !claim.StartClaimSupported {
				f.t.Fatal("backend claimed an unsupported identity or generation", err)
			}
			return claim
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.t.Fatal("public enqueue did not produce a claimable task")
	return daemonapi.Claim{}
}

func (f *backendContract) assertAuthority(claim daemonapi.Claim, ownerID string) workspace.AuthorityEvidence {
	f.t.Helper()
	authority, err := f.api.ObserveAuthority(f.t.Context(), claim)
	if err != nil || !authority.ValidForReuse() || authority.PrincipalID != ownerID || authority.AgentOwnerID != ownerID {
		f.t.Fatal("real task-token authority observation was incomplete", err, authority.Integrations.Reason)
	}
	return authority
}

// This bridge validates live backend data through the production preparation
// adapters. The local descriptor binds input construction; no native process,
// Kubernetes admission, or filesystem preparation is claimed by this test.
func (f *backendContract) prepareAdapterBridge(claim daemonapi.Claim, journal *backendJournal, authority workspace.AuthorityEvidence) {
	f.t.Helper()
	assignment, err := f.api.ClaimTaskAssignment(f.t.Context(), claim)
	if err != nil || assignment.Status != "dispatched" || assignment.RuntimeID != claim.RuntimeID {
		f.t.Fatal("preparation did not begin under the actual dispatched task", err)
	}
	if err := f.api.PrepareLease(f.t.Context(), claim.RuntimeID, claim.ID); err != nil {
		f.t.Fatal("actual preparation lease was refused", err)
	}
	skills, err := f.api.ExecutionSkills(f.t.Context(), claim)
	if err != nil || len(claim.Agent.SkillRefs) == 0 || len(skills) != len(claim.Agent.SkillRefs) {
		f.t.Fatal("actual task skill references did not resolve through the production adapter", err)
	}
	manifests := make(map[string]skillbundle.Manifest)
	var skillProof []map[string]any
	platformSkill := false
	for i, bundle := range skills {
		var reference struct {
			ID        string `json:"id"`
			Source    string `json:"source"`
			FileCount int    `json:"file_count"`
		}
		if json.Unmarshal(claim.Agent.SkillRefs[i], &reference) != nil || reference.ID != bundle.ID || reference.Source != bundle.Source || reference.FileCount != len(bundle.Files) {
			f.t.Fatal("resolved files differ from the actual claim's skill reference")
		}
		files := make([]skillbundle.File, 0, len(bundle.Files))
		for _, file := range bundle.Files {
			if file.SHA256 != "sha256:"+core.Digest([]byte(file.Content)) || file.SizeBytes != int64(len(file.Content)) {
				f.t.Fatal("actual resolved skill file has inconsistent content evidence")
			}
			files = append(files, skillbundle.File{Path: file.Path, Content: file.Content})
		}
		manifest := skillbundle.BuildManifest(skillbundle.Skill{ID: bundle.ID, Source: bundle.Source, Name: bundle.Name,
			Description: bundle.Description, Content: bundle.Content, Files: files})
		if bundle.Hash != manifest.Hash || bundle.SizeBytes != manifest.SizeBytes {
			f.t.Fatal("actual skill bundle does not match the pinned content-hash contract")
		}
		manifests[bundle.Source+":"+bundle.ID] = manifest
		platformSkill = platformSkill || bundle.Source == skillbundle.SourceBuiltin && bundle.Name == "multica-platform" && len(files) > 0
		skillProof = append(skillProof, map[string]any{"id": bundle.ID, "source": bundle.Source, "hash": manifest.Hash, "file_count": manifest.FileCount, "bytes": manifest.SizeBytes})
	}
	if !platformSkill {
		f.t.Fatal("the unchanged backend did not supply its real platform skill bundle")
	}
	mcp, err := f.api.PrepareMCP(f.t.Context(), claim)
	var overlay struct {
		Servers map[string]json.RawMessage `json:"mcpServers"`
	}
	if err != nil || json.Unmarshal(mcp, &overlay) != nil || overlay.Servers == nil || len(overlay.Servers) != 0 || authority.Integrations.State != workspace.IntegrationVerifiedEmpty {
		f.t.Fatal("real native MCP inputs did not establish the supported empty integration case", err)
	}
	groups := []configuration.Group{}
	bundle := configuration.Bundle{Groups: groups, Digest: configuration.Digest(groups)}
	sha, platform := core.Digest([]byte("local adapter bridge descriptor; no native execution")), core.HostPlatform()
	executable := runtimeimage.Executable{Path: "/opt/contract/pi", Version: "contract", SHA256: sha}
	ref := runtimeimage.Ref{Image: "registry.example/contract@sha256:" + sha, Platform: platform, ImageBuildID: uuid.NewString(),
		DescriptorDigest: sha, ConfigurationDigest: configuration.ExecutionDigest(bundle, nil),
		Controller: core.Contract{BuildID: sha, Platform: platform, RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: runtime.Version()},
		Daemon:     runtimeimage.Daemon{Executable: runtimeimage.Executable{Path: "/opt/contract/multica", Version: "contract", SHA256: sha}, AdapterContract: runtimeimage.AdapterContract},
		Providers:  map[string]runtimeimage.Executable{"pi": executable}}
	if err := ref.Validate(); err != nil {
		f.t.Fatal("local input-binding descriptor is invalid", err)
	}
	metadata := wire.Bootstrap{OwnerID: journal.key.OwnerID, TaskID: claim.ID, RuntimeID: claim.RuntimeID, WorkspaceID: claim.WorkspaceID,
		AgentID: claim.AgentID, WorkerSessionID: journal.workerSessionID, WorkspaceAnchorTaskID: journal.storage.WorkspaceAnchorTaskID,
		TaskRoot: journal.storage.TaskRoot, RuntimeRef: ref, Configuration: bundle}
	input, err := daemonapi.ExecutionInput(claim, metadata, executable, daemonapi.ExecutionSettings{Provider: "pi", TaskRoot: journal.storage.TaskRoot, Skills: skills, MCPConfig: mcp})
	if err != nil {
		f.t.Fatal("real claimed context and resolved bundles do not reach native input construction", err)
	}
	var helper struct {
		AgentID, IssueID, ChatSessionID string
		AgentSkills                     []struct {
			Name, Description, Content string
			Files                      []skillbundle.File
		}
	}
	var taskConfig map[string]string
	if json.Unmarshal(input.HelperTask, &helper) != nil || json.Unmarshal(input.Run.TaskConfig, &taskConfig) != nil ||
		input.Run.Options.Cwd != journal.storage.TaskRoot+"/workdir" || helper.AgentID != claim.AgentID || helper.IssueID != claim.IssueID ||
		helper.ChatSessionID != claim.ChatSessionID || len(helper.AgentSkills) != len(skills) || taskConfig["workspace_id"] != claim.WorkspaceID ||
		taskConfig["token"] != claim.AuthToken || taskConfig["server_url"] != wire.RelayURL || input.Run.Environment["MULTICA_TASK_ID"] != claim.ID ||
		input.Run.Environment["MULTICA_TOKEN"] != claim.AuthToken || len(input.SkillRefs) != len(claim.Agent.SkillRefs) || input.Run.Prompt == "" {
		f.t.Fatal("production execution input lost its current task, native root, or task-token binding")
	}
	for i, prepared := range helper.AgentSkills {
		skill := skills[i]
		manifest := skillbundle.BuildManifest(skillbundle.Skill{ID: skill.ID, Source: skill.Source, Name: prepared.Name,
			Description: prepared.Description, Content: prepared.Content, Files: prepared.Files})
		if manifest.Hash != skill.Hash {
			f.t.Fatal("helper input changed resolved skill files or content")
		}
	}
	compatibility, eligible, err := daemonapi.ExecutionCompatibility(claim, journal.key, metadata, input, skills, authority, nil, nil)
	if err != nil || !eligible || compatibility.ProviderOptionsResolved || len(compatibility.SkillHashes) != len(skills) {
		f.t.Fatal("real preparation data cannot form typed compatibility inputs", err)
	}
	for _, skill := range compatibility.SkillHashes {
		if expected, found := manifests[skill.ID]; !found || skill.Digest != strings.TrimPrefix(expected.Hash, "sha256:") {
			f.t.Fatal("compatibility did not bind the actually resolved skill content")
		}
	}
	digest, err := compatibility.Digest()
	if err != nil {
		f.t.Fatal(err)
	}
	run, _ := json.Marshal(input.Run)
	receipt, _ := json.MarshalIndent(map[string]any{"task_id": claim.ID, "kind": claim.Kind, "conversation_kind": journal.key.Kind,
		"task_root": journal.storage.TaskRoot, "preparation_lease": true, "skill_references": len(claim.Agent.SkillRefs), "skills": skillProof,
		"native_mcp_empty": true, "run_sha256": core.Digest(run), "helper_task_sha256": core.Digest(input.HelperTask), "compatibility_digest": digest,
		"native_defaults_resolved": false, "native_execution": false, "local_descriptor_binding": true}, "", "  ")
	if err := os.WriteFile(filepath.Join(f.proof, "adapter-bridge-"+claim.ID+".json"), append(receipt, '\n'), 0600); err != nil {
		f.t.Fatal(err)
	}
	f.prepared[claim.ID] = true
}

func (f *backendContract) assertCurrentTaskBusinessRead(claim daemonapi.Claim) {
	f.t.Helper()
	request, err := http.NewRequestWithContext(f.t.Context(), http.MethodGet, f.origin+"/api/issues/"+f.businessIssueID, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+claim.AuthToken)
	request.Header.Set("X-Workspace-ID", claim.WorkspaceID)
	response, err := f.http.Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	defer response.Body.Close()
	var issue struct{ ID string }
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, daemonapi.MaxPayload)).Decode(&issue) != nil || issue.ID != f.businessIssueID {
		f.t.Fatal("current running task token could not read its workspace issue", response.StatusCode)
	}
	f.t.Log("current task-token business read verified", "kind", claim.Kind, "task_id", claim.ID, "issue_id", issue.ID)
}

func (f *backendContract) complete(j *backendJournal, claim daemonapi.Claim, session string, missing bool) workspace.TaskGrant {
	f.t.Helper()
	if err := f.api.StartClaim(f.t.Context(), claim); err != nil {
		f.t.Fatal(err)
	}
	if f.prepared[claim.ID] {
		f.assertCurrentTaskBusinessRead(claim)
	}
	body, _ := json.Marshal(daemonapi.TaskCompleteRequest{Output: "Observed task " + claim.ID, SessionID: session,
		WorkDir: j.storage.TaskRoot + "/workdir", SessionRolloutMissing: missing})
	row, err := f.api.TerminalObservation(f.t.Context(), claim.ID, "complete", body)
	if err != nil || !row.AcceptsCompletion(claim, body) {
		f.t.Fatal("real backend did not return the accepted producing request", err)
	}
	return j.record(f.t, claim, "complete", body, session, nil, false)
}

func (f *backendContract) selectSource(j *backendJournal, claim daemonapi.Claim, mode workspace.SelectionMode, sessionSource, workspaceSource workspace.TaskGrant) workspace.Selection {
	f.t.Helper()
	rows, err := f.api.AgentTaskHistory(f.t.Context(), claim)
	if err != nil {
		f.t.Fatal("ordinary agent history contract failed", err)
	}
	if claim.IssueID != "" {
		rows, err = f.api.IssueTaskHistory(f.t.Context(), claim)
		if err != nil {
			f.t.Fatal("ordinary issue history contract failed", err)
		}
	}
	j.evidence.History = rows
	selection := selectConversationSource(claim, j.key, j.compatibility, true, j.evidence)
	if selection.Mode != mode || selection.SessionSource != grantSource(sessionSource) || selection.WorkspaceSource != grantSource(workspaceSource) {
		f.t.Fatalf("actual backend selection = %s (%s), want %s with the exact sources", selection.Mode, selection.Reason, mode)
	}
	return selection
}

// Local signed witnesses model the journal boundary already covered by Store
// tests. They never fabricate backend claims, rows, or acceptance observations.
type backendJournal struct {
	key             workspace.ConversationKey
	compatibility   string
	workerSessionID string
	keyPair         ed25519.PrivateKey
	storage         workspace.Storage
	evidence        continuityEvidence
}

func newBackendJournal(t *testing.T, claim daemonapi.Claim) *backendJournal {
	t.Helper()
	key := daemonapi.ConversationIdentity(uuid.NewString(), claim)
	root, err := workspace.TaskRoot(wire.WorkspaceRoot, claim.WorkspaceID, claim.ID, claim.WorkspaceSlug, claim.IssueIdentifier)
	if err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := core.Digest([]byte("isolated, fixed local execution configuration"))
	return &backendJournal{key: key, compatibility: compatibility, workerSessionID: uuid.NewString(), keyPair: private,
		storage: workspace.Storage{ID: uuid.NewString(), Conversation: key, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID,
			TaskID: claim.ID, WorkspaceAnchorTaskID: claim.ID, TaskRoot: root, CompatibilityDigest: compatibility},
		evidence: continuityEvidence{Terminals: make(map[string]workspace.Terminal), HistoryKnown: true}}
}

func (j *backendJournal) record(t *testing.T, claim daemonapi.Claim, kind string, body []byte, session string, selected *workspace.Selection, transientResumeFailure bool) workspace.TaskGrant {
	t.Helper()
	sequence := uint64(len(j.evidence.Grants) + 1)
	selection := workspace.Selection{Conversation: j.key, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: workspace.SelectionFreshWorkspace}
	if selected != nil {
		selection = *selected
	}
	g := workspace.TaskGrant{SessionProtocol: true, OwnerID: j.key.OwnerID, WorkspaceID: claim.WorkspaceID, AgentID: claim.AgentID,
		RuntimeID: claim.RuntimeID, TaskID: claim.ID, AttemptID: uuid.NewString(), Conversation: j.key, CompatibilityDigest: j.compatibility,
		StorageID: j.storage.ID, TaskRoot: j.storage.TaskRoot, WorkspaceAnchorTaskID: j.storage.WorkspaceAnchorTaskID,
		WorkerSessionID: j.workerSessionID, TurnSequence: sequence, Generation: 1, StartConfirmed: true, TurnComplete: true,
		Envelope: claim.Envelope, State: "closed", SupervisorKey: j.keyPair.Public().(ed25519.PublicKey), Selection: &selection,
		PendingResume: &workspace.ResumePointers{SessionID: session, WorkDir: j.storage.TaskRoot + "/workdir"}}
	status := "completed"
	if kind == "fail" {
		status = "failed"
		g.Prepared, g.ResumeSession = &workspace.Prepared{Provider: "pi"}, selection.SessionID
		g.PendingResume.ResumeRejectedTransient = transientResumeFailure
	} else {
		var request daemonapi.TaskCompleteRequest
		if err := json.Unmarshal(body, &request); err != nil {
			t.Fatal(err)
		}
		g.PendingResume.RetiredSessionID = request.RetiredSessionID
		g.PendingResume.SessionRolloutMissing = request.SessionRolloutMissing
	}
	terminal := workspace.Terminal{TaskID: claim.ID, AttemptID: g.AttemptID, Source: "worker", Kind: kind, State: "delivered",
		Body: bytes.Clone(body), RequestDigest: workspace.TerminalDigest(kind, body), ResultDigest: core.Digest(body), Nonce: uuid.NewString()}
	terminal.ResultReceipt = &workspace.ResultReceipt{WorkerSessionID: j.workerSessionID, TurnSequence: sequence, TaskID: claim.ID,
		AttemptID: g.AttemptID, RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}
	terminal.ResultReceipt.Signature = ed25519.Sign(j.keyPair, workspace.ResultReceiptMessage(*terminal.ResultReceipt))
	g.CompletionWitness = &workspace.CompletionWitness{TaskID: claim.ID, AttemptID: g.AttemptID, WorkerSessionID: j.workerSessionID,
		StorageID: j.storage.ID, TurnSequence: sequence, RequestDigest: terminal.RequestDigest, ResultDigest: terminal.ResultDigest,
		Status: status, SessionID: session, WorkDir: j.storage.TaskRoot + "/workdir", AcknowledgedAt: time.Now().UTC()}
	j.storage.LatestWriter = grantSource(g)
	j.evidence.Grants = append(j.evidence.Grants, g)
	j.evidence.Terminals[g.AttemptID] = terminal
	j.evidence.Storages = []workspace.Storage{j.storage}
	return g
}

type backendContractTransport struct {
	base   http.RoundTripper
	output io.Writer
	mu     sync.Mutex
}

func (r *backendContractTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var sent []byte
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		sent, _ = io.ReadAll(body)
		_ = body.Close()
	}
	response, err := r.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, daemonapi.MaxPayload+1))
	_ = response.Body.Close()
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	r.mu.Lock()
	defer r.mu.Unlock()
	authorizationKind := "none"
	for prefix, kind := range map[string]string{"Bearer mat_": "task", "Bearer mdt_": "daemon", "Bearer mul_": "personal_access_token"} {
		if strings.HasPrefix(request.Header.Get("Authorization"), prefix) {
			authorizationKind = kind
		}
	}
	err = json.NewEncoder(r.output).Encode(map[string]any{"method": request.Method, "path": request.URL.RequestURI(),
		"workspace": request.Header.Get("X-Workspace-ID"), "status": response.StatusCode, "authorization_kind": authorizationKind,
		"request_sha256": core.Digest(sent), "response_sha256": core.Digest(body),
		"request": json.RawMessage(scrubBackendProof(sent)), "response": json.RawMessage(scrubBackendProof(body))})
	return response, err
}

func scrubBackendProof(raw []byte) []byte {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		encoded, _ := json.Marshal(string(raw))
		return encoded
	}
	var scrub func(any)
	scrub = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			for key, child := range value {
				if key == "token" || key == "auth_token" || key == "remote_mcp_daemon_token" || key == "code" {
					value[key] = "[disposable proof secret]"
				} else if key == "content" || key == "prompt" {
					if text, ok := child.(string); ok {
						value[key] = fmt.Sprintf("[content omitted: bytes=%d sha256=%s]", len(text), core.Digest([]byte(text)))
					} else {
						scrub(child)
					}
				} else {
					scrub(child)
				}
			}
		case []any:
			for _, child := range value {
				scrub(child)
			}
		}
	}
	scrub(value)
	encoded, _ := json.Marshal(value)
	return encoded
}
