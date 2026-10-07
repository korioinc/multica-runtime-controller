package controller

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

type continuityFixture struct {
	key           workspace.ConversationKey
	compatibility string
	runtimeID     string
	sessionID     string
	storage       workspace.Storage
	evidence      continuityEvidence
	base          time.Time
}

func newContinuityFixture(t *testing.T, kind workspace.ConversationKind) *continuityFixture {
	t.Helper()
	key := workspace.ConversationKey{OwnerID: uuid.NewString(), WorkspaceID: uuid.NewString(), AgentID: uuid.NewString(), SubjectID: uuid.NewString(), Kind: kind}
	anchor := uuid.NewString()
	root, err := workspace.TaskRoot(wire.WorkspaceRoot, key.WorkspaceID, anchor, "", "")
	if err != nil {
		t.Fatal(err)
	}
	compatibility := core.Digest([]byte("observed compatible authority"))
	return &continuityFixture{key: key, compatibility: compatibility, runtimeID: uuid.NewString(), sessionID: uuid.NewString(),
		storage: workspace.Storage{ID: uuid.NewString(), Conversation: key, CompatibilityDigest: compatibility,
			WorkspaceAnchorTaskID: anchor, TaskID: anchor, WorkspaceID: key.WorkspaceID, AgentID: key.AgentID, TaskRoot: root},
		evidence: continuityEvidence{Terminals: make(map[string]workspace.Terminal), HistoryKnown: true},
		base:     time.Date(2026, 10, 4, 1, 0, 0, 0, time.UTC)}
}

func (f *continuityFixture) claim(t *testing.T, taskID string, at time.Time) daemonapi.Claim {
	t.Helper()
	fields := map[string]any{"id": taskID, "workspace_id": f.key.WorkspaceID, "agent_id": f.key.AgentID,
		"runtime_id": f.runtimeID, "agent": map[string]string{"id": f.key.AgentID}, "auth_token": "mat_fixture",
		"start_claim_supported": true, "dispatched_at": at.Format(time.RFC3339Nano), "kind": "direct"}
	if f.key.Kind == workspace.ConversationIssue {
		fields["issue_id"] = f.key.SubjectID
	} else {
		fields["kind"], fields["chat_session_id"] = "chat", f.key.SubjectID
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := daemonapi.ParseClaim(raw)
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

// These are asserted journal witnesses and supported task-history observations.
// Store signature/transaction enforcement has its own decision-owning coverage.
func (f *continuityFixture) complete(t *testing.T, session string, at time.Time) workspace.TaskGrant {
	t.Helper()
	sequence := uint64(len(f.evidence.Grants) + 1)
	taskID := uuid.NewString()
	if sequence == 1 {
		taskID = f.storage.WorkspaceAnchorTaskID
	}
	claim := f.claim(t, taskID, at.Add(-time.Second+time.Duration(sequence)*time.Microsecond))
	request := daemonapi.TaskCompleteRequest{Output: taskID, SessionID: session, WorkDir: f.storage.TaskRoot + "/workdir"}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	grant := workspace.TaskGrant{SessionProtocol: true, OwnerID: f.key.OwnerID, WorkspaceID: f.key.WorkspaceID, AgentID: f.key.AgentID,
		TaskID: taskID, AttemptID: uuid.NewString(), Conversation: f.key, CompatibilityDigest: f.compatibility, RuntimeID: f.runtimeID,
		StorageID: f.storage.ID, TaskRoot: f.storage.TaskRoot, WorkspaceAnchorTaskID: f.storage.WorkspaceAnchorTaskID,
		WorkerSessionID: f.sessionID, TurnSequence: sequence, StartConfirmed: true, TurnComplete: true,
		Envelope: claim.Envelope, State: "closed", Generation: 1, SupervisorKey: key.Public().(ed25519.PublicKey),
		Selection:     &workspace.Selection{Conversation: f.key, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: workspace.SelectionFreshWorkspace},
		PendingResume: &workspace.ResumePointers{SessionID: session, WorkDir: request.WorkDir}}
	terminal := workspace.Terminal{TaskID: taskID, AttemptID: grant.AttemptID, Source: "worker", Kind: "complete", State: "delivered",
		Body: body, RequestDigest: workspace.TerminalDigest("complete", body), ResultDigest: core.Digest([]byte(taskID)), Nonce: uuid.NewString()}
	terminal.ResultReceipt = &workspace.ResultReceipt{WorkerSessionID: f.sessionID, TurnSequence: sequence, TaskID: taskID,
		AttemptID: grant.AttemptID, RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}
	terminal.ResultReceipt.Signature = ed25519.Sign(key, workspace.ResultReceiptMessage(*terminal.ResultReceipt))
	grant.CompletionWitness = &workspace.CompletionWitness{TaskID: taskID, AttemptID: grant.AttemptID, WorkerSessionID: f.sessionID,
		StorageID: f.storage.ID, TurnSequence: sequence, RequestDigest: terminal.RequestDigest, ResultDigest: terminal.ResultDigest,
		Status: "completed", SessionID: session, WorkDir: request.WorkDir, AcknowledgedAt: at}
	row := daemonapi.TaskObservation{ID: taskID, WorkspaceID: f.key.WorkspaceID, AgentID: f.key.AgentID, RuntimeID: f.runtimeID,
		IssueID: claim.IssueID, ChatSessionID: claim.ChatSessionID, Kind: claim.Kind, Status: "completed", Result: &request, WorkDir: request.WorkDir,
		CreatedAt: at.Add(-2 * time.Second).Format(time.RFC3339), DispatchedAt: at.Add(-time.Second).Format(time.RFC3339),
		StartedAt: at.Add(-time.Second).Format(time.RFC3339), CompletedAt: at.Format(time.RFC3339)}
	f.evidence.Grants = append(f.evidence.Grants, grant)
	f.evidence.History = append(f.evidence.History, row)
	f.evidence.Terminals[grant.AttemptID] = terminal
	f.storage.LatestWriter = grantSource(grant)
	f.evidence.Storages = []workspace.Storage{f.storage}
	return grant
}

func (f *continuityFixture) followup(t *testing.T, session string) daemonapi.Claim {
	t.Helper()
	claim := f.claim(t, uuid.NewString(), f.base.Add(time.Hour))
	claim.PriorSessionID, claim.PriorWorkDir = session, f.storage.TaskRoot+"/workdir"
	claim.NewCommentsDeltaKnown = f.key.Kind == workspace.ConversationIssue
	return claim
}

func (f *continuityFixture) transientFailure(t *testing.T, source workspace.TaskGrant, at time.Time) workspace.TaskGrant {
	t.Helper()
	grant := f.complete(t, "", at)
	grant.Prepared = &workspace.Prepared{Provider: "pi"}
	grant.ResumeSession = source.CompletionWitness.SessionID
	grant.Selection = &workspace.Selection{Conversation: f.key, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: workspace.SelectionResume,
		StorageID: f.storage.ID, SessionSource: grantSource(source), WorkspaceSource: grantSource(source),
		LatestWriter: grantSource(source), SessionID: grant.ResumeSession, WorkDir: f.storage.TaskRoot + "/workdir"}
	grant.PendingResume.ResumeRejectedTransient = true
	grant.CompletionWitness.Status = "failed"
	errorText := `Pi session file "` + grant.ResumeSession + `" is already in use by another execution`
	body, err := json.Marshal(map[string]string{"error": errorText, "work_dir": grant.PendingResume.WorkDir})
	if err != nil {
		t.Fatal(err)
	}
	terminal := f.evidence.Terminals[grant.AttemptID]
	terminal.Kind, terminal.Body, terminal.RequestDigest = "fail", body, workspace.TerminalDigest("fail", body)
	terminal.ResultReceipt.RequestDigest = terminal.RequestDigest
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	terminal.ResultReceipt.Signature = ed25519.Sign(key, workspace.ResultReceiptMessage(*terminal.ResultReceipt))
	grant.CompletionWitness.RequestDigest = terminal.RequestDigest
	f.evidence.Terminals[grant.AttemptID] = terminal
	f.evidence.Grants[len(f.evidence.Grants)-1] = grant
	row := &f.evidence.History[len(f.evidence.History)-1]
	row.Status, row.Result, row.Error = "failed", nil, errorText
	row.FailureReason = taskfailure.NormalizeDaemonReason(taskfailure.Classify(errorText).String(), errorText).String()
	return grant
}

func (f *continuityFixture) selectSource(claim daemonapi.Claim) workspace.Selection {
	return selectConversationSource(claim, f.key, f.compatibility, true, f.evidence)
}

func TestConversationSelectionPreservesExactProducersAndWriterFences(t *testing.T) {
	t.Run("issue repeated pointers advance the producing task", func(t *testing.T) {
		f := newContinuityFixture(t, workspace.ConversationIssue)
		a := f.complete(t, "same-native-session", f.base)
		if selected := f.selectSource(f.followup(t, a.CompletionWitness.SessionID)); selected.Mode != workspace.SelectionResume || selected.SessionSource != grantSource(a) {
			t.Fatal("a proven first completion could not seed the follow-up")
		}
		b := f.complete(t, a.CompletionWitness.SessionID, f.base.Add(10*time.Second))
		selected := f.selectSource(f.followup(t, b.CompletionWitness.SessionID))
		if selected.Mode != workspace.SelectionResume || selected.SessionSource != grantSource(b) || selected.WorkspaceSource != grantSource(b) {
			t.Fatal("equal pointer strings relabelled the latest completed producer")
		}
	})
	t.Run("named older rerun retains its source and the latest writer", func(t *testing.T) {
		f := newContinuityFixture(t, workspace.ConversationIssue)
		a := f.complete(t, "same-native-session", f.base)
		b := f.complete(t, a.CompletionWitness.SessionID, f.base.Add(10*time.Second))
		claim := f.followup(t, a.CompletionWitness.SessionID)
		claim.NewCommentsDeltaKnown = false
		claim.Attribution = &daemonapi.TaskAttribution{RerunOfTaskID: a.TaskID}
		selected := f.selectSource(claim)
		if selected.Mode != workspace.SelectionResume || selected.SessionSource != grantSource(a) || selected.WorkspaceSource != grantSource(a) || selected.LatestWriter != grantSource(b) {
			t.Fatal("named rerun changed its producer or bypassed the latest writer")
		}
		claim.PriorSessionID = ""
		selected = f.selectSource(claim)
		if selected.Mode != workspace.SelectionFreshSession || selected.WorkspaceSource != grantSource(a) || selected.LatestWriter != grantSource(b) {
			t.Fatal("effective fresh-session rerun lost its proven workspace")
		}
	})
	t.Run("finishing source is selected before quiescence", func(t *testing.T) {
		f := newContinuityFixture(t, workspace.ConversationIssue)
		a := f.complete(t, "native-session", f.base)
		f.evidence.Grants[0].TurnComplete = false
		f.evidence.Grants[0].State = "terminal_received"
		f.evidence.Storages[0].Dirty, f.evidence.Storages[0].ActiveAttempt = true, a.AttemptID
		selected := f.selectSource(f.followup(t, a.CompletionWitness.SessionID))
		if selected.Mode != workspace.SelectionResume || selected.LatestWriter != grantSource(a) {
			t.Fatal("temporary finishing state discarded the claim's selected workspace")
		}
	})
	t.Run("queued named source retains an uncompleted later writer", func(t *testing.T) {
		f := newContinuityFixture(t, workspace.ConversationIssue)
		a := f.complete(t, "native-session", f.base)
		b := f.complete(t, a.CompletionWitness.SessionID, f.base.Add(10*time.Second))
		f.evidence.Grants[1].CompletionWitness = nil
		f.evidence.Grants[1].PendingResume = nil
		f.evidence.Grants[1].TurnComplete = false
		f.evidence.Grants[1].State = "running"
		delete(f.evidence.Terminals, b.AttemptID)
		f.evidence.History[1].Status, f.evidence.History[1].Result, f.evidence.History[1].CompletedAt = "running", nil, ""
		f.evidence.Storages[0].Dirty, f.evidence.Storages[0].ActiveAttempt = true, b.AttemptID
		claim := f.followup(t, a.CompletionWitness.SessionID)
		claim.Attribution = &daemonapi.TaskAttribution{RerunOfTaskID: a.TaskID}
		selected := f.selectSource(claim)
		if selected.Mode != workspace.SelectionResume || selected.SessionSource != grantSource(a) || selected.WorkspaceSource != grantSource(a) || selected.LatestWriter != grantSource(b) {
			t.Fatal("running later writer discarded or relabelled the selected accepted source")
		}
		f.evidence.Grants[1].State = "terminal_received"
		finishing := f.selectSource(claim)
		if finishing.Mode != workspace.SelectionResume || finishing.LatestWriter != grantSource(b) || finishing.EvidenceDigest == selected.EvidenceDigest {
			t.Fatal("writer transition lost its independent fence or observation digest")
		}
		f.evidence.Grants[1].CompatibilityDigest = core.Digest([]byte("foreign writer authority"))
		if rejected := f.selectSource(claim); rejected.Mode != workspace.SelectionFreshWorkspace {
			t.Fatal("foreign latest writer retained workspace authority")
		}
	})
	t.Run("DM same-second causal completions advance both pointers", func(t *testing.T) {
		f := newContinuityFixture(t, workspace.ConversationAgentDM)
		a := f.complete(t, "same-native-session", f.base)
		b := f.complete(t, a.CompletionWitness.SessionID, f.base)
		selected := f.selectSource(f.followup(t, b.CompletionWitness.SessionID))
		if selected.Mode != workspace.SelectionResume || selected.SessionSource != grantSource(b) || selected.WorkspaceSource != grantSource(b) {
			t.Fatal("transactional DM pointer writes lost their proven local order")
		}
	})
	t.Run("DM sessionless successor preserves independent session source", func(t *testing.T) {
		f := newContinuityFixture(t, workspace.ConversationAgentDM)
		a := f.complete(t, "native-session", f.base)
		b := f.complete(t, "", f.base.Add(10*time.Second))
		selected := f.selectSource(f.followup(t, a.CompletionWitness.SessionID))
		if selected.Mode != workspace.SelectionResume || selected.SessionSource != grantSource(a) || selected.WorkspaceSource != grantSource(b) || selected.LatestWriter != grantSource(b) {
			t.Fatal("workspace-only publication overwrote the session's producer")
		}
	})
	for _, scenario := range []string{"issue timestamp tie", "external same-value producer", "incomplete completed source", "unknown issue anchor", "foreign path", "changed compatibility", "late cancelled DM after sessionless successor"} {
		t.Run(scenario, func(t *testing.T) {
			kind := workspace.ConversationIssue
			if scenario == "late cancelled DM after sessionless successor" {
				kind = workspace.ConversationAgentDM
			}
			f := newContinuityFixture(t, kind)
			a := f.complete(t, "native-session", f.base)
			claim := f.followup(t, a.CompletionWitness.SessionID)
			switch scenario {
			case "issue timestamp tie":
				f.complete(t, a.CompletionWitness.SessionID, f.base)
			case "external same-value producer", "incomplete completed source":
				row := f.evidence.History[0]
				row.ID, row.CompletedAt = uuid.NewString(), f.base.Add(time.Second).Format(time.RFC3339)
				if scenario == "incomplete completed source" {
					row.Result = nil
				}
				f.evidence.History = append(f.evidence.History, row)
			case "unknown issue anchor":
				claim.NewCommentsDeltaKnown = false
			case "foreign path":
				claim.PriorWorkDir = "/workspace/foreign/workdir"
			case "changed compatibility":
				f.compatibility = core.Digest([]byte("different authority"))
			case "late cancelled DM after sessionless successor":
				f.complete(t, "", f.base.Add(10*time.Second))
				row := f.evidence.History[0]
				row.ID, row.Status, row.Result, row.StartedAt = uuid.NewString(), "cancelled", nil, ""
				row.CreatedAt, row.CompletedAt = f.base.Add(2*time.Second).Format(time.RFC3339), f.base.Add(3*time.Second).Format(time.RFC3339)
				f.evidence.History = append(f.evidence.History, row)
			}
			if selected := f.selectSource(claim); selected.Mode != workspace.SelectionFreshWorkspace {
				t.Fatal("ambiguous or foreign source evidence authorized inherited files or history")
			}
		})
	}
}

func TestTransientResumeFailurePreservesCompletedSessionProducer(t *testing.T) {
	for _, kind := range []workspace.ConversationKind{workspace.ConversationIssue, workspace.ConversationAgentDM} {
		t.Run(string(kind), func(t *testing.T) {
			f := newContinuityFixture(t, kind)
			a := f.complete(t, "native-session", f.base)
			b := f.transientFailure(t, a, f.base.Add(10*time.Second))
			claim := f.followup(t, a.CompletionWitness.SessionID)
			claim.Attribution = &daemonapi.TaskAttribution{RetryOfTaskID: b.TaskID}
			selected := f.selectSource(claim)
			workspaceProducer := grantSource(a)
			if kind == workspace.ConversationAgentDM {
				workspaceProducer = grantSource(b)
			}
			if selected.Mode != workspace.SelectionResume || selected.SessionSource != grantSource(a) || selected.WorkspaceSource != workspaceProducer || selected.LatestWriter != grantSource(b) {
				t.Fatal("transient failure retired or relabelled the completed session", selected)
			}
			f.evidence.Grants[1].State, f.evidence.Grants[1].TurnComplete = "terminal_received", false
			f.evidence.Storages[0].Dirty, f.evidence.Storages[0].ActiveAttempt = true, b.AttemptID
			if pending := f.selectSource(claim); pending.Mode != workspace.SelectionResume || pending.LatestWriter != grantSource(b) {
				t.Fatal("accepted transient failure lost its pending writer fence")
			}
			claim.PriorSessionID = ""
			if fresh := f.selectSource(claim); fresh.Mode != workspace.SelectionFreshSession || fresh.WorkspaceSource != grantSource(b) || fresh.SessionSource != (workspace.CheckpointSource{}) {
				t.Fatal("explicit workspace-only retry lost the exact failed publisher", fresh)
			}
		})
	}
	for _, scenario := range []string{"missing native transient flag", "unaccepted failure", "wrong accepted error", "unknown completed session source", "different provider", "new session on failure"} {
		t.Run(scenario, func(t *testing.T) {
			f := newContinuityFixture(t, workspace.ConversationAgentDM)
			a := f.complete(t, "native-session", f.base)
			b := f.transientFailure(t, a, f.base.Add(10*time.Second))
			switch scenario {
			case "missing native transient flag":
				f.evidence.Grants[1].PendingResume.ResumeRejectedTransient = false
			case "unaccepted failure":
				f.evidence.Grants[1].CompletionWitness = nil
			case "wrong accepted error":
				f.evidence.History[1].Error = "another failure"
			case "unknown completed session source":
				f.evidence.Grants[1].Selection.SessionSource.TaskID = uuid.NewString()
			case "different provider":
				f.evidence.Grants[1].Prepared.Provider = "codex"
			case "new session on failure":
				f.evidence.Grants[1].PendingResume.SessionID = "unverified-native-session"
			}
			claim := f.followup(t, a.CompletionWitness.SessionID)
			if selected := f.selectSource(claim); selected.Mode != workspace.SelectionFreshWorkspace {
				t.Fatal("generic or unproven failure acquired workspace publication authority")
			}
			claim.PriorSessionID = ""
			claim.Attribution = &daemonapi.TaskAttribution{RetryOfTaskID: b.TaskID}
			if selected := f.selectSource(claim); selected.Mode != workspace.SelectionFreshWorkspace {
				t.Fatal("unproven named failure acquired retained workspace authority")
			}
		})
	}
}
