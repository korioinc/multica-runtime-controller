package controller

import (
	"encoding/json"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type continuityEvidence struct {
	Grants       []workspace.TaskGrant
	Storages     []workspace.Storage
	Terminals    map[string]workspace.Terminal
	History      []daemonapi.TaskObservation
	HistoryKnown bool
	WarmSessions map[string]workspace.WorkerSession
}

type continuityFacts struct {
	key           workspace.ConversationKey
	compatibility string
	history       []daemonapi.TaskObservation
	rows          map[string]daemonapi.TaskObservation
	grants        map[string]workspace.TaskGrant
	witnesses     map[string]workspace.TaskGrant
	storages      map[string]workspace.Storage
	terminals     map[string]workspace.Terminal
}

// selectConversationSource compares hints with accepted producing executions.
// It does not turn a backend path into mount authority or release a writer.
func selectConversationSource(claim daemonapi.Claim, key workspace.ConversationKey, compatibilityDigest string, eligible bool, evidence continuityEvidence) workspace.Selection {
	fresh := workspace.Selection{Conversation: key, ReuseEligible: eligible && key.Kind != workspace.ConversationTask, WorkspaceReuseEligible: eligible,
		Mode: workspace.SelectionFreshWorkspace}
	deny := func(reason string) workspace.Selection { fresh.Reason = reason; return fresh }
	if !eligible || !key.Valid() || key.Kind == workspace.ConversationTask || !core.ValidSHA(compatibilityDigest) ||
		claim.WorkspaceID != key.WorkspaceID || claim.AgentID != key.AgentID || !claim.StartClaimSupported {
		fresh.ReuseEligible = false
		return deny("continuity_authority_unknown")
	}
	if !observesConversation(claim, key) {
		fresh.ReuseEligible = false
		return deny("conversation_scope_mismatch")
	}
	if claim.PriorWorkDir == "" {
		return deny("selected_workspace_unavailable")
	}
	if !path.IsAbs(claim.PriorWorkDir) || path.Clean(claim.PriorWorkDir) != claim.PriorWorkDir || strings.ContainsAny(claim.PriorWorkDir, "\x00\r\n\\") {
		return deny("selected_workspace_invalid")
	}
	facts, ok := observeContinuityFacts(claim, key, compatibilityDigest, evidence)
	if !ok {
		return deny("history_scope_ambiguous")
	}
	var sessionSource, workspaceSource workspace.TaskGrant
	var reason string
	named := ""
	if claim.Attribution != nil {
		if key.Kind == workspace.ConversationIssue {
			named = claim.Attribution.RerunOfTaskID
		}
		if named == "" && claim.PriorSessionID == "" {
			named = claim.Attribution.RetryOfTaskID
		}
	}
	if named != "" {
		workspaceSource, ok = facts.witnesses[named]
		if !ok || workspaceSource.CompletionWitness.WorkDir != claim.PriorWorkDir {
			return deny("named_source_unwitnessed")
		}
		if claim.PriorSessionID != "" {
			sessionSource = workspaceSource
			if sessionSource.CompletionWitness.SessionID != claim.PriorSessionID {
				return deny("named_session_mismatch")
			}
		}
	} else {
		if !evidence.HistoryKnown {
			return deny("history_unavailable")
		}
		if key.Kind == workspace.ConversationIssue {
			if claim.PriorSessionID == "" {
				return deny("workspace_source_unselected")
			}
			if !claim.IssueStateDeltaKnown && !claim.NewCommentsDeltaKnown {
				// The public issue log omits some unstarted cancelled rows.
				// Pointer equality cannot prove the selected row was completed.
				return deny("completed_source_unproven")
			}
			sessionSource, reason = facts.issueSource(claim)
			workspaceSource = sessionSource
		} else {
			sessionSource, workspaceSource, reason = facts.chatSources(claim)
		}
		if reason != "" {
			return deny(reason)
		}
	}
	if claim.PriorSessionID != "" && facts.retired(claim.PriorSessionID) {
		return deny("selected_session_retired")
	}
	storage, ok := facts.storages[workspaceSource.StorageID]
	if !ok || storage.TaskRoot+"/workdir" != claim.PriorWorkDir || storage.Quarantined {
		return deny("selected_storage_unavailable")
	}
	if storage.ActiveAttempt == "" && storage.Dirty {
		owner, accepted := evidence.WarmSessions[storage.ID]
		if !accepted || owner.ProtocolVersion != wire.SessionProtocolVersion || owner.ID != storage.WriterSessionID ||
			owner.Conversation != key || owner.StorageID != storage.ID || owner.State != workspace.SessionIdle || owner.Stop != nil ||
			owner.ActiveAttempt != "" || owner.LastCompleteAttempt != storage.LatestWriter.AttemptID {
			return deny("selected_storage_unavailable")
		}
	}
	if sessionSource.AttemptID != "" && sessionSource.StorageID != storage.ID {
		return deny("independent_sources_use_different_storage")
	}
	latest, ok := facts.grants[storage.LatestWriter.AttemptID]
	if !ok || grantSource(latest) != storage.LatestWriter || !facts.validLocalWriter(latest, storage) {
		return deny("latest_writer_unproven")
	}
	selection := workspace.Selection{Conversation: key, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: workspace.SelectionFreshSession,
		StorageID: storage.ID, WorkspaceSource: grantSource(workspaceSource), LatestWriter: storage.LatestWriter, WorkDir: storage.TaskRoot + "/workdir"}
	if sessionSource.AttemptID != "" {
		selection.Mode, selection.SessionSource, selection.SessionID = workspace.SelectionResume, grantSource(sessionSource), claim.PriorSessionID
	}
	// Canonical ordering here hashes an evidence set. It never breaks a tie
	// when selecting the producing execution.
	history := slices.Clone(facts.history)
	slices.SortFunc(history, func(a, b daemonapi.TaskObservation) int { return strings.Compare(a.ID, b.ID) })
	raw, _ := json.Marshal([]any{"multica-continuity-selection-v1", key, claim.ID, claim.DispatchedAt, named, selection,
		workspaceSource.CompletionWitness, sessionSource.CompletionWitness,
		[]any{grantSource(latest), latest.WorkerSessionID, latest.TurnSequence, latest.State, latest.StartConfirmed, latest.TurnComplete,
			latest.CompletionWitness, storage.ActiveAttempt, storage.Dirty}, evidence.HistoryKnown, history})
	selection.EvidenceDigest = core.Digest(raw)
	return selection
}

func grantSource(grant workspace.TaskGrant) workspace.CheckpointSource {
	return workspace.CheckpointSource{TaskID: grant.TaskID, AttemptID: grant.AttemptID}
}

// The latest local writer can still be running when a claim selects an older
// accepted source. Retaining that fence does not authorize the next writer.
func (f continuityFacts) validLocalWriter(grant workspace.TaskGrant, storage workspace.Storage) bool {
	if grant.Legacy || !grant.SessionProtocol || grant.Conversation != f.key || grant.CompatibilityDigest != f.compatibility ||
		!wire.UUID(grant.TaskID) || !wire.UUID(grant.WorkerSessionID) || !wire.UUID(grant.RuntimeID) || grant.TurnSequence == 0 ||
		grant.Selection == nil || !grant.Selection.ReuseEligible || grant.StorageID != storage.ID || grant.TaskRoot != storage.TaskRoot ||
		grant.WorkspaceAnchorTaskID != storage.WorkspaceAnchorTaskID || storage.Conversation != f.key || grant.WorkspaceCompatibilityDigest != storage.WorkspaceCompatibilityDigest ||
		workspace.ValidateTaskRoot(wire.WorkspaceRoot, storage.TaskRoot, f.key.WorkspaceID, storage.WorkspaceAnchorTaskID) != nil {
		return false
	}
	original, err := daemonapi.ParseClaim(grant.Envelope)
	return err == nil && original.StartClaimSupported && original.ID == grant.TaskID && original.RuntimeID == grant.RuntimeID &&
		original.WorkspaceID == grant.WorkspaceID && original.AgentID == grant.AgentID && observesConversation(original, f.key)
}

func observesConversation(claim daemonapi.Claim, key workspace.ConversationKey) bool {
	if claim.WorkspaceID != key.WorkspaceID || claim.AgentID != key.AgentID || claim.AutopilotRunID != "" || claim.AutopilotID != "" || claim.QuickCreatePrompt != "" {
		return false
	}
	if key.Kind == workspace.ConversationIssue {
		return claim.IssueID == key.SubjectID && claim.ChatSessionID == "" && (claim.Kind == "direct" || claim.Kind == "comment")
	}
	return key.Kind == workspace.ConversationAgentDM && claim.ChatSessionID == key.SubjectID && claim.Kind == "chat" && claim.ChatChannelType == "" && claim.ChatType == ""
}

func observeContinuityFacts(claim daemonapi.Claim, key workspace.ConversationKey, compatibility string, evidence continuityEvidence) (continuityFacts, bool) {
	facts := continuityFacts{key: key, compatibility: compatibility, rows: make(map[string]daemonapi.TaskObservation),
		grants: make(map[string]workspace.TaskGrant), witnesses: make(map[string]workspace.TaskGrant), storages: make(map[string]workspace.Storage), terminals: evidence.Terminals}
	for _, storage := range evidence.Storages {
		if _, duplicate := facts.storages[storage.ID]; duplicate || !wire.UUID(storage.ID) {
			return facts, false
		}
		facts.storages[storage.ID] = storage
	}
	for _, grant := range evidence.Grants {
		if _, duplicate := facts.grants[grant.AttemptID]; duplicate || !wire.UUID(grant.AttemptID) {
			return facts, false
		}
		facts.grants[grant.AttemptID] = grant
	}
	seen := make(map[string]bool)
	for _, row := range evidence.History {
		if !wire.UUID(row.ID) || seen[row.ID] || row.WorkspaceID != key.WorkspaceID {
			return facts, false
		}
		seen[row.ID] = true
		if row.ID == claim.ID || row.AgentID != key.AgentID || key.Kind == workspace.ConversationIssue && row.IssueID != key.SubjectID || key.Kind == workspace.ConversationAgentDM && row.ChatSessionID != key.SubjectID {
			continue
		}
		if _, ok := observationTime(row); !ok {
			return facts, false
		}
		facts.rows[row.ID] = row
		facts.history = append(facts.history, row)
	}
	ambiguous := make(map[string]bool)
	for _, grant := range evidence.Grants {
		if !facts.validWitness(grant) {
			continue
		}
		if row, present := facts.rows[grant.TaskID]; present {
			original, err := daemonapi.ParseClaim(grant.Envelope)
			body := evidence.Terminals[grant.AttemptID].Body
			accepted := grant.CompletionWitness.Status == "completed" && row.AcceptsCompletion(original, body) ||
				grant.CompletionWitness.Status == "failed" && row.AcceptsTransientFailure(original, body)
			if err != nil || !accepted {
				continue
			}
		}
		if _, exists := facts.witnesses[grant.TaskID]; exists {
			ambiguous[grant.TaskID] = true
		}
		facts.witnesses[grant.TaskID] = grant
	}
	for taskID := range ambiguous {
		delete(facts.witnesses, taskID)
	}
	for taskID, grant := range facts.witnesses {
		if grant.CompletionWitness.Status == "failed" && !facts.validTransientSource(grant) {
			delete(facts.witnesses, taskID)
		}
	}
	return facts, true
}

func (f continuityFacts) validWitness(grant workspace.TaskGrant) bool {
	witness := grant.CompletionWitness
	terminal, terminalKnown := f.terminals[grant.AttemptID]
	storage, storageKnown := f.storages[grant.StorageID]
	if !storageKnown || !f.validLocalWriter(grant, storage) || !grant.StartConfirmed || witness == nil || !terminalKnown ||
		terminal.TaskID != grant.TaskID || terminal.AttemptID != grant.AttemptID || terminal.Source != "worker" || terminal.State != "delivered" || terminal.RecoveryFailure != nil || terminal.ResultReceipt == nil ||
		witness.TaskID != grant.TaskID || witness.AttemptID != grant.AttemptID || witness.WorkerSessionID != grant.WorkerSessionID ||
		witness.StorageID != grant.StorageID || witness.TurnSequence != grant.TurnSequence || witness.AcknowledgedAt.IsZero() ||
		witness.RequestDigest != terminal.RequestDigest || witness.ResultDigest != terminal.ResultDigest || witness.WorkDir != grant.TaskRoot+"/workdir" ||
		terminal.RequestDigest != workspace.TerminalDigest(terminal.Kind, terminal.Body) {
		return false
	}
	if grant.PendingResume == nil || grant.PendingResume.WorkDir != witness.WorkDir {
		return false
	}
	if witness.Status == "failed" {
		resume := grant.PendingResume
		return terminal.Kind == "fail" && resume.ResumeRejectedTransient && resume.SessionID == "" && witness.SessionID == "" &&
			!resume.SessionRolloutMissing && resume.MissingSessionID == "" && resume.RetiredSessionID == "" &&
			grant.Prepared != nil && grant.Prepared.Provider == "pi" && grant.Selection.Mode == workspace.SelectionResume &&
			grant.Selection.SessionID != "" && grant.ResumeSession == grant.Selection.SessionID &&
			grant.Selection.StorageID == grant.StorageID && grant.Selection.WorkDir == witness.WorkDir
	}
	if terminal.Kind != "complete" || witness.Status != "completed" || grant.PendingResume.ResumeRejectedTransient {
		return false
	}
	sessionID := grant.PendingResume.SessionID
	if grant.PendingResume.SessionRolloutMissing || sessionID == grant.PendingResume.RetiredSessionID {
		sessionID = ""
	}
	if witness.SessionID != sessionID {
		return false
	}
	return true
}

func (f continuityFacts) validTransientSource(grant workspace.TaskGrant) bool {
	source, found := f.witnesses[grant.Selection.SessionSource.TaskID]
	return found && source.CompletionWitness.Status == "completed" && grantSource(source) == grant.Selection.SessionSource &&
		source.StorageID == grant.StorageID && source.CompletionWitness.SessionID == grant.Selection.SessionID &&
		source.CompletionWitness.WorkDir == grant.Selection.WorkDir
}

func (f continuityFacts) retired(sessionID string) bool {
	for _, storage := range f.storages {
		if storage.Conversation == f.key && slices.Contains(storage.RetiredSessions, sessionID) {
			return true
		}
	}
	for _, row := range f.history {
		if row.Result != nil && row.Result.RetiredSessionID == sessionID {
			return true
		}
	}
	for _, grant := range f.grants {
		if grant.Conversation == f.key && grant.PendingResume != nil {
			if grant.PendingResume.RetiredSessionID == sessionID || grant.PendingResume.MissingSessionID == sessionID {
				return true
			}
		}
	}
	return false
}

func (f continuityFacts) issueSource(claim daemonapi.Claim) (workspace.TaskGrant, string) {
	var candidates []daemonapi.TaskObservation
	for _, row := range f.history {
		if row.Status == "completed" && row.Result != nil && !row.Result.SessionRolloutMissing && row.Result.SessionID == claim.PriorSessionID {
			candidates = append(candidates, row)
		}
	}
	selected, reason := f.latest(candidates, false)
	if reason != "" {
		return workspace.TaskGrant{}, reason
	}
	// A completed source with an unavailable result could have published the
	// same session. The delta-known flag does not identify that producing row.
	for _, row := range f.history {
		if row.Status == "completed" && row.Result == nil {
			order, known := f.compare(selected, row, false)
			if !known || order <= 0 {
				return workspace.TaskGrant{}, "completed_source_ambiguous"
			}
		}
	}
	source, ok := f.witnesses[selected.ID]
	if !ok || selected.RuntimeID != claim.RuntimeID || selected.WorkDir != claim.PriorWorkDir ||
		source.CompletionWitness.SessionID != claim.PriorSessionID || source.CompletionWitness.WorkDir != claim.PriorWorkDir {
		return workspace.TaskGrant{}, "selected_producer_unwitnessed"
	}
	return source, ""
}

func (f continuityFacts) chatSources(claim daemonapi.Claim) (workspace.TaskGrant, workspace.TaskGrant, string) {
	var sessionRows, workspaceRows []daemonapi.TaskObservation
	for _, row := range f.history {
		if row.Status == "failed" {
			if source, accepted := f.witnesses[row.ID]; accepted && source.CompletionWitness.Status == "failed" {
				workspaceRows = append(workspaceRows, row)
			}
			continue
		}
		if row.Status != "completed" || row.Result == nil {
			continue
		}
		if row.Result.SessionID != "" && !row.Result.SessionRolloutMissing {
			sessionRows = append(sessionRows, row)
		}
		if row.Result.WorkDir != "" {
			workspaceRows = append(workspaceRows, row)
		}
	}
	work, reason := f.latest(workspaceRows, true)
	if reason != "" {
		return workspace.TaskGrant{}, workspace.TaskGrant{}, reason
	}
	workspaceSource, ok := f.witnesses[work.ID]
	if !ok || work.WorkDir != claim.PriorWorkDir || workspaceSource.CompletionWitness.WorkDir != claim.PriorWorkDir {
		return workspace.TaskGrant{}, workspace.TaskGrant{}, "workspace_producer_unwitnessed"
	}
	var session daemonapi.TaskObservation
	var sessionSource workspace.TaskGrant
	if claim.PriorSessionID != "" {
		session, reason = f.latest(sessionRows, true)
		if reason != "" {
			return workspace.TaskGrant{}, workspace.TaskGrant{}, reason
		}
		sessionSource, ok = f.witnesses[session.ID]
		if !ok || session.RuntimeID != claim.RuntimeID || session.Result.SessionID != claim.PriorSessionID || sessionSource.CompletionWitness.SessionID != claim.PriorSessionID {
			return workspace.TaskGrant{}, workspace.TaskGrant{}, "session_producer_unwitnessed"
		}
	}
	for _, row := range f.history {
		switch row.Status {
		case "queued":
			continue
		case "cancelled":
			if !cancelledChatPinBlocked(row, sessionRows) {
				return workspace.TaskGrant{}, workspace.TaskGrant{}, "cancelled_chat_producer_unknown"
			}
		case "completed", "failed":
			if row.Status == "completed" && row.Result != nil {
				continue
			}
			if source, accepted := f.witnesses[row.ID]; row.Status == "failed" && accepted && source.CompletionWitness.Status == "failed" {
				continue
			}
			for _, selected := range []daemonapi.TaskObservation{work, session} {
				if selected.ID == "" {
					continue
				}
				order, known := f.compare(selected, row, true)
				if !known || order <= 0 {
					return workspace.TaskGrant{}, workspace.TaskGrant{}, "chat_pointer_writer_unknown"
				}
			}
		default:
			return workspace.TaskGrant{}, workspace.TaskGrant{}, "chat_writer_unresolved"
		}
	}
	return sessionSource, workspaceSource, ""
}

func cancelledChatPinBlocked(cancelled daemonapi.TaskObservation, knownSessionRows []daemonapi.TaskObservation) bool {
	created, err := time.Parse(time.RFC3339Nano, cancelled.CreatedAt)
	if err != nil {
		return false
	}
	for _, newer := range knownSessionRows {
		timestamp, err := time.Parse(time.RFC3339Nano, newer.CreatedAt)
		if err == nil && timestamp.After(created) {
			return true
		}
	}
	return false
}

func observationTime(row daemonapi.TaskObservation) (time.Time, bool) {
	for _, raw := range []string{row.CompletedAt, row.StartedAt, row.DispatchedAt, row.CreatedAt} {
		if raw != "" {
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			return parsed, err == nil
		}
	}
	return time.Time{}, false
}

// DM pointer writes are transactional. Sequential accepted local turns prove
// their commit order even when public timestamps lose subsecond precision.
// The issue SQL selector orders stored timestamps, so it cannot use this rule.
func (f continuityFacts) compare(a, b daemonapi.TaskObservation, chat bool) (int, bool) {
	if a.ID == b.ID {
		return 0, true
	}
	if chat {
		left, leftKnown := f.witnesses[a.ID]
		right, rightKnown := f.witnesses[b.ID]
		if leftKnown && rightKnown && left.WorkerSessionID != "" && left.WorkerSessionID == right.WorkerSessionID && left.TurnSequence != right.TurnSequence {
			if left.TurnSequence > right.TurnSequence {
				return 1, true
			}
			return -1, true
		}
	}
	left, leftKnown := observationTime(a)
	right, rightKnown := observationTime(b)
	if !leftKnown || !rightKnown || left.Equal(right) {
		return 0, false
	}
	return left.Compare(right), true
}

func (f continuityFacts) latest(rows []daemonapi.TaskObservation, chat bool) (daemonapi.TaskObservation, string) {
	if len(rows) == 0 {
		return daemonapi.TaskObservation{}, "selected_producer_unavailable"
	}
	selected := rows[0]
	for _, row := range rows[1:] {
		if order, known := f.compare(row, selected, chat); known && order > 0 {
			selected = row
		}
	}
	for _, row := range rows {
		if row.ID == selected.ID {
			continue
		}
		if order, known := f.compare(selected, row, chat); !known || order <= 0 {
			return daemonapi.TaskObservation{}, "timestamp_ambiguous"
		}
	}
	return selected, ""
}
