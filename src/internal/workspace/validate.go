package workspace

import (
	"crypto/ed25519"
	"encoding/json"
	"net"
	"slices"
)

func (s *Store) validate(st registry) error {
	return s.validateRegistry(st, false)
}

func (s *Store) validateRegistry(st registry, historicalMigration bool) error {
	if st.SchemaVersion != SchemaVersion || st.OwnerID != s.options.OwnerID || st.Grants == nil || st.Storages == nil || st.Terminals == nil || st.Capabilities == nil || st.Conversations == nil || st.Sessions == nil {
		return invalid("journal schema or owner")
	}
	if st.WorkspaceClaim != "" && (st.WorkspaceUID == "" || net.ParseIP(st.NFSServer) == nil) {
		return invalid("workspace endpoint binding")
	}
	for id, g := range st.Grants {
		if id != g.AttemptID || !canonicalUUID(id) || g.OwnerID != st.OwnerID || !canonicalUUID(g.TaskID) || !validOpaque(g.RuntimeID) || !validOpaque(g.WorkspaceID) || !validOpaque(g.AgentID) || g.Generation == 0 || g.CreatedAt.IsZero() || g.RuntimeRef.Validate() != nil || g.Fingerprint != RuntimeFingerprint(g.RuntimeRef) || g.ScopeDigest != scopeDigest(g) || !json.Valid(g.Envelope) || !json.Valid(g.Metadata) || g.RequestDigest != digest(g.Envelope) || int64(len(g.Envelope)) > s.options.MaxRecordBytes || int64(len(g.Metadata)) > s.options.MaxRecordBytes {
			return invalid("grant")
		}
		for i, event := range g.Events {
			if event.Sequence != uint64(i+1) || event.UpstreamSequence == 0 || !json.Valid(event.Body) || int64(len(event.Body)) > s.options.MaxRecordBytes || (event.State != "local" && event.State != "received" && event.State != "forwarding" && event.State != "delivered" && event.State != "uncertain") {
				return invalid("event journal")
			}
		}
		if g.CleanupComplete && g.State != "closed" {
			return invalid("cleanup authority")
		}
		if g.StartConfirmed && (g.PodUID == "" || len(g.SupervisorKey) != ed25519.PublicKeySize || g.State == "intent" || g.State == "assigned" || g.State == "ready" || g.State == "offered" || g.State == "starting") {
			return invalid("confirmed execution start")
		}
		if g.CheckoutProcess != nil && (!g.CheckoutProcess.valid() || !g.CheckoutNeedsFlush || g.Prepared == nil || g.State == "closed") {
			return invalid("checkout writer authority")
		}
		if g.CheckoutNeedsFlush && g.Prepared == nil {
			return invalid("checkout flush obligation")
		}
		if g.PreparationProcess != nil && (!g.PreparationStarted || !g.PreparationProcess.valid()) {
			return invalid("preparation process")
		}
		if len(g.StopKey) != 0 && (len(g.StopKey) != ed25519.PublicKeySize || g.PodUID == "" || g.PVCUID == "" || g.NodeID == "" ||
			len(g.SupervisorKey) != 0 && !slices.Equal(g.StopKey, g.SupervisorKey)) {
			return invalid("stop supervisor identity")
		}
		if stop := g.Stop; stop != nil {
			if !g.ExecutionRevoked || !validOpaque(stop.Reason) || len(stop.Reason) > 256 || stop.Revision == 0 || !canonicalUUID(stop.Nonce) || stop.RequestedAt.IsZero() {
				return invalid("stop intent")
			}
			if stop.Receipt != nil && !validStopReceipt(g, *stop.Receipt) {
				return invalid("stop receipt")
			}
			if stop.Evidence != nil && !validStopEvidence(g, *stop.Evidence) {
				return invalid("stop evidence")
			}
			if g.State == "closed" && stop.Evidence == nil && g.WorkerSessionID == "" {
				return invalid("stop writer release")
			}
		}
		if len(g.Resources) > 0 && (!json.Valid(g.Resources) || int64(len(g.Resources)) > s.options.MaxRecordBytes) {
			return invalid("resource journal")
		}
		if len(g.Bootstrap) > 0 && (!json.Valid(g.Bootstrap) || g.BootstrapDigest != digest(g.Bootstrap) || int64(len(g.Bootstrap)) > s.options.MaxRecordBytes) {
			return invalid("worker bootstrap")
		}
		if g.Repositories == nil || g.ResourceScope == nil || !slices.IsSorted(g.ResourceScope) {
			return invalid("scope")
		}
		for i, r := range g.Repositories {
			if !validOpaque(r.URL) || (i > 0 && g.Repositories[i-1].URL >= r.URL) {
				return invalid("repository")
			}
		}
		for i, r := range g.ResourceScope {
			if !validOpaque(r) || (i > 0 && g.ResourceScope[i-1] >= r) {
				return invalid("resource")
			}
		}
		if g.BackendSelection != nil && (g.BackendSelection.Conversation != g.Conversation || g.Conversation.OwnerID != g.OwnerID ||
			g.Conversation.WorkspaceID != g.WorkspaceID || g.Conversation.AgentID != g.AgentID ||
			g.Conversation.Kind == ConversationTask && g.Conversation.SubjectID != g.TaskID || validateBackendSelectionSources(st, *g.BackendSelection) != nil) {
			return invalid("backend selection journal")
		}
		if g.StorageID == "" {
			if err := validatePendingSessionGrant(g); err != nil {
				return err
			}
			if g.State != "waiting_storage" && g.State != "unexecuted" && g.State != "closed" {
				return invalid("unbound execution")
			}
			if g.PVCName != "" || g.PVCUID != "" || g.PodName != "" || g.PodUID != "" || g.NodeID != "" || len(g.SupervisorKey) != 0 || len(g.StopKey) != 0 || len(g.Bootstrap) != 0 || g.BootstrapDigest != "" || len(g.Resources) != 0 || g.ResumeSession != "" || g.ResumeWorkDir != "" {
				return invalid("pending claim execution")
			}
			if g.State != "waiting_storage" && !(g.State == "closed" && g.Stop != nil && g.Stop.Evidence != nil) {
				t, ok := st.Terminals[id]
				if !ok || t.Source != "controller" || g.State == "closed" && (!g.CleanupComplete || t.State != "delivered" && t.State != "rejected") {
					return invalid("unexecuted failure settlement")
				}
			}
			continue
		}
		if g.WorkerSessionID != "" {
			if err := s.validateSessionGrant(st, g); err != nil {
				return err
			}
			continue
		}
		if g.SessionProtocol || g.Conversation != (ConversationKey{}) || g.TurnSequence != 0 || g.WorkspaceAnchorTaskID != "" ||
			g.CompatibilityDigest != "" || g.WorkspaceCompatibilityDigest != "" || g.Compatibility != nil || g.Selection != nil || g.BackendSelection != nil || len(g.Assignment) != 0 || g.InputDigest != "" || g.TurnComplete ||
			g.TurnAccepted || g.AcceptProof != nil || g.CompletionWitness != nil || g.TurnReceipt != nil || g.TurnExecutionReceipt != nil || g.TurnExecutionProof != nil || g.PendingResume != nil {
			return invalid("unbound session grant")
		}
		storage, ok := st.Storages[g.StorageID]
		if !ok || storage.ScopeDigest != g.ScopeDigest || storage.Fingerprint != g.Fingerprint || storage.PVCName != g.PVCName || storage.PVCUID != g.PVCUID || storage.TaskRoot != g.TaskRoot || storage.WorkspaceID != g.WorkspaceID || storage.TaskID != g.TaskID || storage.AgentID != g.AgentID {
			return invalid("storage binding")
		}
		if g.Prepared != nil {
			p := g.Prepared
			if ValidatePrepared(*p) != nil || p.OwnerID != g.OwnerID || p.WorkspaceID != g.WorkspaceID || p.TaskID != g.TaskID || p.AgentID != g.AgentID || p.AttemptID != id || p.Generation != g.Generation || p.TaskRoot != g.TaskRoot || p.PVCUID != g.PVCUID || !g.PreparationStarted || !g.PreparationStopped || !json.Valid(g.Execution) || int64(len(g.Execution)) > s.options.MaxRecordBytes {
				return invalid("prepared ownership")
			}
		} else if len(g.Execution) != 0 {
			return invalid("unprepared execution")
		}
		if g.PreparationStopped && !g.PreparationStarted {
			return invalid("preparation termination")
		}
		if g.PodName != "task-"+g.AttemptID {
			return invalid("Pod name")
		}
		if g.PodUID != "" && (g.PVCUID == "" || g.Prepared == nil || !g.PreparationStopped) {
			return invalid("Pod volume")
		}
		switch g.State {
		case "intent":
			if len(g.SupervisorKey) > 0 {
				return invalid("unadmitted supervisor")
			}
		case "ready", "offered", "starting", "started", "terminal_received":
			if g.PodUID == "" || g.PVCUID == "" || g.NodeID == "" || len(g.SupervisorKey) != ed25519.PublicKeySize {
				return invalid("admission")
			}
		case "quarantined":
			if !storage.Quarantined {
				return invalid("quarantine")
			}
		case "unexecuted":
			t, ok := st.Terminals[id]
			if !ok || t.Source != "controller" || !preparationStoppedWithoutWorker(g) {
				return invalid("unexecuted preparation")
			}
		case "closed":
			if g.Stop != nil && g.Stop.Evidence != nil {
				if !stoppedClean(g, st.Terminals[id]) && !storage.Dirty {
					return invalid("dirty stopped storage")
				}
				break
			}
			t, ok := st.Terminals[id]
			if ok && t.Source == "controller" && preparationStoppedWithoutWorker(g) && g.CleanupComplete && (t.State == "delivered" || t.State == "rejected") {
				break
			}
			if !ok || t.Source != "worker" || t.Seal == nil || (t.State != "delivered" && t.State != "rejected") {
				return invalid("writer release")
			}
		default:
			return invalid("dispatch state")
		}
		if g.State != "closed" && storage.ActiveAttempt != id {
			return invalid("exclusive writer")
		}
		if g.State == "terminal_received" {
			if _, ok := st.Terminals[id]; !ok {
				return invalid("terminal grant")
			}
		}
	}
	roots := map[string]string{}
	for id, b := range st.Storages {
		if id != b.ID || !canonicalUUID(id) || b.PVCName != st.WorkspaceClaim || b.PVCUID != st.WorkspaceUID || !canonicalUUID(b.WorkspaceID) || !canonicalUUID(b.TaskID) || !fingerprint(b.ScopeDigest) || !fingerprint(b.Fingerprint) {
			return invalid("storage")
		}
		if err := ValidateTaskRoot("/workspace", b.TaskRoot, b.WorkspaceID, b.TaskID); err != nil {
			return invalid("canonical task root")
		}
		if roots[b.TaskRoot] != "" {
			return invalid("shared task root")
		}
		roots[b.TaskRoot] = id
		if b.WorkspaceAnchorTaskID != "" {
			if err := validateConversationStorage(st, b); err != nil {
				return err
			}
			continue
		}
		if b.Conversation != (ConversationKey{}) || b.CompatibilityDigest != "" || b.WorkspaceCompatibilityDigest != "" || b.WriterSessionID != "" || b.LatestWriter != (CheckpointSource{}) || b.Checkpoint != nil || len(b.RetiredSessions) != 0 {
			return invalid("unanchored conversation storage")
		}
		if b.Prepared != nil {
			p := b.Prepared
			if ValidatePrepared(*p) != nil || p.OwnerID != st.OwnerID || p.TaskID != b.TaskID || p.WorkspaceID != b.WorkspaceID || p.AgentID != b.AgentID || p.TaskRoot != b.TaskRoot || p.PVCUID != b.PVCUID {
				return invalid("retained preparation")
			}
		}
		if b.ActiveAttempt != "" {
			g, ok := st.Grants[b.ActiveAttempt]
			if !ok || g.StorageID != id || g.State == "closed" {
				return invalid("active storage writer")
			}
		}
	}
	for key, c := range st.Capabilities {
		g, ok := st.Grants[c.AttemptID]
		if !ok || g.StorageID == "" || key != digest([]byte(c.Token)) || !validOpaque(c.Token) || !validOpaque(c.Audience) || c.Generation != g.Generation || c.PodUID != g.PodUID || c.ExpiresAt.IsZero() || c.WorkerSessionID != g.WorkerSessionID || c.TurnSequence != g.TurnSequence {
			return invalid("capability")
		}
	}
	for id, t := range st.Terminals {
		g, ok := st.Grants[id]
		if !ok || t.AttemptID != id || t.TaskID != g.TaskID || t.PodUID != g.PodUID || t.PVCUID != g.PVCUID || !canonicalUUID(t.ReceiptID) || !canonicalUUID(t.Nonce) || !json.Valid(t.Body) || int64(len(t.Body)) > s.options.MaxRecordBytes || t.RequestDigest != TerminalDigest(t.Kind, t.Body) || t.ReceivedAt.IsZero() {
			return invalid("terminal")
		}
		if t.Kind != "complete" && t.Kind != "fail" && t.Kind != "cancel-ack" {
			return invalid("terminal kind")
		}
		if recovery := t.RecoveryFailure; recovery != nil {
			if t.Source != "worker" || t.State != "received" || t.Seal != nil || t.ResultReceipt != nil || recovery.DecidedAt.IsZero() || !json.Valid(recovery.Body) || int64(len(recovery.Body)) > s.options.MaxRecordBytes ||
				g.Stop == nil || g.Stop.Evidence == nil || g.Stop.Evidence.Kind != "terminated" || !validStopEvidence(g, *g.Stop.Evidence) || !g.CheckoutClosed {
				return invalid("unsealed result recovery")
			}
			switch recovery.State {
			case "received", "forwarding", "uncertain", "delivered", "rejected":
			default:
				return invalid("recovery delivery state")
			}
		}
		if g.State != "terminal_received" && g.State != "quarantined" && g.State != "unexecuted" && g.State != "closed" {
			return invalid("terminal dispatch")
		}
		switch t.Source {
		case "worker":
			if !fingerprint(t.ResultDigest) {
				return invalid("provider result digest")
			}
			if g.PodUID == "" || g.PVCUID == "" || len(g.SupervisorKey) != ed25519.PublicKeySize {
				return invalid("native terminal identity")
			}
		case "controller":
			if t.Kind != "fail" || (g.State != "quarantined" && g.State != "unexecuted" && !(g.State == "closed" && (g.StorageID == "" || preparationStoppedWithoutWorker(g) || g.Stop != nil && g.Stop.Evidence != nil))) || t.Seal != nil || t.ResultReceipt != nil || t.State == "sealed" {
				return invalid("controller failure authority")
			}
		default:
			return invalid("terminal source")
		}
		switch t.State {
		case "received":
			if t.Seal != nil {
				return invalid("unsealed receipt")
			}
		case "sealed":
			if t.Seal == nil {
				return invalid("missing storage seal")
			}
		case "forwarding", "delivered", "rejected", "uncertain":
			if t.Source == "worker" && t.Seal == nil && t.ResultReceipt == nil {
				return invalid("missing result authentication")
			}
		default:
			return invalid("delivery state")
		}
		if t.Seal != nil {
			r := *t.Seal
			if r.TaskID != g.TaskID || r.AttemptID != id || r.PodUID != g.PodUID || r.PVCUID != g.PVCUID || r.RequestDigest != t.RequestDigest || r.Nonce != t.Nonce || !r.WritersStopped || !r.FlushOK || len(g.SupervisorKey) != ed25519.PublicKeySize || !ed25519.Verify(g.SupervisorKey, ReceiptMessage(r), r.Signature) {
				return invalid("signed receipt")
			}
		}
		if t.ResultReceipt != nil && !validResultReceipt(g, t, *t.ResultReceipt) {
			return invalid("signed provider result")
		}
	}
	return s.validateConversations(st, historicalMigration)
}
