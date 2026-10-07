package workspace

// Historical payloads remain byte-for-byte unchanged. Only the journal's
// immutable protocol binding and a required live-session drain are additive.
func restoreHistoricalSessionProtocol(st *registry) error {
	for id, session := range st.Sessions {
		if session.ProtocolVersion != 0 {
			return invalid("previous session protocol")
		}
		session.ProtocolVersion = LegacySessionProtocolVersion
		st.Sessions[id] = session
	}
	for _, g := range st.Grants {
		if g.TurnExecutionReceipt != nil || g.TurnExecutionProof != nil || g.Prepared != nil && g.Prepared.NativeMetadata != nil {
			return invalid("previous resident execution proof")
		}
	}
	for _, storage := range st.Storages {
		if storage.Prepared != nil && storage.Prepared.NativeMetadata != nil {
			return invalid("previous resident metadata")
		}
		if storage.Checkpoint != nil && (storage.Checkpoint.SealingSessionID != "" || !storage.Checkpoint.SealedAt.IsZero()) {
			return invalid("previous resident storage seal")
		}
	}
	return nil
}

// Schema 14 bound every turn to the initial full compatibility digest. Derive
// its workspace projection from that existing evidence without changing leases.
func migrateWorkspaceCompatibility(st *registry) error {
	for id, g := range st.Grants {
		if g.WorkspaceCompatibilityDigest != "" || g.Selection != nil && g.Selection.WorkspaceReuseEligible ||
			g.BackendSelection != nil && g.BackendSelection.WorkspaceReuseEligible {
			return invalid("previous workspace reuse authority")
		}
		if g.Compatibility != nil {
			full, err := g.Compatibility.Digest()
			if err != nil || full != g.CompatibilityDigest {
				return invalid("previous full compatibility")
			}
			g.WorkspaceCompatibilityDigest, err = g.Compatibility.WorkspaceDigest()
			if err != nil {
				return err
			}
		}
		if g.Selection != nil {
			if (!g.Selection.ReuseEligible || g.Conversation.Kind == ConversationTask) && g.Selection.Mode != SelectionFreshWorkspace ||
				g.Selection.Mode != SelectionFreshWorkspace && (g.BackendSelection == nil || !sameSelectedSources(*g.Selection, *g.BackendSelection)) {
				return invalid("previous continuity selection")
			}
			if g.Selection.ReuseEligible && (g.Compatibility == nil || !g.Compatibility.ProviderOptionsResolved ||
				!g.Compatibility.Authority.ValidForReuse() || g.Conversation.Kind == ConversationTask) {
				return invalid("previous native reuse authority")
			}
			g.Selection.WorkspaceReuseEligible = g.Selection.ReuseEligible
		}
		if g.BackendSelection != nil {
			if g.BackendSelection.Conversation.Kind == ConversationTask && (g.BackendSelection.ReuseEligible || g.BackendSelection.Mode != SelectionFreshWorkspace) {
				return invalid("previous task continuity")
			}
			g.BackendSelection.WorkspaceReuseEligible = g.BackendSelection.ReuseEligible
		}
		if g.WorkerSessionID != "" {
			storage, session := st.Storages[g.StorageID], st.Sessions[g.WorkerSessionID]
			if g.CompatibilityDigest != storage.CompatibilityDigest || g.CompatibilityDigest != session.CompatibilityDigest {
				return invalid("previous session compatibility")
			}
		}
		st.Grants[id] = g
	}
	for id, storage := range st.Storages {
		if storage.WorkspaceCompatibilityDigest != "" {
			return invalid("previous workspace compatibility")
		}
		if storage.Conversation != (ConversationKey{}) {
			writer, ok := st.Grants[storage.LatestWriter.AttemptID]
			if !ok || writer.StorageID != id || writer.CompatibilityDigest != storage.CompatibilityDigest || !fingerprint(writer.WorkspaceCompatibilityDigest) {
				return invalid("previous storage compatibility")
			}
			storage.WorkspaceCompatibilityDigest = writer.WorkspaceCompatibilityDigest
		}
		if checkpoint := storage.Checkpoint; checkpoint != nil {
			g := st.Grants[checkpoint.Source.AttemptID]
			if checkpoint.WorkspaceCompatibilityDigest != "" || checkpoint.CompatibilityDigest != storage.CompatibilityDigest ||
				g.Selection == nil || !g.Selection.ReuseEligible || !nativeCheckpointOutcome(g, st.Terminals[g.AttemptID]) {
				return invalid("previous checkpoint compatibility")
			}
			checkpoint.WorkspaceCompatibilityDigest = storage.WorkspaceCompatibilityDigest
		}
		st.Storages[id] = storage
	}
	for id, session := range st.Sessions {
		storage := st.Storages[session.StorageID]
		if session.WorkspaceCompatibilityDigest != "" || session.CompatibilityDigest != storage.CompatibilityDigest ||
			session.State == SessionIdle && (storage.Checkpoint == nil || session.Conversation.Kind == ConversationTask) {
			return invalid("previous resident compatibility")
		}
		prefix := string(session.Conversation.Kind)
		if session.Conversation.Kind == ConversationAgentDM {
			prefix = "dm"
		}
		if !canonicalUUID(id) || session.PodName != prefix+"-"+session.Conversation.Digest()[:16]+"-"+id[:12] {
			return invalid("previous resident Pod identity")
		}
		session.WorkspaceCompatibilityDigest = storage.WorkspaceCompatibilityDigest
		st.Sessions[id] = session
	}
	return nil
}
