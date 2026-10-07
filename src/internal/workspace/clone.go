package workspace

import (
	"maps"
	"slices"
)

// Every published registry owns its mutable values. Transactions and callers
// receive separate copies so neither can change committed authority in memory.
func cloneRegistry(st registry) registry {
	st.Grants = maps.Clone(st.Grants)
	for id, grant := range st.Grants {
		st.Grants[id] = cloneGrant(grant)
	}
	st.Storages = maps.Clone(st.Storages)
	for id, storage := range st.Storages {
		storage.CreatedIssueIDs = slices.Clone(storage.CreatedIssueIDs)
		storage.Prepared = clonePrepared(storage.Prepared)
		storage.Checkpoint = clonePointer(storage.Checkpoint)
		storage.RetiredSessions = slices.Clone(storage.RetiredSessions)
		st.Storages[id] = storage
	}
	st.Terminals = maps.Clone(st.Terminals)
	for id, terminal := range st.Terminals {
		st.Terminals[id] = cloneTerminal(terminal)
	}
	st.Capabilities = maps.Clone(st.Capabilities)
	st.Conversations = maps.Clone(st.Conversations)
	st.Sessions = maps.Clone(st.Sessions)
	for id, session := range st.Sessions {
		st.Sessions[id] = cloneSession(session)
	}
	return st
}

func cloneGrant(g TaskGrant) TaskGrant {
	g.CheckoutProcess = clonePointer(g.CheckoutProcess)
	g.PreparationProcess = clonePointer(g.PreparationProcess)
	g.Repositories = slices.Clone(g.Repositories)
	g.ResourceScope = slices.Clone(g.ResourceScope)
	g.RuntimeRef.Providers = maps.Clone(g.RuntimeRef.Providers)
	g.Envelope = slices.Clone(g.Envelope)
	g.Metadata = slices.Clone(g.Metadata)
	g.Resources = slices.Clone(g.Resources)
	g.Bootstrap = slices.Clone(g.Bootstrap)
	g.SupervisorKey = slices.Clone(g.SupervisorKey)
	g.StopKey = slices.Clone(g.StopKey)
	g.Execution = slices.Clone(g.Execution)
	g.Assignment = slices.Clone(g.Assignment)
	g.AcceptProof = cloneProof(g.AcceptProof)
	g.TurnExecutionProof = cloneProof(g.TurnExecutionProof)
	g.CompletionWitness = clonePointer(g.CompletionWitness)
	g.Selection = clonePointer(g.Selection)
	g.BackendSelection = clonePointer(g.BackendSelection)
	g.Compatibility = cloneCompatibility(g.Compatibility)
	g.PendingResume = clonePointer(g.PendingResume)
	g.TurnReceipt = clonePointer(g.TurnReceipt)
	if g.TurnReceipt != nil {
		g.TurnReceipt.Signature = slices.Clone(g.TurnReceipt.Signature)
	}
	g.TurnExecutionReceipt = clonePointer(g.TurnExecutionReceipt)
	if g.TurnExecutionReceipt != nil {
		g.TurnExecutionReceipt.Signature = slices.Clone(g.TurnExecutionReceipt.Signature)
	}
	g.Prepared = clonePrepared(g.Prepared)
	g.Stop = clonePointer(g.Stop)
	if g.Stop != nil {
		g.Stop.Receipt = clonePointer(g.Stop.Receipt)
		if g.Stop.Receipt != nil {
			g.Stop.Receipt.Signature = slices.Clone(g.Stop.Receipt.Signature)
		}
		g.Stop.Evidence = clonePointer(g.Stop.Evidence)
	}
	g.Events = slices.Clone(g.Events)
	for i := range g.Events {
		g.Events[i].Body = slices.Clone(g.Events[i].Body)
	}
	return g
}

func cloneCompatibility(c *ConversationCompatibilityV1) *ConversationCompatibilityV1 {
	c = clonePointer(c)
	if c == nil {
		return nil
	}
	c.Authority.Memberships = slices.Clone(c.Authority.Memberships)
	c.Repositories = slices.Clone(c.Repositories)
	c.ResourceScope = slices.Clone(c.ResourceScope)
	c.RuntimeRef.Providers = maps.Clone(c.RuntimeRef.Providers)
	c.CustomArgs = slices.Clone(c.CustomArgs)
	c.CustomEnv = maps.Clone(c.CustomEnv)
	c.SkillHashes = slices.Clone(c.SkillHashes)
	c.DisabledRuntimeSkills = slices.Clone(c.DisabledRuntimeSkills)
	c.StableMCP = slices.Clone(c.StableMCP)
	c.StablePlugins = slices.Clone(c.StablePlugins)
	return c
}

func cloneProof(p *SessionProof) *SessionProof {
	p = clonePointer(p)
	if p != nil {
		p.Signature = slices.Clone(p.Signature)
	}
	return p
}

func cloneSession(session WorkerSession) WorkerSession {
	session.RuntimeRef.Providers = maps.Clone(session.RuntimeRef.Providers)
	session.SupervisorKey = slices.Clone(session.SupervisorKey)
	session.Bootstrap = slices.Clone(session.Bootstrap)
	session.Resources = slices.Clone(session.Resources)
	session.Stop = clonePointer(session.Stop)
	if session.Stop != nil {
		session.Stop.Receipt = clonePointer(session.Stop.Receipt)
		if session.Stop.Receipt != nil {
			session.Stop.Receipt.Signature = slices.Clone(session.Stop.Receipt.Signature)
		}
		session.Stop.Evidence = clonePointer(session.Stop.Evidence)
		session.Stop.RequestProof = cloneProof(session.Stop.RequestProof)
		session.Stop.ReceiptProof = cloneProof(session.Stop.ReceiptProof)
	}
	return session
}

func clonePrepared(p *Prepared) *Prepared {
	p = clonePointer(p)
	if p != nil {
		p.Conversation = clonePointer(p.Conversation)
		p.Repositories = slices.Clone(p.Repositories)
		p.SkillIDs = slices.Clone(p.SkillIDs)
		p.CleanupManifest = slices.Clone(p.CleanupManifest)
		p.AllowedLinks = maps.Clone(p.AllowedLinks)
		p.NativeMetadata = clonePointer(p.NativeMetadata)
		if p.NativeMetadata != nil {
			p.NativeMetadata.Artifacts = slices.Clone(p.NativeMetadata.Artifacts)
			p.NativeMetadata.CodexConfig = slices.Clone(p.NativeMetadata.CodexConfig)
			p.NativeMetadata.ClaudeSettings = slices.Clone(p.NativeMetadata.ClaudeSettings)
			p.NativeMetadata.ServiceTier = slices.Clone(p.NativeMetadata.ServiceTier)
			p.NativeMetadata.TaskMarker = slices.Clone(p.NativeMetadata.TaskMarker)
			p.NativeMetadata.ProjectResources = slices.Clone(p.NativeMetadata.ProjectResources)
		}
	}
	return p
}

func cloneTerminal(t Terminal) Terminal {
	t.Body = slices.Clone(t.Body)
	t.Seal = clonePointer(t.Seal)
	if t.Seal != nil {
		t.Seal.Signature = slices.Clone(t.Seal.Signature)
	}
	t.ResultReceipt = clonePointer(t.ResultReceipt)
	if t.ResultReceipt != nil {
		t.ResultReceipt.Signature = slices.Clone(t.ResultReceipt.Signature)
	}
	t.RecoveryFailure = clonePointer(t.RecoveryFailure)
	if t.RecoveryFailure != nil {
		t.RecoveryFailure.Body = slices.Clone(t.RecoveryFailure.Body)
	}
	return t
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
