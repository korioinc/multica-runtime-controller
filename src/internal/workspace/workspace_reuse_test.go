package workspace

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func newWorkspacePolicyFixture(t *testing.T, kind ConversationKind, change func(*ConversationCompatibilityV1)) *sessionFixture {
	t.Helper()
	s, options := testStore(t)
	clock := new(atomic.Int64)
	clock.Store(time.Now().UTC().UnixNano())
	options.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	s.options.Now = options.Now
	input := testGrant()
	input.RuntimeID, input.SessionProtocol = uuid.NewString(), true
	key := ConversationKey{OwnerID: options.OwnerID, WorkspaceID: input.WorkspaceID, AgentID: input.AgentID, Kind: kind, SubjectID: uuid.NewString()}
	if kind == ConversationTask {
		key.SubjectID = input.TaskID
	}
	selection := Selection{Conversation: key, Mode: SelectionFreshWorkspace}
	queued, err := s.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := testConversationCompatibility(queued, selection)
	change(&compatibility)
	selection.WorkspaceReuseEligible = compatibility.Authority.ValidForWorkspaceReuse()
	selection.ReuseEligible = compatibility.ProviderOptionsResolved && compatibility.Authority.ValidForReuse()
	digest := recordWorkspacePolicy(t, s, queued, selection, compatibility)
	if err := s.RecordSelection(queued.AttemptID, selection, digest); err != nil {
		t.Fatal(err)
	}
	grant, session, err := s.ReserveTurn(queued.AttemptID, selection, digest, sessionFixtureRetention, 4)
	if err != nil {
		t.Fatal(err)
	}
	f := &sessionFixture{store: s, options: options, clock: clock, input: input, selection: selection, compatibility: digest, grant: grant, session: session}
	f.prepareAndStart(t)
	return f
}

func recordWorkspacePolicy(t *testing.T, s *Store, g TaskGrant, selection Selection, compatibility ConversationCompatibilityV1) string {
	t.Helper()
	if err := s.RecordBackendSelection(g.AttemptID, selection); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordCompatibility(g.AttemptID, compatibility); err != nil {
		t.Fatal(err)
	}
	digest, err := compatibility.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func workspaceFollowup(f *sessionFixture) (TaskGrant, Selection) {
	input := f.input
	if f.selection.Conversation.Kind != ConversationTask {
		input.TaskID = uuid.NewString()
	}
	source := CheckpointSource{TaskID: f.grant.TaskID, AttemptID: f.grant.AttemptID}
	return input, Selection{Conversation: f.selection.Conversation, WorkspaceReuseEligible: true, Mode: SelectionFreshSession,
		StorageID: f.grant.StorageID, WorkspaceSource: source, LatestWriter: source, WorkDir: f.grant.TaskRoot + "/workdir"}
}

func TestWorkspaceReuseDoesNotRequireNativeHistory(t *testing.T) {
	for _, kind := range []ConversationKind{ConversationIssue, ConversationAgentDM, ConversationTask} {
		for _, scenario := range []string{"unresolved_options", "opaque_integration"} {
			t.Run(string(kind)+"/"+scenario, func(t *testing.T) {
				change := func(c *ConversationCompatibilityV1) {
					if scenario == "unresolved_options" {
						c.ProviderOptionsResolved = false
					} else {
						c.Authority.Integrations = IntegrationEvidence{State: IntegrationUnknown, Reason: "integration_authority_opaque"}
						c.StableMCP = json.RawMessage(`{"tool":{"url":"https://tools.example/api"}}`)
					}
				}
				f := newWorkspacePolicyFixture(t, kind, change)
				f.settle(t, f.receive(t, ""), "")
				previous, idle := f.grant, f.session
				storage, err := f.store.GetStorage(previous.StorageID)
				if err != nil || idle.State != SessionIdle || idle.IdleDeadline != idle.IdleSince.Add(sessionFixtureRetention) ||
					!storage.Dirty || storage.Checkpoint != nil || storage.WriterSessionID != idle.ID {
					t.Fatal("a settled turn lost its retained writer lease when native history was unavailable", err)
				}
				input, selection := workspaceFollowup(f)
				queued, err := f.store.QueueClaim(input)
				if err != nil {
					t.Fatal(err)
				}
				compatibility := testConversationCompatibility(queued, selection)
				change(&compatibility)
				compatibility.Model, compatibility.AgentInstructions = "next-model", "new turn instructions"
				compatibility.CustomArgs = []string{"--model", "next-model"}
				compatibility.CustomEnv = map[string]string{"TURN_CONFIGURATION": "next"}
				backend := Selection{Conversation: selection.Conversation, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
				digest := recordWorkspacePolicy(t, f.store, queued, backend, compatibility)
				if err := f.store.RecordSelection(queued.AttemptID, selection, digest); err != nil {
					t.Fatal(err)
				}
				f.clock.Store(idle.IdleSince.Add(599 * time.Second).UnixNano())
				grant, session, err := f.store.ReserveTurn(queued.AttemptID, selection, digest, sessionFixtureRetention, 4)
				if err != nil || grant.PodUID != previous.PodUID || session.ID != idle.ID || grant.TaskRoot != previous.TaskRoot ||
					grant.ResumeSession != "" || grant.ResumeWorkDir != previous.TaskRoot+"/workdir" || grant.TurnSequence != previous.TurnSequence+1 {
					t.Fatal("fresh native history did not reuse the exact compatible resident", err)
				}
				f.grant, f.session, f.input, f.selection = grant, session, input, selection
				f.prepareAndStart(t)
				f.settle(t, f.receive(t, ""), "")
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(f.options)
				if err != nil {
					t.Fatal("changed native inputs invalidated the persistent resident", err)
				}
				defer reopened.Close()
				current, _ := reopened.GetSession(idle.ID)
				actual, _ := reopened.Get(f.grant.AttemptID)
				if current.State != SessionIdle || current.PodUID != previous.PodUID || actual.Legacy || actual.ResumeSession != "" {
					t.Fatal("reopen lost the fresh turn's modern resident authority")
				}
			})
		}
	}
}

func TestWorkspaceReuseRequiresCurrentCoreAuthority(t *testing.T) {
	for _, scenario := range []string{"unknown_principal", "changed_principal", "changed_role", "changed_repository", "changed_scope"} {
		t.Run(scenario, func(t *testing.T) {
			f := newSessionFixture(t)
			f.settle(t, f.receive(t, ""), "")
			previous := f.grant
			input, selection := workspaceFollowup(f)
			if scenario == "changed_repository" {
				input.Repositories = []Repository{{URL: "https://github.com/example/other"}}
			}
			if scenario == "changed_scope" {
				input.ResourceScope = []string{"another-resource"}
			}
			queued, err := f.store.QueueClaim(input)
			if err != nil {
				t.Fatal(err)
			}
			compatibility := testConversationCompatibility(queued, selection)
			switch scenario {
			case "unknown_principal":
				compatibility.Authority.PrincipalID = ""
			case "changed_principal":
				id := uuid.NewString()
				compatibility.Authority.PrincipalID, compatibility.Authority.AgentOwnerID = id, id
				compatibility.Authority.Memberships = []AuthorityMembership{{UserID: id, Role: "owner"}}
			case "changed_role":
				compatibility.Authority.Memberships[0].Role = "admin"
			}
			digest := recordWorkspacePolicy(t, f.store, queued, selection, compatibility)
			if err := f.store.RecordSelection(queued.AttemptID, selection, digest); !errors.Is(err, ErrConflict) {
				t.Fatal("changed or unknown authority gained workspace selection", err)
			}
			if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, digest, sessionFixtureRetention, 4); !errors.Is(err, ErrConflict) {
				t.Fatal("changed or unknown authority gained the previous writer lease", err)
			}
			resident, _ := f.store.GetSession(previous.WorkerSessionID)
			storage, _ := f.store.GetStorage(previous.StorageID)
			if resident.State != SessionIdle || resident.PodUID != previous.PodUID || resident.ActiveAttempt != "" || storage.ActiveAttempt != "" || !storage.Dirty {
				t.Fatal("rejected authority changed the retained workspace")
			}
		})
	}
}

func TestEqualUnknownAuthorityCanRetainButCannotReuseWorkspace(t *testing.T) {
	unknown := func(c *ConversationCompatibilityV1) { c.Authority.PrincipalID = "" }
	f := newWorkspacePolicyFixture(t, ConversationAgentDM, unknown)
	f.settle(t, f.receive(t, ""), "")
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	if f.session.State != SessionIdle || storage.Checkpoint != nil {
		t.Fatal("physical retention invented reusable authority or discarded clean compute")
	}
	input, selection := workspaceFollowup(f)
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := testConversationCompatibility(queued, selection)
	unknown(&compatibility)
	digest := recordWorkspacePolicy(t, f.store, queued, selection, compatibility)
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, digest, sessionFixtureRetention, 4); !errors.Is(err, ErrConflict) {
		t.Fatal("equal unknown principals authorized access to previous files", err)
	}
}

func TestAcceptedOrdinaryFailureRetainsWorkspaceWithoutNativeHistory(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	original := f.grant
	input, selection := f.followup()
	f.input = input
	f.reserve(t, selection, 4)
	f.prepareAndStart(t)
	body, _ := json.Marshal(map[string]string{"error": "provider execution failed", "work_dir": f.grant.TaskRoot + "/workdir"})
	terminal, err := f.store.ReceiveTerminal(f.grant.AttemptID, "fail", body, ResumePointers{WorkDir: f.grant.TaskRoot + "/workdir"}, digest([]byte("actual failed result")))
	if err != nil {
		t.Fatal(err)
	}
	f.authenticateResult(t, terminal)
	if err := f.store.CompleteTurn(f.grant.AttemptID); !errors.Is(err, ErrConflict) {
		t.Fatal("a result without acceptance and quiescence released storage", err)
	}
	f.settle(t, terminal, "")
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	if f.session.State != SessionIdle || storage.Checkpoint != nil || !storage.Dirty || storage.WriterSessionID != f.session.ID {
		t.Fatal("ordinary failure discarded clean compute or inherited older native history")
	}
	input, fresh := workspaceFollowup(f)
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := recordTestCompatibility(t, f.store, queued, fresh)
	attemptedResume := fresh
	attemptedResume.Mode, attemptedResume.ReuseEligible = SelectionResume, true
	attemptedResume.SessionID, attemptedResume.SessionSource = nativeID, CheckpointSource{TaskID: original.TaskID, AttemptID: original.AttemptID}
	nativeInput := input
	nativeInput.TaskID = uuid.NewString()
	nativeQueued, err := f.store.QueueClaim(nativeInput)
	if err != nil {
		t.Fatal(err)
	}
	nativeCompatibility := recordTestCompatibility(t, f.store, nativeQueued, attemptedResume)
	if _, _, err := f.store.ReserveTurn(nativeQueued.AttemptID, attemptedResume, nativeCompatibility, sessionFixtureRetention, 4); !errors.Is(err, ErrConflict) {
		t.Fatal("an older valid native pointer bypassed the intervening ordinary failure", err)
	}
	grant, session, err := f.store.ReserveTurn(queued.AttemptID, fresh, compatibility, sessionFixtureRetention, 4)
	if err != nil || session.PodUID != original.PodUID || grant.TaskRoot != original.TaskRoot || grant.ResumeSession != "" {
		t.Fatal("accepted failure prevented safe fresh execution in the retained Pod", err)
	}
}

func TestLocalWorkspaceFallbackCannotAddNativeAuthority(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	previous := f.grant
	input, selection := workspaceFollowup(f)
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	backend := Selection{Conversation: selection.Conversation, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
	compatibility := testConversationCompatibility(queued, selection)
	digest := recordWorkspacePolicy(t, f.store, queued, backend, compatibility)
	resume := selection
	resume.Mode, resume.ReuseEligible, resume.SessionID = SelectionResume, true, nativeID
	resume.SessionSource = CheckpointSource{TaskID: previous.TaskID, AttemptID: previous.AttemptID}
	if err := f.store.RecordSelection(queued.AttemptID, resume, digest); !errors.Is(err, ErrConflict) {
		t.Fatal("local workspace evidence replaced the backend's missing native producer", err)
	}
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, resume, digest, sessionFixtureRetention, 4); !errors.Is(err, ErrConflict) {
		t.Fatal("local workspace evidence granted unselected native history", err)
	}
	if err := f.store.RecordSelection(queued.AttemptID, selection, digest); err != nil {
		t.Fatal(err)
	}
	grant, session, err := f.store.ReserveTurn(queued.AttemptID, selection, digest, sessionFixtureRetention, 4)
	if err != nil || session.PodUID != previous.PodUID || grant.ResumeSession != "" || grant.TaskRoot != previous.TaskRoot {
		t.Fatal("native-history refusal prevented the independently authorized fresh turn", err)
	}
}
