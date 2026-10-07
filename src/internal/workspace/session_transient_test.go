package workspace

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func transientSessionFixture(t *testing.T) (*sessionFixture, Terminal) {
	t.Helper()
	f := newSessionProviderFixture(t, "pi")
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	input, selection := f.followup()
	f.input = input
	f.reserve(t, selection, 4)
	f.prepareAndStart(t)
	body, _ := json.Marshal(map[string]string{"error": "pi session is already in use by another execution", "work_dir": f.grant.TaskRoot + "/workdir"})
	terminal, err := f.store.ReceiveTerminal(f.grant.AttemptID, "fail", body,
		ResumePointers{WorkDir: f.grant.TaskRoot + "/workdir", ResumeRejectedTransient: true}, digest([]byte("signed Pi transient result")))
	if err != nil {
		t.Fatal(err)
	}
	f.authenticateResult(t, terminal)
	return f, terminal
}

func TestSessionTransientFailureNeedsAcceptanceAndQuiescence(t *testing.T) {
	f, terminal := transientSessionFixture(t)
	if err := f.store.CompleteTurn(f.grant.AttemptID); !errors.Is(err, ErrConflict) {
		t.Fatal("transient result alone released its writer", err)
	}
	f.settle(t, terminal, "")
	g, _ := f.store.Get(f.grant.AttemptID)
	storage, _ := f.store.GetStorage(g.StorageID)
	if !g.PendingResume.ResumeRejectedTransient || g.CompletionWitness.Status != "failed" || !g.TurnComplete ||
		storage.Checkpoint != nil || !storage.Dirty || storage.WriterSessionID != f.session.ID || f.session.State != SessionIdle {
		t.Fatal("accepted, quiescent transient outcome lost its exact failed witness")
	}
}

func TestSettledResidentTransientCanRequestReplacementWhileStorageStaysDirty(t *testing.T) {
	f, terminal := transientSessionFixture(t)
	f.settle(t, terminal, "")
	reopenResidentFixture(t, f)
	input := f.input
	input.TaskID = uuid.NewString()
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := recordTestCompatibility(t, f.store, queued, f.selection)
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, f.selection, compatibility, sessionFixtureRetention, 4); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("replacement bypassed the live resident writer", err)
	}
	resident, _ := f.store.GetSession(f.session.ID)
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	if resident.State != SessionDraining || resident.Stop == nil || !storage.Dirty || storage.WriterSessionID != resident.ID || storage.Checkpoint != nil {
		t.Fatal("accepted transient settlement could not progress replacement without falsifying storage proof")
	}
}

func TestSessionUnresolvedTransientCannotForkFromMissingHints(t *testing.T) {
	for _, change := range []string{"empty_hints", "same_session", "unresolved_defaults", "changed_model"} {
		t.Run(change, func(t *testing.T) {
			f, _ := transientSessionFixture(t)
			f.stopAndClose(t, false, true)
			input := f.input
			input.TaskID = uuid.NewString()
			if change == "same_session" {
				input.Envelope, _ = json.Marshal(map[string]string{"prior_session_id": f.grant.ResumeSession})
			}
			queued, err := f.store.QueueClaim(input)
			if err != nil {
				t.Fatal(err)
			}
			selection := f.selection
			compatibility := testConversationCompatibility(queued, selection)
			retention := sessionFixtureRetention
			if change == "unresolved_defaults" {
				selection.ReuseEligible, compatibility.ProviderOptionsResolved, retention = false, false, 0
			}
			if change == "changed_model" {
				compatibility.Model = "another-resolved-model"
			}
			if err := f.store.RecordBackendSelection(queued.AttemptID, selection); err != nil {
				t.Fatal(err)
			}
			if err := f.store.RecordCompatibility(queued.AttemptID, compatibility); err != nil {
				t.Fatal(err)
			}
			digest, err := compatibility.Digest()
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.RecordSelection(queued.AttemptID, selection, digest); err != nil {
				t.Fatal(err)
			}
			grant, session, err := f.store.ReserveTurn(queued.AttemptID, selection, digest, retention, 4)
			if change == "changed_model" {
				if err != nil || session.ID == f.session.ID || grant.StorageID == f.grant.StorageID {
					t.Fatal("proven static incompatibility did not create a separate generation", err)
				}
				f.grant, f.session, f.input = grant, session, input
				f.prepareAndStart(t)
				nativeID := uuid.NewString()
				f.settle(t, f.receive(t, nativeID), nativeID)
				f.stopAndClose(t, true, true)
				changedRoot := f.grant.StorageID
				f.input.TaskID = uuid.NewString()
				// Returning to the original model is a new, proven change from
				// the current generation. The superseded hold cannot revive.
				f.reserve(t, f.selection, 4)
				if f.grant.StorageID == changedRoot || f.grant.Compatibility.Model != "fixture-model" {
					t.Fatal("returning to the original model did not allocate its new generation")
				}
			} else if !errors.Is(err, ErrStorageBusy) {
				t.Fatal("unresolved transient resumed on a fresh root", err)
			}
		})
	}
}

func TestSessionTransientFlagCannotAuthorizeOtherOutcomes(t *testing.T) {
	for _, provider := range []string{"codex", "pi"} {
		t.Run(provider+"_fresh_session", func(t *testing.T) {
			f := newSessionProviderFixture(t, provider)
			if _, err := f.store.ReceiveTerminal(f.grant.AttemptID, "fail", []byte(`{"error":"session busy"}`),
				ResumePointers{WorkDir: f.grant.TaskRoot + "/workdir", ResumeRejectedTransient: true}, digest([]byte("invalid transient claim"))); err == nil {
				t.Fatal("unselected or unsupported native resume gained transient authority")
			}
		})
	}
}

func TestSessionCleanedLegacyPodReleasesResidentCapacityBeforeDelivery(t *testing.T) {
	s, _ := testStore(t)
	legacyInput := testGrant()
	legacy, key, _ := readyGrant(t, s, legacyInput)
	startGrant(t, s, legacy)
	terminal, err := s.ReceiveTerminal(legacy.AttemptID, "complete", []byte(`{"output":"legacy success"}`), ResumePointers{}, digest([]byte("legacy result")))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordResultReceipt(legacy.AttemptID, outcomeReceipt(legacy, terminal, key)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(legacy.AttemptID, "execution_finished"); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseCheckouts(legacy.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(legacy.AttemptID, StopEvidence{Kind: "terminated", PodUID: legacy.PodUID, PVCUID: legacy.PVCUID, ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(legacy.AttemptID); err != nil {
		t.Fatal(err)
	}
	input := testGrant()
	input.SessionProtocol, input.RuntimeID = true, uuid.NewString()
	queued, err := s.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	selection := Selection{Conversation: ConversationKey{OwnerID: queued.OwnerID, WorkspaceID: queued.WorkspaceID, AgentID: queued.AgentID,
		Kind: ConversationIssue, SubjectID: uuid.NewString()}, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
	compatibility := recordTestCompatibility(t, s, queued, selection)
	if _, _, err := s.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 1); !errors.Is(err, ErrResidentCapacity) {
		t.Fatal("uncleaned legacy Pod stopped consuming capacity", err)
	}
	if err := s.MarkCleaned(legacy.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 1); err != nil {
		t.Fatal("deleted legacy Pod still consumed resident capacity", err)
	}
	terminal, _ = s.Terminal(legacy.AttemptID)
	if terminal.State != "received" {
		t.Fatal("resident accounting changed terminal delivery")
	}
	if _, err := s.Create(legacyInput); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("capacity release also released unsettled legacy storage", err)
	}
}
