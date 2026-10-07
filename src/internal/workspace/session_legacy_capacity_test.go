package workspace

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSessionImportedPreparingIntentReservesResidentCapacity(t *testing.T) {
	for _, resourcesRecorded := range []bool{false, true} {
		name := "before_resource_record"
		if resourcesRecorded {
			name = "before_pod_request"
		}
		t.Run(name, func(t *testing.T) {
			s, options := testStore(t)
			legacy, err := s.Create(testGrant())
			if err != nil {
				t.Fatal(err)
			}
			if resourcesRecorded {
				if err := s.SetResources(legacy.AttemptID, json.RawMessage(`{"podCreateRequested":false,"secretCreateRequested":false,"reference":{}}`)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.BeginPreparation(legacy.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://legacy-preparation"}); err != nil {
				t.Fatal(err)
			}
			s = reopenLegacyJournal(t, s, options)
			legacy, err = s.Get(legacy.AttemptID)
			if err != nil || !legacy.Legacy || legacy.State != "intent" || legacy.Stop != nil ||
				!legacy.PreparationStarted || legacy.PreparationStopped || !noWorkerCreated(legacy) {
				t.Fatal("migration did not retain the live original preparation", err)
			}
			// An unrelated conversation has execution room, but the imported
			// intent already owns the sole future physical Pod slot.
			input := testGrant()
			input.SessionProtocol, input.RuntimeID = true, uuid.NewString()
			queued, err := s.QueueClaim(input)
			if err != nil {
				t.Fatal(err)
			}
			selection := Selection{Conversation: ConversationKey{OwnerID: queued.OwnerID, WorkspaceID: queued.WorkspaceID,
				AgentID: queued.AgentID, Kind: ConversationIssue, SubjectID: uuid.NewString()}, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
			compatibility := recordTestCompatibility(t, s, queued, selection)
			const contenders = 8
			start := make(chan struct{})
			results := make(chan error, contenders)
			for range contenders {
				go func() {
					<-start
					_, _, err := s.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 1)
					results <- err
				}()
			}
			close(start)
			var unexpected []error
			for range contenders {
				if err := <-results; !errors.Is(err, ErrResidentCapacity) {
					unexpected = append(unexpected, err)
				}
			}
			if len(unexpected) != 0 {
				t.Fatalf("live legacy preparation lost its resident reservation: %v", unexpected)
			}
			current, _ := s.Get(queued.AttemptID)
			if current.WorkerSessionID != "" || current.State != "waiting_storage" {
				t.Fatal("capacity denial granted a modern writer")
			}
			// Cancellation before any Pod request prevents legacy provisioning.
			// Its unrelated preparation cleanup need not occupy a physical slot.
			if _, err := s.RequestStop(legacy.AttemptID, "cancelled_before_worker"); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 1); err != nil {
				t.Fatal("a stopped no-worker intent still occupied resident capacity", err)
			}
		})
	}
}

func reopenLegacyJournal(t *testing.T, s *Store, options Options) *Store {
	t.Helper()
	snapshot, err := s.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.SchemaVersion = 13
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	encoder := json.NewEncoder(&raw)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(options.Directory, "journal.json"), raw.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func importedLegacyWaiters(t *testing.T, count int) (*Store, Options, []TaskGrant) {
	t.Helper()
	s, options := testStore(t)
	var grants []TaskGrant
	for range count {
		grant, err := s.QueueClaim(testGrant())
		if err != nil {
			t.Fatal(err)
		}
		grants = append(grants, grant)
	}
	s = reopenLegacyJournal(t, s, options)
	for index, grant := range grants {
		imported, err := s.Get(grant.AttemptID)
		if err != nil || !imported.Legacy || imported.SessionProtocol || imported.State != "waiting_storage" || imported.StorageID != "" {
			t.Fatal("migration changed queued legacy authority", err)
		}
		grants[index] = imported
	}
	return s, options, grants
}

func TestSessionLegacyAndModernActivationShareOneResidentSlot(t *testing.T) {
	for _, order := range []string{"modern_first", "legacy_first", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			s, _, legacy := importedLegacyWaiters(t, 1)
			input := testGrant()
			input.SessionProtocol, input.RuntimeID = true, uuid.NewString()
			modern, err := s.QueueClaim(input)
			if err != nil {
				t.Fatal(err)
			}
			selection := Selection{Conversation: ConversationKey{OwnerID: modern.OwnerID, WorkspaceID: modern.WorkspaceID,
				AgentID: modern.AgentID, Kind: ConversationIssue, SubjectID: uuid.NewString()}, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
			compatibility := recordTestCompatibility(t, s, modern, selection)
			modern, _ = s.Get(modern.AttemptID)
			activate := func(isLegacy bool) error {
				if isLegacy {
					_, err := s.Create(legacy[0], 1)
					return err
				}
				_, _, err := s.ReserveTurn(modern.AttemptID, selection, compatibility, sessionFixtureRetention, 1)
				return err
			}
			var outcomes []error
			if order == "concurrent" {
				start := make(chan struct{})
				results := make(chan error, 2)
				for _, isLegacy := range []bool{true, false} {
					go func() { <-start; results <- activate(isLegacy) }()
				}
				close(start)
				outcomes = []error{<-results, <-results}
			} else {
				legacyFirst := order == "legacy_first"
				outcomes = []error{activate(legacyFirst), activate(!legacyFirst)}
			}
			admitted, blocked := 0, 0
			for _, err := range outcomes {
				if err == nil {
					admitted++
				} else if errors.Is(err, ErrResidentCapacity) {
					blocked++
				} else {
					t.Fatal("unexpected activation outcome", err)
				}
			}
			if admitted != 1 || blocked != 1 {
				t.Fatal("legacy and modern activation consumed the same resident slot", outcomes)
			}
			for _, original := range []TaskGrant{legacy[0], modern} {
				current, _ := s.Get(original.AttemptID)
				if current.State == "waiting_storage" && !sameJSON(current, original) {
					t.Fatal("resident denial changed a queued claim")
				}
				if current.TaskID != original.TaskID || current.Legacy != original.Legacy || !bytes.Equal(current.Envelope, original.Envelope) {
					t.Fatal("activation changed original task identity or source")
				}
			}
			storages, _ := s.ListStorages()
			if len(storages) != 1 {
				t.Fatal("denied activation allocated another storage root")
			}
		})
	}
}

func TestSessionImportedLegacyWaitersCompeteAtomically(t *testing.T) {
	s, _, grants := importedLegacyWaiters(t, 2)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, grant := range grants {
		go func() { <-start; _, err := s.Create(grant, 1); results <- err }()
	}
	close(start)
	admitted, blocked := 0, 0
	for range grants {
		err := <-results
		if err == nil {
			admitted++
		} else if errors.Is(err, ErrResidentCapacity) {
			blocked++
		} else {
			t.Fatal(err)
		}
	}
	if admitted != 1 || blocked != 1 {
		t.Fatal("imported waiters consumed more than one physical reservation")
	}
	var waiting TaskGrant
	for _, original := range grants {
		current, _ := s.Get(original.AttemptID)
		if current.State == "intent" {
			if _, err := s.RequestStop(current.AttemptID, "cancelled_before_worker"); err != nil {
				t.Fatal(err)
			}
		} else {
			if !sameJSON(current, original) {
				t.Fatal("blocked legacy activation mutated its original queued claim")
			}
			waiting = original
		}
	}
	activated, err := s.Create(waiting, 1)
	if err != nil || activated.TaskID != waiting.TaskID || activated.AttemptID != waiting.AttemptID || !activated.Legacy {
		t.Fatal("released capacity dropped or replaced the queued legacy execution", err)
	}
}

func TestSessionLegacyActivationPersistsOnlyRequiredIdleEviction(t *testing.T) {
	s, options, waiting := importedLegacyWaiters(t, 1)
	clock := new(atomic.Int64)
	clock.Store(time.Now().UTC().UnixNano())
	s.options.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	input := testGrant()
	input.SessionProtocol, input.RuntimeID = true, uuid.NewString()
	selection := Selection{Conversation: ConversationKey{OwnerID: options.OwnerID, WorkspaceID: input.WorkspaceID, AgentID: input.AgentID,
		Kind: ConversationIssue, SubjectID: uuid.NewString()}, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
	first := &sessionFixture{store: s, options: options, clock: clock, input: input, selection: selection}
	first.reserve(t, selection, 2)
	first.prepareAndStart(t)
	nativeID := uuid.NewString()
	first.settle(t, first.receive(t, nativeID), nativeID)
	clock.Add(int64(time.Minute))
	input.TaskID, selection.Conversation.SubjectID = uuid.NewString(), uuid.NewString()
	second := &sessionFixture{store: s, options: options, clock: clock, input: input, selection: selection}
	second.reserve(t, selection, 2)
	second.prepareAndStart(t)
	nativeID = uuid.NewString()
	second.settle(t, second.receive(t, nativeID), nativeID)
	changed := waiting[0]
	changed.Metadata = json.RawMessage(`{"changed":true}`)
	if _, err := s.Create(changed, 2); !errors.Is(err, ErrConflict) {
		t.Fatal("changed queued input was not rejected", err)
	}
	for _, id := range []string{first.session.ID, second.session.ID} {
		current, _ := s.GetSession(id)
		if current.State != SessionIdle || current.Stop != nil {
			t.Fatal("rejected activation evicted a resident")
		}
	}
	for range 3 {
		if _, err := s.Create(waiting[0], 2); !errors.Is(err, ErrResidentCapacity) {
			t.Fatal("legacy activation bypassed the physical ceiling", err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(options.Directory, "journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted registry
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Sessions[first.session.ID].State != SessionDraining || persisted.Sessions[second.session.ID].State != SessionIdle ||
		!sameJSON(persisted.Grants[waiting[0].AttemptID], waiting[0]) || len(persisted.Storages) != 2 {
		t.Fatal("denial lost its eviction, evicted extra residents, or activated the waiting claim")
	}
	first.stopAndClose(t, true, true)
	activated, err := s.Create(waiting[0], 2)
	if err != nil || activated.AttemptID != waiting[0].AttemptID || activated.TaskID != waiting[0].TaskID || !activated.Legacy {
		t.Fatal("final resident cleanup did not release the original legacy claim", err)
	}
}
