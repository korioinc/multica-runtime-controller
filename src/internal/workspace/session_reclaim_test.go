package workspace

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func unusedSessionReservation(t *testing.T) *sessionFixture {
	t.Helper()
	store, options := testStore(t)
	input := testGrant()
	input.SessionProtocol, input.RuntimeID = true, uuid.NewString()
	input.Envelope = json.RawMessage(`{"workspace_slug":"original-workspace","issue_identifier":"ISSUE-1"}`)
	selection := Selection{Conversation: ConversationKey{OwnerID: options.OwnerID, WorkspaceID: input.WorkspaceID,
		AgentID: input.AgentID, Kind: ConversationIssue, SubjectID: uuid.NewString()}, ReuseEligible: true, WorkspaceReuseEligible: true, Mode: SelectionFreshWorkspace}
	f := &sessionFixture{store: store, options: options, input: input, selection: selection}
	f.reserve(t, selection, 1)
	return f
}

func closeUnusedSession(t *testing.T, f *sessionFixture, cleaned bool) {
	t.Helper()
	if _, err := f.store.RequestSessionStop(f.session.ID, "assignment_changed"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ObserveSessionStop(f.session.ID, StopEvidence{Kind: "no-worker", PVCUID: f.session.PVCUID, ObservedAt: f.store.now()}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CloseSession(f.session.ID); err != nil {
		t.Fatal(err)
	}
	if cleaned {
		if err := f.store.MarkSessionCleaned(f.session.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSessionReclaimUsesTheUnusedFrozenTaskRoot(t *testing.T) {
	for _, renamed := range []bool{false, true} {
		name := "unchanged_labels"
		if renamed {
			name = "renamed_labels"
		}
		t.Run(name, func(t *testing.T) {
			f := unusedSessionReservation(t)
			original := f.grant
			closeUnusedSession(t, f, true)
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			f.store, err = Open(f.options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.store.Close() }()
			if renamed {
				f.input.Envelope = json.RawMessage(`{"workspace_slug":"renamed-workspace","issue_identifier":"ISSUE-2"}`)
				// Reclaim the existing slot even when new storage is unavailable.
				f.store.options.MaxTasks = 1
			}
			queued, err := f.store.QueueClaim(f.input)
			if err != nil {
				t.Fatal(err)
			}
			compatibility := recordTestCompatibility(t, f.store, queued, f.selection)
			var contenders sync.WaitGroup
			results := make(chan TaskGrant, 8)
			failures := make(chan error, 8)
			for range cap(results) {
				contenders.Go(func() {
					grant, _, err := f.store.ReserveTurn(queued.AttemptID, f.selection, compatibility, sessionFixtureRetention, 1)
					results <- grant
					failures <- err
				})
			}
			contenders.Wait()
			close(results)
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatal("settled unused task could not be reclaimed", err)
				}
			}
			var reserved TaskGrant
			for grant := range results {
				if grant.AttemptID == original.AttemptID || grant.Generation <= original.Generation || grant.WorkerSessionID == original.WorkerSessionID ||
					grant.StorageID != original.StorageID || grant.TaskRoot != original.TaskRoot || grant.Reuse || grant.ResumeSession != "" || grant.ResumeWorkDir != "" {
					t.Fatal("reclaim lost the frozen root or inherited old execution authority")
				}
				if reserved.AttemptID != "" && (reserved.AttemptID != grant.AttemptID || reserved.WorkerSessionID != grant.WorkerSessionID) {
					t.Fatal("concurrent reservation allocated more than one writer")
				}
				reserved = grant
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.store, err = Open(f.options)
			if err != nil {
				t.Fatal(err)
			}
			current, err := f.store.Get(reserved.AttemptID)
			if err != nil || current.TaskRoot != original.TaskRoot || current.WorkerSessionID != reserved.WorkerSessionID {
				t.Fatal("reopening lost the reclaimed writer binding", err)
			}
			old, err := f.store.Get(original.AttemptID)
			if err != nil || old.State != "closed" || !old.ExecutionRevoked || old.PreparationStarted {
				t.Fatal("reclaim changed the retired attempt's no-writer proof", err)
			}
		})
	}
}

func TestSessionReclaimWaitsForSettlementAndCleanup(t *testing.T) {
	for _, pending := range []string{"resources", "delivery"} {
		t.Run(pending, func(t *testing.T) {
			f := unusedSessionReservation(t)
			if pending == "delivery" {
				if _, err := f.store.ReceiveFailure(f.grant.AttemptID, json.RawMessage(`{"error":"dispatch changed"}`)); err != nil {
					t.Fatal(err)
				}
				if err := f.store.BeginFailureForward(f.grant.AttemptID); err != nil {
					t.Fatal(err)
				}
				if err := f.store.FinishForward(f.grant.AttemptID, "uncertain"); err != nil {
					t.Fatal(err)
				}
			}
			closeUnusedSession(t, f, pending != "resources")
			queued, err := f.store.QueueClaim(f.input)
			if err != nil {
				t.Fatal(err)
			}
			compatibility := recordTestCompatibility(t, f.store, queued, f.selection)
			if _, _, err := f.store.ReserveTurn(queued.AttemptID, f.selection, compatibility, sessionFixtureRetention, 1); !errors.Is(err, ErrStorageBusy) {
				t.Fatalf("unsettled %s allowed reclaim: %v", pending, err)
			}
			if pending == "resources" {
				err = f.store.MarkSessionCleaned(f.session.ID)
			} else {
				err = f.store.RejectFailure(f.grant.AttemptID)
			}
			if err != nil {
				t.Fatal(err)
			}
			grant, _, err := f.store.ReserveTurn(queued.AttemptID, f.selection, compatibility, sessionFixtureRetention, 1)
			if err != nil || grant.StorageID != f.grant.StorageID {
				t.Fatal("settled fence did not release the unused reservation", err)
			}
		})
	}
}

func TestSessionReclaimDoesNotBypassRetainedOrIncompatibleStorage(t *testing.T) {
	for _, prior := range []string{"partial_preparation", "completed_provider", "changed_scope"} {
		t.Run(prior, func(t *testing.T) {
			var f *sessionFixture
			if prior == "completed_provider" {
				f = newSessionFixture(t)
				nativeSession := uuid.NewString()
				f.settle(t, f.receive(t, nativeSession), nativeSession)
				f.stopAndClose(t, true, true)
			} else {
				f = unusedSessionReservation(t)
				if prior == "partial_preparation" {
					if err := f.store.BeginPreparation(f.grant.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://partial"}); err != nil {
						t.Fatal(err)
					}
					if err := f.store.CompletePreparation(f.grant.AttemptID, nil, nil); err != nil {
						t.Fatal(err)
					}
				}
				closeUnusedSession(t, f, true)
			}
			before, err := f.store.GetStorage(f.grant.StorageID)
			if err != nil {
				t.Fatal(err)
			}
			f.input.Envelope = json.RawMessage(`{"workspace_slug":"renamed-workspace","issue_identifier":"ISSUE-2"}`)
			if prior == "changed_scope" {
				f.input.ResourceScope = []string{"changed-authority"}
			}
			queued, err := f.store.QueueClaim(f.input)
			if err != nil {
				t.Fatal(err)
			}
			compatibility := recordTestCompatibility(t, f.store, queued, f.selection)
			if _, _, err := f.store.ReserveTurn(queued.AttemptID, f.selection, compatibility, sessionFixtureRetention, 1); err == nil {
				t.Fatal("renamed labels bypassed the frozen task's retained or incompatible storage")
			}
			after, err := f.store.GetStorage(f.grant.StorageID)
			if err != nil || !sameJSON(before, after) {
				t.Fatal("refused reclaim changed retained data or its writer proof", err)
			}
		})
	}
}

func TestSessionReclaimStillRejectsAnotherTasksShortPathCollision(t *testing.T) {
	f := unusedSessionReservation(t)
	closeUnusedSession(t, f, true)
	f.input.TaskID = uuid.NewString()[:24] + f.input.TaskID[24:]
	f.selection.Conversation.SubjectID = uuid.NewString()
	queued, err := f.store.QueueClaim(f.input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := recordTestCompatibility(t, f.store, queued, f.selection)
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, f.selection, compatibility, sessionFixtureRetention, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("another task adopted the matching shortened path: %v", err)
	}
}
