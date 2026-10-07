package workspace

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSessionRetentionBoundaryUsesIdleTime(t *testing.T) {
	for _, elapsed := range []time.Duration{599 * time.Second, sessionFixtureRetention} {
		t.Run(elapsed.String(), func(t *testing.T) {
			f := newSessionFixture(t)
			// Active execution may outlive retention. Its eventual idle interval
			// begins only after backend acceptance and signed writer quiescence.
			f.clock.Add(int64(2 * sessionFixtureRetention))
			if stopped, err := f.store.ExpireSession(f.session.ID, sessionFixtureRetention); err != nil || stopped {
				t.Fatal("active execution consumed an idle timeout", err)
			}
			nativeID := uuid.NewString()
			f.settle(t, f.receive(t, nativeID), nativeID)
			idle := f.session
			input, selection := f.followup()
			queued, err := f.store.QueueClaim(input)
			if err != nil {
				t.Fatal(err)
			}
			compatibility := recordTestCompatibility(t, f.store, queued, selection)
			f.clock.Store(idle.IdleSince.Add(elapsed).UnixNano())
			grant, session, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 4)
			if elapsed < sessionFixtureRetention {
				if err != nil || session.ID != idle.ID || grant.PodUID != f.grant.PodUID || grant.TaskRoot != f.grant.TaskRoot {
					t.Fatal("599-second follow-up did not reuse its compatible resident", err)
				}
			} else {
				if !errors.Is(err, ErrStorageBusy) {
					t.Fatal("600-second follow-up bypassed closure", err)
				}
				actual, _ := f.store.GetSession(idle.ID)
				if actual.State != SessionDraining || actual.ActiveAttempt != "" {
					t.Fatal("deadline equality allocated a new writer")
				}
			}
		})
	}
}

func TestSessionZeroRetentionPreservesCheckpointUntilColdReplacement(t *testing.T) {
	f := newSessionRetentionFixture(t, "codex", 0)
	nativeID := ""
	f.settle(t, f.receive(t, nativeID), nativeID)
	previous := f.grant
	if f.session.State != SessionDraining {
		t.Fatal("zero retention kept idle compute")
	}
	storage, _ := f.store.GetStorage(previous.StorageID)
	if storage.Checkpoint != nil || !storage.Dirty || storage.WriterSessionID != f.session.ID {
		t.Fatal("zero retention certified live storage before final shutdown")
	}
	input, selection := f.followup()
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := recordTestCompatibility(t, f.store, queued, selection)
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, 0, 4); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("cold replacement started before actual termination", err)
	}
	f.stopAndClose(t, true, true)
	grant, session, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, 0, 4)
	if err != nil || session.ID == previous.WorkerSessionID || grant.TaskRoot != previous.TaskRoot || grant.ResumeSession != nativeID || grant.StorageID != previous.StorageID {
		t.Fatal("cold replacement lost frozen storage or selected session", err)
	}
}

func TestSessionRestartAndShorterPolicyNeverExtendIdleDeadline(t *testing.T) {
	f := newSessionFixture(t)
	nativeID := uuid.NewString()
	f.settle(t, f.receive(t, nativeID), nativeID)
	idle := f.session
	f.clock.Store(idle.IdleSince.Add(2 * time.Minute).UnixNano())
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(f.options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stopped, err := reopened.ExpireSession(idle.ID, 2*sessionFixtureRetention); err != nil || stopped {
		t.Fatal("restart rejected a valid original idle interval", err)
	}
	actual, _ := reopened.GetSession(idle.ID)
	if actual.IdleDeadline != idle.IdleDeadline {
		t.Fatal("restart or longer policy extended the stored deadline")
	}
	shorter := 5 * time.Minute
	if stopped, err := reopened.ExpireSession(idle.ID, shorter); err != nil || stopped {
		t.Fatal("shorter policy prematurely ended its remaining interval", err)
	}
	actual, _ = reopened.GetSession(idle.ID)
	if actual.IdleDeadline != idle.IdleSince.Add(shorter) {
		t.Fatal("shorter policy did not use the original idle start")
	}
	f.clock.Store(actual.IdleDeadline.UnixNano())
	if stopped, err := reopened.ExpireSession(idle.ID, sessionFixtureRetention); err != nil || !stopped {
		t.Fatal("restored longer policy resurrected an expired shorter deadline", err)
	}
}
