package workspace

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func residentExecutionEvidence(t *testing.T, f *sessionFixture, terminal Terminal) (TurnExecutionReceipt, SessionProof) {
	t.Helper()
	g := f.grant
	if err := f.store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckoutsFlushed(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	receipt := TurnExecutionReceipt{WorkerSessionID: g.WorkerSessionID, Conversation: g.Conversation, StorageID: g.StorageID,
		TaskID: g.TaskID, AttemptID: g.AttemptID, Generation: g.Generation, TurnSequence: g.TurnSequence,
		InputDigest: g.InputDigest, PodUID: g.PodUID, PVCUID: g.PVCUID, ResultDigest: terminal.ResultDigest,
		RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce, TaskProcessesStopped: true, LocalRequestsClosed: true, PrivateStateCleared: true}
	receipt.Signature = ed25519.Sign(f.key, TurnExecutionReceiptMessage(receipt))
	body, _ := json.Marshal(receipt)
	return receipt, f.proof(t, SessionOperationTurnExecutionReceipt, body)
}

func residentCompletionWitness(f *sessionFixture, terminal Terminal) CompletionWitness {
	g := f.grant
	return CompletionWitness{TaskID: g.TaskID, AttemptID: g.AttemptID, WorkerSessionID: g.WorkerSessionID,
		StorageID: g.StorageID, TurnSequence: g.TurnSequence, RequestDigest: terminal.RequestDigest, ResultDigest: terminal.ResultDigest,
		Status: "completed", WorkDir: g.TaskRoot + "/workdir", AcknowledgedAt: f.store.now()}
}

func reopenResidentFixture(t *testing.T, f *sessionFixture) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = Open(f.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Close() })
}

func TestResidentCompletionKeepsExactDirtyLeaseAcrossEvidenceOrders(t *testing.T) {
	for _, order := range [][]string{{"execution", "backend", "event"}, {"execution", "event", "backend"},
		{"backend", "execution", "event"}, {"backend", "event", "execution"}, {"event", "execution", "backend"}, {"event", "backend", "execution"}} {
		t.Run(order[0]+"_"+order[1]+"_"+order[2], func(t *testing.T) {
			f := newSessionFixture(t)
			if _, err := f.store.ReceiveEvent(f.grant.AttemptID, 1, []byte(`{"progress":"accepted provider work"}`)); err != nil {
				t.Fatal(err)
			}
			terminal := f.receive(t, "")
			input, selection := workspaceFollowup(f)
			queued, err := f.store.QueueClaim(input)
			if err != nil {
				t.Fatal(err)
			}
			compatibility := recordTestCompatibility(t, f.store, queued, selection)
			for i, step := range order {
				switch step {
				case "execution":
					receipt, proof := residentExecutionEvidence(t, f, terminal)
					if err := f.store.RecordTurnExecutionReceipt(f.grant.AttemptID, receipt, proof); err != nil {
						t.Fatal(err)
					}
				case "backend":
					if err := f.store.RecordCompletionWitness(f.grant.AttemptID, residentCompletionWitness(f, terminal)); err != nil {
						t.Fatal(err)
					}
				case "event":
					if err := f.store.EventDelivery(f.grant.AttemptID, 1, "received", "local"); err != nil {
						t.Fatal(err)
					}
				}
				if i != len(order)-1 {
					if err := f.store.CompleteTurn(f.grant.AttemptID); !errors.Is(err, ErrConflict) {
						t.Fatal("partial evidence settled task authority", err)
					}
					if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 4); !errors.Is(err, ErrStorageBusy) {
						t.Fatal("the next writer bypassed outstanding task evidence", err)
					}
				}
				reopenResidentFixture(t, f)
			}
			if err := f.store.CompleteTurn(f.grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			reopenResidentFixture(t, f)
			storage, _ := f.store.GetStorage(f.grant.StorageID)
			owner, err := f.store.WarmSession(storage.ID)
			if err != nil || owner.ID != f.session.ID || !storage.Dirty || storage.Checkpoint != nil ||
				storage.ActiveAttempt != "" || storage.WriterSessionID != owner.ID {
				t.Fatal("reopen lost the live dirty lease or fabricated cold storage proof", err)
			}
			next, session, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 4)
			if err != nil || next.StorageID != storage.ID || session.ID != owner.ID || session.PodUID != owner.PodUID {
				t.Fatal("the exact accepted live owner could not continue", err)
			}
		})
	}
}

func TestResidentReceiptRejectsFalseOrReboundEvidenceAndExpiresFreshProof(t *testing.T) {
	changes := map[string]func(*TurnExecutionReceipt){
		"task_process":  func(r *TurnExecutionReceipt) { r.TaskProcessesStopped = false },
		"local_request": func(r *TurnExecutionReceipt) { r.LocalRequestsClosed = false },
		"private_state": func(r *TurnExecutionReceipt) { r.PrivateStateCleared = false },
		"conversation":  func(r *TurnExecutionReceipt) { r.Conversation.SubjectID = uuid.NewString() },
		"storage":       func(r *TurnExecutionReceipt) { r.StorageID = uuid.NewString() },
		"attempt":       func(r *TurnExecutionReceipt) { r.AttemptID = uuid.NewString() },
		"input":         func(r *TurnExecutionReceipt) { r.InputDigest = digest([]byte("another assignment")) },
		"result":        func(r *TurnExecutionReceipt) { r.ResultDigest = digest([]byte("another result")) },
		"pod":           func(r *TurnExecutionReceipt) { r.PodUID = uuid.NewString() },
		"pvc":           func(r *TurnExecutionReceipt) { r.PVCUID = uuid.NewString() },
		"challenge":     func(r *TurnExecutionReceipt) { r.Nonce = uuid.NewString() },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			f := newSessionFixture(t)
			terminal := f.receive(t, "")
			receipt, _ := residentExecutionEvidence(t, f, terminal)
			change(&receipt)
			receipt.Signature = ed25519.Sign(f.key, TurnExecutionReceiptMessage(receipt))
			body, _ := json.Marshal(receipt)
			proof := f.proof(t, SessionOperationTurnExecutionReceipt, body)
			if err := f.store.RecordTurnExecutionReceipt(f.grant.AttemptID, receipt, proof); err == nil {
				t.Fatal("false or differently bound task-stop evidence was committed")
			}
			reopenResidentFixture(t, f)
			storage, _ := f.store.GetStorage(f.grant.StorageID)
			if storage.ActiveAttempt != f.grant.AttemptID || storage.WriterSessionID != f.session.ID || !storage.Dirty {
				t.Fatal("rejected evidence released the current writer")
			}
		})
	}
	t.Run("fresh proof expiry and accepted retry", func(t *testing.T) {
		f := newSessionFixture(t)
		terminal := f.receive(t, "")
		receipt, proof := residentExecutionEvidence(t, f, terminal)
		f.clock.Store(proof.ExpiresAt.UnixNano())
		if err := f.store.RecordTurnExecutionReceipt(f.grant.AttemptID, receipt, proof); !errors.Is(err, ErrUnauthorized) {
			t.Fatal("an expired unaccepted challenge gained authority", err)
		}
		body, _ := json.Marshal(receipt)
		proof = f.proof(t, SessionOperationTurnExecutionReceipt, body)
		if err := f.store.RecordTurnExecutionReceipt(f.grant.AttemptID, receipt, proof); err != nil {
			t.Fatal(err)
		}
		f.clock.Add(int64(2 * SessionChallengeTTL))
		reopenResidentFixture(t, f)
		if err := f.store.RecordTurnExecutionReceipt(f.grant.AttemptID, receipt, proof); err != nil {
			t.Fatal("durable accepted retry was lost", err)
		}
		changed := proof
		changed.Nonce = uuid.NewString()
		changed.Signature = ed25519.Sign(f.key, SessionProofMessage(changed))
		if err := f.store.RecordTurnExecutionReceipt(f.grant.AttemptID, receipt, changed); err == nil {
			t.Fatal("another proof replaced the accepted receipt")
		}
	})
}

func TestResidentCancellationWinsConcurrentCompletion(t *testing.T) {
	f := newSessionFixture(t)
	terminal := f.receive(t, "")
	receipt, proof := residentExecutionEvidence(t, f, terminal)
	if err := f.store.RecordTurnExecutionReceipt(f.grant.AttemptID, receipt, proof); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordCompletionWitness(f.grant.AttemptID, residentCompletionWitness(f, terminal)); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(2)
	done.Go(func() {
		ready.Done()
		<-start
		if err := f.store.CompleteTurn(f.grant.AttemptID); err != nil {
			t.Error(err)
		}
	})
	done.Go(func() {
		ready.Done()
		<-start
		if _, err := f.store.RequestSessionStop(f.session.ID, "cancelled"); err != nil {
			t.Error(err)
		}
	})
	ready.Wait()
	close(start)
	done.Wait()
	reopenResidentFixture(t, f)
	session, _ := f.store.GetSession(f.session.ID)
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	if session.State != SessionDraining || session.Stop == nil || storage.WriterSessionID != session.ID || !storage.Dirty || storage.Checkpoint != nil {
		t.Fatal("completion overrode cancellation or released the live resident writer")
	}
}

func TestResidentFinalSealCannotLabelUnacceptedLatestWriter(t *testing.T) {
	f := newSessionFixture(t)
	f.settle(t, f.receive(t, ""), "")
	accepted := f.grant
	input, selection := workspaceFollowup(f)
	f.input = input
	f.reserve(t, selection, 1)
	f.prepareAndStart(t)
	terminal := f.receive(t, "")
	f.stopAndClose(t, true, true)
	reopenResidentFixture(t, f)
	storage, _ := f.store.GetStorage(f.grant.StorageID)
	if storage.Dirty || storage.WriterSessionID != "" || storage.ActiveAttempt != "" || storage.Checkpoint != nil {
		t.Fatal("final seal labelled unaccepted latest files with a predecessor's checkpoint")
	}
	if err := f.store.CompleteTurn(accepted.AttemptID); err != nil {
		t.Fatal(err)
	}
	storage, _ = f.store.GetStorage(f.grant.StorageID)
	if storage.Checkpoint != nil {
		t.Fatal("late predecessor delivery changed the latest writer lineage")
	}
	retained, _ := f.store.Terminal(f.grant.AttemptID)
	if retained.ResultDigest != terminal.ResultDigest || retained.ResultReceipt == nil || retained.RecoveryFailure != nil {
		t.Fatal("missing producer acceptance overwrote the authenticated SDK result")
	}
	input, selection = workspaceFollowup(f)
	queued, err := f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility := recordTestCompatibility(t, f.store, queued, selection)
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, sessionFixtureRetention, 1); err == nil {
		t.Fatal("physically clean files without accepted lineage became selectable")
	}
	input.TaskID = uuid.NewString()
	selection = f.selection
	selection.Conversation.SubjectID = uuid.NewString()
	queued, err = f.store.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	compatibility = recordTestCompatibility(t, f.store, queued, selection)
	if _, _, err := f.store.ReserveTurn(queued.AttemptID, selection, compatibility, time.Minute, 1); err != nil {
		t.Fatal("proven final termination retained physical capacity", err)
	}
}

func TestResidentReopenRejectsAssignmentDifferentFromPreparedExecution(t *testing.T) {
	f := unusedSessionReservation(t)
	f.prepareAssignment(t)
	st, err := f.store.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := st.Grants[f.grant.AttemptID]
	var assignment, run map[string]json.RawMessage
	if err := json.Unmarshal(g.Assignment, &assignment); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(assignment["run"], &run); err != nil {
		t.Fatal(err)
	}
	run["prompt"], _ = json.Marshal("a different task's execution input")
	assignment["run"], _ = json.Marshal(run)
	raw, _ := json.Marshal(assignment)
	g.InputDigest, err = TurnInputDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	assignment["inputDigest"], _ = json.Marshal(g.InputDigest)
	g.Assignment, _ = json.Marshal(assignment)
	st.Grants[g.AttemptID] = g
	session := st.Sessions[g.WorkerSessionID]
	session.InputDigest = g.InputDigest
	st.Sessions[session.ID] = session
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(st)
	if err := os.WriteFile(filepath.Join(f.options.Directory, "journal.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(f.options)
	if err == nil {
		opened.Close()
		t.Fatal("reopen admitted execution input that differed from its prepared grant")
	}
}

func TestResidentPrivateScratchDoesNotReplaceJournalAuthorityOnRestart(t *testing.T) {
	f := newSessionFixture(t)
	if err := os.Mkdir(f.store.PreparationDirectory(), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.store.PreparationDirectory(), "journal.json"), []byte(`{"grants":"helper scratch is not authority"}`), 0600); err != nil {
		t.Fatal(err)
	}
	reopenResidentFixture(t, f)
	grant, err := f.store.Get(f.grant.AttemptID)
	storage, storageErr := f.store.GetStorage(f.grant.StorageID)
	if err != nil || storageErr != nil || !grant.StartConfirmed || grant.WorkerSessionID != f.session.ID ||
		storage.ActiveAttempt != grant.AttemptID || storage.WriterSessionID != grant.WorkerSessionID {
		t.Fatal("restart adopted scratch state or lost the original task authority", err, storageErr)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(f.store.PreparationDirectory()); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), f.store.PreparationDirectory()); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(f.options)
	if err == nil {
		opened.Close()
		t.Fatal("restart accepted an unowned redirected preparation namespace")
	}
}
