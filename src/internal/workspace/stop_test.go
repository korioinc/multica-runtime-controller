package workspace

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func stopReceipt(g TaskGrant, key ed25519.PrivateKey, flush bool) StopReceipt {
	r := StopReceipt{TaskID: g.TaskID, AttemptID: g.AttemptID, Generation: g.Generation, PodUID: g.PodUID, PVCUID: g.PVCUID,
		Revision: g.Stop.Revision, Nonce: g.Stop.Nonce, WritersStopped: true, FlushOK: flush}
	r.Signature = ed25519.Sign(key, StopReceiptMessage(r))
	return r
}

func TestStopBeforeAdmissionSurvivesRestartAndAllowsCleanRetry(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	g, daemonToken := preparedGrant(t, s, input)
	stopToken, err := s.IssueCapability(g.AttemptID, "stop", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	g, err = s.RequestStop(g.AttemptID, "cancelled")
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PinStopSupervisor(g.AttemptID, g.PodUID, g.PVCUID, g.NodeID, pub); err != nil {
		t.Fatal("cancelled initialization could not register its cleanup supervisor", err)
	}
	if _, err := s.Authorize(daemonToken, "daemon"); err == nil {
		t.Fatal("cancelled initialization retained task API access")
	}
	if _, err := s.Authorize(stopToken, "daemon"); err == nil {
		t.Fatal("stop token acquired task API authority")
	}
	if err := s.Admit(g.AttemptID, pub); err == nil {
		t.Fatal("cancelled initialization gained provider execution authority")
	}
	receipt := stopReceipt(g, key, true)
	if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err == nil {
		t.Fatal("worker certified clean cancellation without closing controller checkout writers")
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err == nil {
		t.Fatal("receipt alone released a potentially running Pod")
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "terminated", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.AuthorizeStop(stopToken); err != nil {
		t.Fatal("restart lost stop-only reporting authority", err)
	}
	if _, err := s.RequestStop(g.AttemptID, "another observation"); err != nil {
		t.Fatal(err)
	}
	if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err != nil {
		t.Fatal("durable stop retry changed the signed challenge", err)
	}
	if err := s.CloseStopped(g.AttemptID); err != nil {
		t.Fatal("restart lost positive termination evidence", err)
	}
	if _, err := s.Create(input); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("same task reused storage before resource cleanup", err)
	}
	if err := s.MarkCleaned(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err != nil {
		t.Fatal("lost receipt ACK could not be retried after closure", err)
	}
	next, err := s.Create(input)
	if err != nil || !next.Reuse || next.TaskRoot != g.TaskRoot {
		t.Fatal("clean cancellation lost its task workspace", err)
	}
	if _, err := s.AuthorizeStop(stopToken); err == nil {
		t.Fatal("old stop token reached a replacement attempt")
	}
	if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err == nil {
		t.Fatal("old supervisor changed journal after replacement")
	}
}

func TestStopReceiptRejectsChangedIdentityAndCannotReplaceFailure(t *testing.T) {
	s, _ := testStore(t)
	g, key, _ := readyGrant(t, s, testGrant())
	stopToken, err := s.IssueCapability(g.AttemptID, "stop", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PinStopSupervisor(g.AttemptID, g.PodUID, g.PVCUID, g.NodeID, key.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"error":"worker initialization failed"}`)
	if _, err := s.ReceiveFailure(g.AttemptID, body); err != nil {
		t.Fatal(err)
	}
	g, err = s.RequestStop(g.AttemptID, "initialization_failed")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AuthorizeStop(stopToken); err != nil {
		t.Fatal("quarantine removed stop reporting authority", err)
	}
	receipt := stopReceipt(g, key, true)
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	forged := receipt
	forged.Signature = bytes.Clone(receipt.Signature)
	forged.Signature[0] ^= 0xff
	if err := s.ReceiveStopReceipt(g.AttemptID, forged); err == nil {
		t.Fatal("unsigned evidence acquired clean storage authority")
	}
	for _, mutate := range []func(*StopReceipt){
		func(r *StopReceipt) { r.PodUID = uuid.NewString() },
		func(r *StopReceipt) { r.PVCUID = uuid.NewString() },
		func(r *StopReceipt) { r.Generation++ },
		func(r *StopReceipt) { r.AttemptID = uuid.NewString() },
		func(r *StopReceipt) { r.TaskID = uuid.NewString() },
		func(r *StopReceipt) { r.Revision++ },
		func(r *StopReceipt) { r.Nonce = uuid.NewString() },
	} {
		other := receipt
		mutate(&other)
		other.Signature = ed25519.Sign(key, StopReceiptMessage(other))
		if err := s.ReceiveStopReceipt(g.AttemptID, other); err == nil {
			t.Fatal("another execution acquired stop proof authority")
		}
	}
	if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err != nil {
		t.Fatal("controller failure prevented independent stop evidence", err)
	}
	changed := stopReceipt(g, key, false)
	if err := s.ReceiveStopReceipt(g.AttemptID, changed); err == nil {
		t.Fatal("conflicting report overwrote committed stop proof")
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "terminated", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err != nil {
		t.Fatal("pending controller failure blocked proven execution termination", err)
	}
	terminal, err := s.Terminal(g.AttemptID)
	if err != nil || !bytes.Equal(terminal.Body, body) || terminal.Source != "controller" {
		t.Fatal("stop proof replaced the original failure", err)
	}
}

func TestProvenTerminationReleasesExecutionButKeepsDirtyData(t *testing.T) {
	for _, receiptFlush := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_receipt", true: "failed_flush"}[receiptFlush], func(t *testing.T) {
			s, options := testStore(t)
			input := testGrant()
			g, key, _ := readyGrant(t, s, input)
			var err error
			g, err = s.RequestStop(g.AttemptID, "cancelled")
			if err != nil {
				t.Fatal(err)
			}
			if receiptFlush {
				if err := s.PinStopSupervisor(g.AttemptID, g.PodUID, g.PVCUID, g.NodeID, key.Public().(ed25519.PublicKey)); err != nil {
					t.Fatal(err)
				}
				if err := s.CloseCheckouts(g.AttemptID); err != nil {
					t.Fatal(err)
				}
				if err := s.ReceiveStopReceipt(g.AttemptID, stopReceipt(g, key, false)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "terminated", ObservedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			if err := s.CloseStopped(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkCleaned(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			s.Close()
			s, err = Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			stopped, err := s.Get(g.AttemptID)
			if err != nil || stopped.State != "closed" {
				t.Fatal("proven termination retained execution capacity", err)
			}
			if _, err := s.Create(input); !errors.Is(err, ErrStorageBusy) {
				t.Fatal("unflushed workspace was reused", err)
			}
			if _, err := s.Create(testGrant()); err != nil {
				t.Fatal("dirty task data blocked independent task storage", err)
			}
		})
	}
}

func TestStopRejectsNewResourceCreation(t *testing.T) {
	for _, tc := range []struct{ name, before, after string }{
		{"secret", `{}`, `{"secretCreateRequested":true}`},
		{"pod", `{"secretCreateRequested":true,"podCreateRequested":false}`, `{"secretCreateRequested":true,"podCreateRequested":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testStore(t)
			g, err := s.Create(testGrant())
			if err != nil {
				t.Fatal(err)
			}
			if err := s.SetResources(g.AttemptID, json.RawMessage(tc.before)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.RequestStop(g.AttemptID, "cancelled"); err != nil {
				t.Fatal(err)
			}
			if err := s.SetResources(g.AttemptID, json.RawMessage(tc.after)); !errors.Is(err, ErrConflict) {
				t.Fatal("cancelled attempt issued new resource creation authority", err)
			}
		})
	}
}

func TestStopCannotReleaseAmbiguousPodOrPreparationWriter(t *testing.T) {
	for _, writer := range []string{"preparation", "pod"} {
		t.Run(writer, func(t *testing.T) {
			s, _ := testStore(t)
			input := testGrant()
			g, err := s.Create(input)
			if err != nil {
				t.Fatal(err)
			}
			if writer == "preparation" {
				process := PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://preparing"}
				if err := s.BeginPreparation(g.AttemptID, process); err != nil {
					t.Fatal(err)
				}
			} else if err := s.SetResources(g.AttemptID, json.RawMessage(`{"podCreateRequested":true}`)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.RequestStop(g.AttemptID, "cancelled"); err != nil {
				t.Fatal(err)
			}
			evidence := StopEvidence{PVCUID: g.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()}
			if err := s.ObserveStop(g.AttemptID, evidence); err == nil {
				t.Fatal("unproven writer was mistaken for no worker")
			}
			if writer == "pod" {
				if err := s.SetResources(g.AttemptID, json.RawMessage(`{"podCreateRequested":false}`)); err == nil {
					t.Fatal("late resource update erased uncertain Pod creation")
				}
			}
			if err := s.CloseStopped(g.AttemptID); err == nil {
				t.Fatal("uncertain writer released task ownership")
			}
			if _, err := s.Create(input); !errors.Is(err, ErrStorageBusy) {
				t.Fatal("unknown writer gained a competing attempt", err)
			}
			if writer == "preparation" {
				if err := s.CompletePreparation(g.AttemptID, nil, nil); err != nil {
					t.Fatal("cancellation prevented preparation from recording its exit", err)
				}
			}
		})
	}
}

func TestNeverStartedWorkerNeedsDurablePreparationForReuse(t *testing.T) {
	for _, flushed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unflushed_preparation", true: "durable_preparation"}[flushed], func(t *testing.T) {
			s, _ := testStore(t)
			input := testGrant()
			g, _ := preparedGrant(t, s, input)
			if _, err := s.RequestStop(g.AttemptID, "init_failed"); err != nil {
				t.Fatal(err)
			}
			if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "never-started", ObservedAt: time.Now().UTC(), PreparedFlushOK: flushed}); err != nil {
				t.Fatal(err)
			}
			if err := s.CloseStopped(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkCleaned(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			_, err := s.Create(input)
			if flushed && err != nil {
				t.Fatal("verified unused durable preparation could not be reused", err)
			}
			if !flushed && !errors.Is(err, ErrStorageBusy) {
				t.Fatal("helper exit alone was treated as durable workspace proof", err)
			}
		})
	}
}

func TestStopWaitingClaimClosesWithoutFabricatedProviderResult(t *testing.T) {
	s, _ := testStore(t)
	input := testGrant()
	g, err := s.QueueClaim(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(g.AttemptID, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{Kind: "no-worker", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err != nil {
		t.Fatal("unallocated cancellation could not settle", err)
	}
	if _, err := s.Terminal(g.AttemptID); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("unexecuted cancellation invented a provider result", err)
	}
	if err := s.MarkCleaned(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(input); err != nil {
		t.Fatal("unallocated cancellation permanently blocked its task", err)
	}
}

func TestUncertainResultAllowsStopCleanupButNeverTaskReplay(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	g, key, _ := readyGrant(t, s, input)
	startGrant(t, s, g)
	terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", []byte(`{"output":"finished"}`), ResumePointers{}, core.Digest([]byte("provider result")))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Seal(g.AttemptID, sealReceipt(g, terminal, key)); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginForward(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(g.AttemptID, "result_delivery_pending"); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "terminated", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err != nil {
		t.Fatal("delivery uncertainty held a positively stopped process", err)
	}
	if err := s.MarkCleaned(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.BeginForward(g.AttemptID); err == nil {
		t.Fatal("closed execution authorized ambiguous result retransmission")
	}
	if _, err := s.Create(input); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("resource cleanup replayed a task with unresolved backend delivery", err)
	}
	st, err := s.snapshot()
	if err != nil || st.Storages[g.StorageID].Dirty {
		t.Fatal("valid worker seal was discarded because delivery was uncertain", err)
	}
}

func TestPreviousJournalMigrationPreservesQuarantineAndPendingDelivery(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	g, _, _ := readyGrant(t, s, input)
	if _, err := s.ReceiveFailure(g.AttemptID, []byte(`{"error":"old initialization failure"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginFailureForward(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	st, err := s.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	st.SchemaVersion = 6
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(st); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(options.Directory, "journal.json"), encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal("previous authority could not migrate safely", err)
	}
	defer s.Close()
	if _, err := s.Create(input); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("migration released an unproven writer", err)
	}
	if err := s.BeginFailureForward(g.AttemptID); err == nil {
		t.Fatal("migration replayed an ambiguously delivered failure")
	}
	if err := s.CloseStopped(g.AttemptID); err == nil {
		t.Fatal("migration fabricated positive stop evidence")
	}
	if _, err := s.RequestStop(g.AttemptID, "legacy_failure"); err != nil {
		t.Fatal("migrated quarantine cannot enter recovery", err)
	}
}

func TestUnsupportedJournalAuthorityIsPreservedWithoutRewrite(t *testing.T) {
	s, options := testStore(t)
	s.Close()
	path := filepath.Join(options.Directory, "journal.json")
	raw := []byte(`{"schemaVersion":5,"grants":{"legacy":{"runtimeRef":{"controllerABI":3}}}}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(options); err == nil {
		opened.Close()
		t.Fatal("unsupported historical authority was silently adopted")
	}
	retained, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(retained, raw) {
		t.Fatal("rejected migration altered historical authority", err)
	}
}
