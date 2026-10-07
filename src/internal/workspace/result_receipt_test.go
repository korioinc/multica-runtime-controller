package workspace

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func outcomeReceipt(g TaskGrant, terminal Terminal, key ed25519.PrivateKey) ResultReceipt {
	r := ResultReceipt{TaskID: g.TaskID, AttemptID: g.AttemptID, PodUID: g.PodUID, PVCUID: g.PVCUID, RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}
	r.Signature = ed25519.Sign(key, ResultReceiptMessage(r))
	return r
}

func TestResultAuthenticationAndRecoveryChooseOneDurableOutcome(t *testing.T) {
	s, options := testStore(t)
	g, key, _ := readyGrant(t, s, testGrant())
	startGrant(t, s, g)
	body := []byte(`{"output":"completed provider work"}`)
	terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(g.AttemptID, "worker_unsealed"); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{Kind: "terminated", PodUID: g.PodUID, PVCUID: g.PVCUID, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	receipt := outcomeReceipt(g, terminal, key)
	failure := []byte(`{"error":"shutdown evidence missing"}`)
	start := make(chan struct{})
	authenticated, recovered := make(chan error, 1), make(chan error, 1)
	go func() { <-start; authenticated <- s.RecordResultReceipt(g.AttemptID, receipt) }()
	go func() { <-start; recovered <- s.RecordRecoveryFailure(g.AttemptID, failure) }()
	close(start)
	authErr, recoveryErr := <-authenticated, <-recovered
	if (authErr == nil) == (recoveryErr == nil) {
		t.Fatal("provider outcome and inferred failure did not choose one owner", authErr, recoveryErr)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if authErr == nil {
		if err := s.RecordRecoveryFailure(g.AttemptID, failure); !errors.Is(err, ErrConflict) {
			t.Fatal("restart let recovery replace the authenticated provider outcome", err)
		}
		if err := s.BeginForward(g.AttemptID); err != nil {
			t.Fatal("authenticated provider result lost delivery authority", err)
		}
	} else {
		if err := s.RecordResultReceipt(g.AttemptID, receipt); !errors.Is(err, ErrConflict) {
			t.Fatal("restart let a late result replace the selected recovery outcome", err)
		}
		if err := s.BeginRecoveryForward(g.AttemptID); err != nil {
			t.Fatal("selected recovery failure lost delivery authority", err)
		}
		if err := s.BeginForward(g.AttemptID); !errors.Is(err, ErrConflict) {
			t.Fatal("recovery also authorized unsigned native delivery", err)
		}
	}
}

func TestSchemaElevenMigrationDoesNotInventEarlyResultAuthority(t *testing.T) {
	for _, inject := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-unsealed", true: "injected-result-proof"}[inject], func(t *testing.T) {
			s, options := testStore(t)
			g, key, _ := readyGrant(t, s, testGrant())
			startGrant(t, s, g)
			body := []byte(`{"output":"legacy outcome"}`)
			terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest(body))
			if err != nil {
				t.Fatal(err)
			}
			receipt := outcomeReceipt(g, terminal, key)
			if inject {
				if err := s.RecordResultReceipt(g.AttemptID, receipt); err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := s.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			snapshot.SchemaVersion = 11
			var encoded bytes.Buffer
			encoder := json.NewEncoder(&encoded)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(snapshot); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(options.Directory, "journal.json"), encoded.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			s, err = Open(options)
			if inject {
				if err == nil {
					s.Close()
					t.Fatal("legacy schema admitted unsupported early delivery authority")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.BeginForward(g.AttemptID); !errors.Is(err, ErrConflict) {
				t.Fatal("migration authorized an unproven legacy result", err)
			}
			if err := s.RecordResultReceipt(g.AttemptID, receipt); err != nil {
				t.Fatal("migration lost the supervisor's original signing authority", err)
			}
			if err := s.BeginForward(g.AttemptID); err != nil {
				t.Fatal("newly authenticated legacy result could not be delivered", err)
			}
		})
	}
}

func TestSchemaTwelveMigrationPreservesAuthenticatedResultAuthority(t *testing.T) {
	s, options := testStore(t)
	g, key, _ := readyGrant(t, s, testGrant())
	startGrant(t, s, g)
	body := []byte(`{"output":"authenticated provider outcome"}`)
	terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(g.AttemptID, "worker_unsealed"); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{Kind: "terminated", PodUID: g.PodUID, PVCUID: g.PVCUID, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordResultReceipt(g.AttemptID, outcomeReceipt(g, terminal, key)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot.SchemaVersion = 12
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(options.Directory, "journal.json"), encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RecordRecoveryFailure(g.AttemptID, []byte(`{"error":"shutdown evidence missing"}`)); !errors.Is(err, ErrConflict) {
		t.Fatal("migration let recovery replace an authenticated provider outcome", err)
	}
	if err := s.BeginForward(g.AttemptID); err != nil {
		t.Fatal("migration lost the signed provider result's delivery authority", err)
	}
}
