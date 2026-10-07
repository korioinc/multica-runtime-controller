package workspace

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func TestMigrationPreservesOnlyProvenResultRetryAuthority(t *testing.T) {
	for _, stage := range []string{"started", "terminal", "closed"} {
		t.Run(stage, func(t *testing.T) {
			s, options := testStore(t)
			g, key, _ := readyGrant(t, s, testGrant())
			startGrant(t, s, g)
			pendingResult := func() {
				t.Helper()
				body := []byte(`{"output":"completed work"}`)
				terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest(body))
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
				if err := s.FinishForward(g.AttemptID, "uncertain"); err != nil {
					t.Fatal(err)
				}
			}
			if stage != "started" {
				pendingResult()
			}
			if stage == "closed" {
				if _, err := s.RequestStop(g.AttemptID, "execution_finished"); err != nil {
					t.Fatal(err)
				}
				if err := s.ObserveStop(g.AttemptID, StopEvidence{Kind: "terminated", PodUID: g.PodUID, PVCUID: g.PVCUID, ObservedAt: time.Now()}); err != nil {
					t.Fatal(err)
				}
				if err := s.CloseStopped(g.AttemptID); err != nil {
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
			writePreviousNamingJournal(t, options, snapshot, 10)
			s, err = Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if stage == "started" {
				pendingResult()
			}
			// Reopen the migrated record before exercising its retry authority.
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			err = s.ResumeForward(g.AttemptID)
			if stage == "started" {
				if err != nil {
					t.Fatal("migration lost the acknowledged start needed to recover delivery", err)
				}
				if err := s.BeginForward(g.AttemptID); err != nil {
					t.Fatal("proven execution could not resume result delivery", err)
				}
			} else if !errors.Is(err, ErrConflict) {
				t.Fatal("migration invented start authority for an ambiguous historical result", err)
			}
		})
	}
}

func TestOldJournalCannotInjectResultRecoveryAuthority(t *testing.T) {
	for _, injection := range []string{"start", "recovery"} {
		t.Run(injection, func(t *testing.T) {
			s, options := testStore(t)
			g, _, _ := readyGrant(t, s, testGrant())
			startGrant(t, s, g)
			if injection == "recovery" {
				body := []byte(`{"output":"unsealed work"}`)
				if _, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest(body)); err != nil {
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
				if err := s.RecordRecoveryFailure(g.AttemptID, []byte(`{"error":"shutdown proof missing"}`)); err != nil {
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
			snapshot.SchemaVersion = 10
			if injection == "recovery" {
				grant := snapshot.Grants[g.AttemptID]
				grant.StartConfirmed = false
				snapshot.Grants[g.AttemptID] = grant
			}
			var encoded bytes.Buffer
			encoder := json.NewEncoder(&encoded)
			encoder.SetEscapeHTML(false)
			if err := encoder.Encode(snapshot); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(options.Directory, "journal.json")
			if err := os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(options); err == nil {
				opened.Close()
				t.Fatal("old journal admitted unsupported delivery authority")
			}
			retained, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(retained, encoded.Bytes()) {
				t.Fatal("rejected authority migration altered recovery evidence", err)
			}
		})
	}
}

func TestSealAndRecoveryCompeteForOneDurableDelivery(t *testing.T) {
	s, options := testStore(t)
	g, key, _ := readyGrant(t, s, testGrant())
	startGrant(t, s, g)
	body := []byte(`{"output":"work awaiting shutdown proof"}`)
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
	receipt := sealReceipt(g, terminal, key)
	failure := []byte(`{"error":"shutdown proof missing"}`)
	start := make(chan struct{})
	sealed, recovered := make(chan error, 1), make(chan error, 1)
	go func() { <-start; sealed <- s.Seal(g.AttemptID, receipt) }()
	go func() { <-start; recovered <- s.RecordRecoveryFailure(g.AttemptID, failure) }()
	close(start)
	sealErr, recoveryErr := <-sealed, <-recovered
	if (sealErr == nil) == (recoveryErr == nil) {
		t.Fatal("native success and recovery failure did not choose one owner", sealErr, recoveryErr)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if sealErr == nil {
		if err := s.RecordRecoveryFailure(g.AttemptID, failure); !errors.Is(err, ErrConflict) {
			t.Fatal("recovery displaced a committed native success after restart", err)
		}
		if err := s.BeginForward(g.AttemptID); err != nil {
			t.Fatal("committed native success lost delivery authority", err)
		}
	} else {
		if err := s.Seal(g.AttemptID, receipt); !errors.Is(err, ErrConflict) {
			t.Fatal("late seal displaced a committed recovery failure after restart", err)
		}
		if err := s.BeginRecoveryForward(g.AttemptID); err != nil {
			t.Fatal("committed recovery failure lost delivery authority", err)
		}
		if err := s.BeginForward(g.AttemptID); !errors.Is(err, ErrConflict) {
			t.Fatal("recovery also authorized unsealed native success", err)
		}
	}
}
