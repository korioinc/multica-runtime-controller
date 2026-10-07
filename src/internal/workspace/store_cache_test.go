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

func TestStoreExternalJournalChangeRevokesCachedAuthority(t *testing.T) {
	for _, change := range []string{"replace", "rewrite", "permissions", "missing"} {
		t.Run(change, func(t *testing.T) {
			s, options := testStore(t)
			g, _, token := readyGrant(t, s, testGrant())
			path := filepath.Join(options.Directory, "journal.json")
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			saved := filepath.Join(t.TempDir(), "saved-journal")
			if err := os.Rename(path, saved); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "replace":
				err = os.WriteFile(path, original, 0600)
			case "rewrite", "permissions":
				err = os.Rename(saved, path)
				if err == nil && change == "rewrite" {
					err = os.WriteFile(path, append(original, '\n'), 0600)
				} else if err == nil {
					err = os.Chmod(path, 0640)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Authorize(token, "daemon"); err == nil {
				t.Fatal("external journal change retained task authority")
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			if change == "replace" || change == "missing" {
				err = os.Rename(saved, path)
			} else {
				err = os.WriteFile(path, original, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Authorize(token, "daemon"); err == nil {
				t.Fatal("restoring a journal silently revived an invalidated owner")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if authorized, err := s.Authorize(token, "daemon"); err != nil || authorized.TaskID != g.TaskID {
				t.Fatal("validated reopen lost the original task authority", err)
			}
		})
	}
}

func TestStoreFailedTransactionPreservesCommittedAuthority(t *testing.T) {
	for _, failure := range []string{"callback", "validation", "publication"} {
		t.Run(failure, func(t *testing.T) {
			s, options := testStore(t)
			g, _, token := readyGrant(t, s, testGrant())
			path := filepath.Join(options.Directory, "journal.json")
			saved := filepath.Join(t.TempDir(), "saved-journal")
			err := s.transaction(func(st *registry) error {
				candidate := st.Grants[g.AttemptID]
				candidate.Prepared.AllowedLinks["forged"] = "/forged"
				candidate.SupervisorKey[0] ^= 1
				st.Grants[g.AttemptID] = candidate
				if failure == "callback" {
					return errors.New("abort transaction")
				}
				if failure == "validation" {
					return nil
				}
				delete(candidate.Prepared.AllowedLinks, "forged")
				candidate.SupervisorKey[0] ^= 1
				candidate.CheckoutClosed = true
				st.Grants[g.AttemptID] = candidate
				if err := os.Rename(path, saved); err != nil {
					t.Fatal(err)
				}
				return os.Mkdir(path, 0700) // Atomic rename cannot replace a directory.
			})
			if err == nil {
				t.Fatal("failed transaction reported a commit")
			}
			if failure == "publication" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(saved, path); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Authorize(token, "daemon"); err == nil {
					t.Fatal("failed publication still authorized work from memory")
				}
			} else {
				if authorized, err := s.Authorize(token, "daemon"); err != nil || !bytes.Equal(authorized.SupervisorKey, g.SupervisorKey) {
					t.Fatal("rejected transaction changed existing signing authority", err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			retained, err := s.Authorize(token, "daemon")
			if err != nil || retained.CheckoutClosed || !bytes.Equal(retained.SupervisorKey, g.SupervisorKey) {
				t.Fatal("failed mutation changed durable execution authority", err)
			}
		})
	}
}

func TestStoreMutationInputsAndResultsCannotRewriteAuthority(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	g, err := s.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	want := input.RuntimeRef.Providers["codex"]
	input.RuntimeRef.Providers["codex"] = input.RuntimeRef.Providers["missing"]
	g.RuntimeRef.Providers["codex"] = input.RuntimeRef.Providers["missing"]
	resources := json.RawMessage(`{"podCreateRequested":false,"secretCreateRequested":false}`)
	if err := s.SetResources(g.AttemptID, resources); err != nil {
		t.Fatal("caller aliases changed the committed runtime authority", err)
	}
	resources[0] = '!'
	stopped, err := s.RequestStop(g.AttemptID, "cancelled")
	if err != nil {
		t.Fatal(err)
	}
	stopped.Stop.Reason = "forged reason"
	stopped.Stop.Nonce = "forged nonce"
	for _, reopen := range []bool{false, true} {
		if reopen {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
		}
		retained, err := s.Get(g.AttemptID)
		if err != nil || retained.RuntimeRef.Providers["codex"] != want || retained.Stop.Reason != "cancelled" || !canonicalUUID(retained.Stop.Nonce) {
			t.Fatal("caller mutation rewrote committed runtime or stop authority", err)
		}
	}
}

func TestStoreReturnedSnapshotsCannotAlterSignedOutcome(t *testing.T) {
	s, options := testStore(t)
	g, key, _ := readyGrant(t, s, testGrant())
	token, err := s.IssueCapability(g.AttemptID, "supervisor", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PinStopSupervisor(g.AttemptID, g.PodUID, g.PVCUID, g.NodeID, g.SupervisorKey); err != nil {
		t.Fatal(err)
	}
	startGrant(t, s, g)
	eventBody := json.RawMessage(`{"content":"provider observation"}`)
	event, err := s.ReceiveEvent(g.AttemptID, 1, eventBody)
	if err != nil {
		t.Fatal(err)
	}
	event.Body[0] = '!'
	if _, err := s.ReceiveEvent(g.AttemptID, 1, eventBody); err != nil {
		t.Fatal("mutating returned event changed the owned observation", err)
	}
	body := []byte(`{"output":"completed provider work"}`)
	terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest(body))
	if err != nil {
		t.Fatal(err)
	}
	resultReceipt := outcomeReceipt(g, terminal, key)
	if err := s.RecordResultReceipt(g.AttemptID, resultReceipt); err != nil {
		t.Fatal(err)
	}
	terminal.Body[0] = '!'
	resultReceipt.Signature[0] ^= 1
	stopped, err := s.RequestStop(g.AttemptID, "execution_finished")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	stop := stopReceipt(stopped, key, true)
	if err := s.ReceiveStopReceipt(g.AttemptID, stop); err != nil {
		t.Fatal(err)
	}
	stop.Signature[0] ^= 1
	if err := s.ObserveStop(g.AttemptID, StopEvidence{Kind: "terminated", PodUID: g.PodUID, PVCUID: g.PVCUID, ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	seal := sealReceipt(g, terminal, key)
	if err := s.Seal(g.AttemptID, seal); err != nil {
		t.Fatal(err)
	}
	seal.Signature[0] ^= 1
	returned, err := s.Authorize(token, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	returned.SupervisorKey[0] ^= 1
	returned.StopKey[0] ^= 1
	returned.Stop.Receipt.Signature[0] ^= 1
	returned.Stop.Evidence.Kind = "forged"
	returned.PreparationProcess.ContainerID = "forged"
	returned.Prepared.CleanupManifest[0] = '!'
	returned.Prepared.AllowedLinks["forged"] = "/forged"
	returned.Events[0].Body[0] = '!'
	returned.Envelope[0] = '!'
	returned.Metadata[0] = '!'
	returned.Repositories[0].URL = "forged"
	returned.RuntimeRef.Providers["codex"] = returned.RuntimeRef.Providers["missing"]
	prepared, err := s.PreviousPrepared(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	prepared.CleanupManifest[0] = '!'
	prepared.AllowedLinks["forged"] = "/forged"
	observed, err := s.Terminal(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	observed.Body[0] = '!'
	observed.Seal.Signature[0] ^= 1
	observed.ResultReceipt.Signature[0] ^= 1
	if err := s.BeginForward(g.AttemptID); err != nil {
		t.Fatal("returned snapshots damaged authenticated delivery authority", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal("caller mutation persisted damaged signing or storage authority", err)
	}
	defer s.Close()
	retained, err := s.Terminal(g.AttemptID)
	if err != nil || !bytes.Equal(retained.Body, body) {
		t.Fatal("caller mutation replaced the durable provider outcome", err)
	}
}
