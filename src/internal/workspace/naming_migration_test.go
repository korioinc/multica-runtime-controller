package workspace

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

func TestTaskNamingMigrationPreservesExecutionAndStorageAuthority(t *testing.T) {
	for _, version := range []int{7, 8, 9, 10} {
		for _, scenario := range []string{"active", "stopping", "quarantined", "dirty", "uncertain", "clean"} {
			t.Run(fmt.Sprintf("%d/%s", version, scenario), func(t *testing.T) {
				s, options := testStore(t)
				input := testGrant()
				if version >= 8 {
					input.Envelope = json.RawMessage(`{"issue_identifier":"KOR-219"}`)
				}
				if version == 9 {
					input.Envelope = json.RawMessage(`{"workspace_slug":"kor-io","issue_identifier":"KOR-219"}`)
				}
				g, key, daemonToken := readyGrant(t, s, input)
				stopToken, err := s.IssueCapability(g.AttemptID, "stop", time.Now().Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				startGrant(t, s, g)
				var receipt StopReceipt
				body := []byte(`{"result":"retained provider work"}`)
				switch scenario {
				case "stopping", "dirty", "clean":
					g, err = s.RequestStop(g.AttemptID, "cancelled")
					if err != nil {
						t.Fatal(err)
					}
					if err := s.PinStopSupervisor(g.AttemptID, g.PodUID, g.PVCUID, g.NodeID, key.Public().(ed25519.PublicKey)); err != nil {
						t.Fatal(err)
					}
					receipt = stopReceipt(g, key, scenario != "dirty")
					if err := s.CloseCheckouts(g.AttemptID); err != nil {
						t.Fatal(err)
					}
					if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err != nil {
						t.Fatal(err)
					}
					if scenario != "stopping" {
						if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "terminated", ObservedAt: time.Now().UTC()}); err != nil {
							t.Fatal(err)
						}
						if err := s.CloseStopped(g.AttemptID); err != nil {
							t.Fatal(err)
						}
						if err := s.MarkCleaned(g.AttemptID); err != nil {
							t.Fatal(err)
						}
					}
				case "quarantined":
					if err := s.Quarantine(g.AttemptID); err != nil {
						t.Fatal(err)
					}
				case "uncertain":
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
				}
				st, err := s.snapshot()
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				writePreviousNamingJournal(t, options, st, version)
				s, err = Open(options)
				if err != nil {
					t.Fatal("previous task authority could not reopen", err)
				}
				defer s.Close()
				retained, err := s.Get(g.AttemptID)
				if err != nil || retained.TaskRoot != g.TaskRoot {
					t.Fatal("migration disconnected the attempt from its task data", err)
				}
				if _, err := s.Authorize(daemonToken, "daemon"); scenario == "active" && err != nil || scenario != "active" && err == nil {
					t.Fatal("migration changed task API execution authority", err)
				}
				if receipt.AttemptID != "" {
					if _, err := s.AuthorizeStop(stopToken); err != nil {
						t.Fatal("migration lost the original cleanup supervisor authority", err)
					}
					if err := s.ReceiveStopReceipt(g.AttemptID, receipt); err != nil {
						t.Fatal("migration lost the signed stop challenge or committed receipt", err)
					}
				}
				if scenario == "stopping" || scenario == "quarantined" {
					if err := s.CloseStopped(g.AttemptID); err == nil {
						t.Fatal("migration fabricated positive writer termination evidence")
					}
				}
				if scenario == "uncertain" {
					if err := s.BeginForward(g.AttemptID); err == nil {
						t.Fatal("migration authorized an ambiguous delivery to replay")
					}
					terminal, err := s.Terminal(g.AttemptID)
					if err != nil || !bytes.Equal(terminal.Body, body) {
						t.Fatal("migration lost acknowledged provider work", err)
					}
				}
				input.Envelope = json.RawMessage(`{"workspace_slug":"renamed-workspace","issue_identifier":"KOR-220"}`)
				next, err := s.Create(input)
				if scenario == "clean" {
					if err != nil || !next.Reuse || next.TaskRoot != g.TaskRoot || next.StorageID != g.StorageID {
						t.Fatal("renaming rules replaced the existing task workspace on retry", err)
					}
				} else if !errors.Is(err, ErrStorageBusy) {
					t.Fatal("migration released busy, dirty, or unresolved task storage", err)
				}
			})
		}
	}
}

func TestTaskNamingMigrationRejectsChangedHistoricalRootsWithoutWriting(t *testing.T) {
	for _, version := range []int{6, 7, 8} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			s, options := testStore(t)
			input := testGrant()
			if version == 8 {
				input.Envelope = json.RawMessage(`{"issue_identifier":"KOR-219"}`)
			}
			g, err := s.Create(input)
			if err != nil {
				t.Fatal(err)
			}
			st, err := s.snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			workspaceSlug := ""
			if version == 8 {
				workspaceSlug = "changed-workspace"
			}
			root, err := TaskRoot("/workspace", g.WorkspaceID, g.TaskID, workspaceSlug, "KOR-219")
			if err != nil {
				t.Fatal(err)
			}
			g.TaskRoot = root
			st.Grants[g.AttemptID] = g
			storage := st.Storages[g.StorageID]
			storage.TaskRoot = root
			st.Storages[g.StorageID] = storage
			raw := writePreviousNamingJournal(t, options, st, version)
			if opened, err := Open(options); err == nil {
				opened.Close()
				t.Fatal("migration adopted a root that the previous journal could not authorize")
			}
			retained, err := os.ReadFile(filepath.Join(options.Directory, "journal.json"))
			if err != nil || !bytes.Equal(retained, raw) {
				t.Fatal("failed migration rewrote historical authority", err)
			}
		})
	}
}

func writePreviousNamingJournal(t *testing.T, options Options, st registry, version int) []byte {
	t.Helper()
	st.SchemaVersion = version
	for id, g := range st.Grants {
		g.StartConfirmed = false
		g.CheckoutNeedsFlush = false
		g.CheckoutClosed = false
		g.CheckoutProcess = nil
		st.Grants[id] = g
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(st); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(options.Directory, "journal.json"), encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}
