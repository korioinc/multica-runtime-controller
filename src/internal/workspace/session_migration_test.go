package workspace

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/google/uuid"
)

func writeHistoricalSession(t *testing.T, f *sessionFixture, schema int) (TaskGrant, WorkerSession) {
	t.Helper()
	st, err := f.store.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	st.SchemaVersion = schema
	for id, g := range st.Grants {
		if g.WorkerSessionID == "" {
			continue
		}
		if g.Prepared != nil {
			g.Prepared.NativeMetadata = nil
			g.Prepared.AllowedLinks = map[string]string{}
			g.Prepared.Digest = preparedDigest(*g.Prepared)
		}
		if len(g.Execution) != 0 {
			var run map[string]json.RawMessage
			if err := json.Unmarshal(g.Execution, &run); err != nil {
				t.Fatal(err)
			}
			delete(run, "nativeMetadata")
			g.Execution, _ = json.Marshal(run)
		}
		if len(g.Assignment) != 0 {
			var bootstrap, assignment map[string]json.RawMessage
			_ = json.Unmarshal(g.Bootstrap, &bootstrap)
			delete(bootstrap, "nativeMetadataDigest")
			g.Bootstrap, _ = json.Marshal(bootstrap)
			g.BootstrapDigest = digest(g.Bootstrap)
			_ = json.Unmarshal(g.Assignment, &assignment)
			assignment["bootstrap"], assignment["run"] = g.Bootstrap, g.Execution
			raw, _ := json.Marshal(assignment)
			g.InputDigest, err = TurnInputDigest(raw)
			if err != nil {
				t.Fatal(err)
			}
			assignment["inputDigest"], _ = json.Marshal(g.InputDigest)
			g.Assignment, _ = json.Marshal(assignment)
			if g.AcceptProof != nil {
				body, _ := json.Marshal(struct {
					TurnSequence uint64 `json:"turnSequence"`
					InputDigest  string `json:"inputDigest"`
				}{g.TurnSequence, g.InputDigest})
				g.AcceptProof.BodyDigest = digest(body)
				g.AcceptProof.Signature = ed25519.Sign(f.key, SessionProofMessage(*g.AcceptProof))
			}
		}
		if g.TurnExecutionReceipt != nil {
			terminal := st.Terminals[id]
			r := TurnReceipt{WorkerSessionID: g.WorkerSessionID, TaskID: g.TaskID, AttemptID: id,
				Generation: g.Generation, TurnSequence: g.TurnSequence, InputDigest: g.InputDigest,
				PodUID: g.PodUID, PVCUID: g.PVCUID, ResultDigest: terminal.ResultDigest,
				RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce, WritersStopped: true, FlushOK: true}
			r.Signature = ed25519.Sign(f.key, TurnReceiptMessage(r))
			g.TurnReceipt = &r
		}
		g.TurnExecutionReceipt, g.TurnExecutionProof = nil, nil
		st.Grants[id] = g
	}
	for id, session := range st.Sessions {
		session.ProtocolVersion = 0
		if len(session.Bootstrap) != 0 {
			var bootstrap map[string]json.RawMessage
			_ = json.Unmarshal(session.Bootstrap, &bootstrap)
			bootstrap["version"], _ = json.Marshal(LegacySessionProtocolVersion)
			session.Bootstrap, _ = json.Marshal(bootstrap)
			session.BootstrapDigest = digest(session.Bootstrap)
		}
		if session.LastAttemptID != "" {
			session.InputDigest = st.Grants[session.LastAttemptID].InputDigest
		}
		if session.Stop != nil && session.Stop.Receipt != nil {
			r := session.Stop.Receipt
			r.InputDigest = session.InputDigest
			r.Signature = ed25519.Sign(f.key, SessionStopReceiptMessage(*r))
			body, _ := json.Marshal(r)
			session.Stop.ReceiptProof.BodyDigest = digest(body)
			session.Stop.ReceiptProof.Signature = ed25519.Sign(f.key, SessionProofMessage(*session.Stop.ReceiptProof))
		}
		st.Sessions[id] = session
	}
	for id, storage := range st.Storages {
		if storage.WorkspaceAnchorTaskID == "" {
			continue
		}
		g := st.Grants[storage.LatestWriter.AttemptID]
		storage.Prepared = clonePrepared(g.Prepared)
		if g.TurnReceipt != nil && g.CompletionWitness != nil {
			storage.Dirty = false
			storage.Checkpoint = completedCheckpoint(&st, g, storage, f.store.now(), false)
			storage.SessionID = storage.Checkpoint.SessionID
		}
		st.Storages[id] = storage
	}
	if schema == 14 {
		for id, session := range st.Sessions {
			prefix := string(session.Conversation.Kind)
			if session.Conversation.Kind == ConversationAgentDM {
				prefix = "dm"
			}
			session.PodName = prefix + "-" + session.Conversation.Digest()[:16] + "-" + id[:12]
			session.WorkspaceCompatibilityDigest = ""
			st.Sessions[id] = session
		}
		for id, g := range st.Grants {
			g.WorkspaceCompatibilityDigest = ""
			if g.Selection != nil {
				g.Selection.WorkspaceReuseEligible = false
			}
			if g.BackendSelection != nil {
				g.BackendSelection.WorkspaceReuseEligible = false
			}
			if g.WorkerSessionID != "" {
				g.PodName = st.Sessions[g.WorkerSessionID].PodName
			}
			st.Grants[id] = g
		}
		for id, storage := range st.Storages {
			storage.WorkspaceCompatibilityDigest = ""
			if storage.Checkpoint != nil {
				storage.Checkpoint.WorkspaceCompatibilityDigest = ""
			}
			st.Storages[id] = storage
		}
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(st); err != nil {
		t.Fatal(err)
	}
	// Remove new fields without re-encoding signed assignments or task envelopes.
	raw := bytes.ReplaceAll(encoded.Bytes(), []byte(`"protocolVersion":0,`), nil)
	if schema == 14 {
		raw = bytes.ReplaceAll(raw, []byte(`,"workspaceCompatibilityDigest":""`), nil)
		raw = bytes.ReplaceAll(raw, []byte(`,"workspaceReuseEligible":false`), nil)
	}
	if err := os.WriteFile(filepath.Join(f.options.Directory, "journal.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return st.Grants[f.grant.AttemptID], st.Sessions[f.session.ID]
}

func TestHistoricalSessionMigrationDrainsBeforeServing(t *testing.T) {
	for _, schema := range []int{14, 15} {
		for _, state := range []string{"creating", "running", "idle", "closed", "uncertain", "quarantined"} {
			t.Run(strconv.Itoa(schema)+"/"+state, func(t *testing.T) {
				f := newSessionFixture(t)
				switch state {
				case "creating":
					f.input.TaskID, f.selection.Conversation.SubjectID = uuid.NewString(), uuid.NewString()
					f.reserve(t, f.selection, 4)
				case "idle", "closed":
					f.settle(t, f.receive(t, ""), "")
					if state == "closed" {
						f.stopAndClose(t, true, true)
					}
				case "uncertain":
					f.receive(t, "")
					if err := f.store.BeginForward(f.grant.AttemptID); err != nil {
						t.Fatal(err)
					}
					if err := f.store.FinishForward(f.grant.AttemptID, "uncertain"); err != nil {
						t.Fatal(err)
					}
				case "quarantined":
					if err := f.store.QuarantineSession(f.session.ID); err != nil {
						t.Fatal(err)
					}
				}
				previous, resident := writeHistoricalSession(t, f, schema)
				reopened, err := Open(f.options)
				if err != nil {
					t.Fatal(err)
				}
				f.store = reopened
				t.Cleanup(func() { _ = f.store.Close() })
				actual, _ := reopened.Get(previous.AttemptID)
				session, _ := reopened.GetSession(resident.ID)
				storage, _ := reopened.GetStorage(actual.StorageID)
				terminal, _ := reopened.Terminal(actual.AttemptID)
				if session.PodName != resident.PodName || session.PodUID != resident.PodUID || session.ActiveAttempt != resident.ActiveAttempt ||
					!bytes.Equal(session.Bootstrap, resident.Bootstrap) || !bytes.Equal(session.Resources, resident.Resources) ||
					!bytes.Equal(actual.Assignment, previous.Assignment) || actual.TurnReceipt != nil && !validTurnReceipt(actual, terminal, *actual.TurnReceipt) {
					t.Fatal("migration changed original Pod, payload, or signed proof")
				}
				if actual.TurnReceipt != nil {
					if err := reopened.RecordTurnReceipt(actual.AttemptID, *actual.TurnReceipt); err != nil {
						t.Fatal("original v1 receipt could not retry after migration", err)
					}
				}
				if state == "closed" {
					if session.State != SessionClosed || storage.WriterSessionID != "" || storage.Dirty || storage.Checkpoint == nil {
						t.Fatal("migration discarded proven closed storage")
					}
				} else {
					if session.Stop == nil || storage.WriterSessionID != session.ID || session.State != SessionDraining && session.State != SessionQuarantined {
						t.Fatal("historical live session served before durable drain")
					}
					if _, err := reopened.Authorize(f.token, "daemon"); !errors.Is(err, ErrUnauthorized) {
						t.Fatal("migration retained old task authority", err)
					}
					if _, err := reopened.WarmSession(storage.ID); err == nil {
						t.Fatal("v1 proof authorized v2 live reuse")
					}
				}
				if state == "running" {
					f.grant, f.session = actual, session
					if _, err := reopened.ReceiveEvent(actual.AttemptID, 1, []byte(`{"progress":"old running SDK"}`)); err != nil {
						t.Fatal("in-flight v1 event was lost", err)
					}
					f.receive(t, "")
					recorded, _ := reopened.Terminal(actual.AttemptID)
					if recorded.ResultReceipt == nil || !validResultReceipt(actual, recorded, *recorded.ResultReceipt) {
						t.Fatal("in-flight v1 result was lost")
					}
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
				f.store, err = Open(f.options)
				if err != nil {
					t.Fatal("migrated journal did not reopen", err)
				}
				durable, _ := f.store.GetSession(session.ID)
				if state != "closed" && (durable.Stop == nil || durable.Stop.Nonce != session.Stop.Nonce || durable.Stop.Revision != session.Stop.Revision) {
					t.Fatal("reopen replaced the required historical drain")
				}
			})
		}
	}
}
