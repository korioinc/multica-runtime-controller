package controller

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMissingSessionRootCannotCertifyCleanReuse(t *testing.T) {
	for _, phase := range []string{"running", "idle", "checkout_flush_pending", "unsigned_checkout_flush_pending"} {
		t.Run(phase, func(t *testing.T) {
			f := newSessionControllerFixture(t)
			authenticated := phase != "unsigned_checkout_flush_pending"
			if phase == "checkout_flush_pending" || !authenticated {
				process := workspace.PreparationProcess{PodName: f.C.Owner.Name, PodUID: f.C.Owner.UID, ContainerID: "containerd://finished-checkout"}
				if err := f.C.Store.BeginCheckout(f.Grant.AttemptID, process); err != nil {
					t.Fatal(err)
				}
				if err := f.C.Store.CompleteCheckout(f.Grant.AttemptID, process); err != nil {
					t.Fatal(err)
				}
			}
			terminal := receiveFixtureTurnResult(t, f, authenticated)
			if phase == "idle" {
				quiesceFixtureTurn(t, f, terminal)
				if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.C.Store.RequestSessionStop(f.Session.ID, "worker_shutdown"); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			command, err := f.C.sessionStopCommand(t.Context(), f.Session)
			if err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			receipt := workspace.SessionStopReceipt{WorkerSessionID: f.Session.ID, AttemptID: command.AttemptID, InputDigest: command.InputDigest,
				TurnSequence: command.TurnSequence, PodUID: f.Session.PodUID, PVCUID: f.Session.PVCUID, Revision: command.Revision,
				Nonce: command.Nonce, WritersStopped: true, FlushOK: true}
			if err := recordSessionFinalReceipt(t, f, receipt); err == nil {
				t.Fatal("missing task files authorized a clean final receipt")
			}
			finishTaskPod(t, f.C, f.Grant)
			if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
				t.Fatal(err)
			}
			grace, err := f.C.sessionTerminationGrace(f.Session)
			if err != nil {
				t.Fatal(err)
			}
			// Wait through the issued receipt grace; do not fabricate an earlier
			// termination history or assert an elapsed-time performance threshold.
			time.Sleep(grace)
			if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
				t.Fatal(err)
			}
			f.refresh(t)
			storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
			if err != nil || !storage.Dirty || storage.Checkpoint != nil || storage.WriterSessionID != "" || !f.Session.ResourcesCleaned {
				t.Fatal("terminated missing-root session did not release compute while fencing its files", err)
			}
			if err := f.C.reconcileDelivery(t.Context(), f.Grant.AttemptID); err != nil {
				t.Fatal(err)
			}
			retained, err := f.C.Store.Terminal(f.Grant.AttemptID)
			if err != nil || !bytes.Equal(retained.Body, terminal.Body) {
				t.Fatal("dirty closure discarded the observed SDK result", err)
			}
			if authenticated && (retained.ResultReceipt == nil || retained.RecoveryFailure != nil) {
				t.Fatal("dirty closure replaced authenticated SDK authority")
			}
			if !authenticated && retained.RecoveryFailure == nil {
				t.Fatal("missing result authentication acquired an inferred SDK success")
			}
			queued, selection, compatibility := queueAfterFinalReceipt(t, f)
			if _, _, err := f.C.Store.ReserveTurn(queued.AttemptID, selection, compatibility, f.C.ConversationIdleTimeout, 1); err != nil {
				t.Fatal("terminated missing-root session retained unrelated resident capacity", err)
			}
		})
	}
}

func TestMissingSessionRootCannotFenceAnUnobservedPod(t *testing.T) {
	f := newSessionControllerFixture(t)
	receiveFixtureTurnResult(t, f, true)
	if _, err := f.C.Store.RequestSessionStop(f.Session.ID, "worker_shutdown"); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).Delete(t.Context(), f.Session.PodName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
	if err != nil || storage.WriterSessionID != f.Session.ID || f.Session.ResourcesCleaned {
		t.Fatal("missing task files and Pod released an unproven writer", err)
	}
	queued, selection, compatibility := queueAfterFinalReceipt(t, f)
	if _, _, err := f.C.Store.ReserveTurn(queued.AttemptID, selection, compatibility, f.C.ConversationIdleTimeout, 1); !errors.Is(err, workspace.ErrResidentCapacity) {
		t.Fatal("unknown termination released resident capacity", err)
	}
}

func TestSessionFinalStopReceiptReleasesTerminatedResident(t *testing.T) {
	for _, status := range []string{"failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newSessionControllerFixture(t)
				terminal, receipt := prepareSessionFinalResult(t, f, status, true)
				if err := recordSessionFinalReceipt(t, f, receipt); err != nil {
					t.Fatal(err)
				}
				queued, selection, compatibility := queueAfterFinalReceipt(t, f)
				if _, _, err := f.C.Store.ReserveTurn(queued.AttemptID, selection, compatibility, f.C.ConversationIdleTimeout, 1); !errors.Is(err, workspace.ErrResidentCapacity) {
					t.Fatal("signed receipts released a Pod that was still running", err)
				}
				if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
					t.Fatal(err)
				}
				f.refresh(t)
				if f.Session.State == workspace.SessionClosed || f.Session.ResourcesCleaned {
					t.Fatal("signed receipts replaced actual Pod termination evidence")
				}
				reopenStopRegressionStore(t, f.C, f.Options)
				finishTaskPod(t, f.C, f.Grant)
				// The fake clock does not advance. Closure must follow the signed
				// proofs and observed termination, never expiration of receipt grace.
				if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
					t.Fatal(err)
				}
				f.refresh(t)
				if f.Session.State != workspace.SessionClosed || !f.Session.ResourcesCleaned {
					t.Fatal("complete final-stop proof left the terminated resident held")
				}
				if _, err := f.C.Kube.API.CoreV1().Pods(f.C.Kube.Namespace).Get(t.Context(), f.Session.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
					t.Fatal("proven final stop did not clean the terminated Pod", err)
				}
				if _, _, err := f.C.Store.ReserveTurn(queued.AttemptID, selection, compatibility, f.C.ConversationIdleTimeout, 1); err != nil {
					t.Fatal("cleaned resident capacity did not admit the waiting conversation", err)
				}
				retained, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil || retained.Kind != terminal.Kind || retained.ResultReceipt == nil || retained.RecoveryFailure != nil ||
					!bytes.Equal(retained.Body, terminal.Body) || retained.ResultDigest != terminal.ResultDigest {
					t.Fatal("final stop replaced the authenticated provider outcome", err)
				}
			})
		})
	}
}

func TestSessionFinalStopRecoveryKeepsIncompleteProofFenced(t *testing.T) {
	for _, missing := range []string{"receipt", "flush", "writers", "result_authentication", "attempt_binding", "turn_binding", "pod_binding"} {
		t.Run(missing, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newSessionControllerFixture(t)
				terminal, receipt := prepareSessionFinalResult(t, f, "failed", missing != "result_authentication")
				if missing != "receipt" {
					switch missing {
					case "flush":
						receipt.FlushOK = false
					case "writers":
						receipt.WritersStopped = false
					case "attempt_binding":
						receipt.AttemptID = uuid.NewString()
					case "turn_binding":
						receipt.TurnSequence++
					case "pod_binding":
						receipt.PodUID = uuid.NewString()
					}
					err := recordSessionFinalReceipt(t, f, receipt)
					if missing == "attempt_binding" || missing == "turn_binding" || missing == "pod_binding" {
						if err == nil {
							t.Fatal("another execution's final proof was accepted")
						}
					} else if err != nil {
						t.Fatal(err)
					}
				}
				finishTaskPod(t, f.C, f.Grant)
				if err := f.C.reconcileSession(t.Context(), f.Session.ID); err != nil {
					t.Fatal(err)
				}
				f.refresh(t)
				storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
				if err != nil || f.Session.State == workspace.SessionClosed || f.Session.ResourcesCleaned || storage.WriterSessionID != f.Session.ID {
					t.Fatal("incomplete final proof released its writer during receipt grace", err)
				}
				queued, selection, compatibility := queueAfterFinalReceipt(t, f)
				if _, _, err := f.C.Store.ReserveTurn(queued.AttemptID, selection, compatibility, f.C.ConversationIdleTimeout, 1); !errors.Is(err, workspace.ErrResidentCapacity) {
					t.Fatal("incomplete proof released physical resident capacity", err)
				}
				retained, err := f.C.Store.Terminal(f.Grant.AttemptID)
				if err != nil || retained.RecoveryFailure != nil || !bytes.Equal(retained.Body, terminal.Body) {
					t.Fatal("receipt grace overwrote the pending provider outcome", err)
				}
			})
		})
	}
}

func prepareSessionFinalResult(t *testing.T, f *sessionControllerFixture, status string, authenticated bool) (workspace.Terminal, workspace.SessionStopReceipt) {
	t.Helper()
	if _, err := f.C.Store.RequestSessionStop(f.Session.ID, "worker_shutdown"); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	command, err := f.C.receiveResult(f.Grant, wire.ProviderResult{Result: agent.Result{Status: status, Error: "actual provider " + status}})
	if err != nil {
		t.Fatal(err)
	}
	if authenticated {
		receipt := workspace.ResultReceipt{WorkerSessionID: f.Session.ID, TurnSequence: f.Grant.TurnSequence, TaskID: f.Grant.TaskID,
			AttemptID: f.Grant.AttemptID, PodUID: f.Grant.PodUID, PVCUID: f.Grant.PVCUID, RequestDigest: command.RequestDigest, Nonce: command.Nonce}
		receipt.Signature = ed25519.Sign(f.Key, workspace.ResultReceiptMessage(receipt))
		if err := f.C.Store.RecordResultReceipt(f.Grant.AttemptID, receipt); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.C.Store.CloseCheckouts(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.CheckoutsFlushed(f.Grant.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.C.Store.RecordSessionControllerFlush(f.Session.ID); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	receipt := workspace.SessionStopReceipt{WorkerSessionID: f.Session.ID, AttemptID: f.Session.LastAttemptID, InputDigest: f.Session.InputDigest,
		TurnSequence: f.Session.TurnSequence, PodUID: f.Session.PodUID, PVCUID: f.Session.PVCUID,
		Revision: f.Session.Stop.Revision, Nonce: f.Session.Stop.Nonce, WritersStopped: true, FlushOK: true}
	terminal, err := f.C.Store.Terminal(f.Grant.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return terminal, receipt
}

func recordSessionFinalReceipt(t *testing.T, f *sessionControllerFixture, receipt workspace.SessionStopReceipt) error {
	t.Helper()
	receipt.Signature = ed25519.Sign(f.Key, workspace.SessionStopReceiptMessage(receipt))
	body, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := f.C.Store.CreateSessionChallenge(f.Session.ID, f.Session.ControlToken, workspace.SessionOperationStopReceipt, wire.Digest(body))
	if err != nil {
		t.Fatal(err)
	}
	proof := workspace.SessionProof{SessionChallenge: challenge}
	proof.Signature = ed25519.Sign(f.Key, workspace.SessionProofMessage(proof))
	return f.C.Store.RecordSessionStopReceipt(f.Session.ID, receipt, proof)
}

func queueAfterFinalReceipt(t *testing.T, f *sessionControllerFixture) (workspace.TaskGrant, workspace.Selection, string) {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(f.Grant.Envelope, &fields); err != nil {
		t.Fatal(err)
	}
	id, issue := uuid.NewString(), uuid.NewString()
	fields["id"], fields["issue_id"], fields["auth_token"] = id, issue, "mat_"+id
	envelope, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, AgentID: f.Grant.AgentID,
		WorkspaceID: f.Grant.WorkspaceID, RuntimeID: f.Grant.RuntimeID, RuntimeRef: f.Grant.RuntimeRef, Metadata: f.Grant.Metadata,
		Envelope: envelope, ResourceScope: []string{"issue:" + issue}})
	if err != nil {
		t.Fatal(err)
	}
	selection, compatibility := selectConversationFixtureTurn(t, f.C, queued, true)
	return queued, selection, compatibility
}
