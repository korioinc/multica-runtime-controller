package controller

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func sessionRecord(session workspace.WorkerSession) (resourceRecord, error) {
	var record resourceRecord
	if len(session.Resources) == 0 {
		return record, errPreparePending
	}
	if json.Unmarshal(session.Resources, &record) != nil {
		return record, errors.New("invalid session resource journal")
	}
	ref := record.Reference
	if ref.WorkerSessionID != session.ID || ref.WorkspaceID != session.Conversation.WorkspaceID || ref.OwnerID != session.Conversation.OwnerID ||
		ref.WorkspaceAnchorTaskID != session.WorkspaceAnchorTaskID || ref.StorageID != session.StorageID || ref.PodName != session.PodName ||
		ref.PVCName != session.PVCName || ref.PVCUID != session.PVCUID || ref.TaskRoot != session.TaskRoot ||
		ref.RequestDigest != session.BootstrapDigest || !ref.RuntimeRef.Equal(session.RuntimeRef) ||
		ref.TaskID != "" || ref.AttemptID != "" || ref.PodUID != "" && ref.PodUID != session.PodUID {
		return record, errors.New("session resource identity differs from journal")
	}
	// Binding can commit before the resource payload's observational fields.
	record.Reference.PodUID, record.Reference.NodeID = session.PodUID, session.NodeID
	return record, nil
}

func (c *Controller) workerSessionControl(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 5 || parts[1] != "internal" || parts[2] != "worker-sessions" || !wire.UUID(parts[3]) || r.URL.RawPath != "" || r.URL.RawQuery != "" {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	id, action := parts[3], parts[4]
	if action == "admit" || action == "challenge" || action == workspace.SessionOperationStopControl && r.Method == http.MethodGet {
		c.sessionEnrollment(w, r, id, action)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	var request wire.SessionControlRequest
	if decodeBody(r, &request) != nil || request.Proof.WorkerSessionID != id || request.Proof.Operation != action ||
		!json.Valid(request.Body) || request.Proof.BodyDigest != wire.Digest(request.Body) {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	var err error
	switch action {
	case workspace.SessionOperationPoll:
		var poll wire.SessionPoll
		if runtimeimage.Decode(request.Body, &poll) != nil {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		var session workspace.WorkerSession
		session, err = c.Store.AuthorizeSession(request.Proof)
		if err == nil {
			var response wire.SessionPollResponse
			response, err = c.sessionPoll(r.Context(), session, poll, request.Proof)
			if err == nil {
				writeJSON(w, response)
				return
			}
		}
	case workspace.SessionOperationAccept:
		var accepted wire.SessionAccept
		if runtimeimage.Decode(request.Body, &accepted) != nil {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		// The durable old proof receives only its original acknowledgement.
		// A replay must never render the session's newer assignment.
		old, oldErr := c.Store.Get(request.Proof.AttemptID)
		if oldErr == nil && old.WorkerSessionID == id && old.TurnSequence == accepted.TurnSequence && old.InputDigest == accepted.InputDigest && sameControlProof(old.AcceptProof, request.Proof) {
			writeJSON(w, struct{}{})
			return
		}
		var session workspace.WorkerSession
		session, err = c.Store.GetSession(id)
		if err == nil {
			err = c.liveSession(r.Context(), session)
		}
		if err == nil {
			var grant workspace.TaskGrant
			grant, err = c.Store.AcceptTurn(id, accepted.TurnSequence, accepted.InputDigest, request.Proof)
			if err == nil {
				c.enqueueAttempt(grant.AttemptID)
			}
		}
	case workspace.SessionOperationStopControl:
		var empty struct{}
		if runtimeimage.Decode(request.Body, &empty) != nil {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		var session workspace.WorkerSession
		session, err = c.Store.AuthorizeSession(request.Proof)
		if err == nil {
			var command wire.SessionStopCommand
			command, err = c.sessionStopCommand(r.Context(), session)
			if err == nil {
				writeJSON(w, command)
				return
			}
		}
	case workspace.SessionOperationStopRequest:
		var body wire.SessionStopRequest
		if runtimeimage.Decode(request.Body, &body) != nil {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		var session workspace.WorkerSession
		session, err = c.Store.RequestSessionStopSigned(id, body.Reason, request.Proof)
		if err == nil {
			c.enqueueSession(id)
			var command wire.SessionStopCommand
			command, err = c.sessionStopCommand(r.Context(), session)
			if err == nil {
				writeJSON(w, command)
				return
			}
		}
	case workspace.SessionOperationStopReceipt:
		var receipt workspace.SessionStopReceipt
		if runtimeimage.Decode(request.Body, &receipt) != nil {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		err = c.Store.RecordSessionStopReceipt(id, receipt, request.Proof)
		if err == nil {
			c.enqueueSession(id)
		}
	case workspace.SessionOperationTurnExecutionReceipt:
		var receipt workspace.TurnExecutionReceipt
		if runtimeimage.Decode(request.Body, &receipt) != nil || receipt.WorkerSessionID != id {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		err = c.Store.RecordTurnExecutionReceipt(receipt.AttemptID, receipt, request.Proof)
		if err == nil {
			c.enqueueDelivery(receipt.AttemptID)
			c.enqueueSession(id)
			c.wakeClaim()
		}
	default:
		err = workspace.ErrUnauthorized
	}
	if err != nil {
		writeSessionError(w, err)
		return
	}
	writeJSON(w, struct{}{})
}

func sameControlProof(stored *workspace.SessionProof, supplied workspace.SessionProof) bool {
	return stored != nil && stored.SessionChallenge == supplied.SessionChallenge && bytes.Equal(stored.Signature, supplied.Signature)
}

func writeSessionError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	if errors.Is(err, workspace.ErrUnauthorized) {
		status = http.StatusForbidden
	} else if errors.Is(err, workspace.ErrConflict) {
		status = http.StatusConflict
	}
	http.Error(w, "session operation unavailable", status)
}

func (c *Controller) sessionEnrollment(w http.ResponseWriter, r *http.Request, id, action string) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	session, err := c.Store.AuthorizeSessionEnrollment(id, token)
	if err != nil {
		writeSessionError(w, err)
		return
	}
	if action == workspace.SessionOperationStopControl && r.Method == http.MethodGet {
		if len(session.SupervisorKey) != 0 {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		writeJSON(w, sessionStopIdentity(session))
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	if action == "challenge" {
		var input wire.SessionChallengeRequest
		if decodeBody(r, &input) != nil {
			http.Error(w, "denied", http.StatusForbidden)
			return
		}
		challenge, err := c.Store.CreateSessionChallenge(id, token, input.Operation, input.BodyDigest)
		if err != nil {
			writeSessionError(w, err)
			return
		}
		writeJSON(w, challenge)
		return
	}
	var input wire.SessionAdmission
	if decodeBody(r, &input) != nil || input.WorkerSessionID != id || !wire.UUID(input.PodUID) || input.PVCUID != session.PVCUID ||
		input.BootstrapDigest != session.BootstrapDigest || len(input.PublicKey) != ed25519.PublicKeySize {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	if session.PodUID == "" {
		// Reconciliation must observe the originally requested Pod. A worker's
		// submitted identity cannot supply the missing durable UID binding.
		writeSessionError(w, errPreparePending)
		return
	}
	if input.PodUID != session.PodUID {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	if len(session.SupervisorKey) != 0 {
		if !bytes.Equal(session.SupervisorKey, input.PublicKey) {
			http.Error(w, "denied", http.StatusForbidden)
		} else {
			writeJSON(w, struct{}{})
		}
		return
	}
	record, err := sessionRecord(session)
	if err == nil {
		record.Reference.Owner = c.Owner
		pod, observationErr := c.Kube.AuthorizeStop(r.Context(), record.Reference)
		err = observationErr
		if err == nil {
			if pod.Spec.NodeName == "" {
				err = errPreparePending
			} else {
				err = c.Store.BindSessionPod(id, session.PodName, session.PodUID, pod.Spec.NodeName)
			}
		}
	}
	if err == nil {
		err = c.Store.AdmitSession(id, input.BootstrapDigest, input.PublicKey)
	}
	if err != nil {
		writeSessionError(w, err)
		return
	}
	c.enqueueSession(id)
	if session.ActiveAttempt != "" {
		c.enqueueAttempt(session.ActiveAttempt)
	}
	writeJSON(w, struct{}{})
}

func (c *Controller) liveSession(ctx context.Context, session workspace.WorkerSession) error {
	record, err := sessionRecord(session)
	if err != nil {
		return err
	}
	record.Reference.Owner = c.Owner
	_, err = c.Kube.Authorize(ctx, record.Reference)
	return err
}

func (c *Controller) sessionPoll(ctx context.Context, session workspace.WorkerSession, poll wire.SessionPoll, proof workspace.SessionProof) (wire.SessionPollResponse, error) {
	response := wire.SessionPollResponse{WorkerSessionID: session.ID, TurnSequence: session.TurnSequence, State: session.State, IdleDeadline: session.IdleDeadline}
	switch poll.State {
	case workspace.SessionCreating, workspace.SessionReserved, workspace.SessionRunning, workspace.SessionFinishing, workspace.SessionIdle:
	default:
		return response, workspace.ErrConflict
	}
	if poll.TurnSequence > session.TurnSequence {
		return response, workspace.ErrConflict
	}
	if session.LastCompleteAttempt != "" {
		grant, err := c.Store.Get(session.LastCompleteAttempt)
		if err != nil || !grant.TurnComplete || grant.WorkerSessionID != session.ID {
			return response, workspace.ErrConflict
		}
		response.SettledTurnSequence = grant.TurnSequence
	}
	if session.Stop != nil {
		command, err := c.sessionStopCommand(ctx, session)
		response.Stop = &command
		return response, err
	}
	if err := c.liveSession(ctx, session); err != nil {
		return response, err
	}
	// Live observation can overlap a reservation, cancellation or expiry.
	// A fresh challenge can retry that read without reviving the old binding.
	current, err := c.Store.AuthorizeSession(proof)
	if err != nil || current.Revision != session.Revision || current.ActiveAttempt != session.ActiveAttempt || current.Stop != nil {
		return response, errPreparePending
	}
	if session.ActiveAttempt != "" {
		grant, err := c.Store.Get(session.ActiveAttempt)
		if err != nil || grant.WorkerSessionID != session.ID || grant.TurnSequence != session.TurnSequence {
			return response, workspace.ErrConflict
		}
		if poll.TurnSequence < session.TurnSequence && (session.State == workspace.SessionRunning || session.State == workspace.SessionFinishing) {
			return response, workspace.ErrConflict
		}
		if session.State == workspace.SessionReserved && len(grant.Assignment) != 0 {
			var assignment wire.TurnAssignment
			bootstrap, err := wire.DecodeSessionBootstrap(session.Bootstrap)
			if err != nil || json.Unmarshal(grant.Assignment, &assignment) != nil || assignment.Validate(bootstrap, session.PodUID) != nil ||
				assignment.InputDigest != grant.InputDigest || !time.Now().Before(assignment.Deadline) {
				return response, workspace.ErrConflict
			}
			response.Assignment = &assignment
		}
	}
	return response, nil
}

func sessionStopIdentity(session workspace.WorkerSession) wire.SessionStopCommand {
	if session.Stop == nil {
		return wire.SessionStopCommand{}
	}
	command := wire.SessionStopCommand{Revision: session.Stop.Revision, Nonce: session.Stop.Nonce, TurnSequence: session.TurnSequence,
		AttemptID: session.LastAttemptID, InputDigest: session.InputDigest, ControllerWritersStopped: session.Stop.ControllerWritersStopped}
	if session.ProtocolVersion == wire.SessionProtocolVersion {
		command.Reason = session.Stop.Reason
	}
	return command
}

func (c *Controller) sessionStopCommand(ctx context.Context, session workspace.WorkerSession) (wire.SessionStopCommand, error) {
	if session.Stop == nil {
		return wire.SessionStopCommand{}, nil
	}
	if !session.Stop.ControllerWritersStopped {
		fenced, err := c.fenceSessionWriters(ctx, session)
		if err != nil {
			return wire.SessionStopCommand{}, err
		}
		if fenced {
			if err := c.Store.RecordSessionControllerFlush(session.ID); err != nil {
				return wire.SessionStopCommand{}, err
			}
			var err error
			session, err = c.Store.GetSession(session.ID)
			if err != nil {
				return wire.SessionStopCommand{}, err
			}
		}
	}
	return sessionStopIdentity(session), nil
}

func (c *Controller) stopSessionControllerWriters(ctx context.Context, session workspace.WorkerSession) (bool, error) {
	if session.Stop == nil {
		return false, workspace.ErrConflict
	}
	if id := session.Stop.ActiveAttempt; id != "" {
		c.mu.Lock()
		if cancel := c.prepareCancels[id]; cancel != nil {
			cancel()
		}
		c.mu.Unlock()
		if err := c.stopCheckoutWriters(ctx, id); err != nil {
			return false, err
		}
		grant, err := c.Store.Get(id)
		if err != nil {
			return false, err
		}
		if grant.PreparationStarted && !grant.PreparationStopped || grant.CheckoutProcess != nil || !grant.CheckoutClosed {
			return false, nil
		}
	}
	return true, nil
}

func (c *Controller) fenceSessionWriters(ctx context.Context, session workspace.WorkerSession) (bool, error) {
	stopped, err := c.stopSessionControllerWriters(ctx, session)
	if err != nil || !stopped {
		return false, err
	}
	info, err := os.Lstat(session.TaskRoot)
	// Missing or unflushable files never grant clean storage authority. Joined
	// controller writers still permit independent observation of Pod termination.
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, nil
	}
	if err := workspace.SyncTaskFilesystem(session.TaskRoot); err != nil {
		return false, nil
	}
	if id := session.Stop.ActiveAttempt; id != "" {
		if err := c.Store.CheckoutsFlushed(id); err != nil {
			return false, err
		}
	}
	return true, nil
}
