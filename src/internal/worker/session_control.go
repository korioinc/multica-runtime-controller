package worker

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type sessionGateway struct {
	bootstrap wire.SessionBootstrap
	identity  wire.SessionAdmission
	key       ed25519.PrivateKey
	client    *http.Client
}

func sessionControl(ctx context.Context, client *http.Client, bootstrap wire.SessionBootstrap, method, action string, body, result any) error {
	limit := int64(wire.MaxRequestBytes)
	if action == workspace.SessionOperationPoll {
		limit = wire.MaxSessionPollResponseBytes
	}
	return gatewayControl(ctx, client, bootstrap.GatewayURL+"/internal/worker-sessions/"+bootstrap.WorkerSessionID+"/"+action,
		bootstrap.ControlCapability, method, body, result, limit, "worker_session_id", bootstrap.WorkerSessionID)
}

func (s *sessionGateway) enroll(ctx context.Context) error {
	for {
		err := sessionControl(ctx, s.client, s.bootstrap, http.MethodPost, "admit", s.identity, nil)
		if err == nil || !errors.Is(err, errGatewayUnavailable) {
			return err
		}
		if err := waitControl(ctx); err != nil {
			return err
		}
	}
}

func (s *sessionGateway) proof(ctx context.Context, operation string, body any, minimumSequence uint64) (wire.SessionControlRequest, error) {
	raw, err := json.Marshal(body)
	if err != nil || len(raw) > wire.MaxRequestBytes {
		return wire.SessionControlRequest{}, errors.New("invalid signed session body")
	}
	request := wire.SessionChallengeRequest{Operation: operation, BodyDigest: wire.Digest(raw)}
	var challenge workspace.SessionChallenge
	for {
		err = sessionControl(ctx, s.client, s.bootstrap, http.MethodPost, "challenge", request, &challenge)
		if err == nil || !errors.Is(err, errGatewayUnavailable) {
			break
		}
		if err = waitControl(ctx); err != nil {
			break
		}
	}
	if err != nil {
		return wire.SessionControlRequest{}, err
	}
	if challenge.Operation != operation || challenge.BodyDigest != request.BodyDigest || challenge.WorkerSessionID != s.bootstrap.WorkerSessionID ||
		challenge.PodUID != s.identity.PodUID || challenge.PVCUID != s.bootstrap.PVCUID || challenge.TurnSequence < minimumSequence ||
		!wire.UUID(challenge.Nonce) || !challenge.ExpiresAt.After(time.Now()) || len(s.key) != ed25519.PrivateKeySize {
		return wire.SessionControlRequest{}, errors.New("session challenge differs from the admitted supervisor")
	}
	proof := workspace.SessionProof{SessionChallenge: challenge}
	proof.Signature = ed25519.Sign(s.key, workspace.SessionProofMessage(proof))
	return wire.SessionControlRequest{Proof: proof, Body: raw}, nil
}

// submit retries exactly the accepted mutation bytes. A lost acknowledgement
// cannot authorize a different turn, payload, or operation under a new proof.
func (s *sessionGateway) submit(ctx context.Context, request wire.SessionControlRequest, result any) error {
	for {
		err := sessionControl(ctx, s.client, s.bootstrap, http.MethodPost, request.Proof.Operation, request, result)
		if err == nil || !errors.Is(err, errGatewayUnavailable) {
			return err
		}
		if err := waitControl(ctx); err != nil {
			return err
		}
	}
}

// A read can lose its challenge when reservation changes the current turn.
// Obtain a fresh challenge only for these operations, which cannot execute work.
func (s *sessionGateway) read(ctx context.Context, operation string, body, result any, minimumSequence uint64) error {
	if operation != workspace.SessionOperationPoll && operation != workspace.SessionOperationStopControl {
		return errors.New("session operation is not a read")
	}
	for {
		request, err := s.proof(ctx, operation, body, minimumSequence)
		if err != nil {
			return err
		}
		err = s.submit(ctx, request, result)
		if err == nil || !errors.Is(err, errActionRefused) {
			return err
		}
		if err := waitControl(ctx); err != nil {
			return err
		}
	}
}
