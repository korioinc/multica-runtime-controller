package controller

import (
	"context"
	"errors"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func turnAttributes(g workspace.TaskGrant) []any {
	attributes := diagnostics.TaskAttributes(g.TaskID, g.AttemptID)
	if g.WorkerSessionID != "" {
		attributes = append(attributes, "conversation_kind", g.Conversation.Kind, "conversation", g.Conversation.Digest(),
			"storage", g.StorageID, "generation", g.Generation, "worker_session_id", g.WorkerSessionID,
			"podUID", g.PodUID, "turn_sequence", g.TurnSequence, "warm", g.TurnSequence > 1)
	}
	return attributes
}

func failureReason(err error, fallback string) string {
	var diagnostic *diagnostics.Error
	if errors.As(err, &diagnostic) {
		return diagnostic.Reason
	}
	switch {
	case errors.Is(err, workspace.ErrPreparationWritersUnproven):
		return "preparation_writers_unproven"
	case errors.Is(err, workspace.ErrConflict):
		return "authority_conflict"
	case errors.Is(err, workspace.ErrUnauthorized):
		return "attempt_unauthorized"
	case errors.Is(err, workspace.ErrStorageBusy):
		return "task_storage_busy"
	case errors.Is(err, context.Canceled):
		return "operation_cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "operation_timeout"
	case daemonapi.RequestNotSent(err):
		return "backend_request_not_sent"
	}
	var response *daemonapi.ResponseError
	if errors.As(err, &response) {
		return "backend_request_failed"
	}
	return fallback
}

func failureAttributes(err error, fallback string) []any {
	if err == nil {
		return nil
	}
	attributes := []any{"reason", failureReason(err, fallback)}
	if errors.Is(err, context.DeadlineExceeded) {
		attributes = append(attributes, "timeout", true)
	}
	if errors.Is(err, context.Canceled) {
		attributes = append(attributes, "cancelled", true)
	}
	var response *daemonapi.ResponseError
	if errors.As(err, &response) {
		attributes = append(attributes, "httpStatus", response.StatusCode)
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		attributes = append(attributes, "httpStatus", status.Status().Code)
	}
	return attributes
}
