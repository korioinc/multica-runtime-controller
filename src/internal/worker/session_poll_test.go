package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func pollAssignment(t *testing.T, size int) (wire.SessionBootstrap, wire.TurnAssignment) {
	t.Helper()
	sha := core.Digest([]byte("poll transport fixture"))
	bundle := configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)}
	executable := runtimeimage.Executable{Path: "/opt/fixture/provider", Version: "fixture", SHA256: sha}
	ref := runtimeimage.Ref{Image: "registry.example/runtime@sha256:" + sha, Platform: "linux/arm64", ImageBuildID: uuid.NewString(),
		DescriptorDigest: sha, ConfigurationDigest: configuration.ExecutionDigest(bundle, nil),
		Controller: core.Contract{BuildID: sha, Platform: "linux/arm64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.6"},
		Daemon:     runtimeimage.Daemon{Executable: executable, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"pi": executable}}
	workspaceID, taskID := uuid.NewString(), uuid.NewString()
	root, err := workspace.TaskRoot(wire.WorkspaceRoot, workspaceID, taskID, "", "")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.FixedZone("fixture", 23*60*60+59*60))
	session := wire.SessionBootstrap{Version: wire.SessionProtocolVersion, WorkerSessionID: uuid.NewString(),
		Conversation: workspace.ConversationKey{OwnerID: uuid.NewString(), WorkspaceID: workspaceID, AgentID: uuid.NewString(), Kind: workspace.ConversationIssue, SubjectID: uuid.NewString()},
		StorageID:    uuid.NewString(), WorkspaceAnchorTaskID: taskID, CompatibilityDigest: sha,
		TaskRoot: root, PVCName: "workspace", PVCUID: uuid.NewString(), NFSServer: "127.0.0.1", GatewayURL: "http://127.0.0.1",
		Provider: "pi", RuntimeID: uuid.NewString(), RuntimeRef: ref, ControlCapability: strings.Repeat("c", 32), TerminationGraceSeconds: 1}
	bootstrap := wire.Bootstrap{WorkerSessionID: session.WorkerSessionID, TurnSequence: ^uint64(0), WorkspaceAnchorTaskID: taskID,
		OwnerID: session.Conversation.OwnerID, WorkspaceID: workspaceID, TaskID: taskID, AttemptID: uuid.NewString(), AgentID: session.Conversation.AgentID,
		RuntimeID: session.RuntimeID, StorageID: session.StorageID, Generation: 1, TaskRoot: root, NFSServer: session.NFSServer,
		PVCName: session.PVCName, PVCUID: session.PVCUID, Provider: session.Provider, RuntimeRef: ref, PreparedDigest: sha,
		GatewayURL: session.GatewayURL, APICapability: session.ControlCapability, SupervisorCapability: session.ControlCapability,
		StopCapability: session.ControlCapability, CacheCapability: session.ControlCapability, Configuration: bundle,
		ExpiresAt: deadline.Format(time.RFC3339Nano), TerminationGraceSeconds: session.TerminationGraceSeconds}
	assignment := wire.TurnAssignment{WorkerSessionID: session.WorkerSessionID, PodUID: uuid.NewString(), TurnSequence: bootstrap.TurnSequence,
		Bootstrap: bootstrap, Deadline: deadline, InputDigest: sha,
		Run: wire.Run{Provider: session.Provider, Prompt: "<>&\u2028\u2029", Options: agent.ExecOptions{Cwd: root + "/workdir", Timeout: time.Hour}}}
	raw, err := json.Marshal(assignment)
	if err != nil || len(raw) > size {
		t.Fatal("invalid assignment fixture size", err)
	}
	assignment.Run.Prompt += strings.Repeat("x", size-len(raw))
	assignment.InputDigest = assignment.Digest()
	raw, err = json.Marshal(assignment)
	if err != nil || len(raw) != size {
		t.Fatal("assignment fixture did not reach the requested encoded boundary", err)
	}
	return session, assignment
}

func pollResponseBytes(t *testing.T, assignment wire.TurnAssignment) []byte {
	t.Helper()
	response := wire.SessionPollResponse{WorkerSessionID: assignment.WorkerSessionID, TurnSequence: assignment.TurnSequence,
		SettledTurnSequence: assignment.TurnSequence - 1, State: workspace.SessionQuarantined, Assignment: &assignment,
		IdleDeadline: assignment.Deadline, Stop: &wire.SessionStopCommand{Revision: ^uint64(0), Nonce: uuid.NewString(),
			TurnSequence: assignment.TurnSequence, AttemptID: assignment.Bootstrap.AttemptID, InputDigest: assignment.InputDigest}}
	// Populate all optional fields to bound the envelope. A live poll normally
	// sends either an assignment or a stop; this decoder test covers both sizes.
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(response); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func readPollResponse(t *testing.T, session wire.SessionBootstrap, body []byte) (wire.SessionPollResponse, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	session.GatewayURL = server.URL
	var response wire.SessionPollResponse
	err := sessionControl(context.Background(), server.Client(), session, http.MethodPost, workspace.SessionOperationPoll, struct{}{}, &response)
	return response, err
}

func TestSessionPollDeliversTheMaximumAssignment(t *testing.T) {
	session, assignment := pollAssignment(t, wire.MaxAssignmentBytes)
	raw, _ := json.Marshal(assignment)
	if _, err := wire.DecodeTurnAssignment(raw); err != nil {
		t.Fatal("maximum assignment failed the existing logical bound", err)
	}
	body := pollResponseBytes(t, assignment)
	if len(body) <= wire.MaxAssignmentBytes {
		t.Fatal("fixture did not cross the assignment-only transport bound")
	}
	response, err := readPollResponse(t, session, body)
	if err != nil || response.Assignment == nil {
		t.Fatal("maximum published assignment could not reach the worker", err)
	}
	if err := response.Assignment.Validate(session, assignment.PodUID); err != nil {
		t.Fatal("delivered assignment lost its admitted binding", err)
	}
	if response.WorkerSessionID != session.WorkerSessionID || response.Assignment.Run.Prompt != assignment.Run.Prompt {
		t.Fatal("poll decoding changed the current task input")
	}
	t.Logf("assignment bytes=%d, full response bytes=%d, envelope including newline=%d", len(raw), len(body), len(body)-len(raw))
}

func TestSessionPollEnvelopeDoesNotEnlargeTheAssignment(t *testing.T) {
	session, assignment := pollAssignment(t, wire.MaxAssignmentBytes+1)
	raw, _ := json.Marshal(assignment)
	if _, err := wire.DecodeTurnAssignment(raw); err == nil {
		t.Fatal("standalone decoding accepted an oversized assignment")
	}
	response, err := readPollResponse(t, session, pollResponseBytes(t, assignment))
	if err != nil || response.Assignment == nil {
		t.Fatal("fixture did not reach assignment validation within the larger poll envelope", err)
	}
	if err := response.Assignment.Validate(session, assignment.PodUID); err == nil {
		t.Fatal("poll envelope headroom authorized oversized task input")
	}
}

func TestSessionPollRejectsAnOversizedFullResponse(t *testing.T) {
	session, assignment := pollAssignment(t, wire.MaxAssignmentBytes)
	body := pollResponseBytes(t, assignment)
	padding := wire.MaxSessionPollResponseBytes - len(body) + 1
	if padding <= 0 {
		t.Fatal("valid envelope no longer fits its response budget")
	}
	// Trailing whitespace remains valid JSON. Only the bounded reader should
	// reject this response, independently of the inner assignment's size.
	body = append(body, bytes.Repeat([]byte(" "), padding)...)
	if _, err := readPollResponse(t, session, body); err == nil {
		t.Fatal("poll accepted a response beyond its bounded reader")
	}
}

func TestSessionPollKeepsMalformedAndForeignAssignmentsDenied(t *testing.T) {
	session, assignment := pollAssignment(t, wire.MaxAssignmentBytes)
	body := pollResponseBytes(t, assignment)
	if _, err := readPollResponse(t, session, body[:len(body)-2]); err == nil {
		t.Fatal("poll accepted malformed response JSON")
	}
	foreign := assignment
	foreign.WorkerSessionID = uuid.NewString()
	foreign.Bootstrap.WorkerSessionID = foreign.WorkerSessionID
	foreign.InputDigest = foreign.Digest()
	raw, _ := json.Marshal(foreign)
	if _, err := wire.DecodeTurnAssignment(raw); err != nil {
		t.Fatal("foreign fixture did not establish a coherent separate session", err)
	}
	response, err := readPollResponse(t, session, pollResponseBytes(t, foreign))
	if err != nil || response.Assignment == nil {
		t.Fatal("foreign fixture did not reach the admitted-session check", err)
	}
	if err := response.Assignment.Validate(session, assignment.PodUID); err == nil {
		t.Fatal("a larger poll response rebound another session's assignment")
	}
}
