package worker

import (
	"context"
	"os"
	"path/filepath"
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

func TestConsumedOrChangedAssignmentCannotStartAnotherExecution(t *testing.T) {
	testConsumedOrChangedAssignmentCannotStartAnotherExecution(t, "codex")
}

func TestConsumedOrChangedClaudeAssignmentCannotStartAnotherExecution(t *testing.T) {
	testConsumedOrChangedAssignmentCannotStartAnotherExecution(t, "claude")
}

func testConsumedOrChangedAssignmentCannotStartAnotherExecution(t *testing.T, provider string) {
	t.Helper()
	store, err := workspace.Open(workspace.Options{Directory: filepath.Join(t.TempDir(), "journal"), OwnerID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	g := preparedEventGrant(t, store)
	g.RuntimeRef.Providers = map[string]runtimeimage.Executable{provider: g.RuntimeRef.Providers["codex"]}
	sessionID := uuid.NewString()
	deadline := time.Now().Add(time.Hour)
	capability := strings.Repeat("a", 32)
	b := wire.Bootstrap{WorkerSessionID: sessionID, TurnSequence: 1, WorkspaceAnchorTaskID: g.TaskID,
		OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AttemptID: g.AttemptID, AgentID: g.AgentID,
		RuntimeID: g.RuntimeID, Generation: g.Generation, StorageID: g.StorageID, TaskRoot: g.TaskRoot,
		NFSServer: "127.0.0.1", PVCName: g.PVCName, PVCUID: g.PVCUID, Provider: provider, RuntimeRef: g.RuntimeRef,
		PreparedDigest: g.Prepared.Digest, GatewayURL: "http://127.0.0.1", APICapability: capability,
		SupervisorCapability: capability, CacheCapability: capability, StopCapability: capability,
		ExpiresAt: deadline.Format(time.RFC3339Nano), TerminationGraceSeconds: 1,
		Configuration: configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)}}
	session := wire.SessionBootstrap{Version: wire.SessionProtocolVersion, WorkerSessionID: sessionID,
		Conversation: workspace.ConversationKey{OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, Kind: workspace.ConversationIssue, SubjectID: uuid.NewString()},
		StorageID:    g.StorageID, WorkspaceAnchorTaskID: g.TaskID, CompatibilityDigest: core.Digest([]byte("compatible")),
		TaskRoot: g.TaskRoot, PVCName: g.PVCName, PVCUID: g.PVCUID, NFSServer: b.NFSServer, GatewayURL: b.GatewayURL,
		Provider: b.Provider, RuntimeID: b.RuntimeID, RuntimeRef: b.RuntimeRef, ControlCapability: capability, TerminationGraceSeconds: b.TerminationGraceSeconds}
	assignment := wire.TurnAssignment{WorkerSessionID: sessionID, PodUID: g.PodUID, TurnSequence: b.TurnSequence,
		Bootstrap: b, Deadline: deadline, Run: wire.Run{Provider: b.Provider, Options: agent.ExecOptions{Cwd: g.TaskRoot + "/workdir", Timeout: time.Hour}}}
	assignment.InputDigest = assignment.Digest()
	if err := assignment.Validate(session, g.PodUID); err != nil {
		t.Fatal(err)
	}
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "already consumed", true: "rebound task"}[changed], func(t *testing.T) {
			supervisor := sessionSupervisor{gateway: sessionGateway{bootstrap: session, identity: wire.SessionAdmission{PodUID: g.PodUID}}, sequence: assignment.TurnSequence, inputDigest: assignment.InputDigest}
			candidate := assignment
			if changed {
				candidate.Bootstrap.TaskID = uuid.NewString()
			}
			if err := supervisor.execute(context.Background(), candidate); err == nil {
				t.Fatal("a consumed or altered turn gained another execution")
			}
		})
	}
}

func TestPID1SequentialTurnsFenceEarlierDetachedWriters(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1 and Python")
	}
	request := runnerFixtureRequest(t, false)
	lateWrite := filepath.Join(request.Options.Cwd, "late-writer")
	request.Environment["PROVIDER_LATE_WRITE"] = lateWrite
	first := startRunnerFixture(t, request)
	for range first.Session.Messages {
	}
	if result, ok := <-first.Session.Result; !ok || result.Status != "completed" {
		t.Fatal("the first turn did not retain its actual provider outcome")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var before []byte
	for len(before) == 0 && ctx.Err() == nil {
		before, _ = os.ReadFile(lateWrite)
		if len(before) == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	if len(before) == 0 {
		t.Fatal("the first provider did not leave a real detached writer")
	}
	firstTurn := workerLifecycle{bootstrap: wire.Bootstrap{TaskRoot: request.Options.Cwd, TerminationGraceSeconds: 1}, runner: first}
	cleanup := firstTurn.cleanup(ctx, func(context.Context) error { return nil })
	if cleanup.err != nil || !cleanup.writersStopped || !cleanup.flushOK {
		t.Fatal("the first turn could not fence its writer", cleanup.err)
	}
	before, err := os.ReadFile(lateWrite)
	if err != nil {
		t.Fatal(err)
	}
	delete(request.Environment, "PROVIDER_LATE_WRITE")
	request.Environment["PROVIDER_PROOF"] = uuid.NewString()
	second := startRunnerFixture(t, request)
	for range second.Session.Messages {
	}
	result, ok := <-second.Session.Result
	if !ok || result.Status != "completed" || result.Output != request.Environment["PROVIDER_PROOF"] {
		t.Fatal("the next SDK execution inherited the first turn's input or outcome")
	}
	secondTurn := workerLifecycle{bootstrap: firstTurn.bootstrap, runner: second}
	cleanup = secondTurn.cleanup(ctx, func(context.Context) error { return nil })
	if cleanup.err != nil {
		t.Fatal(cleanup.err)
	}
	after, err := os.ReadFile(lateWrite)
	if err != nil || string(after) != string(before) {
		t.Fatal("an earlier turn's detached writer modified the next turn's workspace", err)
	}
}
