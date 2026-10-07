package workspace

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSessionLatePodBindingPreservesControllerFailure(t *testing.T) {
	f := unusedSessionReservation(t)
	g, session := f.grant, f.session
	if err := f.store.BeginPreparation(g.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://preparer"}); err != nil {
		t.Fatal(err)
	}
	prepared := Prepared{Conversation: clonePointer(&g.Conversation), WorkspaceAnchorTaskID: g.WorkspaceAnchorTaskID,
		OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID,
		Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, Provider: "codex", Executable: "/opt/tools/runner",
		RuntimeDigest: g.Fingerprint, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, CreatedAt: f.store.now(),
		CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{},
		Environment: NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}}
	marker, _ := json.Marshal(map[string]string{"managed_by": "multica-daemon-task", "agent_id": g.AgentID})
	prepared.NativeMetadata = &NativeMetadata{WorkerSessionID: session.ID, TurnSequence: g.TurnSequence,
		ArtifactRoot: NativeArtifactRoot(g.TaskRoot, session.ID, g.TurnSequence), Artifacts: []NativeArtifact{}, TaskMarker: marker,
		CodexConfig: []byte("model = \"fixture-model\"\n")}
	prepared.AllowedLinks = NativeProjectionLinks(prepared.Provider)
	prepared.Digest = preparedDigest(prepared)
	run, _ := json.Marshal(map[string]any{"nativeMetadata": prepared.NativeMetadata})
	if err := f.store.CompletePreparation(g.AttemptID, &prepared, run); err != nil {
		t.Fatal(err)
	}
	bootstrap, _ := json.Marshal(map[string]any{"version": SessionProtocolVersion, "workerSessionID": session.ID, "conversation": session.Conversation,
		"storageID": session.StorageID, "workspaceAnchorTaskID": session.WorkspaceAnchorTaskID, "compatibilityDigest": session.CompatibilityDigest,
		"taskRoot": session.TaskRoot, "pvcName": session.PVCName, "pvcUID": session.PVCUID, "runtimeID": session.RuntimeID, "controlCapability": session.ControlToken})
	if err := f.store.SetSessionBootstrap(session.ID, bootstrap); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetSessionResources(session.ID, json.RawMessage(`{"podCreateRequested":true,"secretCreateRequested":true,"reference":{}}`)); err != nil {
		t.Fatal(err)
	}
	stopToken, err := f.store.IssueCapability(g.AttemptID, "stop", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	businessToken, err := f.store.IssueCapability(g.AttemptID, "daemon", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	failure, err := f.store.ReceiveFailure(g.AttemptID, json.RawMessage(`{"error":"Pod creation response was lost"}`))
	if err != nil || failure.PodUID != "" {
		t.Fatal("fixture did not retain the failure before Pod identity was observed", err)
	}
	if err := f.store.ObserveSessionStop(session.ID, StopEvidence{Kind: "no-worker", PVCUID: session.PVCUID, ObservedAt: f.store.now()}); !errors.Is(err, ErrConflict) {
		t.Fatal("uncertain Pod creation was treated as no worker", err)
	}
	uid := uuid.NewString()
	if err := f.store.BindSessionPod(session.ID, session.PodName, uid, "node-a"); err != nil {
		t.Fatal("recovered Pod could not bind the pending controller failure", err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = Open(f.options)
	if err != nil {
		t.Fatal("late Pod binding did not survive journal reopen", err)
	}
	defer f.store.Close()
	failure.PodUID = uid
	retained, err := f.store.Terminal(g.AttemptID)
	if err != nil || !sameJSON(retained, failure) {
		t.Fatal("late binding changed failure evidence beyond its empty Pod UID", err)
	}
	if stopped, err := f.store.AuthorizeStop(stopToken); err != nil || stopped.PodUID != uid || stopped.AttemptID != g.AttemptID {
		t.Fatal("late binding did not atomically bind cleanup authority", err)
	}
	if _, err := f.store.Authorize(businessToken, "daemon"); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("late Pod observation renewed execution authority", err)
	}
	if err := f.store.BindSessionPod(session.ID, session.PodName, uid, "node-a"); err != nil {
		t.Fatal("identical recovered Pod observation was not idempotent", err)
	}
	if err := f.store.BindSessionPod(session.ID, session.PodName, uuid.NewString(), "node-a"); !errors.Is(err, ErrConflict) {
		t.Fatal("another Pod replaced the recovered controller failure identity", err)
	}
	if stopped, err := f.store.AuthorizeStop(stopToken); err != nil || stopped.PodUID != uid {
		t.Fatal("rejected Pod identity changed cleanup authority", err)
	}
	if err := f.store.CloseSession(session.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("Pod observation alone released its writer", err)
	}
	if err := f.store.MarkSessionCleaned(session.ID); !errors.Is(err, ErrConflict) {
		t.Fatal("Pod observation alone proved resource cleanup", err)
	}
	if err := f.store.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckoutsFlushed(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RecordSessionControllerFlush(session.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.ObserveSessionStop(session.ID, StopEvidence{Kind: "never-started", PodUID: uid, PVCUID: session.PVCUID, PreparedFlushOK: true, ObservedAt: f.store.now()}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CloseSession(session.ID); err != nil {
		t.Fatal("proven termination could not close the recovered Pod", err)
	}
	if err := f.store.MarkSessionCleaned(session.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RejectFailure(g.AttemptID); err != nil {
		t.Fatal("recovered failure could not settle without overwriting the backend assignment", err)
	}
}

func TestSessionPodBindingDoesNotRewriteSignedWorkerResults(t *testing.T) {
	f := newSessionFixture(t)
	f.receive(t, "")
	before, err := f.store.Terminal(f.grant.AttemptID)
	if err != nil || before.ResultReceipt == nil {
		t.Fatal("fixture has no signed worker outcome", err)
	}
	if err := f.store.BindSessionPod(f.session.ID, f.session.PodName, f.session.PodUID, f.session.NodeID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.BindSessionPod(f.session.ID, f.session.PodName, uuid.NewString(), f.session.NodeID); !errors.Is(err, ErrConflict) {
		t.Fatal("a signed worker result could be rebound to another Pod", err)
	}
	after, err := f.store.Terminal(f.grant.AttemptID)
	if err != nil || !sameJSON(before, after) || !validResultReceipt(f.grant, after, *after.ResultReceipt) {
		t.Fatal("Pod observation changed authenticated worker evidence", err)
	}
}
