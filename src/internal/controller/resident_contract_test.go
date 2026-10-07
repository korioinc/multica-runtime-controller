package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestQueuedPreviousImageCannotAcquireCurrentSessionAuthority(t *testing.T) {
	f := newSessionControllerFixture(t)
	priorImage := f.C.RuntimeRef
	priorImage.Image = "example.invalid/runtime@sha256:" + core.Digest([]byte("previous protocol image"))
	var fields map[string]any
	if err := json.Unmarshal(f.Grant.Envelope, &fields); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	fields["id"], fields["auth_token"] = id, "mat_"+id
	envelope, _ := json.Marshal(fields)
	queued, err := f.C.Store.QueueClaim(workspace.TaskGrant{SessionProtocol: true, TaskID: id, WorkspaceID: f.Grant.WorkspaceID,
		AgentID: f.Grant.AgentID, RuntimeID: f.Grant.RuntimeID, RuntimeRef: priorImage, Envelope: envelope, Metadata: f.Grant.Metadata,
		Repositories: f.Grant.Repositories, ResourceScope: f.Grant.ResourceScope})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.C.activateTurn(t.Context(), queued); err != nil {
		t.Fatal(err)
	}
	reopenStopRegressionStore(t, f.C, f.Options)
	retained, err := f.C.Store.Get(queued.AttemptID)
	if err != nil || retained.WorkerSessionID != "" || retained.StorageID != "" || retained.PodUID != "" ||
		retained.StartConfirmed || retained.State != "unexecuted" || !retained.RuntimeRef.Equal(priorImage) || !bytes.Equal(retained.Envelope, queued.Envelope) {
		t.Fatal("an old queued image gained v2 execution or changed its frozen authority", err)
	}
	terminal, err := f.C.Store.Terminal(queued.AttemptID)
	if err != nil || terminal.Source != "controller" || terminal.ResultReceipt != nil {
		t.Fatal("old-image admission failure invented a worker result", err)
	}
	f.refresh(t)
	if !f.Grant.StartConfirmed || f.Session.Stop != nil || f.Session.ActiveAttempt != f.Grant.AttemptID {
		t.Fatal("queued-image refusal disrupted an already bound execution")
	}
}

func TestNamedWorkspaceSourceRequiresExactLiveDirtyOwner(t *testing.T) {
	f := newSessionControllerFixture(t)
	terminal := receiveFixtureTurnResult(t, f, true)
	quiesceFixtureTurn(t, f, terminal)
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := daemonapi.ParseClaim(f.Grant.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	claim.Attribution = &daemonapi.TaskAttribution{RerunOfTaskID: f.Grant.TaskID}
	allowed, err := f.C.workspaceIncludesNamedSource(claim, storage)
	if err != nil || !allowed || !storage.Dirty || storage.Checkpoint != nil || storage.WriterSessionID != f.Session.ID {
		t.Fatal("exact accepted warm source required a false clean-storage certificate", err)
	}
	if _, err := f.C.Store.RequestSessionStop(f.Session.ID, "idle_expired"); err != nil {
		t.Fatal(err)
	}
	allowed, err = f.C.workspaceIncludesNamedSource(claim, storage)
	if allowed || !errors.Is(err, workspace.ErrStorageBusy) {
		t.Fatal("stopped owner retained workspace selection authority", err)
	}
}

func TestDelayedLiveCompletionKeepsDirtyStorageAndOriginalDeadline(t *testing.T) {
	f := newSessionControllerFixture(t)
	terminal := receiveFixtureTurnResult(t, f, true)
	quiesceFixtureTurn(t, f, terminal)
	if err := f.C.deliverTurnResult(t.Context(), f.Grant); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	idle := f.Session
	f.Options.Now = func() time.Time { return idle.IdleSince.Add(time.Minute) }
	reopenStopRegressionStore(t, f.C, f.Options)
	for range 2 {
		if err := f.C.finishTurn(t.Context(), f.Grant); err != nil {
			t.Fatal(err)
		}
	}
	f.refresh(t)
	storage, err := f.C.Store.GetStorage(f.Grant.StorageID)
	if err != nil || !storage.Dirty || storage.Checkpoint != nil || storage.WriterSessionID != idle.ID ||
		f.Session.State != workspace.SessionIdle || !f.Session.IdleDeadline.Equal(idle.IdleDeadline) {
		t.Fatal("late delivery cleaned active app storage or renewed compute", err)
	}
}

func TestNativeSelectionReadCannotCrossTaskDuringDirectoryReplacement(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owned, displaced, foreign := filepath.Join(base, "workdir"), filepath.Join(base, "displaced"), filepath.Join(base, "another-task")
	for _, directory := range []string{owned, foreign} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(owned, "config"), []byte("owned task configuration"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "config"), []byte("another task private configuration"), 0600); err != nil {
		t.Fatal(err)
	}
	files, known := nativeSelectorFiles(owned, []string{"config"})
	if !known || string(files["config"]) != "owned task configuration" {
		t.Fatal("owned metadata could not enter native selection")
	}
	stop := make(chan struct{})
	var writers sync.WaitGroup
	writers.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := os.Rename(owned, displaced); err != nil {
				t.Error(err)
				return
			}
			if err := os.Symlink(foreign, owned); err != nil {
				t.Error(err)
				return
			}
			if err := os.Remove(owned); err != nil {
				t.Error(err)
				return
			}
			if err := os.Rename(displaced, owned); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for range 128 {
		files, _ := nativeSelectorFiles(owned, []string{"config"})
		if raw := files["config"]; len(raw) != 0 && string(raw) != "owned task configuration" {
			t.Error("a substituted live workdir exposed another task's metadata")
			break
		}
	}
	close(stop)
	writers.Wait()
}
