package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestPodCreationInterruptionRecoversExecution(t *testing.T) {
	for _, fault := range []string{"preflight", "before_commit", "lost_response"} {
		for _, legacy := range []bool{false, true} {
			name := fault
			if legacy {
				name += "_legacy"
			}
			t.Run(name, func(t *testing.T) {
				c, g, _, options, _ := controllerFixtureBeforeProvision(t)
				api := c.Kube.API.(*fake.Clientset)
				var interrupted bool
				var committed *corev1.Pod
				if fault == "preflight" {
					api.PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
						if interrupted || action.(clienttesting.GetAction).GetName() != c.Owner.Name {
							return false, nil, nil
						}
						pending, err := c.Store.Get(g.AttemptID)
						if err != nil {
							return true, nil, err
						}
						r, err := record(pending)
						if err != nil || !r.PodCreateRequested {
							return false, nil, err
						}
						interrupted = true
						return true, nil, context.DeadlineExceeded
					})
				}
				api.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if interrupted {
						return false, nil, nil
					}
					interrupted = true
					if fault == "before_commit" {
						return true, nil, context.DeadlineExceeded
					}
					committed = action.(clienttesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
					committed.UID = types.UID(uuid.NewString())
					if err := api.Tracker().Create(action.GetResource(), committed, action.GetNamespace()); err != nil {
						return true, nil, err
					}
					return true, nil, io.ErrUnexpectedEOF
				})
				if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
					t.Fatal(err)
				}
				if !interrupted {
					t.Fatal("fixture did not interrupt worker creation")
				}
				if legacy {
					legacyPodJournal(t, c, options, g.AttemptID)
				}
				reopenStopRegressionStore(t, c, options)
				if !legacy || committed != nil {
					c.Policy.NodeSelector = map[string]string{"workload-pool": "next"}
				}
				if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
					t.Fatal(err)
				}
				recovered, err := c.Store.Get(g.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				r, err := record(recovered)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.Kube.PodState(t.Context(), r.Reference); err != nil {
					t.Fatal("interrupted Pod creation prevented task execution", err)
				}
				if committed != nil && recovered.PodUID != string(committed.UID) {
					t.Fatal("recovery replaced the original writer")
				}
				admitStopRegressionWorker(t, c, recovered)
				if _, offered, err := c.Store.Offer(g.AttemptID); err != nil || offered {
					t.Fatal("recovery issued another execution", err)
				}
			})
		}
	}
}

func TestIndependentAssignedTasksRecoverWorkerCapacity(t *testing.T) {
	c, first, _, _, _ := controllerFixtureBeforeProvision(t)
	grants := []workspace.TaskGrant{first}
	for range 5 {
		var envelope map[string]any
		if err := json.Unmarshal(first.Envelope, &envelope); err != nil {
			t.Fatal(err)
		}
		id := uuid.NewString()
		envelope["id"], envelope["issue_id"] = id, uuid.NewString()
		raw, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		g, err := c.Store.Create(workspace.TaskGrant{TaskID: id, RuntimeID: first.RuntimeID, WorkspaceID: first.WorkspaceID, AgentID: first.AgentID, RuntimeRef: first.RuntimeRef, Envelope: raw})
		if err != nil {
			t.Fatal(err)
		}
		prepared := *first.Prepared
		prepared.TaskID, prepared.AttemptID = g.TaskID, g.AttemptID
		prepared.TaskRoot, prepared.Generation = g.TaskRoot, g.Generation
		prepared.Environment.RootDir = g.TaskRoot
		prepared.Environment.WorkDir = g.TaskRoot + "/workdir"
		prepared.Environment.MulticaConfigRoot = g.TaskRoot + "/multica-config"
		prepared.Environment.CodexHome = g.TaskRoot + "/codex-home"
		prepared.Digest = ""
		raw, err = json.Marshal(prepared)
		if err != nil {
			t.Fatal(err)
		}
		prepared.Digest = core.Digest(raw)
		if err := c.Store.BeginPreparation(g.AttemptID, *first.PreparationProcess); err != nil {
			t.Fatal(err)
		}
		if err := c.Store.CompletePreparation(g.AttemptID, &prepared, json.RawMessage(`{}`)); err != nil {
			t.Fatal(err)
		}
		grants = append(grants, g)
	}
	assignments := make([]map[string]string, 0, len(grants))
	for _, g := range grants {
		assignments = append(assignments, map[string]string{"id": g.TaskID, "agent_id": g.AgentID, "workspace_id": g.WorkspaceID, "runtime_id": g.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": "dispatched"})
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agents/"+first.AgentID+"/tasks" {
			_ = json.NewEncoder(w).Encode(assignments)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer backend.Close()
	var err error
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	c.Capacity = len(grants)
	api := c.Kube.API.(*fake.Clientset)
	interrupted := make(map[string]bool)
	api.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		// The fake API serializes its reactors, like the name-keyed create
		// decision. Each independent task loses its first creation request.
		name := action.(clienttesting.CreateAction).GetObject().(*corev1.Pod).Name
		if !interrupted[name] {
			interrupted[name] = true
			return true, nil, context.DeadlineExceeded
		}
		return false, nil, nil
	})
	for range 2 {
		var workers sync.WaitGroup
		failures := make(chan error, len(grants))
		for _, g := range grants {
			workers.Go(func() { failures <- c.reconcileAttempt(t.Context(), g.AttemptID) })
		}
		workers.Wait()
		close(failures)
		for err := range failures {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, g := range grants {
		recovered, err := c.Store.Get(g.AttemptID)
		if err != nil {
			t.Fatal(err)
		}
		r, err := record(recovered)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Kube.PodState(t.Context(), r.Reference); err != nil {
			t.Fatal("assigned task could not use its execution slot after a transient create failure", err)
		}
		admitStopRegressionWorker(t, c, recovered)
	}
}

func TestTimedOutPodCreationReleasesCapacityWithoutExecution(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "persisted_request"
		if legacy {
			name = "legacy_request"
		}
		t.Run(name, func(t *testing.T) {
			c, g, _, options, _ := controllerFixtureBeforeProvision(t)
			api := c.Kube.API.(*fake.Clientset)
			var interrupted bool
			api.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
				if !interrupted {
					interrupted = true
					return true, nil, context.DeadlineExceeded
				}
				return false, nil, nil
			})
			if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if !interrupted {
				t.Fatal("fixture did not interrupt Pod creation")
			}
			if err := c.recordFailure(g, context.DeadlineExceeded); err != nil {
				t.Fatal(err)
			}
			if legacy {
				legacyPodJournal(t, c, options, g.AttemptID)
			}
			reopenStopRegressionStore(t, c, options)
			if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
				t.Fatal("failed creation could not resolve its originally issued worker", err)
			}
			if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
				t.Fatal(err)
			}
			closed, err := c.Store.Get(g.AttemptID)
			if err != nil || closed.State != "closed" {
				t.Fatal("failed Pod creation permanently retained execution capacity", err)
			}
			if closed.StartConfirmed {
				t.Fatal("cleanup recovery started a failed task")
			}
			if _, offered, err := c.Store.Offer(g.AttemptID); offered {
				t.Fatal("failed creation recovery restored provider execution", err)
			}
		})
	}
}

// Reproduce schema 12, before the complete creation body was journaled.
func legacyPodJournal(t *testing.T, c *Controller, options workspace.Options, attempt string) {
	t.Helper()
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(options.Directory, "journal.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var journal map[string]json.RawMessage
	var grants map[string]map[string]json.RawMessage
	var resources map[string]json.RawMessage
	if err := json.Unmarshal(raw, &journal); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(journal["grants"], &grants); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(grants[attempt]["resources"], &resources); err != nil {
		t.Fatal(err)
	}
	delete(resources, "podRequest")
	grants[attempt]["resources"], _ = json.Marshal(resources)
	journal["grants"], _ = json.Marshal(grants)
	journal["schemaVersion"] = json.RawMessage(`12`)
	raw, err = json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}
