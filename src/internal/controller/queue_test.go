package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAttemptHintsPreserveConcurrentProgressAndCancellation(t *testing.T) {
	c, first, _, options, _ := controllerFixtureBeforeProvision(t)
	var envelope map[string]any
	if err := json.Unmarshal(first.Envelope, &envelope); err != nil {
		t.Fatal(err)
	}
	secondID := uuid.NewString()
	envelope["id"] = secondID
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Store.Create(workspace.TaskGrant{TaskID: secondID, RuntimeID: first.RuntimeID, WorkspaceID: first.WorkspaceID, AgentID: first.AgentID, RuntimeRef: first.RuntimeRef, Envelope: raw})
	if err != nil {
		t.Fatal(err)
	}
	completeQueuePreparation(t, c, second, first)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leaseEntered, releaseLease := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseLease) }) }
	tasks := []map[string]string{}
	for _, task := range []workspace.TaskGrant{first, second} {
		tasks = append(tasks, map[string]string{"id": task.TaskID, "agent_id": task.AgentID, "workspace_id": task.WorkspaceID, "runtime_id": task.RuntimeID, "dispatched_at": "2026-09-12T00:00:00Z", "status": "dispatched"})
	}
	assignmentPath := "/api/agents/" + first.AgentID + "/tasks"
	leasePath := "/api/daemon/runtimes/" + first.RuntimeID + "/tasks/" + first.TaskID + "/prepare-lease"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == assignmentPath {
			_ = json.NewEncoder(w).Encode(tasks)
			return
		}
		if r.URL.Path == leasePath {
			enterOnce.Do(func() { close(leaseEntered) })
			select {
			case <-releaseLease:
			case <-r.Context().Done():
				return
			}
		}
		_ = json.NewEncoder(w).Encode(struct{}{})
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	q := newDispatchQueues()
	c.dispatch = q
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() { c.runQueue(ctx, q.attempts, c.reconcileAttempt) })
	}
	defer func() { release(); cancel(); q.attempts.ShutDown(); q.deliveries.ShutDown(); workers.Wait() }()
	c.enqueueAttempt(first.AttemptID)
	select {
	case <-leaseEntered:
	case <-ctx.Done():
		t.Fatal("first task never entered preparation", ctx.Err())
	}
	// Duplicate hints arrive while the first attempt is blocked remotely.
	c.enqueueAttempt(first.AttemptID)
	c.enqueueAttempt(first.AttemptID)
	c.enqueueAttempt(second.AttemptID)
	for {
		second, err = c.Store.Get(second.AttemptID)
		if err != nil {
			t.Fatal(err)
		}
		if second.PodUID != "" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("blocked preparation prevented an independent task from obtaining its worker", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if _, err := c.requestStop(first, "backend_cancelled"); err != nil {
		t.Fatal(err)
	}
	c.enqueueAttempt(first.AttemptID)
	release()
	q.attempts.ShutDown()
	workers.Wait()
	q.deliveries.ShutDown()
	c.runQueue(ctx, q.deliveries, c.reconcileDelivery)
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	first, err = c.Store.Get(first.AttemptID)
	if err != nil || first.State != "closed" || first.Stop == nil {
		t.Fatal("an in-flight hint lost durable cancellation cleanup", err)
	}
	if _, offered, err := c.Store.Offer(first.AttemptID); err == nil && offered {
		t.Fatal("cancelled attempt regained execution authority")
	}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, first.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("cancelled preparation created a worker", err)
	}
	second, err = c.Store.Get(second.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, second.PodName, metav1.GetOptions{})
	if err != nil || string(pod.UID) != second.PodUID || second.Stop != nil {
		t.Fatal("duplicate hints or another task's cancellation lost the admitted worker", err)
	}
}

func TestPendingCompletionCannotOccupyTaskStartup(t *testing.T) {
	c, old, _, options, _ := controllerFixture(t)
	key := admitStopRegressionWorker(t, c, old)
	command, err := c.receiveResult(old, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "preserved work"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Store.CloseCheckouts(old.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.Seal(old.AttemptID, nativeStopRegressionReceipt(old, command, key)); err != nil {
		t.Fatal(err)
	}
	finishTaskPod(t, c, old)
	var claim map[string]any
	if err := json.Unmarshal(old.Envelope, &claim); err != nil {
		t.Fatal(err)
	}
	nextID := uuid.NewString()
	oldIssueID, _ := claim["issue_id"].(string)
	nextIssueID := uuid.NewString()
	claim["id"], claim["issue_id"] = nextID, nextIssueID
	entered, releaseDelivery := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseDelivery) }) }
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/agents/" + old.AgentID + "/tasks":
			_ = json.NewEncoder(w).Encode([]map[string]string{
				{"id": old.TaskID, "agent_id": old.AgentID, "workspace_id": old.WorkspaceID, "runtime_id": old.RuntimeID, "issue_id": oldIssueID, "kind": "direct", "created_at": "2026-09-12T00:00:00Z", "dispatched_at": "2026-09-12T00:00:00Z", "status": "running"},
				{"id": nextID, "agent_id": old.AgentID, "workspace_id": old.WorkspaceID, "runtime_id": old.RuntimeID, "issue_id": nextIssueID, "kind": "direct", "created_at": "2026-09-12T00:00:00Z", "dispatched_at": "2026-09-12T00:00:00Z", "status": "dispatched"},
			})
			return
		case "/api/daemon/tasks/claim":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{claim}})
			return
		case "/api/daemon/tasks/" + old.TaskID + "/complete":
			enterOnce.Do(func() { close(entered) })
			select {
			case <-releaseDelivery:
			case <-r.Context().Done():
				return
			}
		}
		_ = json.NewEncoder(w).Encode(struct{}{})
	}))
	defer backend.Close()
	c.API, err = daemonapi.NewClient(backend.URL, "local-owner-token", c.RuntimeRef.Daemon.Version, backend.Client())
	if err != nil {
		t.Fatal(err)
	}
	c.Capacity = 1
	c.runtimes = map[string]daemonapi.Bootstrap{old.RuntimeID: {Workspace: daemonapi.Workspace{ID: old.WorkspaceID}, Runtime: daemonapi.Runtime{ID: old.RuntimeID, Provider: "codex"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q := newDispatchQueues()
	c.dispatch = q
	var active, deliveries sync.WaitGroup
	defer func() {
		release()
		cancel()
		q.attempts.ShutDown()
		q.deliveries.ShutDown()
		active.Wait()
		deliveries.Wait()
	}()
	// Closing proven stopped compute must finish before result delivery begins.
	if err := c.reconcileAttempt(ctx, old.AttemptID); err != nil {
		t.Fatal(err)
	}
	deliveries.Go(func() { c.runQueue(ctx, q.deliveries, c.reconcileDelivery) })
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("sealed completion did not reach the blocked backend", ctx.Err())
	}
	c.enqueueAttempt(old.AttemptID)
	c.enqueueAttempt(old.AttemptID)
	if err := c.claim(ctx); err != nil {
		t.Fatal("undelivered completion prevented capacity reuse", err)
	}
	grants, err := c.Store.List()
	if err != nil {
		t.Fatal(err)
	}
	var next workspace.TaskGrant
	for _, g := range grants {
		if g.TaskID == nextID {
			next = g
		}
	}
	if next.AttemptID == "" {
		t.Fatal("stopped compute remained charged against single-task capacity")
	}
	next, _ = reserveConversationFixtureTurn(t, c, next, false)
	prepareConversationFixtureTurn(t, c, next)
	// One consumer proves a closed Pod hint cannot occupy even one startup slot.
	active.Go(func() { c.runQueue(ctx, q.attempts, c.reconcileAttempt) })
	for {
		next, err = c.Store.Get(next.AttemptID)
		if err != nil {
			t.Fatal(err)
		}
		if next.PodUID != "" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("pending completion or its Pod hints blocked another task's worker", ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(ctx, next.PodName, metav1.GetOptions{})
	if err != nil || string(pod.UID) != next.PodUID {
		t.Fatal("following task has no worker matching its durable authority", err)
	}
	release()
	q.attempts.ShutDown()
	active.Wait()
	q.deliveries.ShutDown()
	deliveries.Wait()
	reopenStopRegressionStore(t, c, options)
	terminal, err := c.Store.Terminal(old.AttemptID)
	if err != nil || terminal.State != "delivered" {
		t.Fatal("independent startup discarded the pending completion", err)
	}
	next, err = c.Store.Get(next.AttemptID)
	if err != nil || next.PodUID != string(pod.UID) {
		t.Fatal("completion delivery lost the following task's worker binding", err)
	}
}

func completeQueuePreparation(t *testing.T, c *Controller, g, template workspace.TaskGrant) {
	t.Helper()
	prepared := *template.Prepared
	prepared.TaskID, prepared.AttemptID, prepared.Generation, prepared.TaskRoot = g.TaskID, g.AttemptID, g.Generation, g.TaskRoot
	prepared.Environment = workspace.NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}
	prepared.Digest = ""
	raw, err := json.Marshal(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared.Digest = core.Digest(raw)
	if err := c.Store.BeginPreparation(g.AttemptID, *template.PreparationProcess); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.CompletePreparation(g.AttemptID, &prepared, template.Execution); err != nil {
		t.Fatal(err)
	}
}
