package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCheckoutSelectionCannotExpandTaskAuthority(t *testing.T) {
	repository := workspace.Repository{URL: "https://example.invalid/authorized.git", Ref: "main"}
	c, g, _, _, _ := controllerFixture(t, repository)
	admitStopRegressionWorker(t, c, g)
	g, err := c.Store.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	input := wire.CheckoutRequest{URL: repository.URL, Ref: repository.Ref, TaskID: g.TaskID, WorkspaceID: g.WorkspaceID, WorkDir: g.Prepared.Environment.WorkDir}
	credential := "Bearer mat_controller_fixture"
	if _, err := selectedCheckout(g, input, credential); err != nil {
		t.Fatal("fixture did not grant repository access", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*workspace.TaskGrant, *wire.CheckoutRequest, *string)
	}{
		{"another task", func(_ *workspace.TaskGrant, r *wire.CheckoutRequest, _ *string) { r.TaskID = "another-task" }},
		{"another workspace", func(_ *workspace.TaskGrant, r *wire.CheckoutRequest, _ *string) { r.WorkspaceID = "another-workspace" }},
		{"another workdir", func(_ *workspace.TaskGrant, r *wire.CheckoutRequest, _ *string) { r.WorkDir += "/../sibling" }},
		{"another repository", func(_ *workspace.TaskGrant, r *wire.CheckoutRequest, _ *string) {
			r.URL = "https://example.invalid/other.git"
		}},
		{"another ref", func(_ *workspace.TaskGrant, r *wire.CheckoutRequest, _ *string) { r.Ref = "secret-branch" }},
		{"destructive reset", func(_ *workspace.TaskGrant, r *wire.CheckoutRequest, _ *string) { r.Fresh = true }},
		{"forged task credential", func(_ *workspace.TaskGrant, _ *wire.CheckoutRequest, a *string) { *a = "Bearer someone-else" }},
		{"revoked execution", func(g *workspace.TaskGrant, _ *wire.CheckoutRequest, _ *string) { g.ExecutionRevoked = true }},
		{"terminal execution", func(g *workspace.TaskGrant, _ *wire.CheckoutRequest, _ *string) { g.State = "terminal_received" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			grant, request, authorization := g, input, credential
			tc.change(&grant, &request, &authorization)
			if _, err := selectedCheckout(grant, request, authorization); !errors.Is(err, workspace.ErrUnauthorized) {
				t.Fatal("checkout granted access outside the current task authority", err)
			}
		})
	}
}

func TestCheckoutFenceWaitsForWriterAndSurvivesReopen(t *testing.T) {
	repository := workspace.Repository{URL: "https://example.invalid/authorized.git"}
	c, g, _, options, _ := controllerFixture(t, repository)
	admitStopRegressionWorker(t, c, g)
	token, err := c.Store.CapabilityToken(g.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	input := wire.CheckoutRequest{URL: repository.URL, TaskID: g.TaskID, WorkspaceID: g.WorkspaceID, WorkDir: g.Prepared.Environment.WorkDir}
	op, _, _, err := c.beginCheckout(context.Background(), token, g.AttemptID, input, "Bearer mat_controller_fixture")
	if err != nil {
		t.Fatal(err)
	}
	process := workspace.PreparationProcess{PodName: c.Owner.Name, PodUID: c.Owner.UID, ContainerID: "containerd://checkout"}
	if err := c.Store.BeginCheckout(g.AttemptID, process); err != nil {
		t.Fatal(err)
	}
	op.process = &process
	ctx, cancel := context.WithCancel(context.Background())
	fenced := make(chan error, 1)
	go func() { fenced <- c.stopCheckoutWrites(ctx, g.AttemptID) }()
	<-op.ctx.Done()
	// Cancellation is observable while the writer still owns its reservation.
	// An interrupted barrier must not certify that its task can be flushed.
	cancel()
	if err := <-fenced; !errors.Is(err, context.Canceled) {
		t.Fatal("checkout fence acknowledged a writer that had not finished", err)
	}
	if err := c.Store.BeginCheckout(g.AttemptID, process); err == nil {
		t.Fatal("closed checkout authority admitted another task writer")
	}
	c.finishCheckout(g.AttemptID, op)
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	current, err := c.Store.Get(g.AttemptID)
	if err != nil || !current.CheckoutNeedsFlush {
		t.Fatal("restart discarded the unfinished controller flush obligation", err)
	}
	// This fixture has no task writes. Supply the completed flush observation
	// through its journal owner; real syncfs is exercised in the Linux flow.
	if err := c.Store.CheckoutsFlushed(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := c.stopCheckoutWrites(context.Background(), g.AttemptID); err != nil {
		t.Fatal("completed writer could not be fenced", err)
	}
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Store.BeginCheckout(g.AttemptID, process); err == nil {
		t.Fatal("controller restart reopened closed checkout authority")
	}
}

func TestRestartedCheckoutRequiresRecordedContainerTermination(t *testing.T) {
	c, g, _, options, _ := controllerFixture(t)
	admitStopRegressionWorker(t, c, g)
	process := workspace.PreparationProcess{PodName: c.Owner.Name, PodUID: c.Owner.UID, ContainerID: "containerd://old-checkout"}
	if err := c.Store.BeginCheckout(g.AttemptID, process); err != nil {
		t.Fatal(err)
	}
	if err := c.Store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	c.Store, err = workspace.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.stopCheckoutWrites(context.Background(), g.AttemptID); err == nil {
		t.Fatal("empty controller memory certified the previous task writer")
	}
	current, err := c.Store.Get(g.AttemptID)
	if err != nil || current.CheckoutProcess == nil {
		t.Fatal("restart discarded unresolved task writer authority", err)
	}
	pod, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).Get(context.Background(), c.Owner.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "controller", ContainerID: "containerd://replacement", RestartCount: 1,
		State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Now())}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: process.ContainerID, FinishedAt: metav1.Now(), Reason: "Error"}}}}
	if _, err := c.Kube.API.CoreV1().Pods(c.Kube.Namespace).UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.recoverCheckoutWriter(context.Background(), g.AttemptID); err != nil {
		t.Fatal("proven stopped controller still blocked the checkout fence", err)
	}
	current, err = c.Store.Get(g.AttemptID)
	if err != nil || current.CheckoutProcess != nil || !current.CheckoutNeedsFlush {
		t.Fatal("writer termination incorrectly discharged the separate flush obligation", err)
	}
}

func TestAttemptStopCancelsCheckoutAndRefusesNewWriters(t *testing.T) {
	repository := workspace.Repository{URL: "https://example.invalid/authorized.git"}
	c, g, _, _, _ := controllerFixture(t, repository)
	admitStopRegressionWorker(t, c, g)
	token, err := c.Store.CapabilityToken(g.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	input := wire.CheckoutRequest{URL: repository.URL, TaskID: g.TaskID, WorkspaceID: g.WorkspaceID, WorkDir: g.Prepared.Environment.WorkDir}
	op, _, _, err := c.beginCheckout(context.Background(), token, g.AttemptID, input, "Bearer mat_controller_fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer c.finishCheckout(g.AttemptID, op)
	if _, err := c.requestStop(g, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(op.ctx.Err(), context.Canceled) {
		t.Fatal("stopped attempt retained an authorized checkout writer")
	}
	if next, _, _, err := c.beginCheckout(context.Background(), token, g.AttemptID, input, "Bearer mat_controller_fixture"); err == nil {
		c.finishCheckout(g.AttemptID, next)
		t.Fatal("stop allowed a new repository writer")
	}
}

func TestTerminalCancelsCheckoutWithoutCertifyingItsWriter(t *testing.T) {
	repository := workspace.Repository{URL: "https://example.invalid/authorized.git"}
	c, g, _, _, _ := controllerFixture(t, repository)
	admitStopRegressionWorker(t, c, g)
	token, err := c.Store.CapabilityToken(g.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	input := wire.CheckoutRequest{URL: repository.URL, TaskID: g.TaskID, WorkspaceID: g.WorkspaceID, WorkDir: g.Prepared.Environment.WorkDir}
	op, _, _, err := c.beginCheckout(context.Background(), token, g.AttemptID, input, "Bearer mat_controller_fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer c.finishCheckout(g.AttemptID, op)
	// The gateway owns this gate during terminal publication. A registered
	// copy must not retain it while cancellation needs to be published.
	gate := c.Store.ExecutionGate(g.AttemptID)
	if !gate.TryLock() {
		t.Fatal("checkout writer prevented terminal publication")
	}
	_, err = c.receiveResult(g, wire.ProviderResult{Result: agent.Result{Status: "completed", Output: "task complete"}})
	gate.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(op.ctx.Err(), context.Canceled) {
		t.Fatal("terminal result did not revoke the outstanding repository writer")
	}
	if next, _, _, err := c.beginCheckout(context.Background(), token, g.AttemptID, input, "Bearer mat_controller_fixture"); err == nil {
		c.finishCheckout(g.AttemptID, next)
		t.Fatal("terminal supervisor capability authorized a fresh repository write")
	}
}

func TestControllerFailureStopsOutstandingRepositoryAccess(t *testing.T) {
	repository := workspace.Repository{URL: "https://example.invalid/authorized.git"}
	c, g, _, _, _ := controllerFixture(t, repository)
	admitStopRegressionWorker(t, c, g)
	token, err := c.Store.CapabilityToken(g.AttemptID, "supervisor")
	if err != nil {
		t.Fatal(err)
	}
	input := wire.CheckoutRequest{URL: repository.URL, TaskID: g.TaskID, WorkspaceID: g.WorkspaceID, WorkDir: g.Prepared.Environment.WorkDir}
	op, _, _, err := c.beginCheckout(context.Background(), token, g.AttemptID, input, "Bearer mat_controller_fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer c.finishCheckout(g.AttemptID, op)
	if err := c.recordFailure(g, errors.New("worker unavailable")); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(op.ctx.Err(), context.Canceled) {
		t.Fatal("quarantined attempt retained a live repository writer")
	}
	if next, _, _, err := c.beginCheckout(context.Background(), token, g.AttemptID, input, "Bearer mat_controller_fixture"); err == nil {
		c.finishCheckout(g.AttemptID, next)
		t.Fatal("failed attempt opened another repository writer")
	}
}
