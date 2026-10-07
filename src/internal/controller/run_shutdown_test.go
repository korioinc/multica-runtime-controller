package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestControllerRunCancellationJoinsOwnedWork(t *testing.T) {
	for _, held := range []string{"queue", "checkout", "preparation"} {
		t.Run(held, func(t *testing.T) {
			repository := workspace.Repository{URL: "https://example.invalid/authorized.git"}
			c, grant, _, _, assignment := controllerFixture(t, repository)
			admitStopRegressionWorker(t, c, grant)
			token, err := c.Store.CapabilityToken(grant.AttemptID, "supervisor")
			if err != nil {
				t.Fatal(err)
			}

			synctest.Test(t, func(t *testing.T) {
				// Keep API responses in-process so each shutdown barrier is observable
				// without real network scheduling or elapsed-time assertions.
				transport := runShutdownTransport{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/api/daemon/workspaces":
						_ = json.NewEncoder(w).Encode([]daemonapi.Workspace{})
					case "/api/agents/" + grant.AgentID + "/tasks":
						current := assignment.Load()
						_ = json.NewEncoder(w).Encode([]map[string]string{{
							"id": grant.TaskID, "agent_id": grant.AgentID, "workspace_id": grant.WorkspaceID,
							"runtime_id": current.RuntimeID, "dispatched_at": current.DispatchedAt, "status": current.Status,
						}})
					default:
						http.NotFound(w, r)
					}
				})}
				c.API, err = daemonapi.NewClient("http://controller-shutdown.invalid", "local-owner-token", c.RuntimeRef.Daemon.Version, &http.Client{Transport: transport})
				if err != nil {
					t.Fatal(err)
				}
				c.Capacity = 1
				c.PollInterval, c.HeartbeatInterval = time.Hour, time.Hour
				input := wire.CheckoutRequest{URL: repository.URL, TaskID: grant.TaskID, WorkspaceID: grant.WorkspaceID, WorkDir: grant.Prepared.Environment.WorkDir}
				operation, _, _, err := c.beginCheckout(context.Background(), token, grant.AttemptID, input, "Bearer mat_controller_fixture")
				if err != nil {
					t.Fatal(err)
				}
				finishCheckout := sync.OnceFunc(func() { c.finishCheckout(grant.AttemptID, operation) })
				defer finishCheckout()

				queueEntered, releaseQueue := make(chan struct{}), make(chan struct{})
				queueReady := sync.OnceFunc(func() { close(queueEntered) })
				releaseReconciliation := sync.OnceFunc(func() { close(releaseQueue) })
				c.Kube.API.(*fake.Clientset).PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if action.(clienttesting.GetAction).GetName() != grant.PodName {
						return false, nil, nil
					}
					queueReady()
					<-releaseQueue
					return true, nil, context.Canceled
				})

				preparationCanceled, preparationReleased := make(chan struct{}), make(chan struct{})
				releasePreparation := sync.OnceFunc(func() { close(preparationReleased) })
				// Register owned preparation work at its existing join boundary. The
				// barrier models cleanup that must finish after cancellation is observed.
				c.prepareWG.Go(func() {
					select {
					case <-queueEntered:
						<-c.prepareContext.Done()
						close(preparationCanceled)
					case <-preparationReleased:
						return
					}
					<-preparationReleased
				})
				ctx, cancel := context.WithCancel(context.Background())
				runDone := make(chan struct{})
				var runErr error
				go func() { runErr = c.Run(ctx); close(runDone) }()
				defer func() {
					cancel()
					releaseReconciliation()
					finishCheckout()
					releasePreparation()
					<-runDone
				}()
				select {
				case <-queueEntered:
				case <-runDone:
					t.Fatal("controller did not start its reconciliation owner", runErr)
				}
				cancel()
				<-preparationCanceled
				// Release every other owner so each subcase detects its own missing
				// join without depending on the relative order of cancellation.
				if held != "queue" {
					releaseReconciliation()
				}
				if held != "checkout" {
					finishCheckout()
				}
				if held != "preparation" {
					releasePreparation()
				}
				synctest.Wait()
				assertControllerStillJoining(t, runDone)
				if held == "checkout" {
					if !errors.Is(operation.ctx.Err(), context.Canceled) {
						t.Fatal("controller did not cancel its outstanding checkout")
					}
					select {
					case <-operation.done:
						t.Fatal("checkout cancellation was treated as completed writer work")
					default:
					}
				}
				releaseReconciliation()
				finishCheckout()
				releasePreparation()
				<-runDone
				if !errors.Is(runErr, context.Canceled) {
					t.Fatal("controller did not finish its requested shutdown", runErr)
				}
				if _, _, err := c.Store.AuthorizeWithExpiry(token, "supervisor"); err != nil {
					t.Fatal("fixture lost task authority before testing the closed writer gate", err)
				}
				if next, _, _, err := c.beginCheckout(context.Background(), token, grant.AttemptID, input, "Bearer mat_controller_fixture"); !errors.Is(err, workspace.ErrUnauthorized) {
					if next != nil {
						c.finishCheckout(grant.AttemptID, next)
					}
					t.Fatal("stopped controller admitted another repository writer", err)
				}
			})
		})
	}
}

func assertControllerStillJoining(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("controller returned while owned work remained")
	default:
	}
}

type runShutdownTransport struct{ handler http.Handler }

func (transport runShutdownTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response := httptest.NewRecorder()
	transport.handler.ServeHTTP(response, request)
	return response.Result(), nil
}
