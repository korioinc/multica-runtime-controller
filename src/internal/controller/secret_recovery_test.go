package controller

import (
	"context"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestSecretCreationInterruptionRecoversExecution(t *testing.T) {
	for _, failure := range []string{"expired_context", "before_commit", "lost_response", "conflict_disappeared"} {
		for _, restart := range []bool{false, true} {
			name := failure
			if restart {
				name += "_after_restart"
			}
			t.Run(name, func(t *testing.T) {
				c, g, _, options, _ := controllerFixtureBeforeProvision(t)
				api := c.Kube.API.(*fake.Clientset)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var interrupted bool
				var issued *corev1.Secret
				if failure == "expired_context" {
					api.PrependReactor("get", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
						if interrupted || action.(clienttesting.GetAction).GetName() != c.Owner.Name {
							return false, nil, nil
						}
						// Controller authority is checked before the Secret request.
						// End this reconciliation after creation intent is durable.
						pending, err := c.Store.Get(g.AttemptID)
						if err != nil {
							return true, nil, err
						}
						r, err := record(pending)
						if err != nil || !r.SecretCreateRequested {
							return false, nil, err
						}
						interrupted = true
						cancel()
						return true, nil, ctx.Err()
					})
				}
				api.PrependReactor("create", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
					if interrupted || failure == "expired_context" {
						return false, nil, nil
					}
					interrupted = true
					issued = action.(clienttesting.CreateAction).GetObject().(*corev1.Secret).DeepCopy()
					if failure == "before_commit" {
						return true, nil, context.DeadlineExceeded
					}
					issued.UID = types.UID(uuid.NewString())
					if err := api.Tracker().Create(action.GetResource(), issued, action.GetNamespace()); err != nil {
						return true, nil, err
					}
					if failure == "conflict_disappeared" {
						// A competing request wins creation, then its Secret is
						// deleted before the conflict can be resolved by lookup.
						if err := api.Tracker().Delete(action.GetResource(), action.GetNamespace(), issued.Name); err != nil {
							return true, nil, err
						}
						return true, nil, apierrors.NewAlreadyExists(action.GetResource().GroupResource(), issued.Name)
					}
					return true, nil, io.ErrUnexpectedEOF
				})
				if err := c.reconcileAttempt(ctx, g.AttemptID); err != nil {
					t.Fatal(err)
				}
				if !interrupted {
					t.Fatal("fixture did not interrupt bootstrap creation")
				}
				pending, err := c.Store.Get(g.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				original, err := wire.DecodeBootstrap(pending.Bootstrap)
				if err != nil {
					t.Fatal(err)
				}
				if restart {
					reopenStopRegressionStore(t, c, options)
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
					t.Fatal("interrupted Secret creation prevented worker recovery", err)
				}
				if failure == "lost_response" && r.Reference.SecretUID != string(issued.UID) {
					t.Fatal("lost response replaced the committed bootstrap authority")
				}
				admitStopRegressionWorker(t, c, recovered)
				if _, err := c.Store.Authorize(original.APICapability, "daemon"); err != nil {
					t.Fatal("recovery replaced the original task authority", err)
				}
				if _, offered, err := c.Store.Offer(g.AttemptID); err != nil || offered {
					t.Fatal("recovery allowed a second execution offer", err)
				}
			})
		}
	}
}

func TestCancellationDuringSecretRecoveryDoesNotIssueWorker(t *testing.T) {
	c, g, _, options, _ := controllerFixtureBeforeProvision(t)
	api := c.Kube.API.(*fake.Clientset)
	var interrupted bool
	api.PrependReactor("create", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if !interrupted {
			interrupted = true
			secret := action.(clienttesting.CreateAction).GetObject().(*corev1.Secret).DeepCopy()
			secret.UID = types.UID(uuid.NewString())
			if err := api.Tracker().Create(action.GetResource(), secret, action.GetNamespace()); err != nil {
				return true, nil, err
			}
			return true, nil, io.ErrUnexpectedEOF
		}
		return false, nil, nil
	})
	if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
		t.Fatal(err)
	}
	api.PrependReactor("get", "secrets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if _, err := c.requestStop(g, "cancelled"); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
		t.Fatal(err)
	}
	stopped, err := c.Store.Get(g.AttemptID)
	if err != nil || stopped.Stop == nil {
		t.Fatal("fixture did not cancel during Secret recovery", err)
	}
	if _, err := api.CoreV1().Pods(c.Kube.Namespace).Get(t.Context(), g.PodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("Secret recovery created a worker after cancellation", err)
	}
	reopenStopRegressionStore(t, c, options)
	if err := c.reconcileAttempt(t.Context(), g.AttemptID); err != nil {
		t.Fatal(err)
	}
	closed, err := c.Store.Get(g.AttemptID)
	if err != nil || closed.State != "closed" {
		t.Fatal("cancelled Secret recovery retained execution capacity", err)
	}
	if _, offered, err := c.Store.Offer(g.AttemptID); offered {
		t.Fatal("cancelled recovery acquired execution authority", err)
	}
}
