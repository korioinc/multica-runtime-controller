package controller

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/repocache"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

type checkoutOperation struct {
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	process         *workspace.PreparationProcess
	writersUnproven bool
}

// Repository selection remains task-scoped even when its object cache is shared.
func selectedCheckout(g workspace.TaskGrant, input wire.CheckoutRequest, authorization string) (workspace.Repository, error) {
	if g.CheckoutClosed || g.Stop != nil || g.ExecutionRevoked || g.Prepared == nil ||
		(g.State != "ready" && g.State != "offered" && g.State != "starting" && g.State != "started") ||
		input.Fresh || (input.CheckoutMode != "" && input.CheckoutMode != "isolated") ||
		input.TaskID != g.TaskID || input.WorkspaceID != g.WorkspaceID || input.WorkDir != g.Prepared.Environment.WorkDir {
		return workspace.Repository{}, workspace.ErrUnauthorized
	}
	claim, err := daemonapi.ParseClaim(g.Envelope)
	if err != nil || subtle.ConstantTimeCompare([]byte(authorization), []byte("Bearer "+claim.AuthToken)) != 1 {
		return workspace.Repository{}, workspace.ErrUnauthorized
	}
	for _, repository := range g.Repositories {
		if input.URL == repository.URL && (input.Ref == "" || input.Ref == repository.Ref) {
			return repository, nil
		}
	}
	return workspace.Repository{}, workspace.ErrUnauthorized
}

func (c *Controller) repositoryAccess(ctx context.Context, remote string) (githubapp.Access, error) {
	if c.App != nil {
		if repository, err := githubapp.ParseRepositoryURL(remote); err == nil {
			return c.App.RepositoryAccess(ctx, repository)
		}
	}
	return githubapp.Access{Identity: "anonymous"}, nil
}

// beginCheckout orders registration against both terminal publication and stop.
// Only the short authorization/registration step holds the execution gate.
func (c *Controller) beginCheckout(ctx context.Context, token, attemptID string, input wire.CheckoutRequest, authorization string) (*checkoutOperation, workspace.TaskGrant, workspace.Repository, error) {
	gate := c.Store.ExecutionGate(attemptID)
	gate.RLock()
	defer gate.RUnlock()
	c.checkoutMu.Lock()
	defer c.checkoutMu.Unlock()
	g, expiry, err := c.Store.AuthorizeWithExpiry(token, "supervisor")
	if err != nil || g.AttemptID != attemptID || c.checkoutClosed || ctx.Err() != nil {
		return nil, workspace.TaskGrant{}, workspace.Repository{}, workspace.ErrUnauthorized
	}
	repository, err := selectedCheckout(g, input, authorization)
	if err != nil {
		return nil, workspace.TaskGrant{}, workspace.Repository{}, err
	}
	if c.checkoutOps[attemptID] != nil {
		return nil, workspace.TaskGrant{}, workspace.Repository{}, errors.New("repository checkout is already in progress")
	}
	deadline := time.Now().Add(wire.CheckoutTimeout)
	if expiry.Before(deadline) {
		deadline = expiry
	}
	op := &checkoutOperation{done: make(chan struct{})}
	op.ctx, op.cancel = context.WithDeadline(ctx, deadline)
	if c.checkoutOps == nil {
		c.checkoutOps = make(map[string]*checkoutOperation)
	}
	c.checkoutOps[attemptID] = op
	c.checkoutWG.Add(1)
	return op, g, repository, nil
}

func (c *Controller) finishCheckout(attemptID string, op *checkoutOperation) {
	op.cancel()
	c.checkoutMu.Lock()
	if op.process != nil && !op.writersUnproven {
		if err := c.Store.CompleteCheckout(attemptID, *op.process); err != nil {
			slog.Warn("checkout writer completion could not be recorded", "attempt", attemptID)
		}
	}
	delete(c.checkoutOps, attemptID)
	close(op.done)
	c.checkoutMu.Unlock()
	c.checkoutWG.Done()
}

func (c *Controller) cancelCheckoutsLocked(attemptID string) {
	if op := c.checkoutOps[attemptID]; op != nil {
		op.cancel()
	}
}

func (c *Controller) cancelCheckouts(attemptID string) {
	c.checkoutMu.Lock()
	c.cancelCheckoutsLocked(attemptID)
	c.checkoutMu.Unlock()
}

func (c *Controller) closeCheckouts() {
	c.checkoutMu.Lock()
	c.checkoutClosed = true
	for _, op := range c.checkoutOps {
		op.cancel()
	}
	c.checkoutMu.Unlock()
	c.checkoutWG.Wait()
}

func (c *Controller) checkout(w http.ResponseWriter, r *http.Request, attemptID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	var input wire.CheckoutRequest
	if !ok || decodeBody(r, &input) != nil {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	authorization := r.Header.Get(wire.CheckoutTaskAuthorizationHeader)
	op, g, repository, err := c.beginCheckout(r.Context(), token, attemptID, input, authorization)
	if err != nil {
		http.Error(w, "repository checkout is unavailable", http.StatusForbidden)
		return
	}
	defer c.finishCheckout(attemptID, op)
	if c.admission(op.ctx, g) != nil {
		http.Error(w, "worker admission unavailable", http.StatusForbidden)
		return
	}
	access, err := c.repositoryAccess(op.ctx, repository.URL)
	if err != nil {
		http.Error(w, "repository access is unavailable", http.StatusForbidden)
		return
	}
	process, err := c.Kube.ControllerProcess(op.ctx, c.Owner)
	if err == nil {
		c.checkoutMu.Lock()
		err = c.Store.BeginCheckout(attemptID, process)
		if err == nil {
			op.process = &process
		}
		c.checkoutMu.Unlock()
	}
	if err != nil {
		http.Error(w, "checkout writer authority unavailable", http.StatusServiceUnavailable)
		return
	}
	path, retained, err := checkout.Retain(op.ctx, g.TaskRoot, input.WorkDir, repository.URL)
	if errors.Is(err, checkout.ErrCheckoutWritersUnproven) {
		op.writersUnproven = true
	}
	if err != nil {
		http.Error(w, "existing repository checkout is unsafe or incomplete", http.StatusConflict)
		return
	}
	result := wire.CheckoutResult{Path: path}
	if retained {
		result.Kept = "local_work"
	} else {
		if c.Cache == nil {
			http.Error(w, "repository cache unavailable", http.StatusServiceUnavailable)
			return
		}
		baseline, err := c.Cache.Snapshot(op.ctx, repocache.Request{WorkspaceID: g.WorkspaceID, URL: repository.URL, Ref: repository.Ref, Authority: access.Identity, Token: access.Token})
		if err != nil {
			slog.Warn("repository cache preparation failed", "task", g.TaskID, "attempt", attemptID)
			http.Error(w, "repository cache preparation failed", http.StatusBadGateway)
			return
		}
		defer baseline.Close()
		// Refresh can outlive its authorization observation. No cached task
		// files are exposed until repository identity and admission agree again.
		current, err := c.repositoryAccess(op.ctx, repository.URL)
		if err != nil || current.Identity != access.Identity || c.admission(op.ctx, g) != nil {
			http.Error(w, "repository authority changed", http.StatusForbidden)
			return
		}
		if err := c.authorizeCheckout(op, token, input, authorization); err != nil {
			http.Error(w, "task repository access has ended", http.StatusForbidden)
			return
		}
		ctx := op.ctx
		if current.Token.Value != "" {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, current.Token.ExpiresAt)
			defer cancel()
		}
		result.Path, err = checkout.Copy(ctx, g.TaskRoot, input.WorkDir, repository.URL, baseline.Root)
		if errors.Is(err, checkout.ErrCheckoutWritersUnproven) {
			op.writersUnproven = true
		}
		if err != nil {
			http.Error(w, "repository checkout publication failed", http.StatusConflict)
			return
		}
	}
	if err := c.authorizeCheckout(op, token, input, authorization); err != nil {
		http.Error(w, "task repository access has ended", http.StatusForbidden)
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second))
	writeJSON(w, result)
}

func (c *Controller) authorizeCheckout(op *checkoutOperation, token string, input wire.CheckoutRequest, authorization string) error {
	c.checkoutMu.Lock()
	defer c.checkoutMu.Unlock()
	if err := op.ctx.Err(); err != nil {
		return err
	}
	g, _, err := c.Store.AuthorizeWithExpiry(token, "supervisor")
	if err != nil {
		return err
	}
	_, err = selectedCheckout(g, input, authorization)
	return err
}

// checkoutStop is a storage fence, including for early or quarantined workers.
// It uses the stop credential, which never grants repository or task access.
func (c *Controller) checkoutStop(w http.ResponseWriter, r *http.Request, attemptID string) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || r.Method != http.MethodPost {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	g, err := c.Store.AuthorizeStop(token)
	if errors.Is(err, workspace.ErrPodBindingPending) && g.AttemptID == attemptID {
		http.Error(w, "worker binding observation pending", http.StatusServiceUnavailable)
		return
	}
	if err != nil || g.AttemptID != attemptID {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	rec, err := record(g)
	if err != nil {
		http.Error(w, "worker identity unavailable", http.StatusServiceUnavailable)
		return
	}
	rec.Reference.PodUID, rec.Reference.PVCUID = g.PodUID, g.PVCUID
	if _, err := c.Kube.AuthorizeStop(r.Context(), rec.Reference); err != nil {
		http.Error(w, "worker admission unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := c.stopCheckoutWrites(r.Context(), attemptID); err != nil {
		http.Error(w, "checkout writers have not stopped", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, map[string]bool{"accepted": true})
}

func (c *Controller) stopCheckoutWrites(ctx context.Context, attemptID string) error {
	if err := c.stopCheckoutWriters(ctx, attemptID); err != nil {
		return err
	}
	g, err := c.Store.Get(attemptID)
	if err != nil || !g.CheckoutNeedsFlush {
		return err
	}
	if err := workspace.SyncTaskFilesystem(g.TaskRoot); err != nil {
		return err
	}
	return c.Store.CheckoutsFlushed(attemptID)
}

func (c *Controller) stopCheckoutWriters(ctx context.Context, attemptID string) error {
	c.checkoutMu.Lock()
	err := c.Store.CloseCheckouts(attemptID)
	op := c.checkoutOps[attemptID]
	if op != nil {
		op.cancel()
	}
	c.checkoutMu.Unlock()
	if err != nil {
		return err
	}
	if op != nil {
		select {
		case <-op.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.recoverCheckoutWriter(ctx, attemptID)
}

// An empty in-memory operation map cannot certify a writer after restart.
func (c *Controller) recoverCheckoutWriter(ctx context.Context, attemptID string) error {
	c.checkoutMu.Lock()
	active := c.checkoutOps[attemptID] != nil
	g, err := c.Store.Get(attemptID)
	c.checkoutMu.Unlock()
	if err != nil || active || g.CheckoutProcess == nil {
		return err
	}
	stopped, err := c.Kube.PreparationStopped(ctx, *g.CheckoutProcess)
	if err != nil {
		return err
	}
	if !stopped {
		return checkout.ErrCheckoutWritersUnproven
	}
	c.checkoutMu.Lock()
	defer c.checkoutMu.Unlock()
	return c.Store.CompleteCheckout(attemptID, *g.CheckoutProcess)
}
