package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/daemonapi"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/githubauth"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	corev1 "k8s.io/api/core/v1"
)

func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	gateway := &daemonapi.Gateway{Store: c.Store, Client: c.API, Admission: c.admission}
	mux.Handle("/api", gateway)
	mux.Handle("/api/", gateway)
	// Account authentication never reaches the backend through a task gateway.
	// Route it here so callers receive the explicit controller policy denial.
	mux.Handle("/auth", gateway)
	mux.Handle("/auth/", gateway)
	mux.HandleFunc("/internal/attempts/", c.supervisor)
	mux.HandleFunc("/internal/worker-sessions/", c.workerSessionControl)
	mux.HandleFunc(githubauth.Route, c.scmToken)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

func (c *Controller) admission(ctx context.Context, g workspace.TaskGrant) error {
	_, err := c.admittedPod(ctx, g)
	return err
}

// A committed receipt can be acknowledged after the session moved to another
// turn. This route only compares retained proof; it grants no live capability.
func (c *Controller) acknowledgeTurnRetry(w http.ResponseWriter, r *http.Request, id, action string) bool {
	if r.Method != http.MethodPost || action != "turn-receipt" && action != "result-receipt" && action != "result" {
		return false
	}
	g, err := c.Store.Get(id)
	if err != nil || g.WorkerSessionID == "" {
		return false
	}
	terminal, err := c.Store.Terminal(id)
	if err != nil || action == "turn-receipt" && g.TurnReceipt == nil || action != "turn-receipt" && terminal.ResultReceipt == nil {
		return false
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	expected, err := c.Store.CapabilityToken(id, "supervisor")
	if !ok || err != nil || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
		http.Error(w, "denied", http.StatusForbidden)
		return true
	}
	match := false
	switch action {
	case "turn-receipt":
		var receipt workspace.TurnReceipt
		if decodeBody(r, &receipt) == nil {
			actual, _ := json.Marshal(receipt)
			stored, _ := json.Marshal(g.TurnReceipt)
			match = wire.Digest(actual) == wire.Digest(stored)
		}
	case "result-receipt":
		var receipt workspace.ResultReceipt
		if decodeBody(r, &receipt) == nil {
			actual, _ := json.Marshal(receipt)
			stored, _ := json.Marshal(terminal.ResultReceipt)
			match = wire.Digest(actual) == wire.Digest(stored)
		}
	case "result":
		var result wire.ProviderResult
		if decodeBody(r, &result) == nil {
			actual, _ := json.Marshal(result)
			match = wire.Digest(actual) == terminal.ResultDigest
		}
	}
	if !match {
		http.Error(w, "denied", http.StatusForbidden)
	} else if action == "result" {
		writeJSON(w, wire.SealCommand{RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce})
	} else {
		writeJSON(w, struct{}{})
	}
	return true
}

func (c *Controller) admittedPod(ctx context.Context, g workspace.TaskGrant) (*corev1.Pod, error) {
	r, err := record(g)
	if err != nil {
		return nil, err
	}
	bootstrapDigest := g.BootstrapDigest
	if g.WorkerSessionID != "" {
		session, err := c.Store.GetSession(g.WorkerSessionID)
		if err != nil || session.ActiveAttempt != g.AttemptID || session.TurnSequence != g.TurnSequence || session.PodUID != g.PodUID || session.Stop != nil {
			return nil, errors.New("worker session no longer owns this turn")
		}
		bootstrapDigest = session.BootstrapDigest
	}
	if r.Reference.PodUID != g.PodUID || r.Reference.PVCUID != g.PVCUID || r.Reference.RequestDigest != bootstrapDigest || g.PodUID == "" {
		return nil, errors.New("live attempt identity differs from grant")
	}
	r.Reference.Owner = c.Owner
	return c.Kube.Authorize(ctx, r.Reference)
}

func (c *Controller) supervisor(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) != 5 || parts[1] != "internal" || parts[2] != "attempts" || !wire.UUID(parts[3]) || r.URL.RawPath != "" || r.URL.RawQuery != "" {
		http.Error(w, "denied", 403)
		return
	}
	if strings.HasPrefix(parts[4], "stop-") {
		c.stopControl(w, r, parts[3], parts[4])
		return
	}
	if parts[4] == "checkout" {
		c.checkout(w, r, parts[3])
		return
	}
	if parts[4] == "checkout-stop" {
		c.checkoutStop(w, r, parts[3])
		return
	}
	if r.Method == http.MethodGet && parts[4] == "control" {
		c.waitForTerminal(w, r, parts[3])
		return
	}
	if c.acknowledgeTurnRetry(w, r, parts[3], parts[4]) {
		return
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		http.Error(w, "denied", 403)
		return
	}
	g, err := c.Store.Authorize(token, "supervisor")
	if err != nil || g.AttemptID != parts[3] {
		http.Error(w, "denied", 403)
		return
	}
	if g.Stop != nil && (parts[4] == "input" || parts[4] == "admit" || parts[4] == "start") {
		http.Error(w, "attempt stopped", http.StatusGone)
		return
	}
	var startErr error
	if r.Method == http.MethodPost && parts[4] == "start" {
		startErr = errors.New("execution start refused")
		finish := diagnostics.StartPhase("controller_execution_start", turnAttributes(g)...)
		defer func() { finish(startErr, "sinceClaim", time.Since(g.CreatedAt)) }()
	}
	var pod *corev1.Pod
	var admissionErr error
	legacyDraining := false
	if g.WorkerSessionID != "" {
		session, sessionErr := c.Store.GetSession(g.WorkerSessionID)
		legacyDraining = sessionErr == nil && session.ProtocolVersion == workspace.LegacySessionProtocolVersion && session.Stop != nil
	}
	if parts[4] == "result" || parts[4] == "result-receipt" || parts[4] == "receipt" || parts[4] == "turn-receipt" || parts[4] == "event" && legacyDraining {
		rec, err := record(g)
		admissionErr = err
		if err == nil {
			pod, admissionErr = c.Kube.AuthorizeStop(r.Context(), rec.Reference)
		}
	} else {
		pod, admissionErr = c.admittedPod(r.Context(), g)
	}
	if admissionErr != nil {
		http.Error(w, "admission observation pending", http.StatusServiceUnavailable)
		return
	}
	gate := c.Store.ExecutionGate(g.AttemptID)
	if parts[4] == "result" || parts[4] == "start" {
		gate.Lock()
		defer gate.Unlock()
	} else {
		gate.RLock()
		defer gate.RUnlock()
	}
	g, err = c.Store.Authorize(token, "supervisor")
	if err != nil {
		http.Error(w, "denied", 403)
		return
	}
	if g.Stop != nil && (parts[4] == "input" || parts[4] == "admit" || parts[4] == "start") {
		http.Error(w, "attempt stopped", http.StatusGone)
		return
	}
	var actionErr error
	switch {
	case r.Method == http.MethodGet && parts[4] == "input":
		if g.Prepared == nil || (g.State != "intent" && g.State != "ready" && !(g.WorkerSessionID != "" && g.State == "assigned" && g.TurnAccepted)) {
			actionErr = errors.New("prepared input unavailable")
			break
		}
		writeJSON(w, g.Execution)
		return
	case r.Method == http.MethodPost && parts[4] == "start":
		if !c.sameAssignment(r.Context(), g) {
			actionErr = errors.New("task assignment changed")
			break
		}
		if _, ok, err := c.Store.Offer(g.AttemptID); err != nil || !ok {
			actionErr = errors.New("execution permission already consumed")
			break
		}
		if err := c.Store.BeginStart(g.AttemptID); err != nil {
			actionErr = err
			break
		}
		var backendStart error
		if g.WorkerSessionID != "" {
			claim, err := daemonapi.ParseClaim(g.Envelope)
			if err != nil {
				backendStart = err
			} else if claim.StartClaimSupported {
				backendStart = c.API.StartClaim(r.Context(), claim)
			} else {
				backendStart = c.API.Start(r.Context(), g.TaskID)
			}
		} else {
			backendStart = c.API.Start(r.Context(), g.TaskID)
		}
		if backendStart != nil {
			actionErr = backendStart
			break
		}
		actionErr = c.Store.MarkStarted(g.AttemptID)
	case r.Method == http.MethodPost && parts[4] == "event":
		var input wire.ProviderEvent
		if err := decodeBody(r, &input); err != nil {
			actionErr = err
			break
		}
		actionErr = c.receiveEvent(r.Context(), g, input)
	case r.Method == http.MethodPost && parts[4] == "result":
		var input wire.ProviderResult
		if err := decodeBody(r, &input); err != nil {
			actionErr = err
			break
		}
		command, err := c.receiveResult(g, input)
		if err != nil {
			actionErr = err
			break
		}
		c.enqueueAttempt(g.AttemptID)
		writeJSON(w, command)
		return
	case r.Method == http.MethodPost && parts[4] == "result-receipt":
		var receipt workspace.ResultReceipt
		if err := decodeBody(r, &receipt); err != nil {
			actionErr = err
			break
		}
		actionErr = c.Store.RecordResultReceipt(g.AttemptID, receipt)
		if actionErr == nil {
			c.enqueueDelivery(g.AttemptID)
		}
	case r.Method == http.MethodPost && parts[4] == "turn-receipt":
		var receipt workspace.TurnReceipt
		if err := decodeBody(r, &receipt); err != nil {
			actionErr = err
			break
		}
		actionErr = c.Store.RecordTurnReceipt(g.AttemptID, receipt)
		if actionErr == nil {
			c.enqueueDelivery(g.AttemptID)
			c.enqueueSession(g.WorkerSessionID)
			c.wakeClaim()
		}
	case r.Method == http.MethodPost && parts[4] == "admit":
		var input struct {
			PublicKey       []byte `json:"publicKey"`
			PodUID          string `json:"podUID"`
			PVCUID          string `json:"pvcUID"`
			BootstrapDigest string `json:"bootstrapDigest"`
		}
		if err := decodeBody(r, &input); err != nil || input.PodUID != g.PodUID || input.PVCUID != g.PVCUID || input.BootstrapDigest != g.BootstrapDigest || len(input.PublicKey) != ed25519.PublicKeySize {
			http.Error(w, "denied", 403)
			return
		}
		rec, err := record(g)
		if err != nil {
			actionErr = err
			break
		}
		if pod.Spec.NodeName == "" {
			actionErr = errors.New("worker scheduling pending")
			break
		}
		if g.State == "intent" {
			if err := c.Store.BindPod(g.AttemptID, g.PodName, g.PodUID, pod.Spec.NodeName); err != nil {
				actionErr = err
				break
			}
			rec.Reference.NodeID = pod.Spec.NodeName
			if err := c.saveResources(g.AttemptID, rec); err != nil {
				actionErr = err
				break
			}
		}
		actionErr = c.Store.Admit(g.AttemptID, ed25519.PublicKey(input.PublicKey))
	case r.Method == http.MethodPost && parts[4] == "receipt":
		var receipt workspace.SignedReceipt
		if err := decodeBody(r, &receipt); err != nil {
			actionErr = err
			break
		}
		actionErr = c.Store.Seal(g.AttemptID, receipt)
		if actionErr != nil {
			_ = c.Store.Quarantine(g.AttemptID)
			c.cancelCheckouts(g.AttemptID)
		} else {
			c.enqueueDelivery(g.AttemptID)
		}
	case r.Method == http.MethodPost && parts[4] == "abort":
		actionErr = c.recordFailure(g, nil)
	default:
		http.Error(w, "denied", 403)
		return
	}
	if actionErr != nil {
		http.Error(w, "action refused", 409)
		return
	}
	startErr = nil
	if parts[4] != "event" {
		c.enqueueAttempt(g.AttemptID)
	}
	writeJSON(w, map[string]bool{"accepted": true})
}

// control is polled by the worker; no long request holds dispatch or API locks.
func (c *Controller) waitForTerminal(w http.ResponseWriter, r *http.Request, attempt string) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		http.Error(w, "denied", 403)
		return
	}
	g, err := c.Store.Authorize(token, "supervisor")
	if err != nil || g.AttemptID != attempt {
		http.Error(w, "denied", 403)
		return
	}
	if g.WorkerSessionID != "" {
		session, sessionErr := c.Store.GetSession(g.WorkerSessionID)
		if sessionErr == nil && session.ProtocolVersion == workspace.LegacySessionProtocolVersion && session.Stop != nil {
			rec, recordErr := record(g)
			if recordErr != nil {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			if _, err := c.Kube.AuthorizeStop(r.Context(), rec.Reference); err != nil {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, wire.SealCommand{Cancel: true})
			return
		}
	}
	if c.admission(r.Context(), g) != nil {
		http.Error(w, "denied", http.StatusForbidden)
		return
	}
	terminal, err := c.Store.Terminal(attempt)
	if err == nil {
		writeJSON(w, wire.SealCommand{RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce})
		return
	}
	if !errors.Is(err, workspace.ErrUnauthorized) {
		http.Error(w, "unavailable", 503)
		return
	}
	if g.ExecutionRevoked {
		writeJSON(w, wire.SealCommand{Cancel: true})
		return
	}
	assignment, assignmentErr := c.assignment(r.Context(), g)
	if errors.Is(assignmentErr, errAssignmentChanged) || assignmentErr == nil && (assignment.Status == "cancelled" || assignment.Status == "failed" || assignment.Status == "completed") {
		if _, err := c.requestStop(g, "backend_terminal"); err != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		writeJSON(w, wire.SealCommand{Cancel: true})
		return
	}
	if assignmentErr != nil {
		http.Error(w, "assignment observation unavailable", 503)
		return
	}
	writeJSON(w, wire.SealCommand{})
}

func decodeBody(r *http.Request, out any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, wire.MaxRequestBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > wire.MaxRequestBytes {
		return errors.New("payload too large")
	}
	return runtimeimage.Decode(raw, out)
}
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (c *Controller) scmGrant(r *http.Request) (workspace.TaskGrant, error) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return workspace.TaskGrant{}, workspace.ErrUnauthorized
	}
	g, err := c.Store.Authorize(token, "cache")
	if err != nil {
		return g, err
	}
	if c.App == nil {
		return g, workspace.ErrUnauthorized
	}
	return g, c.admission(r.Context(), g)
}

func (c *Controller) scmToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != githubauth.Route || r.URL.RawQuery != "" || r.URL.RawPath != "" {
		http.Error(w, "denied", 403)
		return
	}
	var request githubauth.Request
	if err := decodeBody(r, &request); err != nil {
		http.Error(w, "denied", 403)
		return
	}
	g, err := c.scmGrant(r)
	if err != nil {
		http.Error(w, "denied", 403)
		return
	}
	repos := []githubapp.Repository{}
	for _, entry := range g.Repositories {
		repo, err := githubapp.ParseRepositoryURL(entry.URL)
		if err != nil {
			continue
		}
		if request.Repository == "" {
			repos = append(repos, repo)
		} else if requested, err := githubapp.ParseRepositoryURL(request.Repository); err == nil && requested == repo {
			repos = append(repos, repo)
		}
	}
	if len(repos) == 0 {
		http.Error(w, "denied", 403)
		return
	}
	token, err := c.App.Token(r.Context(), repos)
	if err != nil {
		http.Error(w, "authorization unavailable", 403)
		return
	}
	for _, repo := range repos {
		if err := c.App.Authorize(r.Context(), repo, token); err != nil {
			http.Error(w, "authorization unavailable", 403)
			return
		}
	}
	// Credential exposure is ordered with terminal revocation, after potentially
	// slow SCM requests have finished without holding the coordination lock.
	gate := c.Store.ExecutionGate(g.AttemptID)
	gate.RLock()
	defer gate.RUnlock()
	if _, err := c.scmGrant(r); err != nil {
		http.Error(w, "denied", 403)
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(30 * time.Second))
	writeJSON(w, token)
	_ = http.NewResponseController(w).Flush()
}
