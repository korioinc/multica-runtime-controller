package worker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"github.com/multica-ai/multica/server/pkg/agent"
	"golang.org/x/sys/unix"
)

func TestPID1ShutdownPreservesObservedProviderResult(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1")
	}
	for _, scenario := range []struct {
		name              string
		pendingTranscript bool
		checkoutFailure   bool
		pendingCheckouts  bool
		pendingResult     bool
		receiptFailure    bool
		unprovenRunner    bool
		nestedInitDeath   bool
	}{
		{name: "usage_after_completion"},
		{name: "result_after_transcript_cancellation", pendingTranscript: true},
		{name: "result_survives_unproven_checkout", checkoutFailure: true},
		{name: "result_precedes_checkout_completion", pendingCheckouts: true},
		{name: "cleanup_progresses_during_result_delivery", pendingResult: true},
		{name: "storage_seal_preserves_result_after_authentication_error", receiptFailure: true},
		{name: "lost_runner_wait_retains_task_storage", unprovenRunner: true},
		{name: "real_result_survives_init_death_and_checkout_failure", nestedInitDeath: true, checkoutFailure: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, err := workspace.Open(workspace.Options{Directory: filepath.Join(t.TempDir(), "journal"), OwnerID: uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			g := preparedEventGrant(t, store)
			pub, key, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PinStopSupervisor(g.AttemptID, g.PodUID, g.PVCUID, g.NodeID, pub); err != nil {
				t.Fatal(err)
			}
			if err := store.Admit(g.AttemptID, pub); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Offer(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := store.BeginStart(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := store.MarkStarted(g.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(g.TaskRoot, 0700); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(filepath.Dir(g.TaskRoot))
			outcomeRecorded := make(chan struct{})
			checkoutsStopped := make(chan struct{})
			var outcomeOnce, checkoutOnce sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var actionErr error
				var response any = struct{}{}
				switch filepath.Base(r.URL.Path) {
				case "event":
					// Telemetry can fail after the provider has already completed. It
					// must not replace the provider's durable outcome with a failure.
					http.Error(w, "event refused", http.StatusForbidden)
					return
				case "checkout-stop":
					if scenario.checkoutFailure {
						http.Error(w, "checkout writer remains unproven", http.StatusForbidden)
						return
					}
					if scenario.pendingCheckouts {
						// Cleanup cannot manufacture the provider outcome; publishing
						// it must progress while this storage fence is still pending.
						select {
						case <-outcomeRecorded:
						case <-r.Context().Done():
							return
						}
					}
					actionErr = store.CloseCheckouts(g.AttemptID)
					if actionErr == nil {
						checkoutOnce.Do(func() { close(checkoutsStopped) })
					}
				case "result":
					if scenario.pendingResult {
						select {
						case <-checkoutsStopped:
						case <-r.Context().Done():
							return
						}
					}
					var result wire.ProviderResult
					actionErr = json.NewDecoder(r.Body).Decode(&result)
					if actionErr == nil {
						raw, _ := json.Marshal(result)
						body, _ := json.Marshal(map[string]string{"output": result.Result.Output})
						var terminal workspace.Terminal
						kind := "fail"
						if result.Result.Status == "completed" {
							kind = "complete"
						}
						terminal, actionErr = store.ReceiveTerminal(g.AttemptID, kind, body, workspace.ResumePointers{}, wire.Digest(raw))
						response = wire.SealCommand{RequestDigest: terminal.RequestDigest, Nonce: terminal.Nonce}
					}
				case "result-receipt":
					if scenario.receiptFailure {
						http.Error(w, "outcome authentication refused", http.StatusForbidden)
						return
					}
					var receipt workspace.ResultReceipt
					actionErr = json.NewDecoder(r.Body).Decode(&receipt)
					if actionErr == nil {
						actionErr = store.RecordResultReceipt(g.AttemptID, receipt)
					}
					if actionErr == nil {
						outcomeOnce.Do(func() { close(outcomeRecorded) })
					}
				case "receipt":
					var receipt workspace.SignedReceipt
					actionErr = json.NewDecoder(r.Body).Decode(&receipt)
					if actionErr == nil {
						actionErr = store.Seal(g.AttemptID, receipt)
					}
				case "stop-request":
					// The real controller records a failure if no native result owns
					// the outbox yet. Preserve that consequential race in the fixture.
					if _, err := store.Terminal(g.AttemptID); err != nil {
						_, actionErr = store.ReceiveFailure(g.AttemptID, []byte(`{"error":"worker unavailable"}`))
					}
					current, err := store.RequestStop(g.AttemptID, "worker_shutdown")
					if actionErr == nil {
						actionErr = err
					}
					response = wire.StopCommand{Cancel: true, Revision: current.Stop.Revision, Nonce: current.Stop.Nonce}
				case "stop-receipt":
					var receipt workspace.StopReceipt
					actionErr = json.NewDecoder(r.Body).Decode(&receipt)
					if actionErr == nil {
						actionErr = store.ReceiveStopReceipt(g.AttemptID, receipt)
					}
				}
				if actionErr != nil {
					http.Error(w, "rejected", http.StatusConflict)
					return
				}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			b := wire.Bootstrap{TaskID: g.TaskID, AttemptID: g.AttemptID, Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, GatewayURL: server.URL, SupervisorCapability: strings.Repeat("a", 32), StopCapability: strings.Repeat("b", 32), TerminationGraceSeconds: 1}
			lifecycle := workerLifecycle{bootstrap: b, client: server.Client(), signingKey: key, identity: workerIdentity{PodUID: g.PodUID}, admitted: true}
			if scenario.unprovenRunner {
				command := exec.Command("/bin/sh", "-c", "exit 0")
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				// Exercise a real lost-wait boundary: another reaper consumed
				// the child's status, so cmd.Wait cannot certify its own child.
				var status unix.WaitStatus
				if _, err := unix.Wait4(command.Process.Pid, &status, 0, nil); err != nil {
					t.Fatal(err)
				}
				_ = command.Wait()
				done := make(chan struct{})
				close(done)
				lifecycle.runner = &providerRunner{command: command, Done: done, closed: make(chan struct{})}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			messages := make(chan agent.Message, 1)
			results := make(chan agent.Result, 1)
			completed := agent.Result{Status: "completed", Output: "retained provider work", Usage: map[string]agent.TokenUsage{"model": {InputTokens: 1}}}
			session := &agent.Session{Messages: messages, Result: results}
			if scenario.nestedInitDeath {
				request := runnerFixtureRequest(t, false)
				barrier := filepath.Join(request.Options.Cwd, "complete")
				request.Environment["PROVIDER_COMPLETE_BARRIER"] = barrier
				request.Environment["PROVIDER_LATE_WRITE"] = filepath.Join(request.Options.Cwd, "late-writer")
				request.Environment["PROVIDER_PROOF"] = completed.Output
				lifecycle.runner = startRunnerFixture(t, request)
				_ = runnerFixturePID(t, request.Options.Cwd)
				killRunnerInit(t, lifecycle.runner)
				if err := os.WriteFile(barrier, nil, 0600); err != nil {
					t.Fatal(err)
				}
				session = lifecycle.runner.Session
			} else if scenario.pendingTranscript {
				messages <- agent.Message{Type: agent.MessageText, Content: completed.Output}
				// An adapter may still be draining its transcript when cancellation
				// arrives. Continue consuming it so the actual result can be published.
				go func() {
					select {
					case <-ctx.Done():
					case <-t.Context().Done():
						return
					}
					select {
					case messages <- agent.Message{Type: agent.MessageStatus, Status: "finished"}:
					case <-t.Context().Done():
						return
					}
					close(messages)
					results <- completed
					close(results)
				}()
			} else {
				close(messages)
				results <- completed
				close(results)
			}
			observed, deliveryErr := drainProvider(ctx, cancel, session, server.Client(), b, wire.Run{}, time.Now())
			if err := lifecycle.finish(observed, deliveryErr, true); err != nil && !scenario.checkoutFailure && !scenario.receiptFailure && !scenario.unprovenRunner {
				t.Fatal(err)
			}
			terminal, err := store.Terminal(g.AttemptID)
			if err != nil || terminal.Source != "worker" || terminal.Kind != "complete" || terminal.ResultReceipt == nil && !scenario.receiptFailure {
				t.Fatal("shutdown replaced the observed provider result", err)
			}
			var result map[string]string
			if err := json.Unmarshal(terminal.Body, &result); err != nil || result["output"] != "retained provider work" {
				t.Fatal("shutdown lost provider output", err)
			}
			current, err := store.Get(g.AttemptID)
			if scenario.checkoutFailure || scenario.unprovenRunner {
				if err != nil || terminal.Seal != nil || current.Stop == nil || current.Stop.Receipt == nil || current.Stop.Receipt.FlushOK {
					t.Fatal("outcome authentication certified unproven task storage", err)
				}
				if _, err := store.Create(workspace.TaskGrant{TaskID: g.TaskID, RuntimeID: g.RuntimeID, WorkspaceID: g.WorkspaceID, AgentID: g.AgentID, RuntimeRef: g.RuntimeRef, Envelope: g.Envelope}); !errors.Is(err, workspace.ErrStorageBusy) {
					t.Fatal("published outcome released unproven task storage", err)
				}
				return
			}
			if err != nil || current.Stop == nil || current.Stop.Receipt == nil || !current.Stop.Receipt.FlushOK {
				t.Fatal("result preservation lost independent stop evidence", err)
			}
			if terminal.Seal == nil {
				t.Fatal("clean shutdown lost its separate storage seal")
			}
		})
	}
}

// Use the existing isolated PID 1 test execution path, with writable /workspace
// and /etc/multica/task mounts. This exercises Serve itself before input exists.
func TestPID1EarlyShutdownPersistsStopEvidence(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1")
	}
	for _, scenario := range []struct {
		name        string
		cancelled   bool
		interrupted bool
		missingRoot bool
	}{
		{name: "cancelled_before_input", cancelled: true},
		{name: "startup_failure"},
		{name: "sigterm_before_admission", interrupted: true},
		{name: "unavailable_task_mount", cancelled: true, missingRoot: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			options := workspace.Options{Directory: filepath.Join(t.TempDir(), "journal"), OwnerID: uuid.NewString()}
			store, err := workspace.Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if store != nil {
					_ = store.Close()
				}
			}()
			grant := preparedEventGrant(t, store)
			if scenario.cancelled {
				if _, err := store.RequestStop(grant.AttemptID, "backend_cancelled"); err != nil {
					t.Fatal(err)
				}
			}
			// Only the narrow stop channel is available. Its journal owner
			// validates the signature and binding, including durable replay.
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var actionErr error
				switch filepath.Base(r.URL.Path) {
				case "checkout-stop":
					actionErr = store.CloseCheckouts(grant.AttemptID)
				case "stop-admit":
					var input struct {
						PublicKey      []byte
						PodUID, PVCUID string
					}
					actionErr = json.NewDecoder(r.Body).Decode(&input)
					if actionErr == nil {
						actionErr = store.PinStopSupervisor(grant.AttemptID, input.PodUID, input.PVCUID, grant.NodeID, input.PublicKey)
					}
				case "stop-control":
				case "stop-request":
					_, actionErr = store.RequestStop(grant.AttemptID, "worker_shutdown")
				case "stop-receipt":
					var receipt workspace.StopReceipt
					actionErr = json.NewDecoder(r.Body).Decode(&receipt)
					if actionErr == nil {
						actionErr = store.ReceiveStopReceipt(grant.AttemptID, receipt)
					}
				default:
					http.Error(w, "execution unavailable", http.StatusForbidden)
					return
				}
				if actionErr != nil {
					t.Error(actionErr)
					http.Error(w, "journal rejected stop", http.StatusConflict)
					return
				}
				current, err := store.Get(grant.AttemptID)
				if err != nil {
					t.Error(err)
					return
				}
				command := wire.StopCommand{}
				if current.Stop != nil {
					command = wire.StopCommand{Cancel: true, Revision: current.Stop.Revision, Nonce: current.Stop.Nonce}
				}
				_ = json.NewEncoder(w).Encode(command)
			}))
			defer server.Close()
			bootstrap := wire.Bootstrap{OwnerID: grant.OwnerID, TaskID: grant.TaskID, AttemptID: grant.AttemptID,
				StorageID: grant.StorageID, RuntimeID: grant.RuntimeID, WorkspaceID: grant.WorkspaceID, AgentID: grant.AgentID,
				Generation: grant.Generation, TaskRoot: grant.TaskRoot, NFSServer: "127.0.0.1", Provider: "codex",
				PreparedDigest: grant.Prepared.Digest, PVCName: grant.PVCName, PVCUID: grant.PVCUID, RuntimeRef: grant.RuntimeRef,
				GatewayURL: server.URL, APICapability: strings.Repeat("a", 32), SupervisorCapability: strings.Repeat("b", 32),
				StopCapability: strings.Repeat("c", 32), CacheCapability: strings.Repeat("d", 32), TerminationGraceSeconds: 1,
				Configuration: configuration.Bundle{Groups: []configuration.Group{}, Digest: configuration.Digest(nil)}}
			if err := bootstrap.Validate(); err != nil {
				t.Fatal(err)
			}
			paths := []string{filepath.Dir(wire.RequestPath)}
			if !scenario.missingRoot {
				paths = append(paths, grant.TaskRoot)
			}
			for _, path := range paths {
				if err := os.MkdirAll(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			defer os.RemoveAll(filepath.Dir(grant.TaskRoot))
			raw, err := json.Marshal(bootstrap)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(wire.RequestPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("MULTICA_REQUEST_DIGEST", wire.Digest(raw))
			t.Setenv("POD_UID", grant.PodUID)
			ctx := context.Background()
			if scenario.interrupted {
				var stop context.CancelFunc
				ctx, stop = signal.NotifyContext(ctx, syscall.SIGTERM)
				defer stop()
				if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
				<-ctx.Done()
			}
			_ = Serve(ctx)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = workspace.Open(options)
			if err != nil {
				t.Fatal(err)
			}
			retained, err := store.Get(grant.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			if retained.Stop == nil || retained.Stop.Receipt == nil || !retained.Stop.Receipt.WritersStopped {
				t.Fatal("pre-input shutdown lost verified writer-stop evidence")
			}
			if scenario.missingRoot {
				if retained.Stop.Receipt.FlushOK {
					t.Fatal("inaccessible task filesystem was certified clean")
				}
			} else if !retained.Stop.Receipt.FlushOK {
				t.Fatal("pre-input shutdown lost verified filesystem-flush evidence")
			}
		})
	}
}
