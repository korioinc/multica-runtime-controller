package workspace

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

func testStore(t *testing.T) (*Store, Options) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Directory: filepath.Join(dir, "state"), OwnerID: uuid.NewString()}
	s, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindWorkspace("workspace-new", uuid.NewString(), "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, options
}
func testGrant() TaskGrant {
	sha := core.Digest([]byte("immutable execution"))
	executable := runtimeimage.Executable{Path: "/opt/tools/runner", Version: "1.0.0", SHA256: sha}
	ref := runtimeimage.Ref{Image: "example.invalid/runtime@sha256:" + sha, Platform: "linux/amd64", ImageBuildID: "11111111-1111-4111-8111-111111111111", DescriptorDigest: sha, ConfigurationDigest: sha, Controller: core.Contract{BuildID: sha, Platform: "linux/amd64", RuntimePath: core.Root + "/runtime", RuntimeSHA256: sha, GoVersion: "go1.26.6"}, Daemon: runtimeimage.Daemon{Executable: executable, AdapterContract: runtimeimage.AdapterContract}, Providers: map[string]runtimeimage.Executable{"codex": executable}}
	return TaskGrant{TaskID: uuid.NewString(), RuntimeID: "runtime", WorkspaceID: uuid.NewString(), AgentID: uuid.NewString(), Repositories: []Repository{{URL: "https://example.invalid/private.git"}}, Envelope: json.RawMessage(`{ "opaque_option": { "nested": true, "text": "<tag>&private" } }`), RuntimeRef: ref}
}
func readyGrant(t *testing.T, s *Store, input TaskGrant) (TaskGrant, ed25519.PrivateKey, string) {
	t.Helper()
	g, token := preparedGrant(t, s, input)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Admit(g.AttemptID, pub); err != nil {
		t.Fatal(err)
	}
	g, err = s.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return g, key, token
}

func preparedGrant(t *testing.T, s *Store, input TaskGrant) (TaskGrant, string) {
	t.Helper()
	g, err := s.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.IssueCapability(g.AttemptID, "daemon", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BeginPreparation(g.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://fixture"}); err != nil {
		t.Fatal(err)
	}
	prepared := Prepared{OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID, Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, Provider: "codex", Executable: "/opt/tools/runner", RuntimeDigest: g.Fingerprint, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{}, Environment: NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}}
	prepared.Digest = preparedDigest(prepared)
	if err := s.CompletePreparation(g.AttemptID, &prepared, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err = s.BindPod(g.AttemptID, g.PodName, uuid.NewString(), "node-a"); err != nil {
		t.Fatal(err)
	}
	g, err = s.Get(g.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	return g, token
}
func startGrant(t *testing.T, s *Store, g TaskGrant) {
	t.Helper()
	if _, ok, err := s.Offer(g.AttemptID); err != nil || !ok {
		t.Fatal("task was not offered", err)
	}
	if err := s.BeginStart(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkStarted(g.AttemptID); err != nil {
		t.Fatal(err)
	}
}
func sealReceipt(g TaskGrant, t Terminal, key ed25519.PrivateKey) SignedReceipt {
	r := SignedReceipt{TaskID: g.TaskID, AttemptID: g.AttemptID, RequestDigest: t.RequestDigest, PodUID: g.PodUID, PVCUID: g.PVCUID, Nonce: t.Nonce, WritersStopped: true, FlushOK: true}
	r.Signature = ed25519.Sign(key, ReceiptMessage(r))
	return r
}

func TestConcurrentOfferAndReopenNeverReoffer(t *testing.T) {
	s, options := testStore(t)
	g, _, _ := readyGrant(t, s, testGrant())
	if other, err := Open(options); err == nil {
		other.Close()
		t.Fatal("second controller acquired write authority")
	}
	var offered atomic.Int32
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for range 12 {
		wg.Go(func() {
			<-gate
			_, ok, err := s.Offer(g.AttemptID)
			if err != nil {
				t.Error(err)
			}
			if ok {
				offered.Add(1)
			}
		})
	}
	close(gate)
	wg.Wait()
	if offered.Load() != 1 {
		t.Fatal("same task offered to more than one consumer")
	}
	if err := s.BeginStart(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, ok, err := s.Offer(g.AttemptID); err != nil || ok {
		t.Fatal("lost response authorized a duplicate offer", err)
	}
	if err := s.BeginStart(g.AttemptID); err == nil {
		t.Fatal("ambiguous start was sent a second time")
	}
	input := testGrant()
	input.TaskID = g.TaskID
	if _, err := s.Create(input); err == nil {
		t.Fatal("active task gained another attempt")
	}
}

func TestTerminalPersistenceSealAndResumeAuthority(t *testing.T) {
	s, options := testStore(t)
	g, key, token := readyGrant(t, s, testGrant())
	supervisor, err := s.IssueCapability(g.AttemptID, "supervisor", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	startGrant(t, s, g)
	body := []byte("{\n \"opaque_result\": true\n}\n")
	terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{WorkDir: g.TaskRoot + "/workdir"}, core.Digest([]byte("fixture provider result")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authorize(token, "daemon"); err == nil {
		t.Fatal("terminal retained general task authority")
	}
	if err := s.BeginForward(g.AttemptID); err == nil {
		t.Fatal("unflushed terminal acquired delivery authority")
	}
	s.Close()
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Authorize(supervisor, "supervisor"); err != nil {
		t.Fatal("supervisor lost receipt retry authority", err)
	}
	same, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{WorkDir: g.TaskRoot + "/workdir"}, core.Digest([]byte("fixture provider result")))
	if err != nil || same.ReceiptID != terminal.ReceiptID {
		t.Fatal("durable retry lost terminal ownership", err)
	}
	if _, err := s.ReceiveTerminal(g.AttemptID, "fail", []byte(`{}`), ResumePointers{}, core.Digest([]byte("fixture provider result"))); err == nil {
		t.Fatal("conflicting callback replaced completion")
	}
	persisted, err := s.Terminal(g.AttemptID)
	if err != nil || !bytes.Equal(persisted.Body, body) {
		t.Fatal("acknowledged result bytes lost", err)
	}
	receipt := sealReceipt(g, terminal, key)
	if err := s.Seal(g.AttemptID, receipt); err == nil {
		t.Fatal("worker sealed task data without closing controller checkout writers")
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	receipt.FlushOK = false
	receipt.Signature = ed25519.Sign(key, ReceiptMessage(receipt))
	if err := s.Seal(g.AttemptID, receipt); err == nil {
		t.Fatal("failed flush acquired delivery authority")
	}
	receipt = sealReceipt(g, terminal, key)
	receipt.PodUID = uuid.NewString()
	receipt.Signature = ed25519.Sign(key, ReceiptMessage(receipt))
	if err := s.Seal(g.AttemptID, receipt); err == nil {
		t.Fatal("substituted Pod acquired seal authority")
	}
	if err := s.Seal(g.AttemptID, sealReceipt(g, terminal, key)); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginForward(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishForward(g.AttemptID, "delivered"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(g.AttemptID, "execution_finished"); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "missing", ObservedAt: time.Now().UTC()}); err == nil {
		t.Fatal("missing Pod was accepted as stopped writer")
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{PodUID: g.PodUID, PVCUID: g.PVCUID, Kind: "terminated", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCleaned(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	next := testGrant()
	next.TaskID, next.WorkspaceID, next.AgentID = g.TaskID, g.WorkspaceID, g.AgentID
	stolen := next
	stolen.AgentID = uuid.NewString()
	if _, err := s.Create(stolen); err == nil {
		t.Fatal("another agent acquired private task storage")
	}
	mismatch := next
	mismatch.RuntimeRef.ConfigurationDigest = core.Digest([]byte("changed authority"))
	if _, err := s.Create(mismatch); err == nil {
		t.Fatal("different configuration acquired private task storage")
	}
	resumed, err := s.Create(next)
	if err != nil || resumed.StorageID != g.StorageID || resumed.PVCUID != g.PVCUID || !resumed.Reuse {
		t.Fatal("same task lost retained workspace", err)
	}
	if _, err := s.Authorize(supervisor, "supervisor"); err == nil {
		t.Fatal("previous attempt authorized replacement")
	}
	if err := s.Quarantine(resumed.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(next); err == nil {
		t.Fatal("unknown writer released quarantine")
	}
}

func TestWorkerSealRequiresControllerFlushAfterWriterCompletion(t *testing.T) {
	s, options := testStore(t)
	g, key, _ := readyGrant(t, s, testGrant())
	startGrant(t, s, g)
	process := PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://checkout"}
	if err := s.BeginCheckout(g.AttemptID, process); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckoutsFlushed(g.AttemptID); err == nil {
		t.Fatal("live controller writer lost its flush obligation")
	}
	if err := s.CompleteCheckout(g.AttemptID, process); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	body := []byte(`{"output":"completed provider work"}`)
	terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest(body))
	if err != nil {
		t.Fatal(err)
	}
	receipt := sealReceipt(g, terminal, key)
	if err := s.Seal(g.AttemptID, receipt); err == nil {
		t.Fatal("NFS worker flush certified unflushed controller writes after restart")
	}
	if err := s.CheckoutsFlushed(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Seal(g.AttemptID, receipt); err != nil {
		t.Fatal("completed local and NFS flushes could not seal the task", err)
	}
}

func TestTerminalSurvivesProcessExitWithoutClose(t *testing.T) {
	if os.Getenv("MULTICA_STORE_CRASH_CHILD") == "1" {
		s, err := Open(Options{Directory: os.Getenv("MULTICA_STORE_CRASH_DIR"), OwnerID: os.Getenv("MULTICA_STORE_CRASH_OWNER")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.ReceiveTerminal(os.Getenv("MULTICA_STORE_CRASH_ATTEMPT"), "complete", []byte(`{"durable_result":"owned"}`), ResumePointers{}, core.Digest([]byte("fixture provider result"))); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	s, options := testStore(t)
	g, _, _ := readyGrant(t, s, testGrant())
	startGrant(t, s, g)
	s.Close()
	child := exec.Command(os.Args[0], "-test.run=^TestTerminalSurvivesProcessExitWithoutClose$")
	child.Env = append(os.Environ(), "MULTICA_STORE_CRASH_CHILD=1", "MULTICA_STORE_CRASH_DIR="+options.Directory, "MULTICA_STORE_CRASH_OWNER="+options.OwnerID, "MULTICA_STORE_CRASH_ATTEMPT="+g.AttemptID)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v %s", err, output)
	}
	s, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	terminal, err := s.Terminal(g.AttemptID)
	if err != nil || !bytes.Equal(terminal.Body, []byte(`{"durable_result":"owned"}`)) {
		t.Fatal("acknowledged terminal lost after process exit", err)
	}
}

func TestForeignOrDamagedMetadataCannotAuthorizeStorage(t *testing.T) {
	s, options := testStore(t)
	g, _, _ := readyGrant(t, s, testGrant())
	s.Close()
	if other, err := Open(Options{Directory: options.Directory, OwnerID: uuid.NewString()}); err == nil {
		other.Close()
		t.Fatal("foreign installation acquired authority")
	}
	raw, err := os.ReadFile(filepath.Join(options.Directory, "journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st registry
	if err = json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	storage := st.Storages[g.StorageID]
	storage.PVCUID = uuid.NewString()
	st.Storages[g.StorageID] = storage
	corrupted, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(options.Directory, "journal.json"), corrupted, 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(options); err == nil {
		other.Close()
		t.Fatal("corrupt binding gained write authority")
	}
	kept, err := os.ReadFile(filepath.Join(options.Directory, "journal.json"))
	if err != nil || !bytes.Equal(kept, corrupted) {
		t.Fatal("failed startup rewrote original metadata", err)
	}
}

func TestControllerFailureSurvivesReopenWithoutReleasingStorage(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	g, err := s.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BeginPreparation(g.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://fixture"}); err != nil {
		t.Fatal(err)
	}
	body := []byte("{\n\"error\":\"worker preparation failed\"\n}")
	failure, err := s.ReceiveFailure(g.AttemptID, body)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	retried, err := s.ReceiveFailure(g.AttemptID, body)
	if err != nil || retried.ReceiptID != failure.ReceiptID || !bytes.Equal(retried.Body, body) {
		t.Fatal("controller failure lost durable ownership", err)
	}
	if err = s.BeginForward(g.AttemptID); err == nil {
		t.Fatal("controller failure acquired native success delivery authority")
	}
	if err = s.BeginFailureForward(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.BeginFailureForward(g.AttemptID); err == nil {
		t.Fatal("ambiguous failure delivery was sent again")
	}
	if _, err := s.RequestStop(g.AttemptID, "preparation_failed"); err != nil {
		t.Fatal(err)
	}
	if err = s.CloseStopped(g.AttemptID); err == nil {
		t.Fatal("failure receipt falsely released uncertain writer")
	}
	if _, err = s.Create(input); err == nil {
		t.Fatal("quarantined task acquired another writer")
	}
}

func TestControllerFailureCannotOverwriteNativeTerminal(t *testing.T) {
	s, _ := testStore(t)
	g, _, _ := readyGrant(t, s, testGrant())
	startGrant(t, s, g)
	body := []byte(`{"output":"native result"}`)
	native, err := s.ReceiveTerminal(g.AttemptID, "complete", body, ResumePointers{}, core.Digest([]byte("fixture provider result")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReceiveFailure(g.AttemptID, []byte(`{"error":"controller failure"}`)); err == nil {
		t.Fatal("controller replaced admitted native completion")
	}
	if err = s.BeginFailureForward(g.AttemptID); err == nil {
		t.Fatal("native result bypassed seal through failure forwarding")
	}
	retained, err := s.Terminal(g.AttemptID)
	if err != nil || retained.ReceiptID != native.ReceiptID || !bytes.Equal(retained.Body, body) {
		t.Fatal("controller failure changed durable native result", err)
	}
}

func TestFreshMetadataVolumeEstablishesPrivateDurableAuthority(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(root, 0777|os.ModeSetgid); err != nil {
		t.Fatal(err)
	}
	// Recovery contents must not be traversed or adopted as controller state.
	if err = os.Mkdir(filepath.Join(root, "lost+found"), 0000); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	owner := uuid.NewString()
	s, err := OpenVolume(root, Options{OwnerID: owner})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindWorkspace("workspace-new", uuid.NewString(), "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	g, err := s.Create(testGrant())
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenVolume(root, Options{OwnerID: owner})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Get(g.AttemptID); err != nil {
		t.Fatal("fresh volume lost committed task authority", err)
	}
	after, err := os.Stat(root)
	if err != nil || after.Mode() != before.Mode() {
		t.Fatal("opening journal changed filesystem-root access", err)
	}
	journal, err := os.Stat(filepath.Join(root, "journal"))
	if err != nil || journal.Mode().Perm()&0007 != 0 {
		t.Fatal("fresh authority exposed controller secrets to other users", err)
	}
}

func TestPopulatedMetadataVolumeIsPreservedWithoutAdoption(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	contents := []byte("previous installation authority")
	path := filepath.Join(root, "registry.json")
	if err = os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenVolume(root, Options{OwnerID: uuid.NewString()}); err == nil {
		s.Close()
		t.Fatal("unknown populated volume gained controller authority")
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, contents) {
		t.Fatal("rejected startup altered prior installation data", err)
	}
}

func TestCleanupAuthorityCannotBeSuppliedOrMarkALiveWriter(t *testing.T) {
	s, _ := testStore(t)
	input := testGrant()
	input.CleanupComplete = true
	if _, err := s.QueueClaim(input); !errors.Is(err, ErrInvalidClaim) {
		t.Fatal("claim input supplied cleanup authority", err)
	}
	g, _, token := readyGrant(t, s, testGrant())
	if err := s.MarkCleaned(g.AttemptID); err == nil {
		t.Fatal("live writer was marked cleaned")
	}
	if _, err := s.Authorize(token, "daemon"); err != nil {
		t.Fatal("refused cleanup changed live writer authority", err)
	}
}

func TestUncertainDeliveryCannotAuthorizeAReplayOrTaskReuse(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	g, key, _ := readyGrant(t, s, input)
	startGrant(t, s, g)
	terminal, err := s.ReceiveTerminal(g.AttemptID, "complete", []byte(`{"output":"completed work"}`), ResumePointers{}, core.Digest([]byte("fixture provider result")))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CloseCheckouts(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Seal(g.AttemptID, sealReceipt(g, terminal, key)); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginForward(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	s.Close()
	options.MaxPendingResults = 1
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.BeginForward(g.AttemptID); err == nil {
		t.Fatal("ambiguous callback replayed")
	}
	if _, err := s.RequestStop(g.AttemptID, "delivery_uncertain"); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err == nil {
		t.Fatal("uncertain delivery was mistaken for writer termination")
	}
	if _, err := s.Create(input); err == nil {
		t.Fatal("uncertain completion reran provider")
	}
	if _, err := s.Create(testGrant()); !errors.Is(err, ErrAdmissionBudget) {
		t.Fatal("unresolved result budget failed to stop new task admission", err)
	}
	queued, err := s.QueueClaim(testGrant())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceiveFailure(queued.AttemptID, []byte(`{"error":"admission refused"}`)); !errors.Is(err, ErrAdmissionBudget) {
		t.Fatal("controller failure bypassed the pending result budget", err)
	}
	queued, err = s.Get(queued.AttemptID)
	if err != nil || queued.State != "waiting_storage" || queued.StorageID != "" {
		t.Fatal("unrecorded rejection changed queued authority", err)
	}
}

func TestAdmissionBudgetRetainsExistingTaskAuthority(t *testing.T) {
	s, options := testStore(t)
	s.Close()
	options.MaxTasks = 1
	s, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, err := s.Create(testGrant())
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(options.Directory, "journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(testGrant()); !errors.Is(err, ErrAdmissionBudget) {
		t.Fatal("exhausted admission budget did not explicitly refuse another workspace", err)
	}
	after, err := os.ReadFile(filepath.Join(options.Directory, "journal.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("budget refusal changed retained task authority", err)
	}
	if _, err := s.Get(first.AttemptID); err != nil {
		t.Fatal("budget refusal lost existing work", err)
	}
}

func TestStoppedPreparationRequiresRecoveryWithoutACompletedBaseline(t *testing.T) {
	s, _ := testStore(t)
	input := testGrant()
	g, err := s.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BeginPreparation(g.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://fixture"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CompletePreparation(g.AttemptID, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceiveFailure(g.AttemptID, []byte(`{"error":"preparation failed"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginFailureForward(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishForward(g.AttemptID, "delivered"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(g.AttemptID, "preparation_failed"); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{PVCUID: g.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCleaned(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(input); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("partially prepared task was reused without a clean baseline", err)
	}
	retained, err := s.Get(g.AttemptID)
	if err != nil || retained.TaskRoot != g.TaskRoot || retained.StorageID != g.StorageID || retained.State != "closed" {
		t.Fatal("stopping incomplete preparation lost retained task storage", err)
	}
}

func TestFailedPreparationReleasesOnlyAfterItsWritersStop(t *testing.T) {
	for _, running := range []bool{false, true} {
		s, _ := testStore(t)
		input := testGrant()
		g, err := s.Create(input)
		if err != nil {
			t.Fatal(err)
		}
		if running {
			if err := s.BeginPreparation(g.AttemptID, PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://fixture"}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.ReceiveFailure(g.AttemptID, []byte(`{"error":"preparation lease unavailable"}`)); err != nil {
			t.Fatal(err)
		}
		if err := s.RejectFailure(g.AttemptID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.RequestStop(g.AttemptID, "preparation_failed"); err != nil {
			t.Fatal(err)
		}
		if running {
			if err := s.ObserveStop(g.AttemptID, StopEvidence{PVCUID: g.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()}); err == nil {
				t.Fatal("failure delivery released a still-running preparation")
			}
			if _, err := s.Create(input); err == nil {
				t.Fatal("failed preparation acquired a competing writer before stopping")
			}
			if err := s.CompletePreparation(g.AttemptID, nil, nil); err != nil {
				t.Fatal("actual preparation completion could not settle an earlier failure", err)
			}
		}
		if err := s.ObserveStop(g.AttemptID, StopEvidence{PVCUID: g.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		if err := s.CloseStopped(g.AttemptID); err != nil {
			t.Fatal("failure with no live writer could not close", err)
		}
		if err := s.MarkCleaned(g.AttemptID); err != nil {
			t.Fatal(err)
		}
		_, err = s.Create(input)
		if running && !errors.Is(err, ErrStorageBusy) {
			t.Fatal("stopped incomplete preparation bypassed data recovery", err)
		}
		if !running && err != nil {
			t.Fatal("task with no preparation writes remained blocked", err)
		}
	}
}

func TestInterruptedPreparationRequiresItsContainerAndRetainsCheckpoint(t *testing.T) {
	s, options := testStore(t)
	input := testGrant()
	g, err := s.Create(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetResources(g.AttemptID, json.RawMessage(`{"podCreateRequested":false,"secretCreateRequested":false}`)); err != nil {
		t.Fatal(err)
	}
	process := PreparationProcess{PodName: "controller", PodUID: uuid.NewString(), ContainerID: "containerd://original"}
	if err := s.BeginPreparation(g.AttemptID, process); err != nil {
		t.Fatal(err)
	}
	checkpoint := Prepared{OwnerID: g.OwnerID, WorkspaceID: g.WorkspaceID, TaskID: g.TaskID, AgentID: g.AgentID, AttemptID: g.AttemptID, Generation: g.Generation, PVCUID: g.PVCUID, TaskRoot: g.TaskRoot, Provider: "codex", Executable: "/opt/tools/runner", RuntimeDigest: g.Fingerprint, ConfigurationDigest: g.RuntimeRef.ConfigurationDigest, CreatedAt: time.Now().UTC(), CleanupManifest: json.RawMessage(`{}`), AllowedLinks: map[string]string{}, Environment: NativeEnvironment{RootDir: g.TaskRoot, WorkDir: g.TaskRoot + "/workdir", MulticaConfigRoot: g.TaskRoot + "/multica-config", CodexHome: g.TaskRoot + "/codex-home"}}
	checkpoint.Digest = preparedDigest(checkpoint)
	if err := s.CheckpointPreparation(g.AttemptID, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := s.BindPod(g.AttemptID, g.PodName, uuid.NewString(), "node-a"); err == nil {
		t.Fatal("native checkpoint prematurely authorized a worker")
	}
	if _, err := s.ReceiveFailure(g.AttemptID, []byte(`{"error":"preparation interrupted"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.RejectFailure(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	wrong := process
	wrong.ContainerID = "containerd://replacement"
	if err := s.InterruptPreparation(g.AttemptID, wrong); err == nil {
		t.Fatal("another container released the preparation writer")
	}
	if _, err := s.Create(input); err == nil {
		t.Fatal("unproven interruption allowed another writer")
	}
	if err := s.InterruptPreparation(g.AttemptID, process); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RequestStop(g.AttemptID, "preparation_interrupted"); err != nil {
		t.Fatal(err)
	}
	if err := s.ObserveStop(g.AttemptID, StopEvidence{PVCUID: g.PVCUID, Kind: "no-worker", ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseStopped(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkCleaned(g.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Create(input); !errors.Is(err, ErrStorageBusy) {
		t.Fatal("interrupted checkout was reused without data recovery", err)
	}
	retained, err := s.PreviousPrepared(g.AttemptID)
	if err != nil || retained == nil || retained.Digest != checkpoint.Digest {
		t.Fatal("recovery lost the durable native initialization checkpoint", err)
	}
}
