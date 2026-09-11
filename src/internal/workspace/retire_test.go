package workspace

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type retirementFixture struct {
	Options Options
	Task    Observation
	Binding Binding
	Cutoff  time.Time
}

func prepareRetirementFixture(t *testing.T) (*Store, retirementFixture) {
	t.Helper()
	store, options := testStore(t)
	task := testObservation(testEnvironment("retirement"))
	approve(t, store, task)
	root := prepareRoot(t, options.WorkspaceRoot, task)
	_, binding, err := store.AuthorizeAndBind(task.ID, task.AuthToken, task.WorkspaceID, task.AgentID, root, "", task.RuntimeRef)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(options.WorkspaceRoot, binding.WorkerSubPath, "work"), []byte("retired work"))
	if err := store.locked(func() error {
		state, err := store.read()
		if err != nil {
			return err
		}
		old := state.Claims[task.ID]
		old.ObservedAt = time.Now().Add(-365 * 24 * time.Hour)
		state.Claims[task.ID] = old
		return store.write(state)
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	return store, retirementFixture{options, task, binding, time.Now().Add(-60 * 24 * time.Hour)}
}

func TestRetirementDoesNotOverwriteAuthorityCommittedDuringDeletion(t *testing.T) {
	store, fixture := prepareRetirementFixture(t)
	// A real deletion keeps the collector between quarantine and finalization
	// long enough to observe its lock handoff. No elapsed-time budget is asserted.
	storage := filepath.Join(fixture.Options.WorkspaceRoot, fixture.Binding.WorkerSubPath)
	for i := range 20000 {
		writeFile(t, filepath.Join(storage, fmt.Sprintf("pending-%d", i)), nil)
	}
	command, output, wait := startRetirementProcess(t, fixture, "collect")
	pauseRetirementAfterQuarantine(t, fixture.Options, command, output)
	if release, err := store.AcquireLease(filepath.Base(fixture.Binding.WorkerSubPath)); !errors.Is(err, ErrStorageBusy) {
		if release != nil {
			release()
		}
		t.Fatalf("retirement released its storage while deletion was unfinished: %v", err)
	}
	if _, err := store.Lookup(fixture.Task.ID, fixture.Task.AuthToken, fixture.Task.WorkspaceID, fixture.Task.AgentID); err == nil {
		t.Fatal("pending retirement retained executable authority")
	}
	if _, err := store.ObserveBatch([]Observation{fixture.Task}); err == nil {
		t.Fatal("reobservation revived a retiring task")
	}
	root := prepareRoot(t, fixture.Options.WorkspaceRoot, fixture.Task)
	if _, _, err := store.AuthorizeAndBind(fixture.Task.ID, fixture.Task.AuthToken, fixture.Task.WorkspaceID, fixture.Task.AgentID, root, "", fixture.Task.RuntimeRef); err == nil {
		t.Fatal("credentials read before retirement rebound retiring storage")
	}
	next := testObservation(fixture.Task.RuntimeRef)
	nextRoot := prepareRoot(t, fixture.Options.WorkspaceRoot, next)
	approve(t, store, next)
	_, nextBinding, err := store.AuthorizeAndBind(next.ID, next.AuthToken, next.WorkspaceID, next.AgentID, nextRoot, "", next.RuntimeRef)
	if err != nil {
		t.Fatal(err)
	}
	protected := filepath.Join(fixture.Options.WorkspaceRoot, nextBinding.WorkerSubPath, "work")
	writeFile(t, protected, []byte("concurrent private work"))
	if err := command.Process.Signal(unix.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := wait(); err != nil {
		t.Fatalf("retirement process failed: %v: %s", err, <-output)
	}
	reopened, err := Open(fixture.Options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Lookup(next.ID, next.AuthToken, next.WorkspaceID, next.AgentID); err != nil {
		t.Fatal("retirement overwrote concurrently committed authority", err)
	}
	assertData(t, protected, []byte("concurrent private work"))
	if _, err := reopened.ObserveBatch([]Observation{fixture.Task}); err == nil {
		t.Fatal("completed retirement lost its permanent deny")
	}
	if _, err := os.Stat(storage); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("retirement did not reclaim its inactive worker data", err)
	}
}

func TestRetirementResumesAfterProcessDeathWithoutRevivingAuthority(t *testing.T) {
	for _, boundary := range []string{"marked", "quarantined", "removed", "finalized"} {
		t.Run(boundary, func(t *testing.T) {
			_, fixture := prepareRetirementFixture(t)
			_, output, wait := startRetirementProcess(t, fixture, boundary)
			err := wait()
			if raw := <-output; err == nil || !bytes.Contains(raw, []byte("retirement boundary committed")) {
				t.Fatalf("retirement subprocess did not reach its crash boundary: %v: %s", err, raw)
			}
			store, err := Open(fixture.Options)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Lookup(fixture.Task.ID, fixture.Task.AuthToken, fixture.Task.WorkspaceID, fixture.Task.AgentID); err == nil {
				t.Fatal("process death restored retiring task authority")
			}
			if _, err := store.ObserveBatch([]Observation{fixture.Task}); err == nil {
				t.Fatal("process death allowed the retired task to be observed again")
			}
			root := prepareRoot(t, fixture.Options.WorkspaceRoot, fixture.Task)
			if _, _, err := store.AuthorizeAndBind(fixture.Task.ID, fixture.Task.AuthToken, fixture.Task.WorkspaceID, fixture.Task.AgentID, root, "", fixture.Task.RuntimeRef); err == nil {
				t.Fatal("retired credentials restored storage after process death")
			}
			next := testObservation(fixture.Task.RuntimeRef)
			nextRoot := prepareRoot(t, fixture.Options.WorkspaceRoot, next)
			approve(t, store, next)
			_, binding, err := store.AuthorizeAndBind(next.ID, next.AuthToken, next.WorkspaceID, next.AgentID, nextRoot, "", next.RuntimeRef)
			if err != nil {
				t.Fatal(err)
			}
			protected := filepath.Join(fixture.Options.WorkspaceRoot, binding.WorkerSubPath, "work")
			writeFile(t, protected, []byte("work committed after the crash"))
			_, output, wait = startRetirementProcess(t, fixture, "collect")
			if err := wait(); err != nil {
				t.Fatalf("retirement restart failed: %v: %s", err, <-output)
			}
			if _, err := store.Lookup(next.ID, next.AuthToken, next.WorkspaceID, next.AgentID); err != nil {
				t.Fatal("retirement restart overwrote new task authority", err)
			}
			assertData(t, protected, []byte("work committed after the crash"))
			if _, err := os.Stat(filepath.Join(fixture.Options.WorkspaceRoot, fixture.Binding.WorkerSubPath)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("retirement restart did not reclaim inactive worker data", err)
			}
			if _, err := store.ObserveBatch([]Observation{fixture.Task}); err == nil {
				t.Fatal("retirement restart lost the permanent deny")
			}
		})
	}
}

func TestExistingRetirementCannotRegainAuthorityBeforeCollection(t *testing.T) {
	store, fixture := prepareRetirementFixture(t)
	// Older controllers committed the tombstone before revoking credentials.
	// Reading that supported intermediate state must never renew its authority.
	if err := store.locked(func() error {
		state, err := store.read()
		if err != nil {
			return err
		}
		state.Retired[fixture.Binding.WorkerSubPath] = fixture.Task.ID
		return store.write(state)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Lookup(fixture.Task.ID, fixture.Task.AuthToken, fixture.Task.WorkspaceID, fixture.Task.AgentID); err == nil {
		t.Fatal("an existing retirement granted executable authority")
	}
	if _, err := store.ObserveBatch([]Observation{fixture.Task}); err == nil {
		t.Fatal("an existing retirement accepted a renewed task claim")
	}
	root := prepareRoot(t, fixture.Options.WorkspaceRoot, fixture.Task)
	if _, _, err := store.AuthorizeAndBind(fixture.Task.ID, fixture.Task.AuthToken, fixture.Task.WorkspaceID, fixture.Task.AgentID, root, "", fixture.Task.RuntimeRef); err == nil {
		t.Fatal("an existing retirement accepted a credential binding")
	}
	if _, err := store.Collect(fixture.Options.WorkspaceRoot, fixture.Cutoff, map[string]bool{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ObserveBatch([]Observation{fixture.Task}); err == nil {
		t.Fatal("resuming the existing retirement lost its permanent deny")
	}
}

func startRetirementProcess(t *testing.T, fixture retirementFixture, action string) (*exec.Cmd, <-chan []byte, func() error) {
	t.Helper()
	raw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "retirement-input.json")
	writeFile(t, input, raw)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRetirementProcess$")
	command.Env = append(os.Environ(), "MULTICA_TEST_RETIREMENT_INPUT="+input, "MULTICA_TEST_RETIREMENT_ACTION="+action)
	pipe, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = command.Stdout
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	output := make(chan []byte, 1)
	readDone := make(chan struct{})
	go func() {
		raw, _ := io.ReadAll(pipe)
		output <- raw
		close(readDone)
	}()
	waited := false
	wait := func() error {
		<-readDone
		waited = true
		return command.Wait()
	}
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			_ = wait()
		}
	})
	return command, output, wait
}

func pauseRetirementAfterQuarantine(t *testing.T, options Options, command *exec.Cmd, output <-chan []byte) {
	t.Helper()
	lock, err := openLock(filepath.Join(options.Directory, "registry.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case raw := <-output:
			t.Fatalf("retirement completed without a registry lock handoff while data was quarantined: %s", raw)
		case <-ticker.C:
		}
		entries, err := os.ReadDir(filepath.Join(options.Directory, "retired"))
		if errors.Is(err, os.ErrNotExist) || err == nil && len(entries) == 0 {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); errors.Is(err, unix.EWOULDBLOCK) {
			continue
		} else if err != nil {
			t.Fatal(err)
		}
		// Holding the registry lock here also prevents finalization racing the
		// stop signal. The resumed collector must reload our subsequent writes.
		err = command.Process.Signal(unix.SIGSTOP)
		if err == nil {
			var status unix.WaitStatus
			var pid int
			pid, err = unix.Wait4(command.Process.Pid, &status, unix.WUNTRACED, nil)
			if err == nil && (status.Exited() || status.Signaled()) {
				err = fmt.Errorf("retirement process exited before its pending work could be observed: pid=%d status=%d", pid, status)
			}
		}
		unlockErr := unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		if err != nil || unlockErr != nil {
			t.Fatal(errors.Join(err, unlockErr))
		}
		return
	}
}

func TestRetirementProcess(t *testing.T) {
	input := os.Getenv("MULTICA_TEST_RETIREMENT_INPUT")
	if input == "" {
		t.Skip("retirement subprocess")
	}
	raw, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	var fixture retirementFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	store, err := Open(fixture.Options)
	if err != nil {
		t.Fatal(err)
	}
	action := os.Getenv("MULTICA_TEST_RETIREMENT_ACTION")
	if action == "collect" {
		if _, err := store.Collect(fixture.Options.WorkspaceRoot, fixture.Cutoff, map[string]bool{}); err != nil {
			t.Fatal(err)
		}
		return
	}
	// Run the production durable phases in another process, then kill it
	// without releasing its lease or running deferred cleanup. Collect in a
	// fresh process must finish from exactly the filesystem state left behind.
	release, err := store.AcquireLease(filepath.Base(fixture.Binding.WorkerSubPath))
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	crash := func() {
		fmt.Println("retirement boundary committed")
		if err := unix.Kill(os.Getpid(), unix.SIGKILL); err != nil {
			t.Fatal(err)
		}
		select {}
	}
	var pending retirement
	err = store.locked(func() error {
		state, err := store.read()
		if err != nil {
			return err
		}
		pending, err = store.markRetirement(state, fixture.Binding.WorkerSubPath)
		if err != nil {
			return err
		}
		if action == "marked" {
			crash()
		}
		return store.quarantineRetirement(pending)
	})
	if err != nil {
		t.Fatal(err)
	}
	if action == "quarantined" {
		crash()
	}
	if err := store.removeRetiredData(pending); err != nil {
		t.Fatal(err)
	}
	if action == "removed" {
		crash()
	}
	if err := store.finalizeRetirement(pending); err != nil {
		t.Fatal(err)
	}
	if action == "finalized" {
		crash()
	}
	t.Fatal("unknown retirement subprocess action")
}
