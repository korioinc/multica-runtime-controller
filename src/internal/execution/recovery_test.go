package execution

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

func TestRecoveryLeaseFailurePreservesPendingStorage(t *testing.T) {
	for _, busy := range []bool{true, false} {
		name := "invalid lock inode"
		if busy {
			name = "active execution"
		}
		t.Run(name, func(t *testing.T) {
			j, a := journalAttempt(t)
			if err := j.save(a); err != nil {
				t.Fatal(err)
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			options := workspace.Options{Directory: filepath.Join(root, ".multica-runtime/state"), WorkspaceRoot: root, SessionRoot: filepath.Join(root, "sessions"), OwnerID: j.owner}
			store, err := workspace.Open(options)
			if err != nil {
				t.Fatal(err)
			}
			if busy {
				release, err := store.AcquireLease(a.Ref.StorageID)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
			} else if err := os.Mkdir(filepath.Join(options.Directory, "worker-"+a.Ref.StorageID+".lock"), 0700); err != nil {
				t.Fatal(err)
			}
			runner := &Runner{store: store, journal: j}
			active, err := runner.Reconcile(context.Background())
			if busy && err != nil {
				t.Fatal("a live execution was reported as failed recovery", err)
			}
			if !busy && err == nil {
				t.Fatal("an inaccessible lease was reported as successful recovery")
			}
			if !active[a.Ref.StorageID] {
				t.Fatal("unreconciled storage became eligible for retirement")
			}
			recovered, err := j.read(a.Ref.AttemptID)
			if err != nil || !recovered.Ref.RuntimeRef.Equal(a.Ref.RuntimeRef) {
				t.Fatal("lease failure lost durable recovery authority", err)
			}
		})
	}
}
