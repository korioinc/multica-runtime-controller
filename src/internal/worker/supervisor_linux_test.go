package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Run this test binary as PID 1 in an isolated Linux container. It deliberately
// kills every other process in that namespace, like the production supervisor.
func TestPID1SealStopsLateChildWriters(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1")
	}
	root := t.TempDir()
	path := filepath.Join(root, "late-write")
	command := exec.Command("/bin/sh", "-c", `trap '(trap "" TERM; while :; do echo late >> "$1"; sleep 0.01; done) & exit 0' TERM; echo ready > "$1"; while :; do sleep 1; done`, "writer", path)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer command.Process.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		if _, err := os.Stat(path); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("writer never started")
		case <-time.After(time.Millisecond):
		}
	}
	if err := stopWriters(ctx, 100*time.Millisecond, nil, nil); err != nil {
		t.Fatal(err)
	}
	if count, err := writerCount(); err != nil || count != 0 {
		t.Fatal("writers survive seal", count, err)
	}
	if err := workspace.SyncTaskFilesystem(root); err != nil {
		t.Fatal(err)
	}
	// Deny syncfs in this disposable process's kernel filter, proving the real
	// syscall error path rather than replacing the implementation with a mock.
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_SYNCFS, Jf: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
	}
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, unix.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&program))); errno != 0 {
		t.Fatal(errno)
	}
	if err := workspace.SyncTaskFilesystem(root); err == nil {
		t.Fatal("failed filesystem flush was accepted")
	}
}
