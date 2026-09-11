package execution

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestAttemptMonitorListenerPreservesLiveOwnerAndRecoversAfterCrash(t *testing.T) {
	// Keep the pathname below the Unix socket limit on macOS as well as Linux.
	directory, err := os.MkdirTemp("/tmp", "attempt-listener-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "monitor.sock")
	first, err := ListenAttemptMonitor(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if duplicate, err := ListenAttemptMonitor(path); err == nil {
		duplicate.Close()
		t.Fatal("a second controller replaced the live attempt monitor")
	}
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal("refusing a duplicate controller disrupted the existing monitor", err)
	}
	connection.Close()

	// A process killed without Close leaves the socket inode behind.
	first.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := ListenAttemptMonitor(path)
	if err != nil {
		t.Fatal("a stale socket prevented controller recovery", err)
	}
	defer restarted.Close()
	connection, err = net.Dial("unix", path)
	if err != nil {
		t.Fatal("shims cannot reach the restarted controller", err)
	}
	connection.Close()
}
