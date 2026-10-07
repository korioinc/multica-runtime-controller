package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/initprocess"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
	"github.com/multica-ai/multica/server/pkg/agent"
	"golang.org/x/sys/unix"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "init" {
		os.Exit(initprocess.Run(os.Args[2:]))
	}
	if len(os.Args) == 4 && os.Args[1] == "worker" && os.Args[2] == "run" {
		if strings.HasPrefix(filepath.Base(os.Args[3]), "fixture-result-") {
			os.Exit(runMissingResultFixture(os.Args[3]))
		}
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer cancel()
		if RunProvider(ctx, os.Args[3]) != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) == 4 && os.Args[1] == "worker" && os.Args[2] == "desktop" {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer cancel()
		if RunDesktop(ctx, os.Args[3]) != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	if len(os.Args) >= 3 && os.Args[1] == "worker" && os.Args[2] == "chrome" {
		if Chrome(context.Background(), os.Args[3:]) != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestPID1ProviderRunnerRetainsOutcomeAndReapsOrphans(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1 and Python")
	}
	t.Setenv("PARENT_PRIVATE_PROOF", "must-not-cross-runner-boundary")
	request := runnerFixtureRequest(t, false)
	runner := startRunnerFixture(t, request)
	var transcript strings.Builder
	for message := range runner.Session.Messages {
		if message.Type == agent.MessageText {
			transcript.WriteString(message.Content)
		}
	}
	result, ok := <-runner.Session.Result
	want := request.Environment["PROVIDER_PROOF"]
	if !ok || result.Status != "completed" || result.Output != want || transcript.String() != want {
		t.Fatal("official provider outcome or private execution boundary changed")
	}
	select {
	case <-runner.Done:
	case <-t.Context().Done():
		t.Fatal("provider runner did not join")
	}
	if !runner.Reaped() {
		t.Fatal("SDK runner lost its direct child's wait ownership")
	}
	pid := runnerFixturePID(t, request.Options.Cwd)
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("completed SDK runner left its exited descendant unreaped")
	}
}

func TestPID1ProviderRunnerCancellationJoinsBeforeNamespaceReap(t *testing.T) {
	if os.Getpid() != 1 {
		t.Skip("requires isolated Linux PID 1 and Python")
	}
	request := runnerFixtureRequest(t, true)
	runner := startRunnerFixture(t, request)
	results := make(chan agent.Result, 1)
	go func() {
		for range runner.Session.Messages {
		}
		if result, ok := <-runner.Session.Result; ok {
			results <- result
		}
		close(results)
	}()
	pid := runnerFixturePID(t, request.Options.Cwd)
	runner.Cancel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := stopWriters(ctx, 100*time.Millisecond, runner, nil); err != nil {
		t.Fatal(err)
	}
	if !runner.Reaped() {
		t.Fatal("namespace reaping stole the supervised child's wait status")
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancelled provider left a late writer alive")
	}
	for result := range results {
		if result.Status == "completed" {
			t.Fatal("cancellation manufactured a completed provider outcome")
		}
	}
}

func startRunnerFixture(t *testing.T, request runnerRequest) *providerRunner {
	t.Helper()
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(wire.ControlRoot, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	t.Cleanup(cancel)
	runner, err := startRunner(ctx, request, nil)
	if runner != nil {
		t.Cleanup(func() {
			runner.Cancel()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = stopWriters(ctx, 50*time.Millisecond, runner, nil)
			runner.Close()
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func runnerFixturePID(t *testing.T, root string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(filepath.Join(root, "descendant.pid"))
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err == nil && pid > 1 {
				return pid
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("provider did not create its controlled descendant")
	return 0
}

func runnerFixtureRequest(t *testing.T, wait bool) runnerRequest {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "codex")
	if err := os.WriteFile(path, []byte(runnerCodexFixture), 0700); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{"CODEX_HOME": filepath.Join(root, "codex-home"), "PROVIDER_PROOF": "provider-input-preserved"}
	if wait {
		environment["PROVIDER_WAIT"] = "1"
	}
	return runnerRequest{Provider: "codex", ExecutablePath: path, CLIVersion: "0.154.0", Environment: environment,
		Prompt: "provider proof", Options: agent.ExecOptions{Cwd: root, Model: "fixture", Timeout: 20 * time.Second}}
}

const runnerCodexFixture = `#!/usr/bin/python3
import json,os,signal,struct,sys,time
if '--version' in sys.argv:print('codex-cli 0.154.0');sys.exit(0)
# A leaked supervisor socket would let a provider forge an authenticated result.
for fd in os.listdir('/proc/self/fd'):
 try:
  if int(fd)>2 and os.readlink('/proc/self/fd/'+fd).startswith('socket:'):
   forged=json.dumps({'kind':'result','result':{'status':'completed','output':'forged by provider'}}).encode()
   os.write(int(fd),struct.pack('!I',len(forged))+forged)
 except OSError:pass
def send(x):print(json.dumps(x),flush=True)
wait=os.getenv('PROVIDER_WAIT')=='1'
late_write=os.getenv('PROVIDER_LATE_WRITE','')
complete_barrier=os.getenv('PROVIDER_COMPLETE_BARRIER','')
def descendant():
 child=os.fork()
 if child==0:
  for fd in (0,1,2):
   try:os.close(fd)
   except OSError:pass
  if wait or late_write:
   os.setsid();signal.signal(signal.SIGTERM,signal.SIG_IGN)
   with open('descendant.ready','w') as f:f.write('ready')
   while True:
    if late_write:
     with open(late_write,'a') as f:f.write('earlier-turn\n')
    time.sleep(.01)
  time.sleep(.03);os._exit(0)
 if wait:
  while not os.path.exists('descendant.ready'):time.sleep(.001)
 with open('descendant.pid','w') as f:f.write(str(child))
for line in sys.stdin:
 x=json.loads(line);method=x.get('method')
 if 'id' not in x:continue
 result={}
 if method=='initialize':result={'userAgent':'runner-proof'}
 if method in ('thread/start','thread/resume'):result={'thread':{'id':'proof-thread'}}
 if method=='turn/start':result={'turn':{'id':'proof-turn','status':'inProgress'}}
 send({'id':x['id'],'result':result})
 if method=='turn/start':
  send({'method':'turn/started','params':{'threadId':'proof-thread','turn':{'id':'proof-turn','status':'inProgress'}}})
  if wait:descendant();continue
  if complete_barrier:
   descendant()
   while not os.path.exists(complete_barrier):time.sleep(.001)
  answer=os.getenv('PROVIDER_PROOF','missing')+os.getenv('PARENT_PRIVATE_PROOF','')
  send({'method':'item/completed','params':{'threadId':'proof-thread','turnId':'proof-turn','item':{'id':'answer','type':'agentMessage','text':answer,'phase':'final_answer'}}})
  send({'method':'turn/completed','params':{'threadId':'proof-thread','turn':{'id':'proof-turn','status':'completed'}}})
 if method=='turn/interrupt':
  send({'method':'turn/completed','params':{'threadId':'proof-thread','turn':{'id':'proof-turn','status':'cancelled'}}})
if not wait and not complete_barrier:descendant()
os._exit(0)
`
