//go:build linux

package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/pkg/agent"
	"golang.org/x/sys/unix"
)

const nativeIssueContinuity = "The previous issue context could not be restored. Read the issue and comments before continuing.\n\n"
const nativeDMContinuity = "The previous private chat could not be restored. Explain the missing conversation context.\n\n"

func TestNativeClaudeUsesManagedSettingsAndOnlyTheCurrentResumePrompt(t *testing.T) {
	f := newNativeProtocolFixture(t, "claude")
	policy := filepath.Join(f.root, "managed-claude-settings.json")
	if err := os.WriteFile(policy, []byte(`{"permissions":{"deny":["Skill(private-operator)"]}}`), 0600); err != nil {
		t.Fatal(err)
	}
	first := f.execute(t, "complete", "first Claude turn", agent.ExecOptions{ClaudeSettingsPath: policy, ThinkingLevel: "high"})
	second := f.execute(t, "complete", "current Claude follow-up", agent.ExecOptions{ClaudeSettingsPath: policy,
		ResumeSessionID: first.SessionID, ResumeExpected: true, ThinkingLevel: "high"})
	observations := f.observations(t)
	if first.Status != "completed" || second.Status != "completed" || first.SessionID != f.threadID || second.SessionID != first.SessionID ||
		f.calls != 2 || countNativeEvents(observations, "spawn") != 2 {
		t.Fatal("managed Claude execution lost its native result or duplicated execution")
	}
	var prompts []string
	var launches [][]string
	for _, observation := range observations {
		if observation.Event == "spawn" {
			launches = append(launches, observation.Arguments)
		}
		if observation.Event == "prompt" {
			prompts = append(prompts, observation.Prompt)
		}
	}
	for _, arguments := range launches {
		joined := strings.Join(arguments, " ")
		if !strings.Contains(joined, "--settings "+policy) || !strings.Contains(joined, "--effort high") {
			t.Fatal("the pinned SDK did not consume the managed per-turn skill policy and effort")
		}
	}
	if len(prompts) != 2 || prompts[0] != "first Claude turn" || prompts[1] != "current Claude follow-up" ||
		!strings.Contains(strings.Join(launches[1], " "), "--resume "+first.SessionID) {
		t.Fatal("Claude resume lost its managed ID or replayed an earlier task prompt")
	}
}

// These executables implement controlled provider protocols. The actual pinned
// SDK owns process launch, resume, fallback, event parsing, and its one-shot result.
func TestNativeCodexResumeSendsOnlyTheCurrentTurn(t *testing.T) {
	f := newNativeProtocolFixture(t, "codex")
	firstPrompt, secondPrompt := "first-turn transcript-only sentinel", "current follow-up message"
	first := f.execute(t, "complete", firstPrompt, agent.ExecOptions{})
	second := f.execute(t, "complete", secondPrompt, agent.ExecOptions{ResumeSessionID: first.SessionID,
		ResumeExpected: true, ResumeContinuityNotice: nativeIssueContinuity})
	requests := f.observations(t)
	if first.Status != "completed" || second.Status != "completed" || first.SessionID == "" || second.SessionID != first.SessionID ||
		f.calls != 2 || countNativeEvents(requests, "spawn") != 2 || len(nativeRPCs(requests, "thread/start")) != 1 {
		t.Fatal("sequential native turns did not use one SDK call and process per turn")
	}
	resumes, turns := nativeRPCs(requests, "thread/resume"), nativeRPCs(requests, "turn/start")
	if len(resumes) != 1 || resumes[0].ThreadID != first.SessionID || len(turns) != 2 ||
		len(turns[0].Input) != 1 || turns[0].Input[0].Text != firstPrompt || len(turns[1].Input) != 1 || turns[1].Input[0].Text != secondPrompt {
		t.Fatal("resume replayed an old prompt or duplicated the current message")
	}
	f.logProof(t, second)
}

func TestNativeCodexResumeFallbackRemainsOneExecution(t *testing.T) {
	for _, scenario := range []struct {
		mode          string
		prior         bool
		status        string
		starts, turns int
		rejected      bool
	}{
		{mode: "resume_rejected", prior: true, status: "completed", starts: 1, turns: 1},
		{mode: "resume_without_id", prior: true, status: "completed", starts: 1, turns: 1},
		{mode: "history_unavailable", status: "completed", starts: 1, turns: 1},
		{mode: "start_without_id", prior: true, status: "failed", starts: 1},
		{mode: "exit_during_resume", prior: true, status: "failed"},
		{mode: "resume_overflow", prior: true, status: "failed", rejected: true},
		{mode: "no_turn_result", prior: true, status: "failed", turns: 1},
		{mode: "completed_without_rollout", status: "completed", starts: 1, turns: 1},
	} {
		t.Run(scenario.mode, func(t *testing.T) {
			f := newNativeProtocolFixture(t, "codex")
			const prompt = "perform the current task exactly once"
			options := agent.ExecOptions{ResumeExpected: true, ResumeContinuityNotice: nativeIssueContinuity}
			if scenario.prior {
				options.ResumeSessionID = uuid.NewString()
			} else {
				options.ResumeContinuityNotice = nativeDMContinuity
			}
			result := f.execute(t, scenario.mode, prompt, options)
			observations := f.observations(t)
			turns := nativeRPCs(observations, "turn/start")
			if f.calls != 1 || countNativeEvents(observations, "spawn") != 1 || len(nativeRPCs(observations, "thread/start")) != scenario.starts ||
				len(turns) != scenario.turns || result.Status != scenario.status || result.ResumeRejected != scenario.rejected || result.ResumeRejectedTransient {
				t.Fatalf("one-shot native outcome changed: %+v", result)
			}
			resumes := nativeRPCs(observations, "thread/resume")
			if scenario.prior && (len(resumes) != 1 || resumes[0].ThreadID != options.ResumeSessionID) || !scenario.prior && len(resumes) != 0 {
				t.Fatal("the SDK changed or repeated the requested native resume")
			}
			if result.Status == "completed" {
				if result.Output != "actual native result" || result.SessionID != f.threadID ||
					len(turns[0].Input) != 1 || turns[0].Input[0].Text != options.ResumeContinuityNotice+prompt ||
					strings.Count(turns[0].Input[0].Text, prompt) != 1 {
					t.Fatal("fallback lost its actual completed result or duplicated its prompt/notice")
				}
			}
			if scenario.turns == 0 && result.SessionID != "" {
				t.Fatal("failed thread setup published a usable native pointer")
			}
			if scenario.mode == "completed_without_rollout" {
				if _, err := os.Stat(filepath.Join(f.root, "codex-home/sessions/rollout-fixture-"+result.SessionID+".jsonl")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("fixture unexpectedly created a reusable rollout", err)
				}
			}
			f.logProof(t, result)
		})
	}
}

func TestNativePiResumeDistinguishesLockAndDefinitiveRefusal(t *testing.T) {
	for _, scenario := range []string{"resumed", "locked", "missing_stored_cwd", "empty_rollout"} {
		t.Run(scenario, func(t *testing.T) {
			f := newNativeProtocolFixture(t, "pi")
			path := filepath.Join(f.root, "pi-sessions/native.jsonl")
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			cwd := f.root
			if scenario == "missing_stored_cwd" {
				cwd = filepath.Join(f.root, "removed-workdir")
			}
			header, _ := json.Marshal(map[string]string{"type": "session", "id": uuid.NewString(), "cwd": cwd})
			if scenario != "empty_rollout" {
				if err := os.WriteFile(path, append(header, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "locked" {
				locked, err := os.OpenFile(path, os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer locked.Close()
				if err := unix.Flock(int(locked.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
					t.Fatal(err)
				}
				defer unix.Flock(int(locked.Fd()), unix.LOCK_UN)
			}
			const prompt = "one Pi follow-up message"
			result := f.execute(t, scenario, prompt, agent.ExecOptions{ResumeSessionID: path, ResumeExpected: true,
				ResumeContinuityNotice: nativeDMContinuity})
			observations := f.observations(t)
			starts := countNativeEvents(observations, "spawn")
			if f.calls != 1 {
				t.Fatal("Pi was executed again after native resume rejection")
			}
			switch scenario {
			case "locked":
				if starts != 0 || result.Status != "failed" || !result.ResumeRejectedTransient || result.ResumeRejected || result.SessionID != "" {
					t.Fatal("a live transcript lock started another provider or retired the session")
				}
			case "missing_stored_cwd":
				if starts != 1 || result.Status != "failed" || !result.ResumeRejected || result.ResumeRejectedTransient || result.SessionID != path ||
					countNativeEvents(observations, "prompt") != 0 {
					t.Fatal("definitive native refusal became a retry or transient lock")
				}
			case "resumed":
				if starts != 1 || result.Status != "completed" || result.SessionID != path || result.Output != "actual native result" ||
					result.ResumeRejected || result.ResumeRejectedTransient || countNativeEvents(observations, "prompt") != 1 {
					t.Fatal("normal Pi resume changed its result or repeated the current turn")
				}
				for _, observation := range observations {
					if observation.Event == "prompt" && observation.Prompt != prompt {
						t.Fatal("Pi current prompt changed")
					}
				}
			case "empty_rollout":
				// The unchanged SDK reports its actual zero-exit outcome. A
				// controller must validate this preallocated file before reuse.
				contents, err := os.ReadFile(path)
				if starts != 1 || result.Status != "completed" || result.SessionID != path || result.Output != "" || err != nil || len(contents) != 0 {
					t.Fatal("empty rollout fixture did not expose the native validation boundary", err)
				}
			}
			if scenario == "locked" || scenario == "missing_stored_cwd" {
				contents, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(contents, append(header, '\n')) {
					t.Fatal("resume rejection rewrote the existing transcript", err)
				}
			}
			f.logProof(t, result)
		})
	}
}

type nativeProtocolFixture struct {
	root, executable, audit, provider, threadID string
	calls                                       int
}

func newNativeProtocolFixture(t *testing.T, provider string) *nativeProtocolFixture {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("native protocol fixtures require Python 3", err)
	}
	root := t.TempDir()
	f := &nativeProtocolFixture{root: root, executable: filepath.Join(root, provider), audit: filepath.Join(root, "protocol.jsonl"), provider: provider, threadID: uuid.NewString()}
	if err := os.WriteFile(f.executable, []byte("#!"+python+"\n"+nativeProviderProtocol), 0700); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *nativeProtocolFixture) execute(t *testing.T, mode, prompt string, options agent.ExecOptions) agent.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	backend, err := agent.New(f.provider, agent.Config{ExecutablePath: f.executable, BuiltinRuntime: true,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Env: map[string]string{"CODEX_HOME": filepath.Join(f.root, "codex-home"),
			"IS_SANDBOX": "1", "NATIVE_PROTOCOL_PROVIDER": f.provider, "NATIVE_PROTOCOL_MODE": mode, "NATIVE_PROTOCOL_AUDIT": f.audit, "NATIVE_PROTOCOL_THREAD": f.threadID}})
	if err != nil {
		t.Fatal(err)
	}
	options.Cwd, options.Model, options.Timeout = f.root, "fixture-model", 20*time.Second
	options.HandshakeTimeout, options.ThreadHandshakeTimeout, options.SemanticInactivityTimeout = 3*time.Second, 3*time.Second, 3*time.Second
	f.calls++
	session, err := backend.Execute(ctx, prompt, options)
	if err != nil {
		t.Fatal("the actual SDK did not return its native session", err)
	}
	for range session.Messages {
	}
	result, ok := <-session.Result
	if !ok {
		t.Fatal("the actual SDK closed without a result")
	}
	if _, another := <-session.Result; another {
		t.Fatal("the SDK published more than one result")
	}
	return result
}

type nativeProtocolObservation struct {
	Event     string   `json:"event"`
	Prompt    string   `json:"prompt"`
	Arguments []string `json:"argv"`
	Request   struct {
		Method string               `json:"method"`
		Params nativeProtocolParams `json:"params"`
	} `json:"request"`
}

func (f *nativeProtocolFixture) observations(t *testing.T) []nativeProtocolObservation {
	t.Helper()
	file, err := os.Open(f.audit)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var observations []nativeProtocolObservation
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var observation nativeProtocolObservation
		if err := json.Unmarshal(scanner.Bytes(), &observation); err != nil {
			t.Fatal(err)
		}
		observations = append(observations, observation)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return observations
}

func nativeRPCs(observations []nativeProtocolObservation, method string) []nativeProtocolParams {
	var params []nativeProtocolParams
	for _, observation := range observations {
		if observation.Request.Method == method {
			params = append(params, observation.Request.Params)
		}
	}
	return params
}

type nativeProtocolParams struct {
	ThreadID string                        `json:"threadId"`
	Input    []struct{ Type, Text string } `json:"input"`
}

func countNativeEvents(observations []nativeProtocolObservation, event string) int {
	count := 0
	for _, observation := range observations {
		if observation.Event == event {
			count++
		}
	}
	return count
}

func (f *nativeProtocolFixture) logProof(t *testing.T, result agent.Result) {
	t.Helper()
	observations := f.observations(t)
	proof, _ := json.Marshal(map[string]any{"provider": f.provider, "execute_calls": f.calls, "processes": countNativeEvents(observations, "spawn"),
		"thread_resumes": len(nativeRPCs(observations, "thread/resume")), "thread_starts": len(nativeRPCs(observations, "thread/start")),
		"turn_starts": len(nativeRPCs(observations, "turn/start")), "status": result.Status, "session_id": result.SessionID,
		"resume_rejected": result.ResumeRejected, "resume_rejected_transient": result.ResumeRejectedTransient})
	t.Log(string(proof))
}

const nativeProviderProtocol = `import json,os,sys
mode=os.environ['NATIVE_PROTOCOL_MODE']
provider=os.environ['NATIVE_PROTOCOL_PROVIDER']
def audit(value):
 with open(os.environ['NATIVE_PROTOCOL_AUDIT'],'a') as file:
  file.write(json.dumps(value)+'\n')
def send(value):
 print(json.dumps(value),flush=True)
audit({'event':'spawn','mode':mode,'argv':sys.argv[1:]})
if provider=='claude':
 thread=os.environ['NATIVE_PROTOCOL_THREAD']
 if '--settings' in sys.argv:
  with open(sys.argv[sys.argv.index('--settings')+1]) as file:json.load(file)
 if '--resume' in sys.argv:thread=sys.argv[sys.argv.index('--resume')+1]
 send({'type':'system','subtype':'init','session_id':thread})
 request=json.loads(sys.stdin.readline())
 content=request['message']['content']
 if not isinstance(content,str):content=''.join(block.get('text','') for block in content)
 audit({'event':'prompt','prompt':content})
 send({'type':'assistant','message':{'role':'assistant','model':'fixture-model','content':[{'type':'text','text':'actual native result'}]}})
 send({'type':'result','subtype':'success','session_id':thread,'result':'actual native result','is_error':False})
 sys.exit(0)
if provider=='pi':
 path=sys.argv[sys.argv.index('--session')+1]
 if mode=='missing_stored_cwd':
  with open(path) as file:header=json.loads(file.readline())
  if not os.path.isdir(header['cwd']):
   print('Stored session working directory does not exist: '+header['cwd'],file=sys.stderr,flush=True)
   sys.exit(1)
 prompt=sys.stdin.read()
 audit({'event':'prompt','prompt':prompt})
 if mode=='empty_rollout':sys.exit(0)
 send({'type':'agent_start'})
 send({'type':'turn_start'})
 send({'type':'message_update','assistantMessageEvent':{'type':'text_delta','delta':'actual native result'}})
 send({'type':'turn_end','message':{'role':'assistant','model':'fixture-model','stopReason':'stop','usage':{'input':1,'output':1}}})
 sys.exit(0)
thread=os.environ['NATIVE_PROTOCOL_THREAD']
for line in sys.stdin:
 request=json.loads(line)
 audit({'event':'rpc','request':request})
 if 'id' not in request:continue
 method=request.get('method')
 result={}
 if method=='initialize':result={'userAgent':'controller-native-resume-proof'}
 if method=='thread/resume':
  if mode=='exit_during_resume':sys.exit(2)
  if mode=='resume_overflow':
   sys.stdout.write('{"id":'+str(request['id'])+',"result":{"history":"')
   for unused in range(64):sys.stdout.write('x'*(1024*1024));sys.stdout.flush()
   sys.stdout.write('"}}\n');sys.stdout.flush()
   continue
  if mode in ('resume_rejected','start_without_id'):
   send({'id':request['id'],'error':{'code':-32602,'message':'thread not found'}})
   continue
  if mode=='resume_without_id':result={'thread':{}}
  else:thread=request['params']['threadId'];result={'thread':{'id':thread}}
 if method=='thread/start':
  thread=os.environ['NATIVE_PROTOCOL_THREAD']
  result={'thread':{}} if mode=='start_without_id' else {'thread':{'id':thread}}
 if method=='turn/start':result={'turn':{'id':'native-turn','status':'inProgress'}}
 send({'id':request['id'],'result':result})
 if method=='turn/start':
  send({'method':'turn/started','params':{'threadId':thread,'turn':{'id':'native-turn','status':'inProgress'}}})
  if mode=='no_turn_result':sys.exit(2)
  if mode!='completed_without_rollout':
   directory=os.path.join(os.environ['CODEX_HOME'],'sessions')
   os.makedirs(directory,exist_ok=True)
   with open(os.path.join(directory,'rollout-fixture-'+thread+'.jsonl'),'a') as file:
    file.write(json.dumps({'type':'session_meta','payload':{'id':thread,'cwd':os.getcwd()}})+'\n')
  send({'method':'item/completed','params':{'threadId':thread,'turnId':'native-turn','item':{'id':'answer','type':'agentMessage','text':'actual native result','phase':'final_answer'}}})
  send({'method':'turn/completed','params':{'threadId':thread,'turn':{'id':'native-turn','status':'completed'}}})
`
