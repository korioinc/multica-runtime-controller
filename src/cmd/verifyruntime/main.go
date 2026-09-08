// verifyruntime is the disposable backend, provider and driver used to exercise
// the installed chart with real official daemons and Kubernetes task workers.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/runtimeimage"
)

type runRequest struct {
	Case          string `json:"case"`
	TaskID        string `json:"taskID,omitempty"`
	PriorTaskID   string `json:"priorTaskID,omitempty"`
	Scope         string `json:"scope"`
	Transport     string `json:"transport"`
	Hold          bool   `json:"hold,omitempty"`
	HoldAfterWork bool   `json:"holdAfterWork,omitempty"`
	CacheOverride string `json:"cacheOverride,omitempty"`
}

type providerResult struct {
	TaskID                string           `json:"taskID"`
	Case                  string           `json:"case"`
	Stage                 string           `json:"stage"`
	WorkDir               string           `json:"workDir"`
	Repository            string           `json:"repository"`
	Branch                string           `json:"branch"`
	Session               string           `json:"session"`
	Storage               string           `json:"storage"`
	RuntimeRef            runtimeimage.Ref `json:"runtimeRef"`
	PriorWork             bool             `json:"priorWork"`
	PriorWorkDigest       string           `json:"priorWorkDigest"`
	PriorSession          bool             `json:"priorSession"`
	ContinuityNotice      bool             `json:"continuityNotice"`
	ROChecked             bool             `json:"readOnlyChecked"`
	WritableChecked       bool             `json:"writableChecked"`
	IsolationChecked      bool             `json:"isolationChecked"`
	RepeatCheckoutChecked bool             `json:"repeatCheckoutChecked"`
	ScopeChecked          bool             `json:"scopeChecked"`
	CachePath             string           `json:"cachePath"`
	CacheChecked          bool             `json:"cacheChecked"`
	CoreHash              string           `json:"coreHash"`
	SessionDigest         string           `json:"sessionDigest"`
	ModifiedDigest        string           `json:"modifiedDigest"`
	Request               json.RawMessage  `json:"request,omitempty"`
	Error                 string           `json:"error,omitempty"`
}

type taskRecord struct {
	Epoch      string          `json:"epoch"`
	Input      runRequest      `json:"input"`
	Claim      map[string]any  `json:"claim"`
	Transport  string          `json:"transport,omitempty"`
	Injected   bool            `json:"injected,omitempty"`
	Released   bool            `json:"released,omitempty"`
	Provider   *providerResult `json:"provider,omitempty"`
	Checkpoint *providerResult `json:"checkpoint,omitempty"`
	Completion map[string]any  `json:"completion,omitempty"`
	Failure    map[string]any  `json:"failure,omitempty"`
}

type backendState struct {
	Tasks           map[string]*taskRecord `json:"tasks"`
	Pending         string                 `json:"pending,omitempty"`
	Baseline        string                 `json:"baseline,omitempty"`
	Held            string                 `json:"held,omitempty"`
	CleanupTask     string                 `json:"cleanupTask,omitempty"`
	InterruptedTask string                 `json:"interruptedTask,omitempty"`
	JournalTask     string                 `json:"journalTask,omitempty"`
	LastEnvironment string                 `json:"lastEnvironment,omitempty"`
	Error           string                 `json:"error,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "backend, provider or drive mode is required")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	var err error
	switch os.Args[1] {
	case "access":
		flags := flag.NewFlagSet("access", flag.ExitOnError)
		requestFile := flags.String("request-file", "", "")
		_ = flags.Parse(os.Args[2:])
		err = verifyAccess(ctx, *requestFile)
	case "backend":
		flags := flag.NewFlagSet("backend", flag.ExitOnError)
		listen := flags.String("listen", ":18080", "local backend bind address")
		origin := flags.String("origin", "http://fixture-backend:18080", "container-visible fixture origin")
		evidence := flags.String("evidence", "/evidence", "fixture evidence directory")
		_ = flags.Parse(os.Args[2:])
		err = runBackend(ctx, *listen, *origin, *evidence)
	case "provider":
		err = runProvider(ctx, os.Args[2:])
	case "drive":
		flags := flag.NewFlagSet("drive", flag.ExitOnError)
		backend := flags.String("backend", "http://127.0.0.1:18080", "reachable fixture backend")
		phase := flags.String("phase", "baseline", "baseline, changed, hold, release, cleanup-failure, recovered, interrupted-start or interrupted-resume")
		_ = flags.Parse(os.Args[2:])
		limit, stop := context.WithTimeout(ctx, 12*time.Minute)
		defer stop()
		err = drive(limit, strings.TrimRight(*backend, "/"), *phase)
	default:
		err = errors.New("unsupported fixture mode")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "runtime fixture:", err)
		os.Exit(1)
	}
}

func fixtureOnly() error {
	if os.Getenv("LOCALVERIFY_DISPOSABLE_CLUSTER") != "true" {
		return errors.New("requires an explicitly disposable local cluster")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func decodeRequest(w http.ResponseWriter, r *http.Request, value any) error {
	return json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(value)
}
func readBody(response *http.Response, value any) error {
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("fixture request failed: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(raw)))
	}
	return json.Unmarshal(raw, value)
}
