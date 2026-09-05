package intercept

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBrokerDoesNotDiscloseOtherCachedRepositoriesOrOtherLaunchData(t *testing.T) {
	const ownURL = "https://example.test/current/repo"
	const ownData = "current task's private source bytes"
	const otherData = "stale other task's cached source bytes"
	// This upstream models the official broader workspace authorization: both
	// the assigned repo and an older cached workspace repo can be returned.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			URL     string `json:"url"`
			WorkDir string `json:"workdir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		repo := filepath.Join(request.WorkDir, "repo")
		if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o700); err != nil {
			t.Error(err)
			return
		}
		data := otherData
		if request.URL == ownURL {
			data = ownData
		}
		if err := os.WriteFile(filepath.Join(repo, "private-source"), []byte(data), 0o600); err != nil {
			t.Error(err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"path": repo, "branch_name": "fixture"})
	}))
	defer upstream.Close()
	broker := &checkoutBroker{request: Request{WorkDir: t.TempDir(), RepositoryURLs: []string{ownURL}, Env: []string{"MULTICA_TASK_ID=task", "MULTICA_WORKSPACE_ID=workspace", "MULTICA_TOKEN=mat_fixture"}}, token: "current-launch-capability", upstream: upstream.URL, client: upstream.Client()}
	call := func(repo, capability string) string {
		raw, _ := json.Marshal(map[string]string{"url": repo, "task_id": "task", "workspace_id": "workspace"})
		r := httptest.NewRequest(http.MethodPost, "/repo/checkout", bytes.NewReader(raw))
		r.Header.Set(BrokerTokenHeader, capability)
		w := httptest.NewRecorder()
		broker.ServeHTTP(w, r)
		return w.Body.String()
	}
	if !strings.Contains(call(ownURL, broker.token), ownData) {
		t.Fatal("assigned repository source was unavailable")
	}
	if strings.Contains(call("https://example.test/previous/repo", broker.token), otherData) {
		t.Fatal("another task's cached repository was disclosed")
	}
	if strings.Contains(call(ownURL, "terminated-launch-capability"), ownData) {
		t.Fatal("a previous launch read source through a reused broker port")
	}
}
