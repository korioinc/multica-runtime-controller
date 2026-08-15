package intercept

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestDaemonGatewayAuthenticatesTaskAndForwardsEntireRequest(t *testing.T) {
	const (
		namespace          = "multica"
		taskID             = "11111111-2222-4333-8444-555555555555"
		selectedSecretName = "task-1111111-selected"
		otherSecretName    = "task-1111111-other"
		selectedToken      = "mat_selected_attempt"
		otherToken         = "mat_other_attempt"
		body               = `{"arbitrary":"daemon request"}`
	)
	requestFor := func(requestTaskID, token string) []byte {
		request := Request{
			Provider: "codex",
			Env: []string{
				"MULTICA_TASK_ID=" + requestTaskID,
				"MULTICA_TOKEN=" + token,
			},
			WorkDir: "/workspace/tasks/" + requestTaskID,
		}
		rawRequest, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		return rawRequest
	}
	client := fake.NewSimpleClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: selectedSecretName, Namespace: namespace,
			Labels: map[string]string{ManagedByLabel: ManagedByValue, TaskIDLabel: taskID},
		}, Data: map[string][]byte{RequestKey: requestFor(taskID, selectedToken)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: otherSecretName, Namespace: namespace,
			Labels: map[string]string{ManagedByLabel: ManagedByValue, TaskIDLabel: taskID},
		}, Data: map[string][]byte{RequestKey: requestFor(taskID, otherToken)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: "task-1111111-wrong-label", Namespace: namespace,
			Labels: map[string]string{ManagedByLabel: ManagedByValue, TaskIDLabel: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"},
		}, Data: map[string][]byte{RequestKey: requestFor(taskID, selectedToken)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: "task-1111111-wrong-managed-by", Namespace: namespace,
			Labels: map[string]string{ManagedByLabel: "other-controller", TaskIDLabel: taskID},
		}, Data: map[string][]byte{RequestKey: requestFor(taskID, selectedToken)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: "task-1111111-wrong-payload", Namespace: namespace,
			Labels: map[string]string{ManagedByLabel: ManagedByValue, TaskIDLabel: taskID},
		}, Data: map[string][]byte{RequestKey: requestFor("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", selectedToken)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{
			Name: "task-1111111-malformed", Namespace: namespace,
			Labels: map[string]string{ManagedByLabel: ManagedByValue, TaskIDLabel: taskID},
		}, Data: map[string][]byte{RequestKey: []byte("not-json")}},
	)
	forwarded := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded++
		if r.Method != http.MethodPatch || r.URL.Path != "/daemon/arbitrary" || r.URL.RawQuery != "mode=full" {
			t.Fatalf("upstream request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Basic original-authorization" {
			t.Fatalf("original authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get(DaemonProxyRequestSecretHeader) != "" || r.Header.Get(DaemonProxyTaskIDHeader) != "" || r.Header.Get(DaemonProxyTaskTokenHeader) != "" {
			t.Fatal("gateway authentication headers reached the daemon")
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != body {
			t.Fatalf("forwarded body = %q, want %q", raw, body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	}))
	defer upstream.Close()

	handler, err := NewDaemonGateway(namespace, client, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPatch, "/daemon/arbitrary?mode=full", strings.NewReader(body))
	req.Header.Set("Authorization", "Basic original-authorization")
	req.Header.Set(DaemonProxyRequestSecretHeader, selectedSecretName)
	req.Header.Set(DaemonProxyTaskIDHeader, taskID)
	req.Header.Set(DaemonProxyTaskTokenHeader, selectedToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || forwarded != 1 || rec.Body.String() != `{"accepted":true}` {
		t.Fatalf("authorized daemon request status=%d forwarded=%d body=%s", rec.Code, forwarded, rec.Body.String())
	}
	actions := client.Actions()
	if len(actions) != 1 {
		t.Fatalf("authorized request Kubernetes actions = %d, want one exact Secret GET", len(actions))
	}
	getAction, ok := actions[0].(k8stesting.GetAction)
	if !ok || getAction.GetName() != selectedSecretName {
		t.Fatalf("authorized request selected Secret action = %#v, want %q", actions[0], selectedSecretName)
	}

	for _, tt := range []struct {
		name      string
		selector  string
		requestID string
		token     string
	}{
		{name: "missing selector", requestID: taskID, token: selectedToken},
		{name: "missing task ID", selector: selectedSecretName, token: selectedToken},
		{name: "missing token", selector: selectedSecretName, requestID: taskID},
		{name: "unknown selector", selector: "task-1111111-unknown", requestID: taskID, token: selectedToken},
		{name: "malformed selector", selector: "INVALID SECRET NAME", requestID: taskID, token: selectedToken},
		{name: "wrong attempt token", selector: otherSecretName, requestID: taskID, token: selectedToken},
		{name: "wrong task ID", selector: selectedSecretName, requestID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", token: selectedToken},
		{name: "wrong label", selector: "task-1111111-wrong-label", requestID: taskID, token: selectedToken},
		{name: "wrong managed-by", selector: "task-1111111-wrong-managed-by", requestID: taskID, token: selectedToken},
		{name: "wrong stored task", selector: "task-1111111-wrong-payload", requestID: taskID, token: selectedToken},
		{name: "malformed request", selector: "task-1111111-malformed", requestID: taskID, token: selectedToken},
		{name: "wrong token", selector: selectedSecretName, requestID: taskID, token: "mat_wrong_task"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/shutdown", nil)
			req.Header.Set(DaemonProxyRequestSecretHeader, tt.selector)
			req.Header.Set(DaemonProxyTaskIDHeader, tt.requestID)
			req.Header.Set(DaemonProxyTaskTokenHeader, tt.token)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized || forwarded != 1 {
				t.Fatalf("unauthorized daemon request status=%d forwarded=%d", rec.Code, forwarded)
			}
		})
	}

	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), selectedSecretName, metav1.GetOptions{}); err != nil {
		t.Fatalf("gateway mutated request Secret: %v", err)
	}
}
