package intercept

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDaemonGatewayProtectsDataFromOtherTaskAttempts(t *testing.T) {
	const (
		namespace          = "multica"
		taskID             = "11111111-2222-4333-8444-555555555555"
		selectedSecretName = "task-1111111-selected"
		otherSecretName    = "task-1111111-other"
		selectedToken      = "mat_selected_attempt"
		otherToken         = "mat_other_attempt"
		protectedData      = "private daemon data: ee160938-b339-42b0-ab3a-d6e09cf97aba"
	)
	requestFor := func(requestTaskID, token string) []byte {
		request := Request{
			Env: []string{
				"MULTICA_TASK_ID=" + requestTaskID,
				"MULTICA_TOKEN=" + token,
			},
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
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, protectedData)
	}))
	defer upstream.Close()

	handler, err := NewDaemonGateway(namespace, client, upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set(DaemonProxyRequestSecretHeader, selectedSecretName)
	req.Header.Set(DaemonProxyTaskIDHeader, taskID)
	req.Header.Set(DaemonProxyTaskTokenHeader, selectedToken)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), protectedData) {
		t.Fatal("the owning task attempt could not read daemon data")
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
			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			req.Header.Set(DaemonProxyRequestSecretHeader, tt.selector)
			req.Header.Set(DaemonProxyTaskIDHeader, tt.requestID)
			req.Header.Set(DaemonProxyTaskTokenHeader, tt.token)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if strings.Contains(rec.Body.String(), protectedData) {
				t.Fatal("daemon data was disclosed to an unauthorized task attempt")
			}
		})
	}
}
