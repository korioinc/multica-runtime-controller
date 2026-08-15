package intercept

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const (
	DaemonProxyRequestSecretHeader = "X-Multica-Request-Secret"
	DaemonProxyTaskIDHeader        = "X-Multica-Task-ID"
	DaemonProxyTaskTokenHeader     = "X-Multica-Task-Token"
)

type daemonGateway struct {
	namespace string
	client    kubernetes.Interface
	proxy     *httputil.ReverseProxy
}

func NewDaemonGateway(namespace string, client kubernetes.Interface, upstream string) (http.Handler, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" || client == nil {
		return nil, errors.New("daemon proxy namespace and Kubernetes client are required")
	}
	parsed, err := url.Parse(strings.TrimSpace(upstream))
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("daemon proxy upstream must be an HTTP loopback origin")
	}
	hostname := parsed.Hostname()
	ip := net.ParseIP(hostname)
	if hostname != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("daemon proxy upstream must be an HTTP loopback origin")
	}

	gateway := &daemonGateway{namespace: namespace, client: client}
	gateway.proxy = &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(parsed)
			request.Out.Header.Del(DaemonProxyRequestSecretHeader)
			request.Out.Header.Del(DaemonProxyTaskIDHeader)
			request.Out.Header.Del(DaemonProxyTaskTokenHeader)
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "connect to controller daemon", http.StatusBadGateway)
		},
	}
	return gateway, nil
}

func (g *daemonGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !g.authenticate(
		r.Context(),
		r.Header.Get(DaemonProxyRequestSecretHeader),
		r.Header.Get(DaemonProxyTaskIDHeader),
		r.Header.Get(DaemonProxyTaskTokenHeader),
	) {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	g.proxy.ServeHTTP(w, r)
}

func (g *daemonGateway) authenticate(ctx context.Context, requestSecretName, taskID, token string) bool {
	requestSecretName = strings.TrimSpace(requestSecretName)
	taskID = strings.TrimSpace(taskID)
	token = strings.TrimSpace(token)
	parsedTaskID, err := uuid.Parse(taskID)
	if requestSecretName == "" || len(k8svalidation.IsDNS1123Subdomain(requestSecretName)) != 0 ||
		err != nil || parsedTaskID.String() != taskID || token == "" {
		return false
	}
	secret, err := g.client.CoreV1().Secrets(g.namespace).Get(ctx, requestSecretName, metav1.GetOptions{})
	if err != nil || secret.Labels[ManagedByLabel] != ManagedByValue || secret.Labels[TaskIDLabel] != taskID {
		return false
	}
	var stored Request
	if err := json.Unmarshal(secret.Data[RequestKey], &stored); err != nil {
		return false
	}
	expectedTaskID := requestEnvironmentValue(stored.Env, "MULTICA_TASK_ID")
	expectedToken := requestEnvironmentValue(stored.Env, "MULTICA_TOKEN")
	return expectedTaskID == taskID && expectedToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) == 1
}

func requestEnvironmentValue(environ []string, name string) string {
	prefix := name + "="
	for _, entry := range environ {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}
