package main

import (
	"errors"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/intercept"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

func newTaskDaemonProxy(upstream, requestSecretName, taskID, token string) (http.Handler, error) {
	target, err := url.Parse(strings.TrimSpace(upstream))
	if err != nil || target.Scheme != "http" || target.Host == "" || target.User != nil ||
		(target.Path != "" && target.Path != "/") || target.RawQuery != "" || target.Fragment != "" {
		return nil, errors.New("MULTICA_DAEMON_PROXY_URL must be an HTTP origin")
	}
	parsedTaskID, err := uuid.Parse(strings.TrimSpace(taskID))
	if err != nil || parsedTaskID.String() != strings.TrimSpace(taskID) {
		return nil, errors.New("MULTICA_TASK_ID must be a canonical UUID")
	}
	requestSecretName = strings.TrimSpace(requestSecretName)
	if requestSecretName == "" || len(k8svalidation.IsDNS1123Subdomain(requestSecretName)) != 0 {
		return nil, errors.New("MULTICA_REQUEST_SECRET_NAME must be a valid Kubernetes resource name")
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errors.New("MULTICA_TOKEN is required for daemon proxy authentication")
	}

	return &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.Header.Set(intercept.DaemonProxyRequestSecretHeader, requestSecretName)
			request.Out.Header.Set(intercept.DaemonProxyTaskIDHeader, parsedTaskID.String())
			request.Out.Header.Set(intercept.DaemonProxyTaskTokenHeader, token)
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "connect to runtime controller daemon proxy", http.StatusBadGateway)
		},
	}, nil
}
