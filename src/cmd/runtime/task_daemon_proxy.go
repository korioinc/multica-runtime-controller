package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/checkout"
	"github.com/korioinc/multica-runtime-controller/internal/intercept"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

func newTaskDaemonProxy(upstream, requestSecretName string, request intercept.Request) (http.Handler, error) {
	target, err := url.Parse(strings.TrimSpace(upstream))
	if err != nil || target.Scheme != "http" || target.Host == "" || target.User != nil ||
		(target.Path != "" && target.Path != "/") || target.RawQuery != "" || target.Fragment != "" {
		return nil, errors.New("MULTICA_DAEMON_PROXY_URL must be an HTTP origin")
	}
	taskID := environmentValue(request.Env, "MULTICA_TASK_ID")
	parsedTaskID, err := uuid.Parse(strings.TrimSpace(taskID))
	if err != nil || parsedTaskID.String() != strings.TrimSpace(taskID) {
		return nil, errors.New("MULTICA_TASK_ID must be a canonical UUID")
	}
	requestSecretName = strings.TrimSpace(requestSecretName)
	if requestSecretName == "" || len(k8svalidation.IsDNS1123Subdomain(requestSecretName)) != 0 {
		return nil, errors.New("MULTICA_REQUEST_SECRET_NAME must be a valid Kubernetes resource name")
	}
	token := strings.TrimSpace(environmentValue(request.Env, "MULTICA_TOKEN"))
	if token == "" {
		return nil, errors.New("MULTICA_TOKEN is required for daemon proxy authentication")
	}
	worker, err := checkout.New(request.WorkDir, request.Env)
	if err != nil {
		return nil, err
	}

	return &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.Header.Set(intercept.DaemonProxyRequestSecretHeader, requestSecretName)
			request.Out.Header.Set(intercept.DaemonProxyTaskIDHeader, parsedTaskID.String())
			request.Out.Header.Set(intercept.DaemonProxyTaskTokenHeader, token)
		},
		ModifyResponse: func(response *http.Response) error {
			if response.Request.Method != http.MethodPost || response.Request.URL.Path != "/repo/checkout" || response.StatusCode != http.StatusOK {
				return nil
			}
			// The gateway forces a plan-only response. No Git operation happens
			// until the trusted daemon has approved this task's URL and ref.
			const limit = 1 << 20
			raw, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
			_ = response.Body.Close()
			var plan checkout.Plan
			if err != nil || len(raw) > limit || json.Unmarshal(raw, &plan) != nil || plan.URL == "" {
				setCheckoutResponse(response, http.StatusBadGateway, []byte("controller did not return a worker checkout plan; update the controller and worker images together\n"))
				return nil
			}
			result, err := worker.Checkout(response.Request.Context(), plan)
			if err != nil {
				setCheckoutResponse(response, http.StatusConflict, []byte(err.Error()+"\n"))
				return nil
			}
			raw, err = json.Marshal(result)
			if err != nil {
				return err
			}
			setCheckoutResponse(response, http.StatusOK, raw)
			response.Header.Set("Content-Type", "application/json")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "connect to runtime controller daemon proxy", http.StatusBadGateway)
		},
	}, nil
}

func setCheckoutResponse(response *http.Response, status int, body []byte) {
	response.StatusCode = status
	response.Status = strconv.Itoa(status) + " " + http.StatusText(status)
	response.Body = io.NopCloser(bytes.NewReader(body))
	response.ContentLength = int64(len(body))
	response.TransferEncoding = nil
	response.Header = make(http.Header)
	response.Header.Set("Content-Type", "text/plain; charset=utf-8")
	response.Header.Set("Content-Length", strconv.Itoa(len(body)))
}
