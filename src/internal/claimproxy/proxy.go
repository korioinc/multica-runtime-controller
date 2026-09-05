// Package claimproxy observes the official HTTP and WebSocket claim contracts.
// It does not poll, claim, retry, or select provider sessions itself.
package claimproxy

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
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/korioinc/multica-runtime-controller/internal/taskstate"
)

const maxClaimBytes = 16 << 20

type Proxy struct {
	target *url.URL
	store  *taskstate.Store
	http   *httputil.ReverseProxy
}

func New(upstream string, store *taskstate.Store) (http.Handler, error) {
	normalized, err := NormalizeBackendURL(upstream)
	if err != nil {
		return nil, err
	}
	target, err := url.Parse(normalized)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || store == nil {
		return nil, errors.New("claim proxy requires a backend origin and task state store")
	}
	p := &Proxy{target: target, store: store}
	p.http = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			if claimPath(r.In.Method, r.In.URL.Path) {
				r.Out.Header.Set("Accept-Encoding", "identity")
			}
		},
		ModifyResponse: func(response *http.Response) error {
			inboundPath := strings.TrimPrefix(response.Request.URL.Path, strings.TrimRight(target.Path, "/"))
			if response.StatusCode != http.StatusOK || !claimPath(response.Request.Method, inboundPath) {
				return nil
			}
			if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
				return errors.New("unexpected encoded claim response")
			}
			raw, err := io.ReadAll(io.LimitReader(response.Body, maxClaimBytes+1))
			response.Body.Close()
			if err != nil || len(raw) > maxClaimBytes {
				return errors.New("invalid or oversized claim response")
			}
			raw, err = p.observe(raw)
			if err != nil {
				return err
			}
			response.Body = io.NopCloser(bytes.NewReader(raw))
			response.ContentLength = int64(len(raw))
			response.Header.Set("Content-Length", strconv.Itoa(len(raw)))
			response.Header.Del("ETag")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "official backend request or claim validation failed", http.StatusBadGateway)
		},
	}
	return p, nil
}

// NormalizeBackendURL follows the official server URL contract, including
// WebSocket-form configuration and the canonical legacy /ws suffix.
func NormalizeBackendURL(raw string) (string, error) {
	target, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	switch target.Scheme {
	case "ws":
		target.Scheme = "http"
	case "wss":
		target.Scheme = "https"
	case "http", "https":
	default:
		return "", errors.New("backend URL must use HTTP(S) or WS(S)")
	}
	if target.Host == "" || target.User != nil {
		return "", errors.New("backend URL requires a host without embedded credentials")
	}
	if target.Path == "/ws" {
		target.Path = ""
	}
	target.RawPath, target.RawQuery, target.Fragment = "", "", ""
	return strings.TrimRight(target.String(), "/"), nil
}

func claimPath(method, path string) bool {
	return method == http.MethodPost && (path == "/api/daemon/tasks/claim" || path == "/api/daemon/claim" ||
		(strings.HasPrefix(path, "/api/daemon/runtimes/") && strings.HasSuffix(path, "/tasks/claim")))
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/daemon/ws" && websocket.IsWebSocketUpgrade(r) {
		p.websocket(w, r)
		return
	}
	p.http.ServeHTTP(w, r)
}

func (p *Proxy) observe(raw []byte) ([]byte, error) {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil || envelope == nil {
		return nil, errors.New("invalid claim envelope")
	}
	if tasks, ok := envelope["tasks"]; ok {
		var values []json.RawMessage
		if err := json.Unmarshal(tasks, &values); err != nil {
			return nil, err
		}
		for i, value := range values {
			transformed, err := p.store.Observe(value)
			if err != nil {
				return nil, err
			}
			values[i] = transformed
		}
		envelope["tasks"], _ = json.Marshal(values)
	} else if task, ok := envelope["task"]; ok {
		if string(task) != "null" {
			value, err := p.store.Observe(task)
			if err != nil {
				return nil, err
			}
			envelope["task"] = value
		}
	} else {
		return nil, errors.New("unrecognized official claim envelope")
	}
	return json.Marshal(envelope)
}

func (p *Proxy) websocket(w http.ResponseWriter, r *http.Request) {
	target := *p.target
	target.Path = strings.TrimRight(target.Path, "/") + r.URL.Path
	target.RawQuery = r.URL.RawQuery
	if target.Scheme == "https" {
		target.Scheme = "wss"
	} else {
		target.Scheme = "ws"
	}
	header := r.Header.Clone()
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "sec-websocket-") || strings.EqualFold(key, "Connection") || strings.EqualFold(key, "Upgrade") {
			header.Del(key)
		}
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second, Subprotocols: websocket.Subprotocols(r)}
	upstream, response, err := dialer.DialContext(r.Context(), target.String(), header)
	if err != nil {
		if response != nil {
			response.Body.Close()
		}
		http.Error(w, "connect to official backend WebSocket", http.StatusBadGateway)
		return
	}
	defer upstream.Close()
	upgrader := websocket.Upgrader{Subprotocols: []string{upstream.Subprotocol()}}
	downstream, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer downstream.Close()
	upstream.SetReadLimit(maxClaimBytes)
	downstream.SetReadLimit(maxClaimBytes)
	forwardControl(upstream, downstream)
	forwardControl(downstream, upstream)
	var mu sync.Mutex
	claims := make(map[string]bool)
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			kind, raw, err := downstream.ReadMessage()
			if err != nil {
				return
			}
			var frame rpcFrame
			if kind == websocket.TextMessage && json.Unmarshal(raw, &frame) == nil && frame.Type == "daemon:rpc_request" && frame.Payload.Method == "tasks.claim" {
				mu.Lock()
				claims[frame.Payload.RequestID] = true
				mu.Unlock()
			}
			if err := upstream.WriteMessage(kind, raw); err != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			kind, raw, err := upstream.ReadMessage()
			if err != nil {
				return
			}
			var frame rpcFrame
			if kind == websocket.TextMessage && json.Unmarshal(raw, &frame) == nil && frame.Type == "daemon:rpc_response" {
				mu.Lock()
				claimed := claims[frame.Payload.RequestID]
				delete(claims, frame.Payload.RequestID)
				mu.Unlock()
				if claimed && frame.Payload.Status == http.StatusOK {
					body, err := p.observe(frame.Payload.Body)
					// A successful but unrecorded claim has an uncertain result.
					// Closing preserves official reclaim handling; a synthetic RPC
					// error would incorrectly trigger immediate HTTP fallback.
					if err != nil {
						return
					}
					var envelope map[string]json.RawMessage
					var payload map[string]json.RawMessage
					if json.Unmarshal(raw, &envelope) != nil || json.Unmarshal(envelope["payload"], &payload) != nil {
						return
					}
					payload["body"] = body
					envelope["payload"], _ = json.Marshal(payload)
					raw, err = json.Marshal(envelope)
					if err != nil {
						return
					}
				}
			}
			if err := downstream.WriteMessage(kind, raw); err != nil {
				return
			}
		}
	}()
	<-done
	upstream.Close()
	downstream.Close()
	<-done
}

type rpcFrame struct {
	Type    string `json:"type"`
	Payload struct {
		RequestID string          `json:"request_id"`
		Method    string          `json:"method"`
		Status    int             `json:"status"`
		Body      json.RawMessage `json:"body"`
	} `json:"payload"`
}

func forwardControl(source, destination *websocket.Conn) {
	source.SetPingHandler(func(data string) error {
		return destination.WriteControl(websocket.PingMessage, []byte(data), time.Now().Add(10*time.Second))
	})
	source.SetPongHandler(func(data string) error {
		return destination.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(10*time.Second))
	})
	source.SetCloseHandler(func(code int, text string) error {
		return destination.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, text), time.Now().Add(10*time.Second))
	})
}
