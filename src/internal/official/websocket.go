package official

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type rpcMessage struct {
	Type    string `json:"type"`
	Payload struct {
		ID     string          `json:"request_id"`
		Method string          `json:"method"`
		Status int             `json:"status"`
		Body   json.RawMessage `json:"body"`
	} `json:"payload"`
}

func (b *bridge) websocket(w http.ResponseWriter, r *http.Request) {
	target := *b.target
	target.Path = strings.TrimRight(target.Path, "/") + r.URL.Path
	target.RawQuery = r.URL.RawQuery
	if target.Scheme == "https" {
		target.Scheme = "wss"
	} else {
		target.Scheme = "ws"
	}
	headers := r.Header.Clone()
	for key := range headers {
		if strings.HasPrefix(strings.ToLower(key), "sec-websocket-") || strings.EqualFold(key, "Connection") || strings.EqualFold(key, "Upgrade") {
			headers.Del(key)
		}
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second, Subprotocols: websocket.Subprotocols(r)}
	upstream, response, err := dialer.DialContext(r.Context(), target.String(), headers)
	if err != nil {
		slog.Warn("backend websocket connection failed", "phase", "controller", "error_class", "backend_transport")
		if response != nil {
			response.Body.Close()
		}
		http.Error(w, "official WebSocket transport failed", http.StatusBadGateway)
		return
	}
	defer upstream.Close()
	upgrader := websocket.Upgrader{}
	if protocol := upstream.Subprotocol(); protocol != "" {
		upgrader.Subprotocols = []string{protocol}
	}
	downstream, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer downstream.Close()
	slog.Info("backend websocket connected", "phase", "controller")
	defer slog.Info("backend websocket disconnected", "phase", "controller")
	upstream.SetReadLimit(maxProtocolBytes)
	downstream.SetReadLimit(maxProtocolBytes)
	relayControl(upstream, downstream)
	relayControl(downstream, upstream)
	var mutex sync.Mutex
	pending := map[string]bool{}
	finished := make(chan struct{}, 2)
	go func() {
		defer func() { finished <- struct{}{} }()
		for {
			kind, raw, err := downstream.ReadMessage()
			if err != nil {
				return
			}
			var message rpcMessage
			if kind == websocket.TextMessage && json.Unmarshal(raw, &message) == nil && message.Type == "daemon:rpc_request" {
				claim := message.Payload.Method == "tasks.claim"
				mutex.Lock()
				if message.Payload.ID == "" || pending[message.Payload.ID] || len(pending) >= 4096 {
					mutex.Unlock()
					return
				}
				if claim {
					pending[message.Payload.ID] = true
				}
				mutex.Unlock()
				if claim {
					slog.Info("backend task claim started", "phase", "controller")
				}
			}
			if upstream.WriteMessage(kind, raw) != nil {
				return
			}
		}
	}()
	go func() {
		defer func() { finished <- struct{}{} }()
		for {
			kind, raw, err := upstream.ReadMessage()
			if err != nil {
				return
			}
			var message rpcMessage
			if kind == websocket.TextMessage && json.Unmarshal(raw, &message) == nil && message.Type == "daemon:rpc_response" {
				mutex.Lock()
				claim := pending[message.Payload.ID]
				delete(pending, message.Payload.ID)
				mutex.Unlock()
				if claim {
					slog.Info("backend task claim response received", "phase", "controller", "status", message.Payload.Status)
				}
				if claim && message.Payload.Status == http.StatusOK {
					transformed, err := b.claim(message.Payload.Body)
					// Never synthesize an RPC failure after an upstream success. Closing
					// leaves an uncertain claim to the official daemon's reclaim policy.
					if err != nil {
						return
					}
					var envelope, payload map[string]json.RawMessage
					if json.Unmarshal(raw, &envelope) != nil || json.Unmarshal(envelope["payload"], &payload) != nil {
						return
					}
					payload["body"] = transformed
					envelope["payload"], err = json.Marshal(payload)
					if err != nil {
						return
					}
					raw, err = json.Marshal(envelope)
					if err != nil {
						return
					}
				}
			}
			if downstream.WriteMessage(kind, raw) != nil {
				return
			}
		}
	}()
	<-finished
	upstream.Close()
	downstream.Close()
	<-finished
}

func relayControl(source, target *websocket.Conn) {
	source.SetPingHandler(func(data string) error {
		return target.WriteControl(websocket.PingMessage, []byte(data), time.Now().Add(10*time.Second))
	})
	source.SetPongHandler(func(data string) error {
		return target.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(10*time.Second))
	})
	source.SetCloseHandler(func(code int, reason string) error {
		return target.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(10*time.Second))
	})
}
