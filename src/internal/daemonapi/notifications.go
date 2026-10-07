package daemonapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// WatchTasks subscribes only to hints for the registered runtime set. Work is
// still claimed over HTTP. Reconnects heal dropped hints through connected.
func (c *Client) WatchTasks(ctx context.Context, runtimeIDs []string, available, connected func()) {
	if len(runtimeIDs) == 0 {
		return
	}
	ids := slices.Clone(runtimeIDs)
	slices.Sort(ids)
	for _, id := range ids {
		if !segment(id) {
			return
		}
	}
	backoff := time.Second
	warned := false
	for ctx.Err() == nil {
		var connectedAt time.Time
		_ = c.watchTasks(ctx, ids, available, func() {
			connectedAt = time.Now()
			warned = false
			slog.Info("task notifications connected", "runtimes", len(ids))
			connected()
		})
		if ctx.Err() != nil {
			return
		}
		// A successful upgrade followed by an immediate close is still an
		// outage. Only a stable connection resets the reconnect backoff.
		if !connectedAt.IsZero() && time.Since(connectedAt) >= 10*time.Second {
			backoff = time.Second
		}
		if !warned {
			slog.Warn("task notifications unavailable", "reason", "subscription_unavailable")
			warned = true
		}
		delay := backoff + time.Duration(rand.Int64N(int64(backoff/4)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(30*time.Second, backoff*2)
	}
}

func (c *Client) watchTasks(ctx context.Context, ids []string, available, connected func()) error {
	req, err := c.newRequest(ctx, http.MethodGet, "/api/daemon/ws", c.token, "", nil)
	if err != nil {
		return err
	}
	query := req.URL.Query()
	query.Set("runtime_ids", strings.Join(ids, ","))
	req.URL.RawQuery = query.Encode()
	if req.URL.Scheme == "https" {
		req.URL.Scheme = "wss"
	} else {
		req.URL.Scheme = "ws"
	}
	transport := http.DefaultTransport.(*http.Transport)
	if c.http.Transport != nil {
		var ok bool
		transport, ok = c.http.Transport.(*http.Transport)
		if !ok {
			return errors.New("task notifications require an HTTP transport")
		}
	}
	dialer := websocket.Dialer{Proxy: transport.Proxy, NetDial: transport.Dial, NetDialContext: transport.DialContext, NetDialTLSContext: transport.DialTLSContext, HandshakeTimeout: 10 * time.Second, Jar: c.http.Jar}
	if transport.TLSClientConfig != nil {
		dialer.TLSClientConfig = transport.TLSClientConfig.Clone()
	}
	// Gorilla does not follow redirects. Preserve the HTTP client's TLS trust,
	// proxy and dial settings rather than opening a second default transport.
	conn, response, err := dialer.DialContext(ctx, req.URL.String(), req.Header)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return errors.New("task notification connection failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	conn.SetReadLimit(MaxPayload)
	refresh := func() error { return conn.SetReadDeadline(time.Now().Add(90 * time.Second)) }
	conn.SetPingHandler(func(data string) error {
		if err := refresh(); err != nil {
			return err
		}
		return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
	})
	conn.SetPongHandler(func(string) error { return refresh() })
	if err := refresh(); err != nil {
		return err
	}
	connected()
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return errors.New("task notification stream ended")
		}
		if err := refresh(); err != nil {
			return err
		}
		var message protocol.Message
		if json.Unmarshal(raw, &message) != nil || message.Type != "daemon:task_available" {
			continue
		}
		var hint protocol.TaskAvailablePayload
		if json.Unmarshal(message.Payload, &hint) == nil && slices.Contains(ids, hint.RuntimeID) {
			available()
		}
	}
}
