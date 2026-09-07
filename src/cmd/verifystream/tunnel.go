package main

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type socketPair struct {
	downstream, upstream net.Conn
	once                 sync.Once
}

func (p *socketPair) close() { p.once.Do(func() { _ = p.downstream.Close(); _ = p.upstream.Close() }) }

type connectTunnel struct {
	target   string
	listener net.Listener
	server   *http.Server
	mutex    sync.Mutex
	active   map[*socketPair]bool
	opened   int
	closed   bool
	handlers sync.WaitGroup
}

// This proxy only opens TCP sockets. It forwards the TLS handshake and every
// encrypted SPDY byte unchanged, with no certificate or protocol substitution.
func newTunnel(target string) (*connectTunnel, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	tunnel := &connectTunnel{target: target, listener: listener, active: map[*socketPair]bool{}}
	tunnel.server = &http.Server{Handler: http.HandlerFunc(tunnel.serve), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = tunnel.server.Serve(listener) }()
	return tunnel, nil
}
func (t *connectTunnel) URL() *url.URL {
	return &url.URL{Scheme: "http", Host: t.listener.Addr().String()}
}
func (t *connectTunnel) serve(w http.ResponseWriter, r *http.Request) {
	t.mutex.Lock()
	if t.closed {
		t.mutex.Unlock()
		http.Error(w, "tunnel closed", 503)
		return
	}
	t.handlers.Add(1)
	t.mutex.Unlock()
	defer t.handlers.Done()
	if r.Method != http.MethodConnect || r.Host != t.target {
		http.Error(w, "only the selected local K3s CONNECT destination is allowed", http.StatusForbidden)
		return
	}
	upstream, err := net.DialTimeout("tcp", t.target, 10*time.Second)
	if err != nil {
		http.Error(w, "local K3s socket unavailable", http.StatusBadGateway)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		upstream.Close()
		http.Error(w, "CONNECT unavailable", http.StatusInternalServerError)
		return
	}
	downstream, buffer, err := hijacker.Hijack()
	if err != nil {
		upstream.Close()
		return
	}
	pair := &socketPair{downstream: downstream, upstream: upstream}
	if _, err = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err == nil {
		err = buffer.Flush()
	}
	if err != nil {
		pair.close()
		return
	}
	t.mutex.Lock()
	if t.closed {
		t.mutex.Unlock()
		pair.close()
		return
	}
	t.active[pair] = true
	t.opened++
	t.mutex.Unlock()
	defer func() { pair.close(); t.mutex.Lock(); delete(t.active, pair); t.mutex.Unlock() }()
	copied := make(chan struct{}, 2)
	// The hijacker reader may already hold encrypted client bytes; preserve them.
	go func() { _, _ = io.Copy(upstream, buffer.Reader); copied <- struct{}{} }()
	go func() { _, _ = io.Copy(downstream, upstream); copied <- struct{}{} }()
	<-copied
	pair.close()
	<-copied
}
func (t *connectTunnel) sever() (int, error) {
	t.mutex.Lock()
	pairs := make([]*socketPair, 0, len(t.active))
	for pair := range t.active {
		pairs = append(pairs, pair)
	}
	t.mutex.Unlock()
	if len(pairs) == 0 {
		return 0, errors.New("no live upgraded CONNECT sockets were available to sever")
	}
	for _, pair := range pairs {
		pair.close()
	}
	return len(pairs), nil
}
func (t *connectTunnel) close() {
	t.mutex.Lock()
	t.closed = true
	t.mutex.Unlock()
	_ = t.server.Close()
	_, _ = t.sever()
	t.handlers.Wait()
}
