package daemonapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/workspace"
)

// The backend accepts files up to 100 MiB. Leave room for multipart framing
// without interpreting business payloads or buffering downloaded files.
const maxProxyPayload = 128 << 20

func (g *Gateway) proxy(w http.ResponseWriter, r *http.Request, grant workspace.TaskGrant, token string) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()
	if len(r.URL.RawQuery) > 4096 {
		reject(w, 400)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProxyPayload))
	if err != nil {
		reject(w, 400)
		return
	}
	upstream, err := g.Client.newRequest(ctx, r.Method, r.URL.RequestURI(), token, grant.WorkspaceID, nil)
	if err != nil {
		reject(w, 400)
		return
	}
	// Only transport and native client metadata cross this boundary. Backend
	// authentication derives actor identity from the original task token.
	upstream.Header.Del("Content-Type")
	for _, name := range []string{
		"Content-Type", "Content-Encoding", "Accept", "Accept-Encoding", "Accept-Language",
		"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "Range", "If-Range",
		"Idempotency-Key", "X-Client-Capabilities", "X-Client-Version", "X-Client-Platform", "X-Client-OS",
	} {
		key := http.CanonicalHeaderKey(name)
		if values, exists := r.Header[key]; exists {
			upstream.Header[key] = append([]string(nil), values...)
		}
	}
	r = r.WithContext(ctx)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	proxy := httputil.ReverseProxy{
		Transport: g.Client.http.Transport,
		Rewrite: func(p *httputil.ProxyRequest) {
			p.Out.URL = upstream.URL
			p.Out.Host = upstream.URL.Host
			p.Out.Header = upstream.Header
		},
		ModifyResponse: func(response *http.Response) error {
			if response.StatusCode >= 300 && response.StatusCode < 400 && response.StatusCode != http.StatusNotModified {
				return errors.New("backend redirect refused")
			}
			if r.Method != http.MethodHead && response.ContentLength > maxProxyPayload {
				return errors.New("backend response exceeds transport limit")
			}
			response.Body = http.MaxBytesReader(nil, response.Body, maxProxyPayload)
			response.Header.Del("Set-Cookie")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) { reject(w, 502) },
	}
	proxy.ServeHTTP(w, r)
}
