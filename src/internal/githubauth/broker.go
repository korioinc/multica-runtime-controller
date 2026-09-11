package githubauth

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
)

// TokenSource keeps the token owner independent of its private transport.
type TokenSource interface {
	Token(context.Context, []githubapp.Repository) (githubapp.Token, error)
}

// PrivateHandler is served exclusively on the controller's private Unix socket.
// Callers are the official daemon's helper or an authorized task broker. Never
// mount this socket into a worker or expose this handler on the public gateway.
func PrivateHandler(source TokenSource) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodPost || r.URL.Path != Route || r.URL.RawQuery != "" || r.URL.RawPath != "" || r.URL.Opaque != "" {
			http.Error(w, "GitHub authentication operation unavailable", http.StatusForbidden)
			return
		}
		var input PrivateRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
			http.Error(w, "invalid GitHub token request", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
		defer cancel()
		token, err := source.Token(ctx, input.Repositories)
		if err != nil {
			// GitHub response bodies and credentials must not enter HTTP errors.
			http.Error(w, "GitHub App token unavailable; check installation, repository access and Contents permission", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(token)
	})
}

// ListenPrivate replaces only a stale socket in the controller-owned 0700
// control directory. A live listener or a different filesystem object is never
// replaced during same-Pod recovery.
func ListenPrivate() (net.Listener, error) {
	if info, err := os.Lstat(SocketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("GitHub authentication socket path is not a socket")
		}
		connection, dialErr := net.DialTimeout("unix", SocketPath, time.Second)
		if dialErr == nil {
			_ = connection.Close()
			return nil, errors.New("GitHub authentication broker is already running")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
			return nil, errors.New("GitHub authentication socket is unavailable")
		}
		if err := os.Remove(SocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", SocketPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(SocketPath, 0600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}
