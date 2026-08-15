package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/intercept"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func runDaemonGateway() error {
	namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE"))
	if namespace == "" {
		return errors.New("POD_NAMESPACE is required")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load Kubernetes client config: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	handler, err := intercept.NewDaemonGateway(namespace, client, "http://127.0.0.1:19514")
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	server := &http.Server{Addr: ":19515", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Info("daemon proxy request received", "http_method", r.Method, "http_path", r.URL.Path)
		response := &statusResponseWriter{ResponseWriter: w}
		handler.ServeHTTP(response, r)
		logger.Info("daemon proxy request completed", "http_method", r.Method, "http_path", r.URL.Path, "http_status", response.statusCode())
	}), ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	logger.Info("daemon proxy listening", "namespace", namespace, "address", server.Addr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	select {
	case <-ctx.Done():
		logger.Info("daemon proxy stopping")
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("stop daemon proxy: %w", err)
		}
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve daemon proxy: %w", err)
	}
}
