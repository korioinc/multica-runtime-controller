package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/intercept"
)

func readProviderRequest(path string) (intercept.Request, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return intercept.Request{}, fmt.Errorf("read provider request: %w", err)
	}
	var request intercept.Request
	if err := json.Unmarshal(raw, &request); err != nil {
		return intercept.Request{}, fmt.Errorf("decode provider request: %w", err)
	}
	validated, err := intercept.PrepareRequest(request.Provider, request.Args, request.Env, request.WorkDir)
	if err != nil {
		return intercept.Request{}, err
	}
	request.Args, request.Env, request.WorkDir = validated.Args, validated.Env, validated.WorkDir
	return request, nil
}

func runTaskPod() error {
	request, err := readProviderRequest(intercept.RequestFilePath())
	if err != nil {
		return err
	}
	taskID := environmentValue(request.Env, "MULTICA_TASK_ID")
	if taskID == "" || taskID != strings.TrimSpace(os.Getenv("MULTICA_TASK_ID")) {
		return errors.New("mounted provider request does not match the task Pod")
	}
	port, err := taskDaemonPort(environmentValue(request.Env, "MULTICA_DAEMON_PORT"))
	if err != nil {
		return err
	}
	handler, err := newTaskDaemonProxy(
		os.Getenv("MULTICA_DAEMON_PROXY_URL"),
		os.Getenv("MULTICA_REQUEST_SECRET_NAME"),
		request,
	)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("listen for task daemon proxy: %w", err)
	}

	logger := newTaskPodLogger(os.Stdout, taskID, request.Provider)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.write(taskPodLogRecord{Message: "daemon proxy request received", HTTPMethod: r.Method, HTTPPath: r.URL.Path})
		response := &statusResponseWriter{ResponseWriter: w}
		handler.ServeHTTP(response, r)
		logger.write(taskPodLogRecord{Message: "daemon proxy request completed", HTTPMethod: r.Method, HTTPPath: r.URL.Path, HTTPStatus: response.statusCode()})
	}), ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()

	logger.write(taskPodLogRecord{Message: "task Pod ready; daemon proxy listening"})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	select {
	case <-ctx.Done():
		logger.write(taskPodLogRecord{Message: "task Pod stopping"})
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			return fmt.Errorf("stop task daemon proxy: %w", err)
		}
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve task daemon proxy: %w", err)
	}
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func taskDaemonPort(value string) (int, error) {
	port, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("MULTICA_DAEMON_PORT must be a valid TCP port")
	}
	return port, nil
}
