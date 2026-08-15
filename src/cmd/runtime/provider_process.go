package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/intercept"
)

const maxProtocolMetadataBytes = 64 * 1024

type taskPodLogRecord struct {
	Time       string          `json:"time"`
	Level      string          `json:"level"`
	Message    string          `json:"msg"`
	TaskID     string          `json:"task_id,omitempty"`
	Provider   string          `json:"provider,omitempty"`
	Direction  string          `json:"direction,omitempty"`
	Stream     string          `json:"stream,omitempty"`
	Kind       string          `json:"kind,omitempty"`
	Method     string          `json:"method,omitempty"`
	ID         json.RawMessage `json:"id,omitempty"`
	Bytes      int             `json:"bytes,omitempty"`
	ArgCount   int             `json:"arg_count,omitempty"`
	HTTPMethod string          `json:"http_method,omitempty"`
	HTTPPath   string          `json:"http_path,omitempty"`
	HTTPStatus int             `json:"http_status,omitempty"`
	ExitCode   *int            `json:"exit_code,omitempty"`
}

type taskPodLogger struct {
	mu       sync.Mutex
	output   io.Writer
	taskID   string
	provider string
}

func newTaskPodLogger(output io.Writer, taskID, provider string) *taskPodLogger {
	if output == nil {
		output = io.Discard
	}
	return &taskPodLogger{output: output, taskID: taskID, provider: provider}
}

func (l *taskPodLogger) write(record taskPodLogRecord) {
	record.Time = time.Now().UTC().Format(time.RFC3339Nano)
	record.Level = "INFO"
	record.TaskID = l.taskID
	record.Provider = l.provider
	raw, err := json.Marshal(record)
	if err != nil {
		return
	}
	raw = append(raw, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.output.Write(raw)
}

type protocolObserver struct {
	mu        sync.Mutex
	logger    *taskPodLogger
	direction string
	stream    string
	buffer    []byte
	total     int
}

func newProtocolObserver(logger *taskPodLogger, direction, stream string) *protocolObserver {
	return &protocolObserver{logger: logger, direction: direction, stream: stream}
}

func (o *protocolObserver) observe(data []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for len(data) > 0 {
		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			o.append(data)
			o.total += len(data)
			return
		}
		o.append(data[:newline])
		o.total += newline + 1
		o.emit()
		data = data[newline+1:]
	}
}

func (o *protocolObserver) flush() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.total > 0 {
		o.emit()
	}
}

func (o *protocolObserver) append(data []byte) {
	remaining := maxProtocolMetadataBytes - len(o.buffer)
	if remaining <= 0 {
		return
	}
	if len(data) > remaining {
		data = data[:remaining]
	}
	o.buffer = append(o.buffer, data...)
}

func (o *protocolObserver) emit() {
	record := taskPodLogRecord{
		Message:   "provider protocol activity",
		Direction: o.direction,
		Stream:    o.stream,
		Kind:      "data",
		Bytes:     o.total,
	}
	var envelope struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if len(o.buffer) > 0 && json.Unmarshal(o.buffer, &envelope) == nil {
		record.ID = envelope.ID
		switch {
		case envelope.Method != "" && len(envelope.ID) > 0:
			record.Kind = "request"
			record.Method = truncateMetadata(envelope.Method)
		case envelope.Method != "":
			record.Kind = "notification"
			record.Method = truncateMetadata(envelope.Method)
		case len(envelope.ID) > 0 && len(envelope.Error) > 0:
			record.Kind = "error_response"
		case len(envelope.ID) > 0 && len(envelope.Result) > 0:
			record.Kind = "response"
		}
	}
	o.logger.write(record)
	o.buffer = o.buffer[:0]
	o.total = 0
}

func truncateMetadata(value string) string {
	const limit = 128
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

type observedReader struct {
	source   io.Reader
	observer *protocolObserver
}

func (r *observedReader) Read(data []byte) (int, error) {
	n, err := r.source.Read(data)
	if n > 0 {
		r.observer.observe(data[:n])
	}
	if errors.Is(err, io.EOF) {
		r.observer.flush()
	}
	return n, err
}

type observedWriter struct {
	destination io.Writer
	observer    *protocolObserver
}

func (w *observedWriter) Write(data []byte) (int, error) {
	n, err := w.destination.Write(data)
	if n > 0 {
		w.observer.observe(data[:n])
	}
	return n, err
}

func runProviderProcess(ctx context.Context, providerPath string, request intercept.Request, streams intercept.Streams, podLog io.Writer) error {
	logger := newTaskPodLogger(podLog, environmentValue(request.Env, "MULTICA_TASK_ID"), request.Provider)
	stdinObserver := newProtocolObserver(logger, "controller_to_provider", "stdin")
	stdoutObserver := newProtocolObserver(logger, "provider_to_controller", "stdout")
	stderrObserver := newProtocolObserver(logger, "provider_to_controller", "stderr")

	command := exec.CommandContext(ctx, providerPath, request.Args...)
	command.Cancel = func() error {
		if command.Process == nil {
			return nil
		}
		return command.Process.Signal(syscall.SIGTERM)
	}
	command.WaitDelay = 10 * time.Second
	command.Dir = request.WorkDir
	command.Env = request.Env
	if streams.Stdin != nil {
		command.Stdin = &observedReader{source: streams.Stdin, observer: stdinObserver}
	}
	if streams.Stdout != nil {
		command.Stdout = &observedWriter{destination: streams.Stdout, observer: stdoutObserver}
	}
	if streams.Stderr != nil {
		command.Stderr = &observedWriter{destination: streams.Stderr, observer: stderrObserver}
	}
	if err := command.Start(); err != nil {
		logger.write(taskPodLogRecord{Message: "provider execution failed to start", ArgCount: len(request.Args)})
		return err
	}
	logger.write(taskPodLogRecord{Message: "provider execution started", ArgCount: len(request.Args)})
	err := command.Wait()
	stdinObserver.flush()
	stdoutObserver.flush()
	stderrObserver.flush()
	exitCode := 0
	if err != nil {
		exitCode = 1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() >= 0 {
			exitCode = exitErr.ExitCode()
		}
	}
	logger.write(taskPodLogRecord{Message: "provider execution finished", ExitCode: &exitCode})
	return err
}

func providerProcessExitCode(err error) (int, bool) {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return 0, false
	}
	if code := exitErr.ExitCode(); code >= 0 {
		return code, true
	}
	return 1, true
}

func environmentValue(environ []string, name string) string {
	prefix := name + "="
	for _, entry := range environ {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}
