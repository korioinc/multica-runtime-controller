package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// SDK logs also include commands, stderr and provider configuration. Only the
// lifecycle fields needed to distinguish result delay from cleanup may leave
// the trusted runner. Logs never establish a provider result.
func providerSDKLogger(taskID, runtimeID string) *slog.Logger {
	return slog.New(providerLogHandler{taskID: taskID, runtimeID: runtimeID})
}

type providerLogHandler struct{ taskID, runtimeID string }

func (h providerLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo
}

func (h providerLogHandler) Handle(ctx context.Context, record slog.Record) error {
	var provider, phase string
	switch record.Message {
	case "codex finished":
		provider, phase = "codex", "outcome_observed"
	case "pi finished":
		provider, phase = "pi", "outcome_observed"
	case "codex lifecycle":
		provider = "codex"
		record.Attrs(func(a slog.Attr) bool {
			if a.Key == "phase" && a.Value.Kind() == slog.KindString && a.Value.String() == "cleanup" {
				phase = "cleanup"
			}
			return true
		})
	case "codex did not close stdout after stdin EOF; forcing shutdown":
		provider, phase = "codex", "stdout_shutdown_timeout"
	case "codex process still alive after reader exited; forcing shutdown":
		provider, phase = "codex", "process_shutdown_timeout"
	case "codex stdout reader remained open after bounded process wait":
		provider, phase = "codex", "stdout_shutdown_unconfirmed"
	}
	if phase == "" {
		return nil
	}
	attrs := []slog.Attr{slog.String("provider", provider), slog.String("phase", phase)}
	if wire.UUID(h.taskID) {
		attrs = append(attrs, slog.String("task", h.taskID))
	}
	if wire.UUID(h.runtimeID) {
		attrs = append(attrs, slog.String("runtime", h.runtimeID))
	}
	record.Attrs(func(a slog.Attr) bool {
		v := a.Value.Resolve()
		switch a.Key {
		case "pid", "process_group", "attempt":
			if v.Kind() == slog.KindInt64 && v.Int64() >= 0 {
				attrs = append(attrs, slog.Int64(a.Key, v.Int64()))
			}
		case "reaped":
			if v.Kind() == slog.KindBool {
				attrs = append(attrs, slog.Bool(a.Key, v.Bool()))
			}
		case "duration", "latency", "grace":
			if v.Kind() == slog.KindString {
				if duration, err := time.ParseDuration(v.String()); err == nil && duration >= 0 {
					attrs = append(attrs, slog.String(a.Key, duration.String()))
				}
			}
		case "status":
			if v.Kind() == slog.KindString {
				switch v.String() {
				case "completed", "failed", "cancelled", "aborted", "timeout":
					attrs = append(attrs, slog.String("status", v.String()))
				}
			}
		}
		return true
	})
	slog.Default().LogAttrs(ctx, record.Level, "provider lifecycle", attrs...)
	return nil
}

func (h providerLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h providerLogHandler) WithGroup(string) slog.Handler      { return h }
