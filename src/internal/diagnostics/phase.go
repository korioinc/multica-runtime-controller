package diagnostics

import (
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// StartPhase measures one bounded preparation operation. Callers supply fixed
// phase names and only public identifiers or counts; errors are never emitted.
// Finish outside shared locks so the diagnostic sink cannot extend lock holds.
func StartPhase(phase string, attributes ...any) func(error, ...any) {
	fields := append([]any{"phase", phase}, attributes...)
	slog.Info("runtime phase started", fields...)
	started := time.Now()
	return func(err error, outcome ...any) {
		elapsed := time.Since(started)
		result := "ok"
		if err != nil {
			result = "error"
		}
		finished := append([]any{"phase", phase, "result", result, "elapsed", elapsed}, attributes...)
		slog.Info("runtime phase finished", append(finished, outcome...)...)
	}
}

// TaskAttributes also supports preparation before authorization: only canonical
// identifiers may enter diagnostics. Their shape does not grant authority.
func TaskAttributes(task, attempt string) []any {
	attributes := []any{}
	for _, field := range []struct{ name, value string }{{"task", task}, {"attempt", attempt}} {
		if id, err := uuid.Parse(field.value); err == nil && id.String() == field.value {
			attributes = append(attributes, field.name, field.value)
		}
	}
	return attributes
}
