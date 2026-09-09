// Package diagnostics carries deliberately public failure context. Callers
// choose codes and identifiers; wrapped payloads are never emitted by main.
package diagnostics

import "errors"

type Error struct {
	Reason      string
	Path        string
	SourceGroup string
	AttemptID   string
	StorageID   string
	EntrySHA256 string
	Cause       error
}

func (e *Error) Error() string { return e.Reason }
func (e *Error) Unwrap() error { return e.Cause }

// Attributes emits only context deliberately selected for public diagnostics.
// Raw wrapped errors may contain payload bytes and must not be logged.
func Attributes(err error) []any {
	var e *Error
	if !errors.As(err, &e) {
		return nil
	}
	attributes := []any{"reason", e.Reason}
	for _, field := range []struct{ key, value string }{
		{"path", e.Path}, {"sourceGroup", e.SourceGroup}, {"attempt", e.AttemptID},
		{"storage", e.StorageID}, {"entrySHA256", e.EntrySHA256},
	} {
		if field.value != "" {
			attributes = append(attributes, field.key, field.value)
		}
	}
	return attributes
}

func Wrap(reason string, cause error) error {
	if cause == nil {
		return nil
	}
	var existing *Error
	if errors.As(cause, &existing) {
		return cause
	}
	return &Error{Reason: reason, Cause: cause}
}

func AtPath(reason, path string, cause error) error {
	if cause == nil {
		return nil
	}
	var existing *Error
	if errors.As(cause, &existing) {
		return cause
	}
	return &Error{Reason: reason, Path: path, Cause: cause}
}

func ForGroup(reason, group string, cause error) error {
	if cause == nil {
		return nil
	}
	var existing *Error
	if errors.As(cause, &existing) {
		return cause
	}
	return &Error{Reason: reason, SourceGroup: group, Cause: cause}
}
