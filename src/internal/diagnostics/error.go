// Package diagnostics carries deliberately public failure context. Callers
// choose codes and identifiers; wrapped payloads are never emitted by main.
package diagnostics

import "errors"

type Error struct {
	Reason      string
	Path        string
	SourceGroup string
	Cause       error
}

func (e *Error) Error() string { return e.Reason }
func (e *Error) Unwrap() error { return e.Cause }

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
