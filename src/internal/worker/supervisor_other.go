//go:build !linux

package worker

import (
	"context"
	"errors"
)

func Serve(context.Context) error { return errors.New("worker supervisor requires Linux PID 1") }
