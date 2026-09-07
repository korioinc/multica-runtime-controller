//go:build !linux

package execution

import (
	"context"
	"errors"
	"time"
)

func superviseWorker(context.Context, time.Duration) error {
	return errors.New("worker PID 1 requires Linux")
}
