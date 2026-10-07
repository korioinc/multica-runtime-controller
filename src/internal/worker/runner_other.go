//go:build !linux

package worker

import (
	"context"
	"errors"
)

func RunProvider(context.Context, string) error {
	return errors.New("provider runner requires Linux")
}
