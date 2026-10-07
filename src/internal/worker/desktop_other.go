//go:build !linux

package worker

import (
	"context"
	"errors"
)

func RunDesktop(context.Context, string) error {
	return errors.New("desktop owner requires Linux")
}

func Chrome(context.Context, []string) error {
	return errors.New("runtime Chrome launch requires Linux")
}
