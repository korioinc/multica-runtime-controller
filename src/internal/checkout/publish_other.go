//go:build !linux && !darwin

package checkout

import "errors"

func PublishDirectory(int, string, string) error {
	return errors.New("atomic no-replace publication unsupported on this platform")
}
