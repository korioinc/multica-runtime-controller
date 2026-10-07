//go:build !linux && !darwin

package checkout

import "errors"

func renameCheckout(int, string, string) error {
	return errors.New("atomic checkout publication is unsupported on this platform")
}
