//go:build !linux && !darwin

package checkout

import "errors"

func renameNoReplace(from, to string) error {
	return errors.New("worker checkout requires atomic no-replace directory rename on Linux or macOS")
}
