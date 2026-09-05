package checkout

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Match the v0.4.40 Multica trailer and merge/squash exclusions, without a
// controller-cache state file. Each new plan refreshes this task-local setting.
const coauthorHook = `#!/bin/sh
# multica-runtime-controller:co-authored-by
case "$2" in
  merge|squash) exit 0 ;;
esac
TRAILER="Co-authored-by: multica-agent <github@multica.ai>"
if grep -qF "$TRAILER" "$1"; then
  exit 0
fi
git interpret-trailers --in-place --trailer "$TRAILER" "$1"
`

func reconcileCoauthor(dir string, enabled bool) error {
	path := filepath.Join(dir, ".git", "hooks", "prepare-commit-msg")
	current, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read checkout coauthor hook: %w", err)
	}
	// User-installed hooks belong to the task and are never replaced/deleted.
	if err == nil && !bytes.Equal(current, []byte(coauthorHook)) {
		return nil
	}
	if !enabled {
		if err == nil {
			return os.Remove(path)
		}
		return nil
	}
	if err == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o755)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(coauthorHook)
	closeErr := file.Close()
	return errors.Join(writeErr, closeErr)
}
