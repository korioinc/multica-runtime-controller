// Package wire contains the single current controller/worker data contract.
package wire

import (
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/korioinc/multica-runtime-controller/internal/configuration"
	"github.com/korioinc/multica-runtime-controller/internal/core"
)

const (
	ControllerRoot    = core.Root
	PrivateRoot       = "/opt/multica/private"
	WorkspaceRoot     = "/workspace"
	Home              = configuration.Home
	ControlRoot       = "/run/multica"
	DesktopRoot       = ControlRoot + "/desktop"
	DesktopHome       = DesktopRoot + "/home"
	DesktopLaunchPath = DesktopRoot + "/launch.sock"
	ChromeProfileRoot = configuration.ChromeProfileRoot
	ChromeOriginal    = "/opt/google/chrome/google-chrome.multica-original"
	RequestPath       = "/etc/multica/task/request.json"
	RequestKey        = "request.json"
	MaxRequestBytes   = 1 << 20
	WorkerConfigPath  = "/etc/multica/runtime/worker.json"
)

func Digest(raw []byte) string { return core.Digest(raw) }
func UUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
func Value(env []string, key string) string {
	for i := len(env) - 1; i >= 0; i-- {
		if k, v, ok := strings.Cut(env[i], "="); ok && k == key {
			return v
		}
	}
	return ""
}
func Environment(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}
