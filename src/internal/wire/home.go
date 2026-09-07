package wire

import (
	"path/filepath"
	"strings"
)

const ConfigInputRoot = "/opt/multica/config-input"

// ConfigCopy describes an init-only input and its native HOME destination.
type ConfigCopy struct {
	Source string `json:"source"`
	Target string `json:"target"`
}

// ConfigHomePath permits provider configuration without replacing the daemon's
// identity, assigned skills, or sessions. Parents of protected paths may be
// directories, but cannot be files. Callers copying a tree check every entry.
func ConfigHomePath(path string, directory bool) bool {
	if filepath.Clean(path) != path || !strings.HasPrefix(path, Home+"/") {
		return false
	}
	for _, protected := range []string{PiSessionsRoot, Home + "/.multica/config.json", Home + "/.codex/skills", Home + "/.pi/agent/sessions"} {
		if path == protected || strings.HasPrefix(path, protected+"/") || !directory && strings.HasPrefix(protected, path+"/") {
			return false
		}
	}
	return true
}
