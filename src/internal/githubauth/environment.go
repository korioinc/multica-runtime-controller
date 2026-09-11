package githubauth

import (
	"errors"
	"strconv"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/githubapp"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// WithoutAppCredentials prevents signing authority from entering daemon or task
// subprocesses. Only the controller's token broker needs the App identity.
func WithoutAppCredentials(env []string) []string {
	out := make([]string, 0, len(env))
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if ok && !githubapp.ControllerEnvironmentKey(key) {
			out = append(out, entry)
		}
	}
	return out
}

// GitEnvironment configures a repository-aware helper without writing tokens or
// Git configuration to disk. Existing unrelated environment configuration stays
// in order; the empty helper resets inherited helpers for github.com only.
func GitEnvironment(env []string) ([]string, error) {
	values := map[string]string{}
	for _, entry := range WithoutAppCredentials(env) {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	count := 0
	if raw, exists := values["GIT_CONFIG_COUNT"]; exists {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 || parsed > 1024 {
			return nil, errors.New("GitHub authentication requires valid existing Git environment configuration")
		}
		count = parsed
	}
	for i := 0; i < count; i++ {
		index := strconv.Itoa(i)
		if values["GIT_CONFIG_KEY_"+index] == "" {
			return nil, errors.New("GitHub authentication found incomplete Git environment configuration")
		}
		if _, ok := values["GIT_CONFIG_VALUE_"+index]; !ok {
			return nil, errors.New("GitHub authentication found incomplete Git environment configuration")
		}
	}
	for _, setting := range [][2]string{
		{"credential.https://github.com.helper", ""},
		{"credential.https://github.com.helper", "!" + core.Root + "/runtime github credential"},
		{"credential.https://github.com.useHttpPath", "true"},
	} {
		index := strconv.Itoa(count)
		values["GIT_CONFIG_KEY_"+index] = setting[0]
		values["GIT_CONFIG_VALUE_"+index] = setting[1]
		count++
	}
	values["GIT_CONFIG_COUNT"] = strconv.Itoa(count)
	values["GIT_TERMINAL_PROMPT"] = "0"
	values[EnabledEnv] = "true"
	return wire.Environment(values), nil
}
