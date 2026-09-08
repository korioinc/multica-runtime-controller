package runtimeimage

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var variable = regexp.MustCompile(`\$\{([^}]+)\}`)

type Locations struct{ Home, TmpDir, Workspace string }

func Reserved(name string) bool {
	name = strings.ToUpper(name)
	return name == "PATH" || name == "HOME" || name == "TMPDIR" || name == "PWD" || name == "OLDPWD" || name == "SHELL" || name == "ENV" || name == "BASH_ENV" || name == "LD_PRELOAD" || strings.HasPrefix(name, "MULTICA_") || strings.HasPrefix(name, "KUBERNETES_") || strings.HasPrefix(name, "POD_") || strings.HasPrefix(name, "ENV_") || strings.HasPrefix(name, "TASK_")
}

func expand(value string, loc Locations) (string, error) {
	invalid := false
	out := variable.ReplaceAllStringFunc(value, func(s string) string {
		switch s {
		case "${HOME}":
			return loc.Home
		case "${TMPDIR}":
			return loc.TmpDir
		case "${WORKSPACE}":
			return loc.Workspace
		}
		invalid = true
		return s
	})
	if invalid || strings.Contains(out, "${") || strings.Contains(out, "$(") || strings.ContainsAny(out, "`\x00") {
		return "", errors.New("unsupported runtime image variable expansion")
	}
	return out, nil
}

// Vars applies image defaults. Its caller applies operator values and task
// authority after this step; installed executable selection never comes from PATH.
func Vars(d Descriptor, base []string, loc Locations) ([]string, error) {
	values := map[string]string{}
	for _, entry := range base {
		if k, v, ok := strings.Cut(entry, "="); ok {
			values[k] = v
		}
	}
	for k, v := range d.Env {
		if Reserved(k) || !envName.MatchString(k) {
			return nil, fmt.Errorf("reserved runtime image variable %s", k)
		}
		x, err := expand(v, loc)
		if err != nil {
			return nil, err
		}
		values[k] = x
	}
	dirs := make([]string, 0, len(d.BinDirs))
	for _, path := range d.BinDirs {
		resolved, err := immutableResolved(path)
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(resolved)
		if err != nil || !info.IsDir() {
			return nil, errors.New("runtime image PATH entry is not a directory")
		}
		dirs = append(dirs, resolved)
	}
	values["PATH"] = strings.Join(dirs, ":")
	values["HOME"], values["TMPDIR"] = loc.Home, loc.TmpDir
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(values))
	for _, k := range keys {
		out = append(out, k+"="+values[k])
	}
	return out, nil
}
