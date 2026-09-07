package environment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
)

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var variable = regexp.MustCompile(`\$\{([^}]+)\}`)

// Confined resolves links and rejects paths outside the selected generation.
func Confined(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, "../") {
		return "", fmt.Errorf("unconfined environment path %q", relative)
	}
	base, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	base, err = filepath.Abs(base)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(base, relative))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(base, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("environment path escapes prefix: %q", relative)
	}
	return resolved, nil
}
func executable(root, relative string) (string, error) {
	p, err := Confined(root, relative)
	if err != nil {
		return "", err
	}
	st, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0111 == 0 {
		return "", fmt.Errorf("environment entrypoint is not executable: %s", relative)
	}
	return p, nil
}
func ProviderPath(root string, manifest Manifest, id string) (string, error) {
	p, ok := manifest.Providers[id]
	if !ok {
		return "", fmt.Errorf("provider %s not enabled", id)
	}
	return executable(root, p.Entrypoint)
}

func readManifest(root string, input Input) (Manifest, []byte, error) {
	var m Manifest
	p, err := Confined(root, "environment.json")
	if err != nil {
		return m, nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return m, nil, err
	}
	if err = json.Unmarshal(b, &m); err != nil {
		return m, nil, err
	}
	if m.SchemaVersion != 1 {
		return m, nil, errors.New("unsupported environment manifest")
	}
	enabled := map[string]bool{}
	for _, id := range input.Providers {
		enabled[id] = true
	}
	if len(enabled) != len(m.Providers) {
		return m, nil, errors.New("manifest provider set does not match configured providers")
	}
	for id, p := range m.Providers {
		if !enabled[id] || p.Version == "" {
			return m, nil, fmt.Errorf("invalid manifest provider %s", id)
		}
		if _, err = executable(root, p.Entrypoint); err != nil {
			return m, nil, err
		}
	}
	for _, d := range m.BinDirs {
		p, err := Confined(root, d)
		if err != nil {
			return m, nil, err
		}
		st, err := os.Stat(p)
		if err != nil {
			return m, nil, err
		}
		if !st.IsDir() {
			return m, nil, errors.New("binDirs entry is not a directory")
		}
	}
	for name, value := range m.Env {
		if !envName.MatchString(name) || Reserved(name) {
			return m, nil, fmt.Errorf("reserved/invalid manifest variable %s", name)
		}
		if _, err := expand(value, Locations{}); err != nil {
			return m, nil, err
		}
		if name == "LD_LIBRARY_PATH" {
			if err := libraryPath(root, value); err != nil {
				return m, nil, err
			}
		}
	}
	if m.HomeSeed != "" {
		p, err := Confined(root, m.HomeSeed)
		if err != nil {
			return m, nil, err
		}
		st, err := os.Stat(p)
		if err != nil || !st.IsDir() {
			return m, nil, errors.New("home seed is not a directory")
		}
		if err = validateSeed(p); err != nil {
			return m, nil, err
		}
	}
	for _, check := range m.Checks {
		if len(check.Argv) == 0 || check.TimeoutSeconds < 0 {
			return m, nil, errors.New("invalid generic check")
		}
		if _, err = executable(root, check.Argv[0]); err != nil {
			return m, nil, err
		}
	}
	return m, b, nil
}

type Locations struct{ Root, Home, TmpDir, Workspace string }

func libraryPath(root, value string) error {
	expanded, err := expand(value, Locations{Root: root})
	if err != nil {
		return err
	}
	for _, dir := range strings.Split(expanded, ":") {
		rel, err := filepath.Rel(root, dir)
		if err != nil {
			return err
		}
		resolved, err := Confined(root, rel)
		if err != nil {
			return fmt.Errorf("LD_LIBRARY_PATH must remain within ENV_ROOT: %w", err)
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("LD_LIBRARY_PATH entry is not a directory")
		}
	}
	return nil
}

func expand(value string, loc Locations) (string, error) {
	var invalid string
	out := variable.ReplaceAllStringFunc(value, func(s string) string {
		switch s {
		case "${HOME}":
			return loc.Home
		case "${TMPDIR}":
			return loc.TmpDir
		case "${ENV_ROOT}":
			return loc.Root
		case "${WORKSPACE}":
			return loc.Workspace
		}
		invalid = s
		return s
	})
	if invalid != "" || strings.Contains(out, "${") || strings.Contains(out, "$(") || strings.ContainsRune(out, '`') || strings.ContainsRune(out, 0) {
		return "", errors.New("unsupported manifest variable expansion")
	}
	return out, nil
}

// Vars applies manifest defaults to an image environment. Operator/task values may
// subsequently override non-reserved keys; callers enforce their authority fields last.
func Vars(manifest Manifest, base []string, loc Locations) ([]string, error) {
	values := map[string]string{}
	for _, entry := range base {
		k, v, ok := strings.Cut(entry, "=")
		if ok {
			values[k] = v
		}
	}
	for k, v := range manifest.Env {
		if Reserved(k) || !envName.MatchString(k) {
			return nil, fmt.Errorf("reserved manifest variable %s", k)
		}
		x, err := expand(v, loc)
		if err != nil {
			return nil, err
		}
		values[k] = x
	}
	dirs := make([]string, 0, len(manifest.BinDirs))
	for _, d := range manifest.BinDirs {
		p, err := Confined(loc.Root, d)
		if err != nil {
			return nil, err
		}
		dirs = append(dirs, p)
	}
	if current := values["PATH"]; current != "" {
		dirs = append(dirs, current)
	}
	values["PATH"] = strings.Join(dirs, ":")
	values["HOME"] = loc.Home
	values["TMPDIR"] = loc.TmpDir
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+values[k])
	}
	return out, nil
}

func prohibitedSeed(relative string) bool {
	for _, part := range strings.Split(strings.ToLower(relative), string(filepath.Separator)) {
		switch part {
		case "auth.json", "auth.toml", "credentials", "credentials.json", ".credentials.json", "tokens", "token", "sessions", "session", "rollouts", "logs", ".ssh", ".aws", ".kube":
			return true
		}
		if strings.HasSuffix(part, ".log") || strings.HasSuffix(part, ".jsonl") {
			return true
		}
	}
	return false
}
func validateSeed(root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if prohibitedSeed(rel) {
			return fmt.Errorf("home seed contains credential/session/log path %q", rel)
		}
		st, err := d.Info()
		if err != nil {
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			target, err := Confined(root, rel)
			if err != nil {
				return err
			}
			info, err := os.Stat(target)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return errors.New("home seed symlink must resolve to a regular file")
			}
		} else if !st.IsDir() && !st.Mode().IsRegular() {
			return errors.New("special file in home seed")
		}
		return nil
	})
}

// CopySeed creates independent regular files and never overwrites an existing HOME file.
func CopySeed(root string, manifest Manifest, home string) error {
	if manifest.HomeSeed == "" {
		return nil
	}
	seed, err := Confined(root, manifest.HomeSeed)
	if err != nil {
		return err
	}
	if err = validateSeed(seed); err != nil {
		return err
	}
	if err = os.MkdirAll(home, 0700); err != nil {
		return err
	}
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		return err
	}
	return filepath.WalkDir(seed, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(seed, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		dest := filepath.Join(home, rel)
		// Existing native HOME links may point at credentials or other mounts. Never follow them.
		parent := filepath.Dir(dest)
		resolved, err := filepath.EvalSymlinks(parent)
		if err != nil {
			return err
		}
		if resolved != parent {
			return errors.New("home seed destination has symlink parent")
		}
		if d.IsDir() {
			if err = os.Mkdir(dest, 0700); os.IsExist(err) {
				st, e := os.Lstat(dest)
				if e != nil {
					return e
				}
				if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
					return errors.New("seed destination is not a real directory")
				}
				return nil
			}
			return err
		}
		source, err := Confined(seed, rel)
		if err != nil {
			return err
		}
		in, err := os.Open(source)
		if err != nil {
			return err
		}
		defer in.Close()
		st, err := in.Stat()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, st.Mode().Perm()&0700|0600)
		if os.IsExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, in)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func contentDigest(root string) (string, error) {
	h := sha256.New()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." || rel == ReadyName {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		var value string
		switch {
		case info.Mode().IsRegular():
			value, err = core.HashFile(p)
		case info.IsDir():
			value = "directory"
		case info.Mode()&os.ModeSymlink != 0:
			_, err = Confined(root, rel)
			if err == nil {
				value, err = os.Readlink(p)
			}
		default:
			err = fmt.Errorf("special file in environment: %s", rel)
		}
		if err != nil {
			return err
		}
		record, _ := json.Marshal([]any{rel, uint32(info.Mode()), value})
		h.Write(record)
		h.Write([]byte{'\n'})
		return nil
	})
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
