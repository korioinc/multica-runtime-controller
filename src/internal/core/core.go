// Package core verifies and materializes the immutable executable artifact.
package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
)

const Root = "/opt/multica/core"
const Version = 1

var digestReference = regexp.MustCompile(`^[^\s@]+@sha256:[0-9a-f]{64}$`)
var shaPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Contract describes the bytes in one platform-specific core artifact.
type Contract struct {
	ContractVersion int               `json:"contractVersion"`
	BuildID         string            `json:"buildID"`
	Platform        string            `json:"platform"`
	OfficialVersion string            `json:"officialVersion"`
	OfficialSHA256  string            `json:"officialSHA256"`
	Files           map[string]string `json:"files"`
}

func PinnedImage(value string) bool       { return digestReference.MatchString(value) }
func ValidSHA(value string) bool          { return shaPattern.MatchString(value) }
func HostPlatform() string                { return runtime.GOOS + "/" + runtime.GOARCH }
func SupportedPlatform(value string) bool { return value == "linux/amd64" || value == "linux/arm64" }
func Digest(data []byte) string           { v := sha256.Sum256(data); return hex.EncodeToString(v[:]) }
func HashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (c Contract) Equal(other Contract) bool {
	a, _ := json.Marshal(c)
	b, _ := json.Marshal(other)
	return string(a) == string(b)
}

func (c Contract) Validate(platform string) error {
	if c.ContractVersion != Version || c.BuildID == "" || !SupportedPlatform(c.Platform) || c.Platform != platform {
		return errors.New("core contract/build/platform mismatch")
	}
	if c.OfficialVersion == "" || !ValidSHA(c.OfficialSHA256) || c.Files["multica"] != c.OfficialSHA256 || len(c.Files) != 2 || !ValidSHA(c.Files["runtime"]) {
		return errors.New("invalid core file contract")
	}
	return nil
}

func read(root, platform string) (Contract, error) {
	var c Contract
	st, err := os.Lstat(root)
	if err != nil {
		return c, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return c, errors.New("core root is not a real directory")
	}
	st, err = os.Lstat(filepath.Join(root, "contract.json"))
	if err != nil {
		return c, err
	}
	if !st.Mode().IsRegular() {
		return c, errors.New("core contract is not a regular file")
	}
	b, err := os.ReadFile(filepath.Join(root, "contract.json"))
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if err = c.Validate(platform); err != nil {
		return c, err
	}
	for name, want := range c.Files {
		p := filepath.Join(root, name)
		info, err := os.Lstat(p)
		if err != nil {
			return c, err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			return c, fmt.Errorf("core %s is not an executable regular file", name)
		}
		got, err := HashFile(p)
		if err != nil {
			return c, err
		}
		if got != want {
			return c, fmt.Errorf("core %s digest mismatch", name)
		}
	}
	return c, nil
}

var ErrCompatibility = errors.New("core compatibility validation failed")

func Check(root, platform string) (result Contract, resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = fmt.Errorf("%w: %w", ErrCompatibility, resultErr)
		}
	}()
	c, err := read(root, platform)
	if err != nil {
		return c, err
	}
	runtimeInfo, err := os.Stat(filepath.Join(root, "runtime"))
	if err != nil {
		return c, err
	}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		info, err := os.Lstat(filepath.Join(root, "shims", alias))
		if err != nil {
			return c, err
		}
		if !info.Mode().IsRegular() || !os.SameFile(runtimeInfo, info) {
			return c, fmt.Errorf("core shim %s is not a runtime hardlink", alias)
		}
	}
	disabled, err := os.Lstat(filepath.Join(root, "disabled"))
	if err != nil {
		return c, err
	}
	if !disabled.IsDir() || disabled.Mode()&os.ModeSymlink != 0 {
		return c, errors.New("core disabled namespace is not a real directory")
	}
	entries, err := os.ReadDir(filepath.Join(root, "disabled"))
	if err != nil {
		return c, err
	}
	if len(entries) != 0 {
		return c, errors.New("core disabled namespace is not empty")
	}
	return c, nil
}

// Materialize publishes contract.json last. Its presence commits the artifact:
// a completed destination is checked and never repaired in place. Before that
// commit, a retried init may discard only the materializer's partial files.
func Materialize(source, target, platform string) (Contract, error) {
	c, err := read(source, platform)
	if err != nil {
		return c, err
	}
	if !filepath.IsAbs(target) || filepath.Clean(target) == "/" {
		return c, errors.New("invalid materialization root")
	}
	if err = os.MkdirAll(target, 0755); err != nil {
		return c, err
	}
	info, err := os.Lstat(target)
	if err != nil {
		return c, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return c, errors.New("materialization root is not a real directory")
	}
	if _, err = os.Lstat(filepath.Join(target, "contract.json")); err == nil {
		existing, err := Check(target, platform)
		if err != nil {
			return c, err
		}
		if !existing.Equal(c) {
			return c, errors.New("materialized core differs from source")
		}
		return existing, nil
	} else if !os.IsNotExist(err) {
		return c, err
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return c, err
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "runtime", "multica", "shims", "disabled", ".contract.pending":
		default:
			return c, fmt.Errorf("unexpected file in incomplete core artifact: %s", entry.Name())
		}
	}
	for _, entry := range entries {
		if err = os.RemoveAll(filepath.Join(target, entry.Name())); err != nil {
			return c, err
		}
	}
	for _, name := range []string{"runtime", "multica"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			return c, err
		}
		if Digest(data) != c.Files[name] {
			return c, fmt.Errorf("source core %s changed during materialization", name)
		}
		if err = writeSynced(filepath.Join(target, name), data, 0555); err != nil {
			return c, err
		}
	}
	if err = os.Mkdir(filepath.Join(target, "shims"), 0755); err != nil {
		return c, err
	}
	if err = os.Mkdir(filepath.Join(target, "disabled"), 0555); err != nil {
		return c, err
	}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		if err = os.Link(filepath.Join(target, "runtime"), filepath.Join(target, "shims", alias)); err != nil {
			return c, err
		}
	}
	if err = syncDirectory(filepath.Join(target, "shims")); err != nil {
		return c, err
	}
	data, err := json.Marshal(c)
	if err != nil {
		return c, err
	}
	pending := filepath.Join(target, ".contract.pending")
	if err = writeSynced(pending, data, 0444); err != nil {
		return c, err
	}
	if err = os.Rename(pending, filepath.Join(target, "contract.json")); err != nil {
		return c, err
	}
	if err = syncDirectory(target); err != nil {
		return c, err
	}
	return Check(target, platform)
}

func writeSynced(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
