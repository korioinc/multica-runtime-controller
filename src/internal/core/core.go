// Package core verifies the immutable controller build supplied by the base image.
package core

import (
	"bytes"
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

const Root = "/opt/multica/controller"
const Version = 2
const ABI = 2

var digestReference = regexp.MustCompile(`^[^\s@]+@sha256:[0-9a-f]{64}$`)
var shaPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var goVersionPattern = regexp.MustCompile(`^go[1-9][0-9]*\.[0-9]+\.[0-9]+$`)

// Contract describes controller code and the Go SDK, independently of an
// installed official daemon or the tools in a derived runtime image.
type Contract struct {
	SchemaVersion int               `json:"schemaVersion"`
	ControllerABI int               `json:"controllerABI"`
	BuildID       string            `json:"buildID"`
	Platform      string            `json:"platform"`
	RuntimePath   string            `json:"runtimePath"`
	RuntimeSHA256 string            `json:"runtimeSHA256"`
	ShimPaths     map[string]string `json:"shimPaths"`
	GoVersion     string            `json:"goVersion"`
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
	return bytes.Equal(a, b)
}
func (c Contract) Validate(platform string) error {
	if c.SchemaVersion != Version || c.ControllerABI != ABI || !ValidSHA(c.BuildID) || !SupportedPlatform(c.Platform) || c.Platform != platform {
		return errors.New("controller schema/ABI/build/platform mismatch")
	}
	if !ValidSHA(c.RuntimeSHA256) || !goVersionPattern.MatchString(c.GoVersion) || !filepath.IsAbs(c.RuntimePath) || filepath.Clean(c.RuntimePath) != c.RuntimePath || filepath.Base(c.RuntimePath) != "runtime" || filepath.Dir(c.RuntimePath) == "/" {
		return errors.New("invalid controller executable or Go SDK contract")
	}
	if len(c.ShimPaths) != 4 {
		return errors.New("invalid controller shim contract")
	}
	for _, alias := range []string{"pi", "codex", "copilot", "agy"} {
		if c.ShimPaths[alias] != filepath.Join(filepath.Dir(c.RuntimePath), "shims", alias) {
			return errors.New("invalid controller shim path")
		}
	}
	return nil
}

var ErrCompatibility = errors.New("controller build compatibility validation failed")

// Check validates a rootfs build without installing, copying, or repairing it.
// The declared absolute paths must belong to root; production callers use Root.
func Check(root, platform string) (result Contract, resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = fmt.Errorf("%w: %w", ErrCompatibility, resultErr)
		}
	}()
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return result, errors.New("invalid controller root")
	}
	for _, directory := range []string{root, filepath.Join(root, "shims"), filepath.Join(root, "disabled")} {
		st, err := os.Lstat(directory)
		if err != nil {
			return result, err
		}
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return result, errors.New("controller directory is not a real directory")
		}
	}
	path := filepath.Join(root, "build.json")
	st, err := os.Lstat(path)
	if err != nil {
		return result, err
	}
	if !st.Mode().IsRegular() {
		return result, errors.New("controller build contract is not a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return result, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&result); err != nil {
		return result, err
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return result, errors.New("trailing controller build contract data")
	}
	if err = result.Validate(platform); err != nil {
		return result, err
	}
	if result.RuntimePath != filepath.Join(root, "runtime") {
		return result, errors.New("controller build root mismatch")
	}
	paths := []string{result.RuntimePath}
	for _, path := range result.ShimPaths {
		paths = append(paths, path)
	}
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return result, err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			return result, fmt.Errorf("controller executable is not a regular executable: %s", filepath.Base(path))
		}
		got, err := HashFile(path)
		if err != nil {
			return result, err
		}
		if got != result.RuntimeSHA256 {
			return result, fmt.Errorf("controller executable digest mismatch: %s", filepath.Base(path))
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "disabled"))
	if err != nil {
		return result, err
	}
	if len(entries) != 0 {
		return result, errors.New("controller disabled provider namespace is not empty")
	}
	return result, nil
}
