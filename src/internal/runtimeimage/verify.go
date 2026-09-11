package runtimeimage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
)

var ErrCompatibility = errors.New("runtime image compatibility validation failed")

func Decode(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

func ReadJSON(path string, value any) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return nil, errors.New("invalid runtime JSON file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return raw, Decode(raw, value)
}

func immutableResolved(path string) (string, error) {
	if !ImmutablePath(path) {
		return "", errors.New("installed path is in a mutable filesystem area")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if !ImmutablePath(resolved) {
		return "", errors.New("installed path resolves into a mutable filesystem area")
	}
	return resolved, nil
}

func executable(e Executable, controller core.Contract) (string, error) {
	p, err := immutableResolved(e.Path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(p)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return "", errors.New("installed entrypoint is not an executable regular file")
	}
	runtimeInfo, err := os.Stat(controller.RuntimePath)
	if err != nil {
		return "", err
	}
	if os.SameFile(info, runtimeInfo) || strings.HasPrefix(p, filepath.Dir(controller.RuntimePath)+"/") {
		return "", errors.New("installed entrypoint resolves to controller code")
	}
	actual, err := core.HashFile(p)
	if err != nil {
		return "", err
	}
	if actual != e.SHA256 {
		return "", errors.New("installed executable hash differs from descriptor")
	}
	return p, nil
}

func ProviderPath(d Descriptor, id string) (string, error) {
	p, ok := d.Providers[id]
	if !ok {
		return "", fmt.Errorf("provider %s not enabled", id)
	}
	// Admission has already checked the entrypoint bytes and controller
	// separation. Preserve the installed link's resolved execution path.
	return immutableResolved(p.Path)
}

// ReadMetadata reads execution settings only after controller or task authority
// has been established. The fixed image path cannot be selected by a request.
// It does not verify installed bytes or compare an image reference.
func ReadMetadata() (Descriptor, error) {
	var d Descriptor
	if _, err := ReadJSON(DescriptorPath, &d); err != nil {
		return d, err
	}
	return d, d.Validate(d.Platform)
}

func version(ctx context.Context, e Executable, args []string, env []string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, e.Path, args...)
	cmd.Env = env
	var out limitedOutput
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return errors.New("installed executable version probe failed")
	}
	pattern := regexp.MustCompile(`(^|[^0-9A-Za-z.])v?` + regexp.QuoteMeta(e.Version) + `($|[^0-9A-Za-z.])`)
	if !pattern.Match(out.data) {
		return errors.New("installed executable version differs from descriptor")
	}
	return nil
}

type limitedOutput struct{ data []byte }

func (o *limitedOutput) Write(p []byte) (int, error) {
	n := len(p)
	if len(o.data) < 65536 {
		o.data = append(o.data, p[:min(len(p), 65536-len(o.data))]...)
	}
	return n, nil
}

// ReadInstalled checks bytes and paths without starting an installed process.
// Admission uses this before consulting the receipt or the Kubernetes API.
func ReadInstalled(root, controllerRoot, platform string) (Descriptor, string, error) {
	var d Descriptor
	raw, err := ReadJSON(filepath.Join(root, "image.json"), &d)
	if err != nil {
		return d, "", err
	}
	if err = d.Validate(platform); err != nil {
		return d, "", err
	}
	controller, err := core.Check(controllerRoot, platform)
	if err != nil {
		return d, "", err
	}
	if !controller.Equal(d.Controller) {
		return d, "", errors.New("runtime descriptor controller differs from base")
	}
	if _, err = executable(d.Daemon.Executable, controller); err != nil {
		return d, "", err
	}
	for _, e := range d.Providers {
		if _, err = executable(e, controller); err != nil {
			return d, "", err
		}
	}
	if err = ValidateSeed(d); err != nil {
		return d, "", err
	}
	return d, core.Digest(raw), nil
}

// CheckInstalled verifies installed files and probes the actual tool versions.
// Local adapter integration checks use it before and after their scenarios.
func CheckInstalled(ctx context.Context, root, controllerRoot, platform string) (Descriptor, string, error) {
	d, digest, err := ReadInstalled(root, controllerRoot, platform)
	if err != nil {
		return d, digest, err
	}
	env, err := Vars(d, os.Environ(), Locations{Home: os.Getenv("HOME"), TmpDir: os.TempDir(), Workspace: "/workspace"})
	if err != nil {
		return d, "", err
	}
	if err = version(ctx, d.Daemon.Executable, []string{"version"}, env); err != nil {
		return d, "", fmt.Errorf("official daemon: %w", err)
	}
	for id, e := range d.Providers {
		if err = version(ctx, e, []string{"--version"}, env); err != nil {
			return d, "", fmt.Errorf("provider %s: %w", id, err)
		}
	}
	return d, digest, nil
}

func Check(ctx context.Context, root, controllerRoot, platform string) (d Descriptor, digest string, resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = fmt.Errorf("%w: %w", ErrCompatibility, diagnostics.Wrap("runtime_image_verification_failed", resultErr))
		}
	}()
	if err := ctx.Err(); err != nil {
		return d, digest, err
	}
	return ReadInstalled(root, controllerRoot, platform)
}

func Match(d Descriptor, digest string, r Ref) error {
	if err := r.Validate(); err != nil {
		return err
	}
	expected, err := d.Reference(r.Image, digest, r.ConfigurationDigest)
	if err != nil {
		return err
	}
	if !expected.Equal(r) {
		return errors.New("current runtime image differs from selected task image")
	}
	return nil
}
