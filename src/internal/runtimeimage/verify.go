package runtimeimage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/diagnostics"
)

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

// CodexHelperExecutable resolves the admitted npm launcher to its native package.
// The immutable image supplies this identity; retained task links never select it.
func CodexHelperExecutable(d Descriptor) (Executable, error) {
	launcher, ok := d.Providers["codex"]
	if !ok {
		return Executable{}, errors.New("Codex is absent from the admitted image")
	}
	path, err := executable(launcher, d.Controller)
	if err != nil || filepath.Base(path) != "codex.js" || filepath.Base(filepath.Dir(path)) != "bin" {
		return Executable{}, errors.New("Codex helper requires the admitted npm launcher")
	}
	packageRoot := filepath.Dir(filepath.Dir(path))
	if filepath.Base(packageRoot) != "codex" || filepath.Base(filepath.Dir(packageRoot)) != "@openai" {
		return Executable{}, errors.New("Codex launcher has an unsupported package layout")
	}
	var cpu, triple string
	switch d.Platform {
	case "linux/arm64":
		cpu, triple = "arm64", "aarch64-unknown-linux-musl"
	case "linux/amd64":
		cpu, triple = "x64", "x86_64-unknown-linux-musl"
	default:
		return Executable{}, errors.New("Codex helper requires a supported Linux platform")
	}
	platformPackage := "@openai/codex-linux-" + cpu
	var metadata struct {
		Name, Version        string
		OptionalDependencies map[string]string
		OS, CPU              []string
	}
	readMetadata := func(name string) error {
		if err := rootOwnedImagePath(name); err != nil {
			return err
		}
		info, err := os.Lstat(name)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return errors.New("Codex package metadata is not a bounded regular file")
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		metadata = struct {
			Name, Version        string
			OptionalDependencies map[string]string
			OS, CPU              []string
		}{}
		return json.Unmarshal(raw, &metadata)
	}
	if err := rootOwnedImagePath(path); err != nil {
		return Executable{}, err
	}
	if err := readMetadata(filepath.Join(packageRoot, "package.json")); err != nil || metadata.Name != "@openai/codex" ||
		metadata.Version != launcher.Version || metadata.OptionalDependencies[platformPackage] != "npm:@openai/codex@"+launcher.Version+"-linux-"+cpu {
		return Executable{}, errors.New("Codex launcher package differs from the admitted version")
	}
	nativeRoot := filepath.Join(filepath.Dir(packageRoot), "codex-linux-"+cpu)
	if err := readMetadata(filepath.Join(nativeRoot, "package.json")); err != nil || metadata.Name != "@openai/codex" ||
		metadata.Version != launcher.Version+"-linux-"+cpu || len(metadata.OS) != 1 || metadata.OS[0] != "linux" || len(metadata.CPU) != 1 || metadata.CPU[0] != cpu {
		return Executable{}, errors.New("Codex native package differs from the admitted platform/version")
	}
	native := filepath.Join(nativeRoot, "vendor", triple, "bin", "codex")
	if err := rootOwnedImagePath(native); err != nil {
		return Executable{}, err
	}
	info, err := os.Lstat(native)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return Executable{}, errors.New("Codex native helper is not an immutable executable")
	}
	sha, err := core.HashFile(native)
	return Executable{Path: native, Version: launcher.Version, SHA256: sha}, err
}

func rootOwnedImagePath(path string) error {
	if !ImmutablePath(path) {
		return errors.New("Codex package path is mutable")
	}
	for current := path; current != "/"; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || current != path && !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0022 != 0 {
			return errors.New("Codex package path is indirect or writable")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 || info.Mode().IsRegular() && stat.Nlink != 1 {
			return errors.New("Codex package path has untrusted ownership or shared files")
		}
	}
	return nil
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
	return d, core.Digest(raw), nil
}

func Check(ctx context.Context, root, controllerRoot, platform string) (d Descriptor, digest string, resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = diagnostics.Wrap("runtime_image_verification_failed", resultErr)
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
