package runtimeimage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// CheckImageMounts is an admission check, after installation verification. Every
// path used to resolve image files must belong to the image in both controller
// and worker. Even a read-only volume can replace an otherwise valid image file.
func CheckImageMounts(d Descriptor, mounts []string) error {
	resolvedMounts, err := resolveMounts(mounts)
	if err != nil {
		return err
	}
	mounts = append(slices.Clone(mounts), resolvedMounts...)
	paths := []imagePath{{DescriptorPath, false}, {filepath.Dir(d.Controller.RuntimePath), true}, {d.Daemon.Path, false}}
	for _, provider := range d.Providers {
		paths = append(paths, imagePath{provider.Path, false})
	}
	for _, directory := range d.BinDirs {
		paths = append(paths, imagePath{directory, true})
	}
	if d.HomeSeed != "" {
		paths = append(paths, imagePath{d.HomeSeed, true})
	}
	for _, path := range paths {
		if !ImmutablePath(path.path) {
			return errors.New("installed path is in a mutable filesystem area")
		}
		trace, err := resolveImagePath(path)
		if err != nil {
			return err
		}
		for _, resolved := range trace {
			if !ImmutablePath(resolved.path) {
				return errors.New("installed path traverses a mutable filesystem area")
			}
			if err = resolved.checkMounts(mounts); err != nil {
				return err
			}
		}
	}
	return nil
}

type imagePath struct {
	path      string
	directory bool
}

func (p imagePath) checkMounts(mounts []string) error {
	for _, mount := range mounts {
		if !filepath.IsAbs(mount) || filepath.Clean(mount) != mount || strings.ContainsAny(mount, "\x00\n\r") {
			return errors.New("image admission requires canonical volume mount paths")
		}
		if withinImagePath(mount, p.path) || p.directory && withinImagePath(p.path, mount) {
			return fmt.Errorf("volume mount %s shadows installed image path %s", mount, p.path)
		}
	}
	return nil
}

func withinImagePath(parent, child string) bool {
	return parent == "/" || child == parent || strings.HasPrefix(child, parent+"/")
}

func resolveMounts(mounts []string) ([]string, error) {
	resolved := make([]string, 0, len(mounts))
	traces := make([][]imagePath, 0, len(mounts))
	for _, mount := range mounts {
		if !filepath.IsAbs(mount) || filepath.Clean(mount) != mount || strings.ContainsAny(mount, "\x00\n\r") {
			return nil, errors.New("image admission requires canonical volume mount paths")
		}
		// An init or future worker destination need not exist in the main
		// container yet. Resolve existing prefixes; kubelet creates the rest.
		path, trace, err := traceImagePath(imagePath{mount, false}, true)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, path)
		traces = append(traces, append(trace, imagePath{path, false}))
	}
	// These targets belong to init, main and future workers. A directory
	// mounted in main can hide an image symlink used to resolve another
	// container's target. Its original destination cannot be established from
	// this view, so reject that ambiguity even if today's resolved path looks
	// unrelated to installed files. Identical destinations remain supported.
	for i, trace := range traces {
		for j, mount := range mounts {
			if i == j {
				continue
			}
			for _, path := range trace {
				for _, prefix := range []string{mount, resolved[j]} {
					if prefix != path.path && withinImagePath(prefix, path.path) {
						return nil, errors.New("volume hides another container's mount target resolution")
					}
				}
			}
		}
	}
	return resolved, nil
}

// resolveImagePath records every name consulted during resolution, including
// intermediate links and their targets. EvalSymlinks alone loses those names:
// a mounted middle link could select a valid final file only in the controller.
// Process .. after resolving links, just as filesystem pathname lookup does.
func resolveImagePath(path imagePath) ([]imagePath, error) {
	resolved, trace, err := traceImagePath(path, false)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, err
	}
	if path.directory && !info.IsDir() || !path.directory && !info.Mode().IsRegular() {
		return nil, errors.New("installed image path has an unexpected type")
	}
	return append(trace, imagePath{resolved, path.directory}), nil
}

func traceImagePath(path imagePath, allowMissing bool) (string, []imagePath, error) {
	if !filepath.IsAbs(path.path) || filepath.Clean(path.path) != path.path {
		return "", nil, errors.New("installed path must be absolute and canonical")
	}
	trace := []imagePath{path}
	pending := strings.Split(strings.TrimPrefix(path.path, "/"), "/")
	resolved, links := "/", 0
	for len(pending) != 0 {
		part := pending[0]
		pending = pending[1:]
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			resolved = filepath.Dir(resolved)
			continue
		}
		candidate := filepath.Join(resolved, part)
		info, err := os.Lstat(candidate)
		if errors.Is(err, os.ErrNotExist) && allowMissing {
			return filepath.Join(append([]string{candidate}, pending...)...), trace, nil
		}
		if err != nil {
			return "", nil, err
		}
		trace = append(trace, imagePath{candidate, path.directory && len(pending) == 0})
		if info.Mode()&os.ModeSymlink != 0 {
			links++
			if links > 255 {
				return "", nil, errors.New("installed image path has too many symlinks")
			}
			target, err := os.Readlink(candidate)
			if err != nil {
				return "", nil, err
			}
			if filepath.IsAbs(target) {
				resolved = "/"
			}
			pending = append(strings.Split(target, "/"), pending...)
			continue
		}
		if len(pending) != 0 && !info.IsDir() {
			return "", nil, errors.New("installed image path traverses a non-directory")
		}
		resolved = candidate
	}
	return resolved, trace, nil
}
