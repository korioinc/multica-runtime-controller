package runtimeimage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// PiNPMDirectory is package-manager state, distinct from provider credentials
// and sessions. Only this image-seed subtree accepts installed npm assets.
const PiNPMDirectory = ".pi/agent/npm"

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

func ValidateSeed(d Descriptor) error {
	if d.HomeSeed == "" {
		return nil
	}
	root, err := immutableResolved(d.HomeSeed)
	if err != nil {
		return err
	}
	return ValidateSeedContents(root)
}

// ValidateSeedContents is also used immediately before HOME publication. The
// image descriptor is responsible for binding this tree to immutable storage.
func ValidateSeedContents(root string) error {
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	root = resolved
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return errors.New("home seed is not an image directory")
	}
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if rel == PiNPMDirectory {
			if err := ValidateNPMSeed(path); err != nil {
				return err
			}
			return fs.SkipDir
		}
		if prohibitedSeed(rel) {
			return fmt.Errorf("home seed contains credential/session/log path %q", rel)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("home seed contains link or special file")
		}
		return nil
	})
}

// ValidateNPMSeed validates an installed npm tree before it is admitted or
// copied. Package source names such as token/session are valid inside
// node_modules; the ordinary HOME credential rules still apply outside here.
func ValidateNPMSeed(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() {
		return errors.New("Pi npm seed must be a real directory")
	}
	root, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	manifest := filepath.Join(root, "package.json")
	info, err = os.Lstat(manifest)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return errors.New("Pi npm seed requires a regular package.json")
	}
	raw, err := os.ReadFile(manifest)
	if err != nil {
		return err
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(raw, &metadata); err != nil || metadata == nil {
		return errors.New("Pi npm seed has an invalid package.json")
	}
	info, err = os.Lstat(filepath.Join(root, "node_modules"))
	if err != nil || !info.IsDir() {
		return errors.New("Pi npm seed requires a real node_modules directory")
	}
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == "." {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if relative != "node_modules" && !strings.HasPrefix(relative, "node_modules/") {
			switch relative {
			case "package.json", "package-lock.json", "npm-shrinkwrap.json", ".gitignore":
				if info.Mode().IsRegular() {
					return nil
				}
			}
			return fmt.Errorf("Pi npm seed contains non-package state %q", relative)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return ValidateNPMCommandLink(root, path)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("Pi npm seed contains a special file")
		}
		return nil
	})
}

// ValidateNPMCommandLink checks one relative npm command link and its executable
// target are confined to the supplied npm tree. It does not require an image's
// package manifest or installed package inventory; ValidateNPMSeed owns those.
func ValidateNPMCommandLink(root, path string) error {
	if relative, err := filepath.Rel(root, path); err != nil || relative == "." || !filepath.IsLocal(relative) {
		return errors.New("Pi npm command link is outside its installation")
	}
	parent := filepath.Dir(path)
	if filepath.Base(parent) != ".bin" || filepath.Base(filepath.Dir(parent)) != "node_modules" {
		return errors.New("Pi npm seed only permits npm command links")
	}
	target, err := os.Readlink(path)
	if err != nil {
		return err
	}
	if filepath.IsAbs(target) {
		return errors.New("Pi npm command link must be relative")
	}
	joined := filepath.Join(parent, target)
	if relative, err := filepath.Rel(root, joined); err != nil || !filepath.IsLocal(relative) {
		return errors.New("Pi npm command link escapes its installation")
	}
	resolved, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return err
	}
	if relative, err := filepath.Rel(root, resolved); err != nil || !filepath.IsLocal(relative) {
		return errors.New("Pi npm command link resolves outside its installation")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0100 == 0 {
		return errors.New("Pi npm command link must target an executable regular file")
	}
	return nil
}
