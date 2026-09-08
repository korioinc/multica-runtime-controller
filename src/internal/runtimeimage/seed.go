package runtimeimage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

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
