package checkout

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// The hook content/decision comes from the official checkout. This package
// tracks that exact content so updates cannot overwrite a custom worker hook.
func snapshotHook(repository string) ([]byte, string, error) {
	hooks := filepath.Join(repository, ".git", "hooks")
	info, err := os.Lstat(hooks)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", nil
	}
	if err != nil || !info.IsDir() {
		return nil, "", errors.New("Git hook directory is not a plain directory")
	}
	path := filepath.Join(hooks, "prepare-commit-msg")
	info, err = os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", nil
	}
	if err != nil || !info.Mode().IsRegular() {
		return nil, "", errors.New("commit hook is not a regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(raw)
	return raw, hex.EncodeToString(hash[:]), nil
}

func refreshSnapshotHook(target, stage string, have, want identity) error {
	if have.HookHash == want.HookHash {
		return nil
	}
	_, current, err := snapshotHook(target)
	if err != nil {
		return err
	}
	if current != "" && current != have.HookHash && current != want.HookHash {
		return errors.New("official coauthor setting changed but the worker has a custom commit hook; preserve or move that hook and retry")
	}
	hook := filepath.Join(target, ".git", "hooks", "prepare-commit-msg")
	if want.HookHash == "" {
		if err := os.Remove(hook); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		raw, _, err := snapshotHook(stage)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(hook), 0o755); err != nil {
			return err
		}
		if err := replaceFile(hook, raw, 0o755); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(want)
	if err != nil {
		return err
	}
	return replaceFile(filepath.Join(target, ".git", identityFile), raw, 0o600)
}

func replaceFile(destination string, data []byte, mode os.FileMode) error {
	file, err := os.CreateTemp(filepath.Dir(destination), ".checkout-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	modeErr := file.Chmod(mode)
	if err := errors.Join(writeErr, modeErr, file.Close()); err != nil {
		return err
	}
	return os.Rename(file.Name(), destination)
}
