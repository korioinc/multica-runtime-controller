package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

type storageSnapshot struct {
	Files   map[string]string `json:"files"`
	Session string            `json:"session"`
}

func (s storageSnapshot) digest() string {
	raw, _ := json.Marshal(s)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func snapshotStorage(root, session string) (storageSnapshot, error) {
	snapshot := storageSnapshot{Files: map[string]string{}}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return snapshot, errors.New("bound worker storage must remain a canonical directory")
	}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			snapshot.Files[relative] = "symlink:" + target
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("unsupported file in the fixture storage snapshot")
		}
		digest, err := core.HashFile(path)
		if err != nil {
			return err
		}
		snapshot.Files[relative] = digest
		return nil
	})
	if err != nil {
		return snapshot, err
	}
	if !strings.HasPrefix(session, wire.PiSessionsRoot+"/") || filepath.Dir(session) != wire.PiSessionsRoot {
		return snapshot, errors.New("sample Pi session is unconfined")
	}
	digest, err := core.HashFile(filepath.Join("/verification-workspace/.multica-runtime/sessions", filepath.Base(session)))
	if err != nil {
		return snapshot, err
	}
	snapshot.Session = digest
	return snapshot, nil
}
func snapshotsEqual(a, b storageSnapshot) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}
