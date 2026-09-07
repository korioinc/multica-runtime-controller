package environment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/korioinc/multica-runtime-controller/internal/core"
	"golang.org/x/sys/unix"
)

type storeIdentity struct {
	SchemaVersion int    `json:"schemaVersion"`
	OwnerID       string `json:"ownerID"`
}

func acquire(ctx context.Context, path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func release(f *os.File) { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }
func atomicJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".publish-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0444); err != nil {
		f.Close()
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
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func realDirectory(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("store path is not a real directory: %s", path)
	}
	return nil
}
func Layout(storeRoot, ownerID, environmentID string) error {
	if ownerID == "" || strings.ContainsAny(ownerID, "\x00\r\n") || !core.ValidSHA(environmentID) || !filepath.IsAbs(storeRoot) {
		return errors.New("invalid tools store owner/generation/root")
	}
	if err := os.MkdirAll(storeRoot, 0755); err != nil {
		return err
	}
	if err := realDirectory(storeRoot); err != nil {
		return err
	}
	lock, err := acquire(context.Background(), filepath.Join(storeRoot, ".store.lock"))
	if err != nil {
		return err
	}
	defer release(lock)
	path := filepath.Join(storeRoot, "store.json")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		entries, err := os.ReadDir(storeRoot)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != ".store.lock" && entry.Name() != "lost+found" {
				return errors.New("tools PVC is nonempty and has no current store identity")
			}
		}
		if err = atomicJSON(path, storeIdentity{1, ownerID}); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		var identity storeIdentity
		if json.Unmarshal(b, &identity) != nil || identity.SchemaVersion != 1 || identity.OwnerID != ownerID {
			return errors.New("tools store owner/format mismatch")
		}
	}
	generations := filepath.Join(storeRoot, "generations")
	if err = os.MkdirAll(generations, 0755); err != nil {
		return err
	}
	if err = realDirectory(generations); err != nil {
		return err
	}
	locks := filepath.Join(storeRoot, "locks")
	if err = os.MkdirAll(locks, 0755); err != nil {
		return err
	}
	if err = realDirectory(locks); err != nil {
		return err
	}
	lockFile, err := os.OpenFile(filepath.Join(locks, environmentID+".lock"), os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	if err = lockFile.Close(); err != nil {
		return err
	}
	generation := filepath.Join(generations, environmentID)
	if err = os.MkdirAll(generation, 0755); err != nil {
		return err
	}
	return realDirectory(generation)
}
