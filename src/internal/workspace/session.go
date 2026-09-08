package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// sessionFile permits only the controller-managed session-root link. The
// logical path remains the durable session identity; individual files and every
// other ancestor must remain real paths rather than arbitrary symlink aliases.
func (s *Store) sessionFile(path string) (os.FileInfo, error) {
	if !s.sessionPath(path) {
		return nil, errors.New("Pi session is outside the daemon session directory")
	}
	root := s.options.SessionRoot
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	expected := path
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		if root != DefaultSessionRoot || !ownedSessionEntry(rootInfo) {
			return nil, errors.New("unexpected session-root symlink")
		}
		if err := realDirectory(filepath.Dir(root)); err != nil {
			return nil, err
		}
		destination := filepath.Join(s.options.WorkspaceRoot, ".multica-runtime/sessions")
		target, err := os.Readlink(root)
		if err != nil || target != destination {
			return nil, errors.New("session-root link does not target the owned workspace sessions")
		}
		if err := realDirectory(destination); err != nil {
			return nil, err
		}
		destinationInfo, err := os.Lstat(destination)
		if err != nil || !ownedSessionEntry(destinationInfo) {
			return nil, errors.New("workspace session directory is not owned by the controller user")
		}
		expected = filepath.Join(destination, filepath.Base(path))
	} else if err := realDirectory(root); err != nil {
		return nil, err
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != expected {
		return nil, errors.New("Pi session must resolve to its owned regular file")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("Pi session is not a regular file")
	}
	return info, nil
}

func ownedSessionEntry(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}
