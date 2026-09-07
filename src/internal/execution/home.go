package execution

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// LayoutHome runs before nested session/config mounts. Otherwise kubelet can
// create their parent directories as root, preventing native CLI state writes.
func LayoutHome(arguments []string) error {
	if err := os.MkdirAll(wire.Home, 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(wire.Home)
	if err != nil {
		return err
	}
	defer root.Close()
	paths := []string{".multica/pi-sessions", ".codex/skills", ".pi/agent"}
	for _, argument := range arguments {
		path, ok := strings.CutPrefix(argument, "--home-parent=")
		if !ok || filepath.Clean(path) != path || !strings.HasPrefix(path, wire.Home+"/") {
			return errors.New("home initialization requires confined native HOME parents")
		}
		paths = append(paths, strings.TrimPrefix(path, wire.Home+"/"))
	}
	for _, path := range paths {
		current := ""
		for _, part := range strings.Split(filepath.ToSlash(path), "/") {
			current = filepath.Join(current, part)
			if err := root.Mkdir(current, 0700); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			st, err := root.Lstat(current)
			if err != nil || !st.IsDir() {
				return errors.New("native HOME parent is not a regular directory")
			}
		}
	}
	return nil
}
