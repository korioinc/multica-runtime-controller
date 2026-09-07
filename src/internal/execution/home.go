package execution

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/korioinc/multica-runtime-controller/internal/wire"
)

// LayoutHome creates native parents before session mounts and copies operator
// configuration from init-only mounts into independent writable HOME files.
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
	var copies []wire.ConfigCopy
	for _, argument := range arguments {
		if raw, ok := strings.CutPrefix(argument, "--config-copy="); ok {
			var copy wire.ConfigCopy
			if err := json.Unmarshal([]byte(raw), &copy); err != nil {
				return errors.New("home configuration copy requires source and target JSON")
			}
			index, ok := strings.CutPrefix(copy.Source, wire.ConfigInputRoot+"/")
			number, err := strconv.Atoi(index)
			if !ok || err != nil || number < 0 || strconv.Itoa(number) != index || !wire.ConfigHomePath(copy.Target, true) {
				return errors.New("home configuration copy requires an indexed input and confined native target")
			}
			for _, other := range copies {
				if copy.Target == other.Target || strings.HasPrefix(copy.Target, other.Target+"/") || strings.HasPrefix(other.Target, copy.Target+"/") {
					return errors.New("home configuration copy targets overlap")
				}
			}
			copies = append(copies, copy)
			continue
		}
		return errors.New("unsupported home initialization argument")
	}
	for _, path := range paths {
		if err := homeDirectories(root, path); err != nil {
			return err
		}
	}
	for _, copy := range copies {
		if err := copyHomeConfig(wire.Home, copy.Source, copy.Target); err != nil {
			return err
		}
	}
	return nil
}
