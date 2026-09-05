package intercept

import (
	"errors"
	"path/filepath"
	"strings"
)

// Only package/configuration inputs are copied. Runtime history and sessions
// belong to the task; workers never receive the shared seed mount.
const taskHomeInitScript = `
install -d -m 0700 "$AGENT_HOME" "$AGENT_HOME/.codex" "$AGENT_HOME/.pi/agent" "$AGENT_HOME/.multica/pi-sessions"
for entry in settings.json models.json mcp.json auth.json AGENTS.md SYSTEM.md npm git bin extensions skills prompts themes; do
  if [ -e "$PI_SEED/agent/$entry" ]; then
    cp -a -- "$PI_SEED/agent/$entry" "$AGENT_HOME/.pi/agent/"
  fi
done
`

// taskStorageRoot uses the daemon's reserved configuration-root environment,
// not task-editable Git metadata or repository URLs. Multica can reuse a prior
// task's root, so its directory name need not contain the current task ID.
func taskStorageRoot(request Request) (string, string, error) {
	configRoot := requestEnvironmentValue(request.Env, "MULTICA_TASK_CONFIG_ROOT")
	if !filepath.IsAbs(configRoot) || filepath.Clean(configRoot) != configRoot || filepath.Base(configRoot) != "multica-config" {
		return "", "", errors.New("task requires the daemon's absolute MULTICA_TASK_CONFIG_ROOT")
	}
	root := filepath.Dir(configRoot)
	rel, err := filepath.Rel(workspaceRoot, root)
	if err != nil {
		return "", "", errors.New("task storage must be below the workspace PVC")
	}
	// Both the legacy UUID and current readable layouts have workspace/task
	// components. In particular, never accept .repos, .pi or .task_roots.
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 2 || strings.HasPrefix(parts[0], ".") || strings.HasPrefix(parts[1], ".") {
		return "", "", errors.New("task storage must select one workspace task directory")
	}
	workRoot := filepath.Join(root, "workdir")
	if !filepath.IsAbs(request.WorkDir) || filepath.Clean(request.WorkDir) != request.WorkDir ||
		(request.WorkDir != workRoot && !strings.HasPrefix(request.WorkDir, workRoot+string(filepath.Separator))) {
		return "", "", errors.New("provider work directory must belong to the task configuration root")
	}
	return root, rel, nil
}

// The daemon opens and locks this file before invoking the Pi shim. Mount the
// same file so appends remain visible to its resume check, without exposing the
// other sessions in the daemon's directory.
func taskPiSession(request Request) (string, error) {
	if request.Provider != "pi" {
		return "", nil
	}
	var session string
	for i := 0; i < len(request.Args); i++ {
		arg := request.Args[i]
		var value string
		switch {
		case arg == "--session":
			i++
			if i == len(request.Args) {
				return "", errors.New("Pi requires the daemon's session file")
			}
			value = request.Args[i]
		case strings.HasPrefix(arg, "--session="):
			value = strings.TrimPrefix(arg, "--session=")
		default:
			continue
		}
		if session != "" || filepath.Clean(value) != value || filepath.Dir(value) != piSessionsMountPath ||
			strings.HasPrefix(filepath.Base(value), ".") || filepath.Ext(value) != ".jsonl" {
			return "", errors.New("Pi must select one daemon session file")
		}
		session = value
	}
	if session == "" {
		return "", errors.New("Pi requires the daemon's session file")
	}
	return session, nil
}
