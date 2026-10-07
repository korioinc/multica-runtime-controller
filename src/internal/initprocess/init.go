// Package initprocess owns the fixed init roles in the runtime executable.
package initprocess

import "log/slog"

const (
	exitFailure       = 1
	exitCannotExecute = 126
	exitNotFound      = 127
	signalExitOffset  = 128

	reasonInvalidArguments = "init_invalid_arguments"
	reasonUnsupported      = "init_unsupported_platform"
	reasonExecutable       = "init_executable_unavailable"
	reasonSubreaper        = "init_subreaper_failed"
	reasonSpawn            = "init_spawn_failed"
	reasonWait             = "init_wait_failed"
	reasonSignal           = "init_signal_failed"
	reasonLostOwnership    = "init_wait_ownership_lost"
	reasonCleanup          = "init_cleanup_failed"
	reasonFinalDrain       = "init_final_drain_failed"
	reasonPrimaryExit      = "init_primary_exited"
)

func role(args []string) string {
	if len(args) == 1 && args[0] == "controller" {
		return "controller"
	}
	if len(args) == 3 && args[0] == "worker" && args[2] != "" {
		switch args[1] {
		case "run", "desktop":
			return "worker " + args[1]
		}
	}
	return ""
}

func failure(reason, role string, pid, code int) int {
	slog.Error("runtime init failed", "reason", reason, "role", role, "pid", pid, "exit_code", code)
	return code
}
