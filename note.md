# Runtime Provider Homes

- The chart runs one controller Deployment and shares one ordinary workspace claim with short-lived task Pods.
- The controller uses `HOME=/home/multica/agents` and provider defaults. Do not add a controller-owned provider state root.
- Codex reads shared configuration from `$HOME/.codex`; remove task-local `CODEX_HOME` overrides.
- Pi uses `$HOME/.pi/agent`; do not set `PI_CODING_AGENT_DIR`. Treat provider configuration paths as relative to `$HOME/.pi` instead of adding `agent` during copy.
- The workspace PVC is mounted at `$HOME/.pi` (`.pi`) and `$HOME/.multica/pi-sessions` (`.multica/pi-sessions`) in controller and task Pods.
- Install declared Pi packages with the bundled `pi install` command instead of implementing package layout or reconciliation in Go.
- Convert `GITHUB_PAT_TOKEN`, `GITHUB_USER_NAME`, and `GITHUB_USER_EMAIL` in `daemonEnvironment`: expose `GH_TOKEN`, use environment-scoped Git identity and `gh auth git-credential`, and rewrite GitHub SSH-style URLs to HTTPS so controller cache operations and provider child processes share one non-persistent authentication path.
