# Multica Runtime Controller

This controller runs the **unmodified official Multica 0.4.40 release binary** on Kubernetes and moves provider processes into isolated task Pods. The image does not download, patch, or compile Multica daemon source. Both controller and workers use the same checksum-verified official CLI.

## Runtime Model

One controller Deployment owns the workspace PVC. The runtime wrapper starts the official daemon and a loopback API bridge. Provider shims launch short-lived task Pods and relay stdin, stdout, stderr, cancellation, and exit status through Kubernetes exec.

The controller's preparation directory and the worker's writable directory are physically different. A worker mounts only its bound `.runtime-workers/<storage-id>` PVC subdirectory at the absolute task path expected by the daemon. It cannot see the controller task directories, other worker directories, shared `.repos` cache, or controller state registry. Operator credentials remain shared as configured.

Task claiming, scheduling, prompt construction, default repository refs, provider sessions, and completion reporting remain owned by the official daemon. The wrapper observes successful task claims through the existing HTTP batch/legacy and WebSocket `tasks.claim` contracts. It records only task identity, a credential fingerprint, repository scope, and storage/session bindings in the controller-only `.runtime-controller/state-v2` directory. It preserves unknown API fields and never retries a claim itself.

Before the daemon receives a claim, the bridge verifies any prior work directory and Pi session against that repository scope. Unknown or mismatched references are cleared using the official prior-session fields, including its continuity notice. The provider shim independently requires a matching observed claim and validates the actual prepared root and session before mounting storage. An existing TaskID cannot change its repository scope. Missing registry state does not adopt old worker directories.

Same-scope follow-up tasks retain their worker files and the verified Pi session. Same-TaskID retries also retain worker files when the daemon resets its own scratch directory. A worker-storage lock and an API check for surviving Pods prevent overlapping launches from using that directory after a shim crash. Each launch has its own immutable request Secret and authenticated loopback checkout broker. The Pod name is derived solely from its storage UUID, so Kubernetes permits only one Pod object for that writable directory, including after a delayed create or shim crash. Terminal Pods are replaced only after UID-checked deletion; cleanup cannot delete a later replacement. Cleanup deletes only that launch's resources.

### Repository checkout

`multica repo checkout <url> [--ref <branch-or-sha>]` reaches the task's loopback proxy, then an authenticated controller gateway. The gateway verifies the exact immutable request Secret and routes checkout to that launch's broker. A random broker capability is checked again there, so a recycled TCP port cannot identify another launch.

The broker first checks the URL against the task's observed repository URLs, preventing broader workspace authorization from exposing another task's cached bytes. It then requests the official daemon's supported `checkout_mode: isolated` in a newly created controller-only staging directory. The daemon performs its normal task authentication, workspace repository authorization, default-ref selection, branch creation, coauthor setup, and Git operations. The broker then streams the resulting standalone repository as file bytes, including `.git`. Local-clone hardlinks never cross this boundary. The worker extracts into an empty confined stage and publishes without replacing an existing path. There is no reverse synchronization into controller directories.

Repository names include a URL hash to separate identical basenames. Repeated checkout preserves existing edits and branches; changing an existing checkout's requested ref requires explicit Git commands. Coauthor hook bytes come from the official checkout. A later checkout refreshes the Multica-owned hook without overwriting custom hooks. Live setting changes take effect at the next checkout, rather than the next commit; the controller cache state file is intentionally not mounted into workers.

Workers can proxy checkout and health requests. Controller management operations such as daemon shutdown are not available through the task gateway.

### Context and persistence

Only the official current sidecar manifest's files, runtime `AGENTS.md` brief, and Codex skills are copied into the worker. Previous repositories, staging checkouts, logs, and session directories are not copied. Context writes use confined filesystem operations and replace files without following worker-created symlinks. The root `AGENTS.md` Multica section is refreshed while surrounding worker text is retained. Repository-local files remain worker-owned.

Pi mounts only the separately verified session file selected and locked by the daemon; other sessions remain hidden. Codex continues to use its ephemeral native home and operator-provided configuration as before. The controller does not copy its generated Codex configuration or rollouts into that home, so Codex file/session continuation has the same limitations as the existing intercepted runtime.

The controller stores the daemon-root to worker-storage mapping persistently. Actual edited files live under `.runtime-workers`, while the official task result reports its original daemon work-directory path. Preserve the registry and worker directories together for continuation and recovery. Official daemon GC owns its preparation/cache directories. An hourly controller sweep retires a known worker directory only after all associated preparation roots have disappeared, no task Pod or storage lock holds it, and every associated claim is older than the task deadline (at least six hours) plus a 24-hour grace period. Retirement renames it into controller-owned trash before deleting bytes outside the registry lock. Unknown directories are retained for operator recovery. This follows the official daemon GC policy: self-host completed-task GC remains disabled unless configured through `MULTICA_GC_COMPLETED_TASK_TTL`; the official Cloud default remains 14 days.

This boundary restricts filesystem access. Shared GitHub credentials and the official workspace checkout policy can still permit cloning other remote repositories over the network. Explicit `local_directory` execution is rejected; this runtime supports canonical daemon-managed `workdir` tasks on the PVC.

Compatibility checks for a CLI upgrade must cover the three claim envelopes, prior-session fields, current sidecar manifest, and the official isolated checkout API. These adapters are controller code; there is no forked daemon build to maintain.

## Runtime Image

Controller and task Pods use the same immutable image digest. The image contains:

- the official Multica CLI release downloaded from `multica-ai/multica` and verified by SHA-256;
- the Kubernetes provider interception shim;
- pinned Codex, Copilot, Antigravity, and Pi CLIs;
- PHP 8.5 with Composer plus the MongoDB, Redis, and Zstandard extensions;
- Node.js 26, Python 3 (`python` aliases `python3`), Go, and Rust (`rustc`, Cargo, and rustup);
- Git/Git LFS, GitHub CLI, k9s, kubectx/kubens, kubectl, AWS CLI, OCI CLI, and Google Cloud CLI;
- zlib and Cyrus SASL runtime/development packages;
- a native build toolchain (`gcc`, `g++`, `make`, pkg-config, CMake, and Ninja);
- LLM-friendly source/data utilities (`jq`, `yq`, ripgrep, fd, patch, rsync, zip/xz, file, and tree);
- process/network diagnostics (`procps`, lsof, iproute2, dnsutils, and netcat);
- ShellCheck/shfmt, uv/uvx, and Corepack.

The image never compiles Multica daemon source.

Pinned runtime, extension, cloud CLI, Multica, and provider versions live in
`build/runtime-versions.env`. The four-hour automation checks the official
Multica release, updates `MULTICA_CLI_VERSION`, and increments the root
`VERSION` patch in the same pull request. A release version therefore reaches
`main` only with the CLI update it identifies.

```bash
# Build and load the host architecture for local verification.
make image \
  IMAGE=ghcr.io/korioinc/multica-runtime-controller:dev \
  VERSION=dev
```

After a `develop`-to-`main` merge, the `Release` workflow publishes the
multi-platform image as both the numeric version and `latest`, then creates the
matching `v<VERSION>` GitHub Release. The runtime repository owns and completes
only that image and GitHub Release flow. The Helm repository independently
polls the public GHCR package hourly, or performs the same check when maintainers
run its update workflow manually. That repository owns the chart source,
validation, version update, and publishing. Configure both
`controller.image.reference` and `runtime.image.reference` with the published
digest.

## Public Repository and Release Automation

The repository is public source. No open-source license grant is implied until
a `LICENSE` file is added by the repository owner. Report vulnerabilities only
through the private process in [SECURITY.md](SECURITY.md), never in public
issues, pull requests, logs, or artifacts.

Three workflows own delivery:

1. `CLI Version Update` runs every four hours or manually. It updates the
   Multica CLI pin and patch `VERSION`, opens one PR into `develop`, waits for
   `verify` and `runtime-image`, squash-merges the exact checked head, and asks
   the next workflow to create the promotion PR.
2. `Create develop to main PR` provides the two required PR checks and keeps one
   direct `develop`-to-`main` PR. The promotion remains visible for the
   maintainer to merge.
3. `Release` runs for every `main` push. It reads `VERSION`, builds and pushes
   `linux/amd64` plus `linux/arm64` to GHCR as `<VERSION>` and `latest`, and
   creates the matching GitHub Release. It does not invoke or authenticate to
   the Helm repository.

Bot-created pull requests use an exact-head `repository_dispatch` check run
because events created by the repository `GITHUB_TOKEN` do not recursively
start another workflow. GitHub access uses the ephemeral repository-scoped token
and job-specific permissions. The Helm repository discovers newer stable runtime
images from public GHCR on its hourly schedule or through a manual workflow run.
No release gate, repair workflow, or secondary auto-merge workflow is required.

Stable versions match `MAJOR.MINOR.PATCH`. The CLI updater increments one patch
per actual Multica release change. A release retry accepts only the same main
revision and rejects a tag or GitHub Release owned by another revision.
Scheduled runs are polling;
use the workflow's manual dispatch when a public-repository schedule is paused.

## Mount Runtime Configuration

Provider configuration and authentication files must come from Kubernetes Secrets or ConfigMaps, never from the image or Helm values. `runtime.extraVolumes` and `runtime.extraVolumeMounts` are mounted into both the official daemon Pod and every intercepted task Pod. Mount each Codex file directly below the normal `$HOME/.codex` directory. Before a task worker starts, a non-root init container creates only that native directory in the task's ephemeral `agent-home`; it does not stage or copy configuration.

For Codex:

```bash
kubectl --namespace multica create configmap multica-codex-config \
  --from-file=config.toml=/Users/jaesung/.codex/config.toml

kubectl --namespace multica create secret generic multica-codex-auth \
  --from-file=auth.json=/Users/jaesung/.codex/auth.json
```

```yaml
runtime:
  extraVolumes:
    - name: codex-home
      projected:
        defaultMode: 0440
        sources:
          - configMap:
              name: multica-codex-config
              items:
                - key: config.toml
                  path: config.toml
          - secret:
              name: multica-codex-auth
              items:
                - key: auth.json
                  path: auth.json
  extraVolumeMounts:
    - name: codex-home
      mountPath: /home/multica/agents/.codex/config.toml
      subPath: config.toml
      readOnly: true
    - name: codex-home
      mountPath: /home/multica/agents/.codex/auth.json
      subPath: auth.json
      readOnly: true
```

The controller does not set `CODEX_HOME`, and the interception layer removes any task-local `CODEX_HOME` override supplied by Multica. The official daemon and each task Pod therefore use their own standard `$HOME/.codex` directory directly, with operator files mounted read-only and provider-created state kept in that Pod's ephemeral home volume.

Provider API keys and other environment credentials must come from a Kubernetes
Secret rather than Helm values. Add one or more native `EnvFromSource` entries
through `runtime.extraEnvFrom`; the chart applies them to the official daemon,
and the existing provider interception path forwards inherited provider
environment variables to each task Pod.

```yaml
runtime:
  extraEnvFrom:
    - secretRef:
        name: multica-runtime-provider-env
```

For GitHub repository access, provide `GITHUB_PAT_TOKEN`, `GITHUB_USER_NAME`,
and `GITHUB_USER_EMAIL` in that Secret. The runtime exports the PAT as
`GH_TOKEN`, configures `gh auth git-credential` for GitHub HTTPS credentials,
maps GitHub SSH-style URLs to HTTPS, and supplies the commit author identity
through environment-scoped Git configuration. No SSH agent or writable
`known_hosts` file is required for GitHub repository operations.

## Pi Packages and Persistent State

The chart provisions one workspace PVC and uses standard application directories:

- `$HOME/.codex` is each Pod's native, ephemeral Codex home; it is not redirected to the workspace PVC.
- The controller initializes `$HOME/.pi` on the PVC. A task's trusted init container reads this package seed read-only and copies its package directories and configuration into that Pod's ephemeral `$HOME/.pi`. Shared sessions, logs, and runtime caches are not copied, and the worker never mounts the seed.
- Pi mounts only the daemon-selected `--session` file at its original absolute path. Both processes see the same inode, preserving the daemon's lock and resume checks. The parent directory is ephemeral, so other global sessions remain hidden. New sibling sessions created through Pi's fork/new commands remain local to that Pod. Non-Pi workers receive no persistent Pi session mount.

Pi uses these standard paths directly. The controller does not set `PI_CODING_AGENT_DIR`, and the interception layer removes any task-local override.

Declare reviewed package sources in rollout values. An npm source without a
version resolves the latest release whenever the package init container runs:

```yaml
runtime:
  pi:
    configMapName: multica-runtime-pi-config
    packages:
      - npm:pi-mcp-adapter
      - npm:pi-thinking-level@0.2.1
      - npm:pi-web-access@0.21.0
      - npm:pi-openai-service-tier@0.1.4
      - npm:@dietrichgebert/ponytail@4.9.0
```

When `configMapName` is set, every ConfigMap item path is treated as relative to `$HOME/.pi` and copied there unchanged before package installation. For example, `agent/settings.json` becomes `$HOME/.pi/agent/settings.json`; the initializer does not add an `agent` directory of its own. It ignores `auth.json`, clears only the `packages` field in Pi's standard `agent/settings.json`, and then runs `pi install <source>` for each declaration. Pi still owns package installation, layout, and settings writes, while `runtime.pi.packages` remains the package source of truth. Removed package files can remain as an unused cache, but Pi no longer loads them.

Pi packages are executable code. They run with the Pi process's task credentials, filesystem access, and network access. Package changes therefore belong in reviewed rollout configuration, not task commands. Chart validation accepts unversioned npm sources for latest-at-install resolution, exact npm versions, and commit-pinned Git sources. Authentication remains task-scoped and must not be stored in the Pi ConfigMap or PVC state.

Task changes to Pi packages or settings are local to the Pod. The persistent package seed is an operator-controlled input, not a place for task-produced state.

When upgrading from a version with the full workspace mount, stop old task Pods before accepting new work. Use a clean workspace PVC and rebuild the shared `.pi` seed from reviewed ConfigMap contents and package declarations; old writable Git caches and executable package state must not be assumed trustworthy. Keep the old PVC separately if its source changes or sessions need recovery. Deploy controller and workers together from the official-binary image. Existing Pods retain their old mounts until replaced. Old patched images and signing Secrets are not used by this runtime. Legacy roots and sessions without the new controller registry are not imported automatically; their original contents are retained for recovery.

The official daemon injects task-scoped Multica credentials and agent custom environment variables into the provider shim. Each launch attempt stores the request only in its own immutable, owner-scoped Kubernetes Secret. Normal provider exit deletes that attempt's Secret and Pod; abrupt shim termination may leave them until their existing Kubernetes lifetime bounds remove them.

## Install

Create a Secret containing an official Multica user or Cloud Node token. Daemon-only `mdt_` tokens are not accepted by the official CLI.

```bash
kubectl --namespace multica create secret generic multica-runtime-controller-token \
  --from-literal=token='mul_...'
```

Example values:

```yaml
multica:
  baseURL: https://multica.example.com
  controllerTokenSecret:
    name: multica-runtime-controller-token
    key: token

controller:
  image:
    reference: ghcr.io/korioinc/multica-runtime-controller@sha256:<runtime-image-digest>
    pullPolicy: IfNotPresent

runtime:
  name: runtime-controller
  image:
    reference: ghcr.io/korioinc/multica-runtime-controller@sha256:<runtime-image-digest>
    pullPolicy: IfNotPresent
  capacity: 20
  taskDeadline: 6h
  pi:
    packages:
      - npm:pi-mcp-adapter@2.22.0
```

The controller token determines which workspaces the daemon registers. The chart creates and reuses a Kubernetes Secret containing the stable daemon identity, so operators do not need to discover or configure a workspace UUID.

Install the release:

```bash
helm repo add korioinc https://korioinc.github.io/helm
helm repo update
helm upgrade --install multica-runtime-controller \
  korioinc/multica-runtime-controller \
  --namespace multica \
  --create-namespace \
  -f operator-values.yaml
```

The chart uses one controller replica because the official daemon owns one stable runtime identity. NetworkPolicies allow only managed task Pods to reach the controller's daemon proxy and otherwise deny inbound traffic to controller and task Pods. Outbound traffic remains unrestricted for Multica, provider CLIs, Git, MCP servers, and web access.

The controller container runs with a TTY because the official Multica CLI writes foreground logs to stderr only when it detects a terminal. This makes daemon and task lifecycle logs available through `kubectl logs` and K9s instead of only inside `~/.multica/daemon.log`.

While a task is running, its Pod also emits provider lifecycle and redacted protocol metadata to stdout. The log includes direction, stream, JSON-RPC method and ID, byte count, and exit code, but never request parameters, responses, prompts, tokens, or other protocol payloads.

```bash
kubectl --context local-k3s logs --namespace multica --follow task-<task-id>
```

Pi package installation logs are available from the generated package init containers:

```bash
kubectl --namespace multica logs deployment/multica-runtime-controller \
  --container pi-package-0
```

The runtime release publishes and verifies only its image and GitHub Release.
The Helm repository independently polls public GHCR hourly, and its updater
advances the chart version and both digest-pinned image references together when
it discovers a newer stable runtime. Roll those chart values and image digests
back together on initialization or provider smoke-test failure.

## Development

The repository root owns delivery and orchestration assets, while `src/` is the
sole Go module boundary:

```text
.
├── build/
├── scripts/
└── src/       # go.mod, go.sum, cmd/, internal/
```

Run the supported build and verification workflows from the repository root:

```bash
make build
make test
make verify
```

This runs Go tests, race tests, vet, repository validation, and workflow validation.

For direct Go tool usage, select the nested module explicitly. Root-level
module discovery such as `go test ./...` is not supported.

```bash
go -C src test ./...
```
