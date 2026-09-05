# Multica Runtime Controller

This controller runs Multica on Kubernetes while moving every provider process into an isolated `task-<short-task-id>-<generated-suffix>` Pod. Workers use the official Multica CLI. The controller builds the daemon from the same release with two security patches: checkout planning without worker Git execution, and repository-aware environment reuse. The Multica backend remains unchanged.

## Runtime Model

An installation has one long-lived `multica-runtime-controller` Deployment and one workspace PersistentVolumeClaim. The Deployment runs `multica daemon start --foreground` with the full claim. Each task Pod mounts only its prepared task directory through a Kubernetes `subPath`, at its original absolute path. Pi additionally mounts the single session file selected by the daemon.

The image exposes `agy`, `codex`, `copilot`, and `pi` as provider shims. The official daemon discovers those commands normally. Before the daemon starts, Pi itself installs the declared packages into its normal `$HOME/.pi/agent` directory on the shared workspace claim. When the daemon starts a task, it supplies `MULTICA_TASK_ID`, the prepared work directory, arguments, environment, and stdio. The shim then:

1. Creates an immutable request Secret with a Kubernetes-generated attempt name, then creates a short-lived Pod that references that exact Secret.
2. Mounts that task's directory from the workspace PVC and the operator-provided configuration volumes. Other tasks, the shared `.repos` cache, and the workspace root are not mounted into the worker.
3. Executes the real provider binary in that Pod through the Kubernetes exec streaming protocol.
4. Proxies stdin, stdout, stderr, cancellation, and the provider exit status back to the official daemon.
5. Deletes only that attempt's Pod and request Secret when the provider exits normally.

One logical task may have multiple provider launch attempts. Every attempt receives a distinct generated Secret and Pod name, so residue from a shim process that was killed before deferred cleanup cannot block a retry with `AlreadyExists`. Cleanup remains best effort after abrupt process termination; the Pod deadline and controller-Pod ownership bound any retained resources.

The daemon still owns claiming, progress, cancellation, completion, workspace preparation, and Multica API requests. No custom Multica backend image is required.

Every CLI request in a task Pod goes to a loopback-only proxy on the normal daemon port. That proxy authenticates to a gateway beside the controller with its exact request Secret name, task identity, and token. The gateway gets that Secret directly, verifies its ownership labels and task request, and strips all private authentication headers. For `POST /repo/checkout`, it forces `checkout_mode: worker-plan`. The patched daemon approves the repository and task context, resolves the default ref and coauthor setting, and returns a plan before creating a checkout. The worker executes that plan using its own Git process. Other daemon requests pass through.

Multica 0.4.40 prepares the task's `workdir` before starting its provider. `multica repo checkout <url> [--ref <branch-or-sha>]` then clones directly from the approved remote inside the worker, with independent Git objects and metadata. The controller's bare cache stays private and is not used to accelerate worker clones. Repository paths include a URL hash so repositories with the same basename remain distinct. Repeating the same checkout preserves existing edits; changing its requested ref requires explicit Git commands inside the task.

The mount root comes from the daemon's reserved `MULTICA_TASK_CONFIG_ROOT`, checked against the provider working directory. Missing or inconsistent roots reject Pod creation; there is no full-PVC fallback. A prior root and session can be reused only when the repository URL set and task context match a signed controller record. Changed, missing, or unverifiable grants select a fresh environment and session. An existing current-task directory without a valid matching grant is rejected without deleting its contents. Tasks without repositories remain supported.

Reuse records live in `.runtime-controller/repo-grants` on the PVC and are authenticated with a controller-only signing key stored in an immutable Kubernetes Secret named `multica-repo-grants-<PVC-name-hash>`. The key survives controller replacement, is removed from the daemon environment before child processes start, and is excluded from worker requests. Preserve this Secret alongside the PVC to retain verified continuation; losing it safely invalidates old records.

This runtime supports daemon-managed `workdir` tasks on the workspace PVC. Explicit `local_directory` execution in `worktree` or `in_place` mode is rejected because it uses a different filesystem boundary.

This boundary prevents filesystem access to other cached repositories. GitHub credentials remain shared as configured, and the daemon still controls checkout URL authorization. It does not restrict repositories those credentials or the upstream workspace policy permit an agent to clone over the network. Worker-controlled Git configuration and hooks execute only inside the worker's restricted filesystem view.

This intentionally places authenticated task code in the daemon's control-plane trust boundary. A task can call every daemon route, including control routes such as `POST /shutdown`; the task-scoped token and NetworkPolicy restrict who can reach the proxy, not which daemon operation that task can invoke.

## Runtime Image

Controller and task Pods use the same immutable image digest. The image contains:

- the official Multica CLI release downloaded from `multica-ai/multica` and verified by SHA-256;
- a controller-only daemon built from the same tag with the patches in `build/multica-*.patch`;
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

The image never compiles Multica from a local source checkout.

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

When upgrading from a version with the full workspace mount, stop old task Pods before accepting new work. Use a clean workspace PVC and rebuild the shared `.pi` seed from reviewed ConfigMap contents and package declarations; old writable Git caches and executable package state must not be assumed trustworthy. Keep the old PVC separately if its source changes or sessions need recovery. Deploy controller and workers together from this image, including the daemon patches and Multica CLI 0.4.40. Existing Pods retain their old mounts until replaced. Legacy roots and sessions have no signed grants and are not automatically imported or converted.

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
