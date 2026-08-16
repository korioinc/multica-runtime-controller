# Multica Runtime Controller

This chart runs the official Multica daemon on Kubernetes while moving every provider process into an isolated `task-<short-task-id>-<generated-suffix>` Pod. The Multica backend and task lifecycle remain entirely official; interception happens only at the provider executable boundary.

## Runtime Model

An installation has one long-lived `multica-runtime-controller` Deployment and one ordinary shared workspace PersistentVolumeClaim. The Deployment runs the official `multica daemon start --foreground` process, and both the controller and short-lived task Pods mount the claim.

The image exposes `agy`, `codex`, `copilot`, and `pi` as provider shims. The official daemon discovers those commands normally. Before the daemon starts, Pi itself installs the declared packages into its normal `$HOME/.pi/agent` directory on the shared workspace claim. When the daemon starts a task, it supplies `MULTICA_TASK_ID`, the prepared work directory, arguments, environment, and stdio. The shim then:

1. Creates an immutable request Secret with a Kubernetes-generated attempt name, then creates a short-lived Pod that references that exact Secret.
2. Mounts the same workspace PVC and operator-provided configuration volumes.
3. Executes the real provider binary in that Pod through the Kubernetes exec streaming protocol.
4. Proxies stdin, stdout, stderr, cancellation, and the provider exit status back to the official daemon.
5. Deletes only that attempt's Pod and request Secret when the provider exits normally.

One logical task may have multiple provider launch attempts. Every attempt receives a distinct generated Secret and Pod name, so residue from a shim process that was killed before deferred cleanup cannot block a retry with `AlreadyExists`. Cleanup remains best effort after abrupt process termination; the Pod deadline and controller-Pod ownership bound any retained resources.

The official daemon still owns claiming, progress, cancellation, completion, workspace preparation, and every Multica API request. No custom Multica backend image or private task-worker API is required.

Every official CLI request in a task Pod goes to a loopback-only proxy on the normal daemon port. That proxy authenticates to a gateway beside the controller with its exact request Secret name, task identity, and token. The gateway gets that Secret directly, verifies its ownership labels and immutable task request, strips all private authentication headers, and forwards the original HTTP method, path, query, headers, and body to the one official daemon. No endpoint allowlist is applied, and no Multica CLI or backend source is modified.

This intentionally places authenticated task code in the daemon's control-plane trust boundary. A task can call every daemon route, including control routes such as `POST /shutdown`; the task-scoped token and NetworkPolicy restrict who can reach the proxy, not which daemon operation that task can invoke.

## Runtime Image

Controller and task Pods use the same immutable image digest. The image contains:

- the official Multica CLI release downloaded from `multica-ai/multica` and verified by SHA-256;
- the Kubernetes provider interception shim;
- pinned Codex, Copilot, Antigravity, and Pi CLIs;
- PHP 8.5 with Composer plus the MongoDB, Redis, and Zstandard extensions;
- Node.js 26, Python 3 (`python` aliases `python3`), and Go;
- Git/Git LFS, GitHub CLI, k9s, kubectx/kubens, kubectl, AWS CLI, OCI CLI, and Google Cloud CLI;
- zlib and Cyrus SASL runtime/development packages;
- a native build toolchain (`gcc`, `g++`, `make`, pkg-config, CMake, and Ninja);
- LLM-friendly source/data utilities (`jq`, `yq`, ripgrep, fd, patch, rsync, zip/xz, file, and tree);
- process/network diagnostics (`procps`, lsof, iproute2, dnsutils, and netcat);
- ShellCheck/shfmt, uv/uvx, and Corepack.

The image never compiles Multica from a local source checkout.

Pinned runtime, extension, cloud CLI, Multica, and provider versions live in
`build/runtime-versions.env`. The four-hour automation changes only
`MULTICA_CLI_VERSION`, `CODEX_VERSION`, and `PI_VERSION`. Production release
identity is allocated from the terminal Git tag, GitHub Release, and GHCR
inventory after a reviewed `develop`-to-`main` merge; runtime pin maintenance
does not allocate or increment a stable version. Every current workflow passes
an explicit `ci`, `develop`, or stable version to the image build. During the
two-phase workflow cutover only, the root `VERSION` file remains a bounded
old-`main` compatibility sentinel and an omitted `build-args --version` emits a
deprecation marker. Phase B removes both together.

```bash
# Build and load the host architecture for local verification.
make image \
  IMAGE=ghcr.io/korioinc/multica-runtime-controller:dev \
  VERSION=dev

# Break-glass image build. This does not create an official GitHub Release and
# cannot replace the digest-bound release workflow.
make image-push \
  IMAGE=ghcr.io/korioinc/multica-runtime-controller:recovery-<revision> \
  VERSION=<version> \
  COMMIT=<full-40-character-revision>
```

Official stable publication is owned by the `Release` GitHub Actions workflow.
It builds amd64 on the native `ubuntu-24.04` runner and arm64 on the native
`ubuntu-24.04-arm` runner, pushes each result by digest, and assembles the
immutable multi-platform version manifest only after both builds pass. It then
creates the same-revision GitHub Release and promotes that digest to `latest`.
After publication, configure both `controller.image.reference` and
`runtime.image.reference` with the recorded multi-platform digest.

## Public Repository and Release Automation

The repository is public source. No open-source license grant is implied until
a `LICENSE` file is added by the repository owner. Report vulnerabilities only
through the private process in [SECURITY.md](SECURITY.md), never in public
issues, pull requests, logs, or artifacts.

Eight workflows own delivery:

- `CI` is secret-free. Its build and verification jobs are read-only, and its
  literal `verify` and `runtime-image` jobs are the required checks for every
  pull request. The runtime image check
  fans out to native amd64 and arm64 runners and succeeds only after both
  architecture builds pass. Automation uses a
  `repository_dispatch` event so GitHub always loads the reviewed default-branch
  copy against an exact bot PR head; the workflow rejects a moved trusted ref
  or mismatched pull request. After a successful bot-PR attempt, one isolated
  dispatch job emits an `automation-merge` repository dispatch carrying
  that exact run ID, attempt, workflow SHA, PR, and head SHA.
- `Runtime Version Update` runs at minute 0 every four hours UTC and can also
  be started with the `runtime-version-update` repository-dispatch event. It
  reads only the fixed official Multica, Codex, and
  Pi sources, maintains one `automation/runtime-versions` pull request into
  `develop` with the run-scoped `GITHUB_TOKEN`, and dispatches exact-head CI.
  Its write-capable job loads and hash-verifies the resolver from the exact
  live `main` workflow revision, starts Python in isolated mode, and binds all
  file and Git operations to the absolute Actions workspace instead of
  executing control code from `develop`. It never writes to `main`.
- `Runtime Version Auto Merge` is a separate trusted `repository_dispatch`
  consumer. It waits for and checks the source CI workflow, run attempt,
  actors, repository, branch, head,
  current pull request, files, commit identity, and the exact successful CI
  jobs without checking out or executing pull request code. A least-privilege
  merge job performs matched-head squash merges for env-only runtime updates
  into protected `develop`. A main-ancestry sync PR is the sole merge-commit
  case. A separate Actions-write job dispatches the trusted default-branch
  reconciler for the exact merge SHA.
- `Create develop into main PR` runs only from the reviewed `main` copy: after
  successful `develop` push CI, after `main` pushes, once per hour, and on an
  exact-revision workflow dispatch. It derives one of `sync`, `wait-release`,
  `none`, or `promotion` from live ancestry and release inventory. A sync PR
  restores `main` ancestry to `develop`; otherwise, only a terminal release may
  lead to one direct bot-owned `develop`-to-`main` promotion PR and trusted
  exact-head CI. The promotion requires repository-owner approval; this
  workflow never merges it.
- `Development Image` is repository-dispatch-only and always loads the reviewed
  default-branch workflow with the exact live `develop` SHA as input. A read-only verify job
  resolves all build arguments before separate native amd64 and arm64
  package-write jobs check out the source and push per-architecture digests. A
  final job publishes their verified manifest as `develop` and
  `develop-<full-commit-SHA>`. Each architecture uses its own
  `runtime-develop-<architecture>` write cache and may restore the matching
  trusted `runtime-main-<architecture>` cache. The workflow never changes
  stable version tags, `latest`, or a GitHub Release.
- `Release` evaluates every new `main` revision, accepts only an exact
  bot-authored promotion with an exact-head `jskorlol` approval and merge, and
  allocates the next patch from the complete stable inventory. Recovery uses
  the `stable-release-recovery` repository-dispatch event with the exact full
  revision. Native amd64 and arm64 proofs plus current launcher canaries precede
  immutable tag reservation; only then may the numeric manifest, GitHub
  Release, and `latest` be published in that order.
- `Release Repair` is the primary protected repair launcher. With the rollout
  gate read back as false, it creates and verifies one canonical checkpoint
  Issue, disables and drains the recorded workflow set, seals the post-drain
  surface in one immutable hash-chained Issue comment, and proposes only its
  mode-specific protected PR. It never edits itself.
- `Release Repair Guard` is the independent peer launcher. It owns recovery of
  the primary launcher and shares the same checkpoint lock, seal, nonce, and
  exact restore mapping. Either peer stages and canaries a repaired launcher;
  a failed canary re-disables it, and the Issue closes only after both canaries
  and exact original workflow-state restoration succeed.

The release, development publisher, updater, CI dispatcher, merge, and develop
promotion dispatcher use only GitHub's ephemeral repository-scoped `GITHUB_TOKEN` with
job-specific permissions. There is no updater GitHub App, PAT, registry
credential, repository or Environment secret, or automation Environment. The
privileged merge workflow has no checkout, artifact download, pull request
execution, or cache restore. Tokens must never be placed in source, shell
history, workflow inputs, build arguments, artifacts, or cache keys.

The non-secret repository variable `RELEASE_AUTOMATION_ENABLED` is the rollout
gate. Bootstrap and repair read back literal `false` before mutation. Operators
set and read back `true` only after both launcher blobs and the current ruleset
have successful canary proof and the checkpoint's original workflow mapping is
restored. A retry supplies the same Issue number, canonical body hash, seal ID,
seal hash, and nonce; an edited or duplicate Issue/seal, unexpected ref, PR,
check, package, or release delta, nonzero drained run, widened gate, or 403
fails closed.

GitHub creates an approval-required ordinary workflow run for pull requests
opened or updated by `GITHUB_TOKEN`. Automation therefore sends a second,
validated repository-dispatch CI run for the exact PR head. Repository dispatch
always selects the reviewed default-branch workflow definition. The `develop` ruleset
requires those source-pinned checks with strict base freshness and permits
squash plus the ancestry-sync merge commit. The `main` ruleset is also strict
and requires the same checks plus repository-owner review; neither branch
ruleset has a bypass actor. The release workflow independently revalidates
that `jskorlol` approved the exact `develop` head and merged the promotion.

The first GHCR candidate is private by default. Before a GitHub Release can be
created, an organization owner must make the linked
`multica-runtime-controller` package public once in Package settings; this
visibility change is irreversible. Rerun the same version and exact revision
afterward. The OCI source label links the package to this repository, and the
workflow verifies public visibility before finalizing the release.

Stable image tags match `^[0-9]+\.[0-9]+\.[0-9]+$`; automation never overwrites
them, while `latest` remains mutable. Because GHCR does not provide a stable-tag
immutability control, the recorded manifest digest is authoritative. The
GitHub `v*` tag ruleset denies update and deletion without bypass actors. A
release rerun reconciles
`absent → candidate_private → candidate_verified → release_verified → latest_digest_matched`
and continues only the earliest incomplete step. A different revision, source,
or digest is a terminal conflict and requires a higher patch release.

GitHub's scheduled workflows are a polling cadence, not a four-hour SLA: runs
can be delayed, and a public repository schedule can be disabled after 60 days
without repository activity. Treat a last successful updater run older than
eight hours as an operational alert, re-enable the workflow if needed, and use
the corresponding repository-dispatch event to reconcile it. Kubernetes/Helm deployment remains a separate
operation and is not performed by either automation workflow.

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

The chart provisions one ordinary shared workspace PVC and uses standard application directories:

- `$HOME/.codex` is each Pod's native, ephemeral Codex home; it is not redirected to the workspace PVC.
- `$HOME/.pi` is mounted from the PVC's `.pi` directory for the controller and every task Pod.
- `$HOME/.multica/pi-sessions` is mounted from the PVC's `.multica/pi-sessions` directory, which is the session location Multica passes to Pi.

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

The Pi home is persistent, but it is not isolated from task code. Every task mounts the same workspace PVC read-write and can mutate the shared `.pi` directory. A requirement for task-to-state isolation needs a separate storage and mount boundary.

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
helm upgrade --install multica-runtime-controller \
  deploy/helm/multica-runtime-controller \
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

Publish and verify the new runtime image before updating the chart copy, package values, and both image digests in the deployment repository. Roll those chart values and image digests back together on initialization or provider smoke-test failure.

## Development

The repository root owns delivery and orchestration assets, while `src/` is the
sole Go module boundary:

```text
.
├── src/       # go.mod, go.sum, cmd/, internal/
├── build/
├── deploy/
└── scripts/
```

Run the supported build and verification workflows from the repository root:

```bash
make build
make test
make verify
```

This runs Go tests, race tests, vet, Helm lint, Helm rendering, and strict Kubernetes schema validation.

For direct Go tool usage, select the nested module explicitly. Root-level
module discovery such as `go test ./...` is not supported.

```bash
go -C src test ./...
```
