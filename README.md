# Multica Runtime Controller

Run the unmodified, checksum-verified Multica CLI in your Kubernetes cluster while choosing your own development environment. The runtime observes official task claims and launches each approved provider execution in a separate Pod. The official daemon retains scheduling, prompt construction, checkout policy and completion reporting.

The runtime core and development environment are separate images. Changing PHP, Rust, Go, provider installations or native libraries does not require rebuilding the core.

## Install

The chart lives in the adjacent Helm repository at `../helm/charts/multica-runtime-controller`. Its README contains the complete values and bootstrap examples.

Supply a **new core artifact digest**, a controller token Secret, the Multica backend URL and suitable storage. The chart intentionally has no old runtime image as a fallback. A release of this rewrite must provide the core digest; an old combined runtime image cannot execute this chart.

```sh
helm upgrade --install multica-runtime ../helm/charts/multica-runtime-controller \
  --namespace multica --create-namespace \
  --values operator-values.yaml \
  --set-string runtime.image.reference="$CORE_IMAGE_DIGEST"
```

Both `runtime.image.reference` and `environment.image.reference` require an OCI `@sha256:` reference. The default environment is pinned `buildpack-deps:bookworm-scm`; `linux/amd64` and `linux/arm64` are supported. Select the actual platform in `environment.platform`.

This rewrite uses a new workspace format and a separate tools PVC. Install with fresh PVCs, or reconnect PVCs already initialized by the same installation of this rewrite. There is no legacy reader, migration or fallback to the previous runtime. The stable chart-owned daemon identity must match the stored installation owner.

The controller has one replica and uses `Recreate`. Workspace and tools must use different PVCs. `ReadWriteMany` supports multiple nodes with a suitable driver. If either PVC uses `ReadWriteOnce`, set `scheduling.singleNodeName` to the actual Node name. Both controller and workers retain required affinity to that name, including after controller recreation. A missing fixed Node leaves them Pending. `ReadWriteOncePod` is unsupported.

## Choose an environment

The chart supports three paths through the same preparation contract:

- **Bundled profile:** pinned Node and `pi`, `codex`, `copilot`, `antigravity` installations. The Antigravity executable alias is `agy`.
- **Operator script:** choose `inline`, or a ConfigMap name/key with an expected SHA-256. The chart includes complete Go/Rust and other installation examples.
- **Operator image:** build native OS packages into a compatible Linux/glibc image, then supply a bootstrap that creates wrappers and the environment manifest. The PHP/native-extension and Python venv example follows this path.

The same environment image and installed generation are used by installer, controller and worker. Core init containers inject the identical core artifact into each Pod. Main containers run explicit runtime commands and do not depend on the image ENTRYPOINT.

Bootstrap runs as UID/GID 65532. It can write its tools prefix and temporary directory. It does not receive workspace data, operator runtime credentials or a Kubernetes API token. Installers must install under the provided prefix; changing the init container's root filesystem does not change another container.

The bootstrap inputs are:

| Input | Meaning |
| --- | --- |
| `ENV_ROOT` | `/opt/multica/environment`, the final installation prefix |
| `ENV_PLATFORM` | Selected Linux platform |
| `ENV_REVISION` | Explicit environment revision |
| `ENV_INPUTS_FILE` | Snapshot of non-secret JSON inputs |
| `ENV_MANIFEST_FILE` | Manifest file the script must create |
| `ENV_PROVIDERS` | JSON array of enabled builtin provider IDs |

`environment.bootstrap.secretEnvFrom` belongs only to installation. `operator.envFrom`, `operator.configVolumes` and `operator.configMounts` supply native runtime configuration separately. Operator environment sources preserve their declared prefix. Explicit operator environment values support literals and `valueFrom`, without Kubernetes `$(NAME)` interpolation.

The manifest declares schema version 1, provider entrypoints/versions, relative `binDirs`, optional environment variables, a non-secret `homeSeed`, and optional argv-based checks. Entrypoints must resolve to executable regular files within the tools prefix. A wrapper can invoke a program installed in the environment image.

Manifest variables support `${ENV_ROOT}`, `${HOME}`, `${TMPDIR}` and `${WORKSPACE}`. They are expanded for the actual consumer; task workspace paths do not inherit the controller's expansion. Precedence is image defaults, manifest, operator configuration, then official task overrides. Runtime identity, credentials and executable selection controls remain reserved. A credential-free init snapshots image defaults before installer credentials are introduced.

Provider directories are not added indiscriminately to controller PATH. Every builtin descriptor in the pinned Multica version receives an absolute executable override. Enabled providers use core hardlink shims; disabled providers use nonexistent paths in a read-only core namespace. Backend custom runtime profiles are unsupported and blocked before command discovery, including refresh. Change the enabled environment by starting a new controller/daemon process.

## Preparation and persistence

Environment identity is derived from the core image, environment image, platform, exact script hash, revision, enabled providers and non-secret inputs. Secret contents are not part of that identity. Change the revision when you want to prepare new tools.

Preparation uses a generation lock. The script and all descendants, provider probes and generic checks must finish before the runtime publishes `READY`. Timeouts terminate the process tree. Failed attempts can prepare the incomplete generation again without moving its root. A completed generation is immutable and must pass content, manifest, core and provider fingerprint checks before reuse.

The installation and consumption path is always `/opt/multica/environment`; generation IDs only appear in PVC subPaths. Python venv shebangs and other absolute-prefix installations therefore retain their paths. The core and tools are mounted read-only for consumers. Each task receives private writable HOME/tmp and only its assigned worker storage. Home seeds never receive changes back from a task and cannot contain credentials, sessions, rollouts or logs.

Observed successful HTTP or WebSocket claims establish task credentials and repository scope. A shim invocation alone grants no authority. Unobserved tasks, credential/scope changes and explicit local-directory execution are rejected before resource creation. Requests are stored in immutable Secrets; attempt journals contain resource identities and digests rather than prompts, tokens or environment values.

Official isolated checkouts are transferred as standalone repository archives. Publication cannot replace an existing user directory. Same-scope continuation/retry preserves edits and branches; unrelated scopes cannot reuse that storage. A Pi session also requires the same environment reference. After an environment change, work remains and the official daemon reports a fresh session; historical session files remain intact. Codex receives its prepared task skill directory in its private HOME, without sharing global rollouts.

Execution preserves provider streams and exit status. Cancellation signals the process group, allows the configured grace period and cleans remaining children. Transport and cleanup failures remain distinct from an observed provider exit. The controller records durable create intent and resolves uncertain creation by exact name, payload and owner. Execution and deletion are fenced by Pod/Secret UID. Recovery does not delete user work or adopt same-name replacements.

There is no automatic tools-generation GC. Worker storage retirement requires the official preparation roots to be gone, complete Pod/attempt inventory, an available storage lease and the required retention interval. POSIX locking, fsync, executable permissions and appropriate PVC access semantics are storage prerequisites.

## Diagnostics and local verification

Preparation progress appears in init-container logs. Main readiness follows environment validation and the runtime gateway. Failure categories distinguish configuration, core compatibility, preparation/integrity, provider startup, authorization, transport and pending cleanup. Provider protocol streams do not contain runtime diagnostic records for ordinary provider exit codes.

Core artifacts contain only the static runtime, the original Multica CLI **0.4.40**, and hashed contract metadata. The Go compiler exists only in the build stage. Core compiler and CLI pins are in `build/runtime-versions.env`; language/provider pins belong to Helm environment assets.

```sh
make build
make test
make test-race
make vet
make repository-validate
make workflow-validate
make verify-local
make verify
```

`verify-local` requires Docker, Go, Helm and the external chart source. It builds the current artifacts and uses an explicitly disposable Docker/K3s cluster, local backend and local Git repository. It never reads an ambient kubeconfig. Evidence is saved under a printed temporary directory. `scripts/verify-local.sh --keep-on-failure` retains only that fixture's resources for diagnosis.

The local fixtures exercise real official binaries, preparation, Pods, Secrets, exec, checkout, continuation and recovery. Offline provider/version fixtures do not prove live provider authentication or model-service behavior. Local storage checks establish the tested Docker/K3s behavior; they do not certify every RWX driver or production failover configuration.
