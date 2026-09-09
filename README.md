# Multica Runtime Controller

A Kubernetes controller base for custom Multica runtime images. The base contains the static controller, provider shims and the Go SDK. It **does not contain the official Multica CLI or AI providers**. Install those and your development tools at image build time using [korioinc/multica-runtime](https://github.com/korioinc/multica-runtime).

The official daemon owns scheduling, prompts, checkout policy and completion reporting. This controller observes successful claims and runs authorized task providers in isolated Pods. Controller and workers execute the same completed image.

## Images and installation

Use the independently maintained [Helm chart](https://github.com/korioinc/helm/tree/main/charts/multica-runtime-controller). Its `image` accepts a completed custom image and defaults to `ghcr.io/korioinc/multica-runtime:latest`, with `imagePullPolicy: Always`. The base image alone cannot start a controller.

```yaml
image: ghcr.io/korioinc/multica-runtime:latest
imagePullPolicy: Always
platform: linux/arm64
multica:
  baseURL: https://multica.example.com
  controllerTokenSecret:
    name: multica-runtime-controller-token
    key: token
workspace:
  storage:
    existingClaim: multica-workspace
    accessMode: ReadWriteMany
```

The chart has one controller replica and a `Recreate` strategy. `ReadWriteOnce` requires `scheduling.singleNodeName`; `ReadWriteMany` permits suitable multi-node storage. Workers receive only their task subPath and selected native session file, without the controller registry or Kubernetes token. There is no Tools PVC or startup installer.

At startup, the controller compares its installed descriptor with the init receipt, then validates its actual Pod UID, platform and init/main `imageID` values through Kubernetes. Only a pullable repository digest is accepted. That digest stays in every worker and attempt, including when `latest` moves. The controller never resolves the current registry tag as a fallback. Changing a tag does not restart an existing Pod.

Both `linux/amd64` and `linux/arm64` are build targets. Local development verification runs on the developer host's single native platform. The GitHub Actions release matrix builds and verifies both platform images before publication; cross-build or QEMU output remains distinct from native execution evidence.

## Installed contracts

| Path | Owner and purpose |
| --- | --- |
| `/opt/multica/controller/runtime` | Controller executable |
| `/opt/multica/controller/shims` | Explicit daemon provider entrypoints |
| `/opt/multica/controller/build.json` | Schema 2 controller build and ABI, independent of the official CLI |
| `/usr/local/go` | Base-owned Go SDK |
| `/opt/multica/runtime/image.json` | Completed-image descriptor, installed provider paths and runtime defaults |
| `/opt/multica/runtime/verification.json` | Successful adapter suite bound to descriptor, controller and official CLI bytes |

The image publisher generates a new platform-specific `imageBuildID` for each build, records it in the descriptor and OCI label, and verifies the prepared image before copying its verification record into the final stage. Publication retry reuses the verified artifact. Reusing an old descriptor or report after changing the installed image is unsupported.

The runtime repository owns official CLI/tool versions, checksums, dependency locks, extensions and installation. This repository owns its Go version in `build/runtime-versions.env`. The initial official adapter baseline is Multica `0.4.40`; another pin must pass the actual matching-source adapter suite. Removing a version-string comparison is not evidence of compatibility.

```sh
runtime version
runtime image verify
```

`version` validates the base without requiring an official CLI. `image verify` validates the completed descriptor, actual files and matching verification record; neither installs or downloads tools. Installed provider paths must resolve to immutable executable regular files and cannot resolve to controller shims or private writable areas.

## Operator configuration and private HOME

Helm accepts ConfigMap sources through `operator.configVolumes` and `operator.configMounts`. Terraform retains ownership of original Codex/Pi files. File sources remain ConfigMaps. Environment-variable Secrets, controller tokens and task request Secrets are separate contracts.

The base image registers the `multica` user and group as UID/GID 65532, with `/home/multica/agents` as its home and `/bin/bash` as its shell. This lets interactive shells and tools resolve the runtime user by name.

Each task worker container starts in `/workspace/<workspace>/<task>/workdir`. Every supported task provider starts in this directory, regardless of provider type. It is also the default working directory for `kubectl exec`. HOME remains `/home/multica/agents` for user configuration and credentials.

The `home layout` init runs as UID/GID 65532 and creates private `agents`, `tmp` and `run` children in one emptyDir. Main containers see only those children at `/home/multica/agents`, `/tmp` and `/run/multica`, with private access modes and protected ancestors.

Before copying into controller HOME, init captures all selected projected files and mappings into one committed bundle. A retry reuses that bundle even if the source ConfigMap changes or disappears. Individual controller HOME files are published without overwriting existing files; image seeds fill missing destinations. Partial copies can be retried without mixing source generations.

For each authorized attempt, the controller prepares a complete task HOME from
that committed bundle, image defaults and the official task's provider inputs.
It publishes one archive in the selected worker storage after prior consumers
have been reconciled. The request binds the archive digest, including its task,
attempt and runtime identity. Worker init receives only that archive through a
read-only mount, validates it and publishes the complete private HOME atomically.
It does not compose provider configuration or skills again. A matching completed
HOME survives init retries, including native edits, deleted files and npm changes.
An incomplete or mismatched HOME cannot start a provider.
Native HOME and init receipts are disposable Pod-local data. Init closes their
writes before publication without forcing each file and directory to durable
storage. The controller's archive on the workspace PVC remains durable.
Directory checks are reused only within a newly created staging tree. Worker
init validates archive paths and types during extraction, then verifies package
command links against the completed tree before publishing it.

Provider configuration can map complete directories to `.codex` and `.pi`.
Nested skills, references and other selected files keep their relative paths.
Operator files take precedence over image defaults in a new HOME.
The official adapter interprets provider-specific task inputs and returns the
directories to remove or copy. The common HOME builder applies that plan to the
committed base, validates the resulting tree and publishes the archive.

The image seed may include an installed Pi npm tree at `.pi/agent/npm`.
Package source directories such as `token` are valid within its `node_modules`;
ordinary HOME credential and session paths remain prohibited in image seeds.
Only relative npm `.bin` links confined to that installation are permitted.
Operator npm configuration may contain only `package.json`, or include `.npmrc`
and other selected files; installed-tree requirements apply to image seeds.
Init prepares the complete npm tree privately and publishes it only when the
destination is absent. Existing package additions, updates and removals are
preserved. Providers run directly without an initialization wrapper.

The committed bundle is the controller's configuration source. Its content digest
is part of the selected runtime identity; task preparation checks the bundle
against that selection. Workers receive configuration contents in their prepared
HOME archive. Init verifies its bytes and complete runtime/configuration identity,
then publishes the HOME and its receipt together. The controller does not create
additional ConfigMaps for this configuration and needs no ConfigMap API permissions.

Edits and authentication refreshes in private HOME do not modify source ConfigMaps, image seeds, other task HOMEs or future workers. Controller native config and Pi sessions are protected from overrides. Content-identical configuration retains logical session compatibility across controller Pod identity changes.

During controller task preparation, Codex skills combine image defaults, the
committed operator configuration and the current task assignment. An assigned
top-level skill directory replaces all configured directories with the same
daemon-normalized name (lowercase words joined by hyphens) as a whole;
other configured skills remain available. Removing an assignment restores the
operator version in the next task HOME. Exact global skill links prepared by the
daemon are recognized as inherited configuration; their mutable controller HOME
contents are never promoted to task assignments. Other links are rejected. The
daemon's task `codex-home` also contains authentication and configuration links,
so it is not copied as an independent HOME. Same-attempt retries preserve private
skill edits; a new attempt starts from its selected inputs.
Pi task skills remain in the daemon's workdir `.pi/skills` context, alongside
configured HOME skills in `.pi/agent/skills`.

## Task authority and recovery

A task ID alone grants no execution authority. The controller requires an observed claim, matching credentials and repository scope, canonical managed workspace paths and exact resource identities. Explicit `local_directory` execution is rejected.

Same-scope continuation preserves work, branches and Git hooks. Pi sessions additionally require compatible image, platform, controller build, CLI, providers and configuration contents. Changed inputs create a new session while retaining authorized work. An attempt ID or controller Pod UID alone does not split a session.

The official adapter reads the daemon's sidecar manifest and managed task
instructions. Checkout applies those prepared inputs to worker storage while
preserving user files and instructions outside the managed region. A context
refresh records its ownership before changing files, so an interrupted refresh
can retry without treating partially written context as user-owned data.

Empty claim polls validate the stored registry under its lock without rewriting
unchanged state. Registry reads still validate the complete authority record;
retired task denials remain persistent.

Before creating or resolving task resources and immediately before execution,
the controller checks its live Pod UID, deletion state and configured fixed node.
Execution also rechecks the request Secret and worker Pod identities and payloads.
A removed, replaced or terminating controller cannot authorize a new execution.
These are execution-start checks; they do not continuously revoke running processes.

Provider streams and exit codes are preserved. Cancellation signals the process group and allows its configured grace period. Durable create intent lets recovery reconcile lost API responses by name, owner, payload and UID. Cleanup refuses replaced resources and preserves user work. It can resolve and delete original task resources after their controller disappears. An unresolved create continues to block storage reuse until that exact controller UID is gone.

Requests, controller selections and attempt journals accept only schema 3.
Recovery validates the current record directly and preserves its request and Pod
fingerprints. Registry and runtime-reference schema 2, the configuration digest
algorithm and HOME archive identity remain unchanged.

Finish pending attempt cleanup in the existing deployment before switching record
formats. Replace the controller Pod while preserving its workspace PVC. Same-Pod
process restarts must match the previously admitted selection. Unsupported or
invalid records stop recovery without rewriting or deleting the stored record.

Only a busy storage lease is normal recovery contention. An inaccessible or
invalid lock keeps the storage protected and reports `recovery_lease_failed`
with the attempt and storage identities. Retirement commits a permanent deny and
tombstone before moving data to trash; it holds the storage lease while deleting
outside the registry lock. Finalization reloads current authority, allowing
unrelated claims to proceed without losing their updates.

Invalid attempt journals block recovery rather than being skipped. Diagnostics
identify a canonical attempt file and a reason such as
`attempt_record_invalid_json`; unrecognized filenames are represented by the
SHA-256 of their filename bytes (`entrySHA256`), never treated as task IDs.
To investigate locally, preserve a copy of the authority directory and inspect
the identified record or compare filename hashes in that copy. Correct a lock or
I/O failure before retrying. Repair a corrupt record only from independently
verified recovery evidence: removing it does not prove that its resources have
stopped. Diagnostic output omits record bodies and wrapped parser errors.

## Runtime logs

Controller and worker service processes write INFO-level operational logs to stdout. The controller reports backend requests, response statuses, WebSocket connections and task claim exchanges. Worker logs show gateway startup/shutdown and checkout requests to the controller; task and attempt IDs connect the two sides.

```sh
kubectl -n tools-multica logs -f deployment/multica-runtime-controller -c controller
kubectl -n tools-multica logs -f "$WORKER_POD" -c worker
```

Set `WORKER_POD` to the active task worker Pod name. Routine health probes remain quiet. Operational logs omit credentials, headers, request/response bodies, prompts and provider arguments. Provider protocol streams and their separate diagnostic sink are unchanged; the official daemon's private detailed log is not copied into Pod logs.

## Explicit workspace migration

Normal startup rejects schema 1 data; it never initializes over it or converts it automatically. Use the offline migration command after separately establishing that old writers and task resources are quiescent. It proves filesystem ownership, existing lock availability, empty attempts and source bytes; it cannot prove remote cluster quiescence.

```sh
runtime workspace migrate --root /workspace --owner-id "$OWNER_ID" --dry-run
runtime workspace migrate --root /workspace --owner-id "$OWNER_ID" \
  --expected-source-sha256 "$SOURCE_SHA256" --commit
```

Dry-run writes no files or locks. Commit rechecks source bytes under existing locks, durably preserves the original at `.multica-runtime/state/migrations/v1-to-v2/<source-digest>/registry.v1.json`, and atomically replaces the registry. Old claims become unobserved and Pi sessions archived. Work, session files, ownership, permanent denial and lock inodes are preserved. Fresh claims establish new authority.

Unfinished journals or unknown attempt entries require their existing cleanup first. A valid schema 2 registry is never overwritten from backup. There is no automatic rollback after new schema 2 writes. The legacy reader lives only in the migration package and remains necessary while schema 1 workspaces are supported migration sources.

## Local development and verification

All script orchestration is shell (`.sh`). Source-owned Go verifier commands implement protocol and filesystem fixtures; scripts do not delegate orchestration to Python or Go wrappers.

```sh
make build
make test
make test-race
make vet
make repository-validate
make workflow-validate
make verify-core
make verify
```

`verify-core` builds this checkout's base and checks native execution, Go compilation and official CLI absence. It needs local Docker, Go, Make, Bash, jq, Git and ripgrep, without a chart or completed runtime image.

Build a completed runtime with an explicit local base override using the runtime repository's `scripts/build-image.sh`. Then run integration with both inputs:

```sh
scripts/verify-local.sh \
  --chart ../helm/charts/multica-runtime-controller \
  --runtime-image multica-runtime:local \
  --keep-on-failure
```

The harness checks the image against this checkout, builds disposable provider fixtures, and runs an owned local registry, backend, Git origin and K3s node. It exercises actual task Pods, image/config binding, private volumes, checkout, native session reuse, authorization, process interruption, lost creation responses, UID replacement and transport/cleanup failures. Model generation and shared services are excluded.

Integration additionally requires Helm and the native ORAS CLI. ORAS transfers verified OCI bytes from the host to the owned loopback registry with an empty authentication config, without changing Docker daemon settings. Fixture binaries use the Go SDK version recorded in the inherited controller build contract.

Evidence and owned handles stay in the printed temporary directory. `--resume DIRECTORY` with the same explicit chart and image resumes only a retained, still-running owned fixture after source and image identity checks. It never uses ambient kubeconfig. Fixtures do not certify every storage driver.

## Release code

The main-push workflow builds the controller-only base and GitHub Release from independent `VERSION` and source revision. Both native platforms must pass before a version index or `latest` is promoted. Existing bytes are preserved, retry uses verified candidates, and stale revisions cannot move `latest` backward. The old CLI-version updater is removed.

`.github/scripts/verify-release.sh` uses local persistent GitHub/registry stubs to exercise failure, duplicate revision, partial results, immutable retries and latest protection. Local verification does not publish images, create releases, merge branches or apply a shared cluster.
