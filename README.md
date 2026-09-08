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

The `home layout` init runs as UID/GID 65532 and creates private `agents`, `tmp` and `run` children in one emptyDir. Main containers see only those children at `/home/multica/agents`, `/tmp` and `/run/multica`, with private access modes and protected ancestors.

Before copying into HOME, init captures all selected projected files and mappings into one committed bundle. A retry reuses that bundle even if the source ConfigMap changes or disappears. Individual HOME files are published without overwriting existing files; image seeds fill missing destinations. Partial copies can be retried without mixing source generations.

The controller creates immutable ConfigMap snapshots from the bundle. Each snapshot belongs to the controller Pod and is shared by its workers. The controller checks UID, owner, immutable state and payload before task creation and execution. Worker init checks mounted payload and mapping. It never falls back to a mutable source. Task cleanup does not delete shared snapshots; controller owner GC governs their lifetime.

Edits and authentication refreshes in private HOME do not modify source ConfigMaps, image seeds, other task HOMEs or future workers. Controller native config, Pi sessions and assigned Codex skills are protected from overrides. Content-identical snapshots retain logical session compatibility even when Kubernetes object names or UIDs change.

## Task authority and recovery

Normal selection, request, registry and attempt records use schema 2. A task ID alone grants no execution authority. The controller requires an observed claim, matching credentials and repository scope, canonical managed workspace paths and exact resource identities. Explicit `local_directory` execution is rejected.

Same-scope continuation preserves work, branches and Git hooks. Pi sessions additionally require compatible image, platform, controller, CLI, providers and configuration contents. Changed inputs create a new session while retaining authorized work. An attempt ID or snapshot object UID alone does not split a session.

Provider streams and exit codes are preserved. Cancellation signals the process group and allows its configured grace period. Durable create intent lets recovery reconcile lost API responses by name, owner, payload and UID. Cleanup refuses replaced resources and preserves user work. An expired controller's snapshot may already be garbage-collected; teardown can still resolve its journaled resources without granting new execution authority.

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
