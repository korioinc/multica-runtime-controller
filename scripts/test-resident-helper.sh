#!/usr/bin/env bash
set -euo pipefail
[[ $# == 1 ]] || { echo "Usage: $0 IMAGE" >&2; exit 2; }
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ -n ${DOCKER_CONTEXT:-} ]]; then
  endpoint=$(docker context inspect "$DOCKER_CONTEXT" --format '{{.Endpoints.docker.Host}}')
else
  endpoint=${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}
fi
[[ $endpoint == unix://* ]] || { echo 'A local Unix Docker endpoint is required.' >&2; exit 1; }
docker_cli=(env -u DOCKER_CONTEXT -u DOCKER_HOST docker --host "$endpoint")
image=$("${docker_cli[@]}" image inspect --format '{{.Id}}' "$1")
platform=$("${docker_cli[@]}" version --format '{{.Server.Os}}/{{.Server.Arch}}')
[[ $platform == linux/arm64 || $platform == linux/amd64 ]] || { echo 'Unsupported native engine.' >&2; exit 1; }
[[ $("${docker_cli[@]}" image inspect --format '{{.Os}}/{{.Architecture}}' "$image") == "$platform" ]] || { echo 'Image must match the native engine.' >&2; exit 1; }
proof=$(mktemp -d "${TMPDIR:-/tmp}/multica-resident-helper.XXXXXX")
chmod 0755 "$proof"
trap 'rm -f "$proof/worker.test"' EXIT
# shellcheck source=build/runtime-versions.env
source "$root/build/runtime-versions.env"
export GOTOOLCHAIN="go$GO_VERSION"
{
  echo "native_platform=$platform"
  "${docker_cli[@]}" image inspect --format 'image_id={{.Id}} descriptor={{json .Descriptor}}' "$image"
  go version
} | tee "$proof/environment.log"
CGO_ENABLED=0 GOOS=linux GOARCH=${platform#linux/} go -C "$root/src" test -mod=readonly -c -o "$proof/worker.test" ./internal/worker
shasum -a 256 "$proof/worker.test" | tee -a "$proof/environment.log"
"${docker_cli[@]}" run --rm --network none --read-only --user 65532:65532 \
  --cap-drop ALL --security-opt no-new-privileges --security-opt seccomp=unconfined \
  --mount "type=bind,source=$proof,target=/proof,readonly" \
  --tmpfs '/tmp:rw,exec,mode=1777' \
  --tmpfs '/workspace:rw,uid=65532,gid=65532,mode=0700' \
  --tmpfs '/run/multica:rw,uid=65532,gid=65532,mode=0700' \
  --tmpfs '/home/multica/agents:rw,uid=65532,gid=65532,mode=0700' \
  --env MULTICA_RESIDENT_HELPER_TEST=1 --entrypoint /proof/worker.test "$image" \
  -test.v -test.timeout=120s -test.run '^TestResidentInstalledHelper' 2>&1 | tee "$proof/helper.log"
if grep -q -- '--- SKIP:' "$proof/helper.log" || ! grep -q '^PASS$' "$proof/helper.log"; then
  echo 'Incomplete installed-helper proof.' >&2
  exit 1
fi
echo "Installed-helper proof passed without skips. Evidence: $proof"
