#!/usr/bin/env bash
# Destructive process tests need a fresh, private PID namespace for each test.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 IMAGE [TEST_PATTERN]" >&2
  exit 2
fi
image=$1
pattern=${2:-'^(TestPID1|TestInit)'}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ -n ${DOCKER_CONTEXT:-} ]]; then
  endpoint=$(docker context inspect "$DOCKER_CONTEXT" --format '{{.Endpoints.docker.Host}}')
else
  endpoint=${DOCKER_HOST:-$(docker context inspect --format '{{.Endpoints.docker.Host}}')}
fi
[[ $endpoint == unix://* ]] || { echo 'A local Docker engine is required.' >&2; exit 1; }
# Pin the resolved endpoint and image so later context/tag changes cannot redirect proof.
docker_cli=(env -u DOCKER_CONTEXT -u DOCKER_HOST docker --host "$endpoint")
image=$("${docker_cli[@]}" image inspect --format '{{.Id}}' "$image")
platform=$("${docker_cli[@]}" version --format '{{.Server.Os}}/{{.Server.Arch}}')
case "$platform" in
  linux/arm64|linux/amd64) architecture=${platform#linux/} ;;
  *) echo "Unsupported native engine: $platform" >&2; exit 1 ;;
esac
[[ $("${docker_cli[@]}" image inspect --format '{{.Os}}/{{.Architecture}}' "$image") == "$platform" ]] || {
  echo 'The image must match the native Docker engine architecture.' >&2
  exit 1
}
proof=$(mktemp -d "${TMPDIR:-/tmp}/multica-pid1.XXXXXX")
chmod 0755 "$proof"
trap 'rm -f "$proof/worker.test" "$proof/initprocess.test"' EXIT
# shellcheck source=build/runtime-versions.env
source "$root/build/runtime-versions.env"
export GOTOOLCHAIN="go$GO_VERSION"
{
  echo "native_platform=$platform"
  "${docker_cli[@]}" image inspect --format 'image_id={{.Id}} descriptor={{json .Descriptor}}' "$image"
  go version
} | tee "$proof/environment.log"

# Keep the image's prepared HOME visible, as in the admitted writable worker.
run=("${docker_cli[@]}" run --rm --network none --user 65532:65532
  --cap-drop ALL --security-opt no-new-privileges --security-opt seccomp=unconfined
  --mount "type=bind,source=$proof,target=/proof,readonly"
  --tmpfs '/tmp:rw,exec,mode=1777'
  --tmpfs '/workspace:rw,uid=65532,gid=65532,mode=0700'
  --tmpfs '/run/multica:rw,uid=65532,gid=65532,mode=0700'
  --tmpfs '/etc/multica/task:rw,uid=65532,gid=65532,mode=0700')
count=0
for package in initprocess worker; do
  CGO_ENABLED=0 GOOS=linux GOARCH=$architecture go -C "$root/src" test -mod=readonly -c \
    -o "$proof/$package.test" "./internal/$package"
  shasum -a 256 "$proof/$package.test" | tee -a "$proof/environment.log"
  names=$("${run[@]}" --entrypoint "/proof/$package.test" "$image" -test.list "$pattern")
  while IFS= read -r name; do
    [[ $name == Test* ]] || continue
    count=$((count + 1))
    echo "Running $package/$name on $platform"
    "${run[@]}" --entrypoint "/proof/$package.test" "$image" \
      -test.v -test.timeout=120s -test.run "^${name}$" 2>&1 | tee "$proof/$name.log"
    if grep -q -- '--- SKIP:' "$proof/$name.log" || ! grep -q '^PASS$' "$proof/$name.log"; then
      echo "Incomplete PID 1 evidence: $package/$name" >&2
      exit 1
    fi
  done <<< "$names"
done
[[ $count -gt 0 ]] || { echo 'No matching process tests.' >&2; exit 1; }
echo "Passed $count native process tests without skips. Evidence: $proof"
