#!/usr/bin/env bash
# Build an explicitly disposable provider fixture; orchestration stays in shell.
set -euo pipefail
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
usage() {
  echo 'Usage: local-fixture-image.sh --runtime-image LOCAL_IMAGE --tag LOCAL_TAG --platform linux/amd64|linux/arm64 --build-id UUID --context DIRECTORY'
}
fail() { printf 'local fixture: %s\n' "$*" >&2; exit 1; }
runtime_image='' tag='' platform='' build_id='' context=''
while [[ $# -gt 0 ]]; do
  case $1 in
    --help|-h) usage; exit 0 ;;
    --runtime-image|--tag|--platform|--build-id|--context)
      [[ $# -ge 2 ]] || fail "missing value for $1"
      case $1 in
        --runtime-image) runtime_image=$2 ;; --tag) tag=$2 ;; --platform) platform=$2 ;;
        --build-id) build_id=$2 ;; --context) context=$2 ;;
      esac
      shift 2 ;;
    *) usage >&2; exit 1 ;;
  esac
done
[[ -n $runtime_image && -n $tag && -d $context ]] || { usage >&2; exit 1; }
[[ $build_id =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]] || fail 'canonical UUID required'
[[ $platform == linux/amd64 || $platform == linux/arm64 ]] || fail 'unsupported platform'
context=$(cd -- "$context" && pwd -P)
for tool in docker go jq tar; do command -v "$tool" >/dev/null || fail "$tool is required"; done
if [[ -n ${DOCKER_CONTEXT:-} ]]; then
  docker_endpoint=$(docker context inspect --format '{{.Endpoints.docker.Host}}' "$DOCKER_CONTEXT")
elif [[ -n ${DOCKER_HOST:-} ]]; then
  docker_endpoint=$DOCKER_HOST
else
  docker_endpoint=$(docker context inspect --format '{{.Endpoints.docker.Host}}')
fi
case $docker_endpoint in unix://*) ;; *) fail 'only a local Docker socket is allowed' ;; esac
image_id=$(docker image inspect "$runtime_image" --format '{{.Id}}')
input_tag="multica-runtime-fixture-input:$build_id"
created_input=false
if existing_id=$(docker image inspect "$input_tag" --format '{{.Id}}' 2>/dev/null); then
  [[ $existing_id == "$image_id" ]] || fail 'existing fixture input tag belongs to another image'
else
  docker tag "$image_id" "$input_tag"
  created_input=true
fi
cleanup_input() {
  if [[ $created_input == true && $(docker image inspect "$input_tag" --format '{{.Id}}' 2>/dev/null) == "$image_id" ]]; then
    docker image rm "$input_tag" >/dev/null 2>&1 || true
  fi
}
trap cleanup_input EXIT
actual_platform=$(docker image inspect "$image_id" --format '{{.Os}}/{{.Architecture}}')
[[ $actual_platform == "$platform" ]] || fail 'input image platform differs'
case $(docker info --format '{{.Architecture}}') in aarch64|arm64) native=linux/arm64 ;; x86_64|amd64) native=linux/amd64 ;; *) fail 'unsupported Docker architecture' ;; esac
[[ $native == "$platform" ]] || fail 'native Docker architecture required'
docker run --rm --network none --read-only "$image_id" image verify >&2
docker run --rm --network none --read-only --entrypoint /bin/cat "$image_id" /opt/multica/controller/build.json > "$context/controller-build.json"
toolchain=$(jq -er '.goVersion' "$context/controller-build.json")
[[ $toolchain =~ ^go[1-9][0-9]*\.[0-9]+\.[0-9]+$ ]] || fail 'invalid inherited Go SDK version'
export GOTOOLCHAIN="$toolchain"
export CGO_ENABLED=0 GOOS=linux GOARCH=${platform#linux/}
go -C "$repo_root/src" build -trimpath -buildvcs=false -ldflags '-s -w' -o "$context/runtime" ./cmd/runtime
for binary in verifyofficial verifyruntime verifykube verifystream verifybinding; do
  go -C "$repo_root/src" build -trimpath -buildvcs=false -ldflags '-s -w' -o "$context/$binary" "./cmd/$binary"
done
go -C "$repo_root/src" test -c -tags=handoffintegration -trimpath -buildvcs=false -ldflags '-s -w' -o "$context/verifyhandoff" ./internal/execution
hash_file() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d ' ' -f 1; }
[[ $(hash_file "$context/runtime") == "$(jq -er .runtimeSHA256 "$context/controller-build.json")" ]] || fail 'input image does not reproduce this controller checkout'
source_hash=$(printf '%s\n' "$image_id" "$(hash_file "$context/runtime")" "$(hash_file "$context/verifyruntime")" "$(hash_file "$context/verifyofficial")" "$(hash_file "$context/verifyhandoff")" "$(hash_file "$repo_root/build/localverify.Dockerfile")" "$(hash_file "${BASH_SOURCE[0]}")" > "$context/source-inputs"; hash_file "$context/source-inputs")
cp "$repo_root/build/localverify.Dockerfile" "$context/Dockerfile"
if docker image inspect "$tag" > "$context/existing.json" 2>/dev/null; then
  jq -e --arg id "$build_id" --arg source "$source_hash" '.[0].Config.Labels | .["io.multica.image-build-id"]==$id and .["io.multica.local-fixture-source"]==$source and .["io.multica.verification-fixture"]=="true"' "$context/existing.json" >/dev/null || fail 'existing fixture tag belongs to different inputs'
else
  docker buildx build --load --platform "$platform" --provenance=true \
    --build-arg "RUNTIME_IMAGE=$input_tag" --build-arg "IMAGE_BUILD_ID=$build_id" --build-arg "SOURCE_SHA256=$source_hash" \
    --tag "$tag" "$context" >&2
fi
docker run --rm --network none --read-only "$tag" image verify >&2
fixture_id=$(docker image inspect "$tag" --format '{{.Id}}')
jq -cn --arg imageID "$fixture_id" --arg tag "$tag" --arg buildID "$build_id" --arg platform "$platform" --arg sourceSHA256 "$source_hash" \
  '{imageID:$imageID,tag:$tag,buildID:$buildID,platform:$platform,sourceSHA256:$sourceSHA256}'
