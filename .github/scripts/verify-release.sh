#!/usr/bin/env bash
# Release trust boundaries and latest-pointer recovery. External commands use
# local fixtures; this does not verify native execution or registry formats.
set -euo pipefail
export LC_ALL=C
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
fixture=$(mktemp -d "${TMPDIR:-/tmp}/controller-release-proof.XXXXXX")
trap 'rm -rf -- "$fixture"' EXIT
mkdir -p "$fixture/bin" "$fixture/checkout/build" "$fixture/checkout/.github/scripts" "$fixture/registry" "$fixture/records"
export RELEASE_FIXTURE_ROOT=$fixture
export GH_REPO=fixture/controller
export PATH="$fixture/bin:$PATH"
unset GH_TOKEN GITHUB_TOKEN GITHUB_OUTPUT
revision=1111111111111111111111111111111111111111
other=2222222222222222222222222222222222222222
printf '%s\n' "$revision" >"$fixture/main"
printf '%s\n' "$revision" >"$fixture/head"
printf '%s\n' identical >"$fixture/main-comparison"
printf '%s\n' 1.2.3 >"$fixture/checkout/VERSION"
cp "$fixture/checkout/VERSION" "$fixture/committed-version"
jq -cn --arg sha "$revision" '{object:{type:"commit",sha:$sha}}' >"$fixture/tag.json"
printf '%s\n' GO_VERSION=1.26.1 >"$fixture/checkout/build/runtime-versions.env"
cat >"$fixture/bin/git" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
if [[ $# == 4 && $1 == -C && $3 == rev-parse && $4 == HEAD ]]; then cat "$RELEASE_FIXTURE_ROOT/head"; exit; fi
if [[ $# == 4 && $1 == -C && $3 == show && $4 == "$(cat "$RELEASE_FIXTURE_ROOT/head"):VERSION" ]]; then
  cat "$RELEASE_FIXTURE_ROOT/committed-version"
  exit
fi
printf 'unexpected fixture git command\n' >&2
exit 1
STUB
cat >"$fixture/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
root=$RELEASE_FIXTURE_ROOT
respond() {
  if [[ -f $1 ]]; then printf 'HTTP/2.0 200 OK\nContent-Type: application/json\n\n'; cat "$1"; else printf 'HTTP/2.0 404 Not Found\nContent-Type: application/json\n\n{}\n'; exit 1; fi
}
if [[ $# == 3 && $1 == api && $2 == --include ]]; then
  case $3 in
    repos/fixture/controller/git/ref/heads/main)
      printf 'HTTP/2.0 200 OK\nContent-Type: application/json\n\n'
      jq -cn --arg sha "$(cat "$root/main")" '{object:{type:"commit",sha:$sha}}' ;;
    repos/fixture/controller/git/ref/tags/1.2.3) respond "$root/tag.json" ;;
    repos/fixture/controller/compare/*)
      printf 'HTTP/2.0 200 OK\nContent-Type: application/json\n\n'
      jq -cn --arg status "$(cat "$root/main-comparison")" '{status:$status}' ;;
    repos/fixture/controller/releases/tags/1.2.3) respond "$root/release.json" ;;
    repos/fixture/controller/releases/latest) respond "$root/github-latest.json" ;;
    *) printf 'unexpected fixture GitHub lookup\n' >&2; exit 1 ;;
  esac
  exit
fi
if [[ ${1:-} == release && ${2:-} == create && ${3:-} == 1.2.3 ]]; then
  shift 3
  target=main latest=true
  while [[ $# -gt 0 ]]; do
    case $1 in
      --target) target=$2; shift 2 ;;
      --notes-file|--repo|--title) shift 2 ;;
      --verify-tag) shift ;;
      --latest|--latest=true) latest=true; shift ;;
      --latest=false) latest=false; shift ;;
      *) exit 1 ;;
    esac
  done
  [[ ! -f $root/release.json ]]
  jq -cn --arg target "$target" '{tag_name:"1.2.3",target_commitish:$target,draft:false,prerelease:false}' >"$root/release.json"
  [[ $latest != true ]] || cp "$root/release.json" "$root/github-latest.json"
  if [[ $(cat "$root/fault" 2>/dev/null || :) == after-release ]]; then rm "$root/fault"; exit 1; fi
  exit
fi
if [[ ${1:-} == release && ${2:-} == edit && ${3:-} == 1.2.3 ]]; then
  shift 3
  latest=false
  while [[ $# -gt 0 ]]; do
    case $1 in
      --repo) shift 2 ;;
      --latest|--latest=true) latest=true; shift ;;
      *) exit 1 ;;
    esac
  done
  [[ $latest != true ]] || cp "$root/release.json" "$root/github-latest.json"
  exit
fi
printf 'unexpected fixture gh command\n' >&2
exit 1
STUB
cat >"$fixture/bin/docker" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
root=$RELEASE_FIXTURE_ROOT
revision=$(cat "$root/head")
platform=${RELEASE_FIXTURE_PLATFORM:-linux/amd64}
amd=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
arm=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
config_amd=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
config_arm=sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
native_platform() {
  case $1 in *-amd64|*"@$amd") printf linux/amd64 ;; *-arm64|*"@$arm") printf linux/arm64 ;; *) exit 1 ;; esac
}
manifest_file() {
  case $1 in
    fixture/runtime:build-1.2.3-"$revision"-amd64) printf '%s/registry/amd64.json' "$root" ;;
    fixture/runtime:build-1.2.3-"$revision"-arm64) printf '%s/registry/arm64.json' "$root" ;;
    fixture/runtime:1.2.3) printf '%s/registry/version.json' "$root" ;;
    fixture/runtime:latest) printf '%s/registry/latest.json' "$root" ;;
    *) exit 1 ;;
  esac
}
image() {
  local selected=$1 config
  case $selected in linux/amd64) config=$config_amd ;; linux/arm64) config=$config_arm ;; *) exit 1 ;; esac
  jq -cn --arg platform "$selected" --arg id "$config" --arg revision "$revision" \
    '{Id:$id,Os:"linux",Architecture:($platform|split("/")[1]),Config:{User:"65532:65532",Labels:{"org.opencontainers.image.version":"1.2.3","org.opencontainers.image.revision":$revision}}}'
}
case ${1:-} in
  info) printf '%s\n' "${platform#linux/}"; exit ;;
  image) image "$(native_platform "$3")" | jq -cs .; exit ;;
  run) [[ ${RELEASE_FIXTURE_FAIL_NATIVE:-false} != true ]]; exit ;;
  push)
    selected=$(native_platform "$2")
    if [[ $selected == linux/amd64 ]]; then pin=$amd; else pin=$arm; fi
    jq -cn --arg digest "$pin" '{digest:$digest,mediaType:"application/vnd.oci.image.manifest.v1+json"}' >"$(manifest_file "$2")"
    exit ;;
  buildx) [[ ${2:-} == imagetools ]] ;;
  *) printf 'unexpected fixture docker command\n' >&2; exit 1 ;;
esac
if [[ $3 == inspect ]]; then
  ref=$4
  if [[ ${5:-} == --raw ]]; then
    selected=$(native_platform "$ref")
    if [[ $selected == linux/amd64 ]]; then config=$config_amd; else config=$config_arm; fi
    jq -cn --arg digest "$config" '{config:{digest:$digest}}'
  elif [[ ${6:-} == '{{json .Image}}' ]]; then
    image "$(native_platform "$ref")" | jq -c '{os:.Os,architecture:.Architecture,config:.Config}'
  elif [[ ${6:-} == '{{json .Manifest}}' ]]; then
    file=$(manifest_file "$ref")
    if [[ ! -f $file ]]; then printf 'ERROR: %s: not found\n' "$ref" >&2; exit 1; fi
    cat "$file"
  else exit 1; fi
  exit
fi
[[ $3 == create ]]
shift 3
tag=''
while [[ $# -gt 0 ]]; do
  case $1 in
    --tag) tag=$2; shift 2 ;;
    --annotation) shift 2 ;;
    fixture/runtime@sha256:*) shift ;;
    *) exit 1 ;;
  esac
done
file=$(manifest_file "$tag")
if [[ $tag == fixture/runtime:1.2.3 ]]; then
  jq -cn --arg amd "$amd" --arg arm "$arm" --arg revision "$revision" '
    {schemaVersion:2,mediaType:"application/vnd.oci.image.index.v1+json",digest:"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
     manifests:[{digest:$amd,platform:{os:"linux",architecture:"amd64"}},{digest:$arm,platform:{os:"linux",architecture:"arm64"}}],
     annotations:{"org.opencontainers.image.revision":$revision,"org.opencontainers.image.version":"1.2.3"}}' >"$file"
elif [[ $tag == fixture/runtime:latest ]]; then
  cp "$root/registry/version.json" "$file"
else exit 1; fi
STUB
chmod +x "$fixture/bin/git" "$fixture/bin/gh" "$fixture/bin/docker"
release() {
  "$repository/.github/scripts/release.sh" --root "$fixture/checkout" --image fixture/runtime --revision "$revision" --version 1.2.3 "$@" >"$fixture/result" 2>"$fixture/error"
}
require_success() {
  if ! release "$@"; then cat "$fixture/error" >&2; printf 'fixture operation unexpectedly failed: %s\n' "$*" >&2; exit 1; fi
}
stored_state() {
  local file
  for file in "$@"; do
    [[ -f $file ]] || continue
    printf '%s\n' "${file#"$fixture/"}"
    cat "$file"
  done
}
registry_state() {
  stored_state "$fixture/registry/"*.json "$fixture/tag.json" "$fixture/release.json" "$fixture/github-latest.json"
}
require_rejected_without_writes() {
  registry_state >"$fixture/before-state"
  if release "$@"; then printf 'fixture unsafe operation succeeded: %s\n' "$*" >&2; exit 1; fi
  registry_state >"$fixture/after-state"
  cmp "$fixture/before-state" "$fixture/after-state"
}

require_success record-native --platform linux/amd64 --records "$fixture/records"
export RELEASE_FIXTURE_PLATFORM=linux/arm64 RELEASE_FIXTURE_FAIL_NATIVE=true
require_rejected_without_writes record-native --platform linux/arm64 --records "$fixture/records"
unset RELEASE_FIXTURE_FAIL_NATIVE
require_success record-native --platform linux/arm64 --records "$fixture/records"
require_success publish --records "$fixture/records"
cp "$fixture/registry/latest.json" "$fixture/committed-latest"
cp "$fixture/release.json" "$fixture/committed-release"

# A release source cannot replace the verifier that runs with publish authority.
# Its verifier attempts to overwrite an already published latest image.
cat >"$fixture/checkout/.github/scripts/verify-base.sh" <<'UNTRUSTED'
#!/usr/bin/env bash
cp "$RELEASE_FIXTURE_ROOT/registry/arm64.json" "$RELEASE_FIXTURE_ROOT/registry/latest.json"
UNTRUSTED
require_success record-native --platform linux/arm64 --records "$fixture/records"
cmp "$fixture/registry/latest.json" "$fixture/committed-latest"

# Older releases must preserve both latest pointers, whether their GitHub
# release already exists or is created during this run.
jq --arg revision "$other" '.annotations["org.opencontainers.image.version"]="9.0.0" | .annotations["org.opencontainers.image.revision"]=$revision | .digest="sha256:9999999999999999999999999999999999999999999999999999999999999999"' "$fixture/committed-latest" >"$fixture/registry/latest.json"
cp "$fixture/registry/latest.json" "$fixture/newer-latest"
jq '.tag_name="9.0.0"' "$fixture/committed-release" >"$fixture/github-latest.json"
cp "$fixture/github-latest.json" "$fixture/newer-github-latest"
require_success publish --records "$fixture/records"
cmp "$fixture/registry/latest.json" "$fixture/newer-latest"
cmp "$fixture/github-latest.json" "$fixture/newer-github-latest"
rm "$fixture/release.json"
require_success publish --records "$fixture/records"
cmp "$fixture/registry/latest.json" "$fixture/newer-latest"
cmp "$fixture/github-latest.json" "$fixture/newer-github-latest"

# Retry repairs a GitHub latest pointer that lagged behind the registry.
cp "$fixture/committed-latest" "$fixture/registry/latest.json"
jq '.tag_name="1.2.2"' "$fixture/committed-release" >"$fixture/github-latest.json"
require_success publish --records "$fixture/records"
cmp "$fixture/github-latest.json" "$fixture/committed-release"

# A GitHub create can commit before its response is lost. The retry must still
# complete registry latest promotion from that partially committed state.
rm "$fixture/registry/latest.json" "$fixture/release.json" "$fixture/github-latest.json"
stored_state "$fixture/registry/latest.json" >"$fixture/before-promotion"
printf '%s\n' after-release >"$fixture/fault"
if release publish --records "$fixture/records"; then printf 'lost release response unexpectedly succeeded\n' >&2; exit 1; fi
cmp "$fixture/release.json" "$fixture/committed-release"
stored_state "$fixture/registry/latest.json" >"$fixture/after-promotion"
cmp "$fixture/before-promotion" "$fixture/after-promotion"
require_success publish --records "$fixture/records"
cmp "$fixture/registry/latest.json" "$fixture/committed-latest"
cmp "$fixture/github-latest.json" "$fixture/committed-release"
printf '%s\n' 'PASS: trusted verifier authority, failed native verification, monotonic latest pointers and GitHub publication recovery (local fixtures).'
