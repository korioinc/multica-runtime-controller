#!/usr/bin/env bash
# Isolated release-authority checks. Stub executables cannot reach a registry,
# GitHub, Docker engine, or Git repository, and all mutations stay in this fixture.
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
    repos/fixture/controller/git/tags/*) respond "$root/tag-object.json" ;;
    repos/fixture/controller/compare/*)
      [[ $3 == "repos/fixture/controller/compare/$(cat "$root/head")...$(cat "$root/main")" ]]
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
  target=main latest=true verify_tag=false
  while [[ $# -gt 0 ]]; do
    case $1 in
      --target) target=$2; shift 2 ;;
      --notes-file) [[ -f $2 ]]; shift 2 ;;
      --repo|--title) shift 2 ;;
      --verify-tag) verify_tag=true; shift ;;
      --latest|--latest=true) latest=true; shift ;;
      --latest=false) latest=false; shift ;;
      *) exit 1 ;;
    esac
  done
  [[ ! -f $root/release.json ]]
  if [[ ! -f $root/tag.json ]]; then
    [[ $verify_tag != true ]]
    sha=$target
    [[ $target != main ]] || sha=$(cat "$root/main")
    jq -cn --arg sha "$sha" '{object:{type:"commit",sha:$sha}}' >"$root/tag.json"
  fi
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
  local selected=$1 config pin index kind id media
  case $selected in linux/amd64) config=$config_amd; pin=$amd; index=sha256:1111111111111111111111111111111111111111111111111111111111111111 ;; linux/arm64) config=$config_arm; pin=$arm; index=sha256:2222222222222222222222222222222222222222222222222222222222222222 ;; *) exit 1 ;; esac
  kind=${RELEASE_FIXTURE_IMAGE_ID_KIND:-legacy}
  if [[ -f $root/local-${selected#linux/}-kind ]]; then kind=$(cat "$root/local-${selected#linux/}-kind"); fi
  case $kind in
    legacy) id=$config; media='' ;;
    manifest) id=$pin; media=application/vnd.oci.image.manifest.v1+json ;;
    index) id=$index; media=application/vnd.oci.image.index.v1+json ;;
    *) exit 1 ;;
  esac
  if [[ ${RELEASE_FIXTURE_DIFFERENT_LOCAL:-false} == true ]]; then id=sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee; fi
  jq -cn --arg platform "$selected" --arg id "$id" --arg media "$media" --arg revision "$revision" \
    '{Id:$id,Os:"linux",Architecture:($platform|split("/")[1]),Config:{User:"65532:65532",Env:["PATH=/usr/bin"],Labels:{"org.opencontainers.image.version":"1.2.3","org.opencontainers.image.revision":$revision,"io.multica.controller-abi":"2"}}} | if $media == "" then . else .Descriptor={digest:$id,mediaType:$media} end'
}
case ${1:-} in
  info) printf '%s\n' "${platform#linux/}"; exit ;;
  image) [[ $2 == inspect ]]; image "$(native_platform "$3")" | jq -cs .; exit ;;
  run) [[ ${RELEASE_FIXTURE_FAIL_NATIVE:-false} != true ]]; exit ;;
  pull) exit ;;
  tag)
    selected=$(native_platform "$2")
    printf manifest >"$root/local-${selected#linux/}-kind"
    exit ;;
  push)
    selected=$(native_platform "$2")
    if [[ $selected == linux/amd64 ]]; then pin=$amd; else pin=$arm; fi
    if [[ ${RELEASE_FIXTURE_IMAGE_ID_KIND:-legacy} == index ]]; then
      if [[ $selected == linux/amd64 ]]; then index=sha256:1111111111111111111111111111111111111111111111111111111111111111; else index=sha256:2222222222222222222222222222222222222222222222222222222222222222; fi
      jq -cn --arg index "$index" --arg digest "$pin" --arg arch "${selected#linux/}" '{digest:$index,mediaType:"application/vnd.oci.image.index.v1+json",manifests:[{digest:$digest,platform:{os:"linux",architecture:$arch}}]}' >"$(manifest_file "$2")"
    else
      jq -cn --arg digest "$pin" '{digest:$digest,mediaType:"application/vnd.oci.image.manifest.v1+json"}' >"$(manifest_file "$2")"
    fi
    if [[ ${RELEASE_FIXTURE_PUSH_DIFFERENT:-false} == true ]]; then
      jq '.digest="sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"' "$(manifest_file "$2")" >"$root/different-push"
      mv "$root/different-push" "$(manifest_file "$2")"
    fi
    exit ;;
  buildx) [[ ${2:-} == imagetools ]] ;;
  *) printf 'unexpected fixture docker command\n' >&2; exit 1 ;;
esac
if [[ $3 == inspect ]]; then
  ref=$4
  if [[ $(cat "$root/fault" 2>/dev/null || :) == registry-auth ]]; then printf 'registry access denied\n' >&2; exit 1; fi
  if [[ ${5:-} == --raw ]]; then
    selected=$(native_platform "$ref")
    if [[ $selected == linux/amd64 ]]; then config=$config_amd; else config=$config_arm; fi
    jq -cn --arg digest "$config" '{config:{digest:$digest}}'
  elif [[ ${6:-} == '{{json .Image}}' ]]; then
    selected=$(native_platform "$ref")
    if [[ -f $root/remote-${selected#linux/}.json ]]; then
      cat "$root/remote-${selected#linux/}.json"
    else
      image "$selected" | jq -c '{os:.Os,architecture:.Architecture,config:.Config}'
    fi
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
sources=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --tag) tag=$2; shift 2 ;;
    --annotation) shift 2 ;;
    fixture/runtime@sha256:*) sources+=("$1"); shift ;;
    *) exit 1 ;;
  esac
done
file=$(manifest_file "$tag")
if [[ $tag == fixture/runtime:1.2.3 ]]; then
  [[ ${#sources[@]} == 2 && ${sources[0]} == "fixture/runtime@$amd" && ${sources[1]} == "fixture/runtime@$arm" && ! -f $file ]]
  # Buildx drops --annotation for Docker lists; only OCI indexes retain them.
  jq -cn --arg amd "$amd" --arg arm "$arm" --arg revision "$revision" --arg format "${RELEASE_FIXTURE_INDEX_FORMAT:-oci}" '
    (if $format == "docker" then "application/vnd.docker.distribution.manifest.list.v2+json" else "application/vnd.oci.image.index.v1+json" end) as $media |
    {schemaVersion:2,mediaType:$media,digest:"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
     manifests:[{digest:$amd,platform:{os:"linux",architecture:"amd64"}},{digest:$arm,platform:{os:"linux",architecture:"arm64"}}]} |
    if $format == "docker" then . else .annotations={"org.opencontainers.image.revision":$revision,"org.opencontainers.image.version":"1.2.3"} end' >"$file"
  if [[ $(cat "$root/fault" 2>/dev/null || :) == after-version ]]; then rm "$root/fault"; exit 1; fi
elif [[ $tag == fixture/runtime:latest ]]; then
  [[ ${#sources[@]} == 1 && ${sources[0]} == "fixture/runtime@$(jq -r .digest "$root/registry/version.json")" ]]
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
promotion_state() {
  stored_state "$fixture/registry/latest.json" "$fixture/tag.json" "$fixture/release.json" "$fixture/github-latest.json"
}
require_rejected_without_writes() {
  registry_state >"$fixture/before-state"
  if release "$@"; then printf 'fixture unsafe operation succeeded: %s\n' "$*" >&2; exit 1; fi
  registry_state >"$fixture/after-state"
  cmp "$fixture/before-state" "$fixture/after-state"
}

require_success plan
require_success record-native --platform linux/amd64 --records "$fixture/records"
require_rejected_without_writes publish --records "$fixture/records"
export RELEASE_FIXTURE_PLATFORM=linux/arm64 RELEASE_FIXTURE_FAIL_NATIVE=true
require_rejected_without_writes record-native --platform linux/arm64 --records "$fixture/records"
unset RELEASE_FIXTURE_FAIL_NATIVE
require_success record-native --platform linux/arm64 --records "$fixture/records"
require_success publish --records "$fixture/records"
registry_state >"$fixture/committed-state"
cp "$fixture/registry/latest.json" "$fixture/committed-latest"
cp "$fixture/release.json" "$fixture/committed-release"
require_success publish --records "$fixture/records"
registry_state >"$fixture/retried-state"
cmp "$fixture/retried-state" "$fixture/committed-state"
cmp "$fixture/registry/latest.json" "$fixture/committed-latest"
# A release source cannot replace the verifier that runs with publish authority.
# Its verifier attempts to overwrite an already published latest image.
cat >"$fixture/checkout/.github/scripts/verify-base.sh" <<'UNTRUSTED'
#!/usr/bin/env bash
cp "$RELEASE_FIXTURE_ROOT/registry/arm64.json" "$RELEASE_FIXTURE_ROOT/registry/latest.json"
UNTRUSTED
require_success record-native --platform linux/arm64 --records "$fixture/records"
registry_state >"$fixture/source-verifier-state"
cmp "$fixture/source-verifier-state" "$fixture/committed-state"
# Existing releases can name a branch as target_commitish; the immutable tag
# still owns source identity, and accepting it must preserve published bytes.
jq '.target_commitish="main"' "$fixture/committed-release" >"$fixture/release.json"
cp "$fixture/release.json" "$fixture/github-latest.json"
registry_state >"$fixture/branch-target-state"
require_success publish --records "$fixture/records"
registry_state >"$fixture/branch-target-retry-state"
cmp "$fixture/branch-target-state" "$fixture/branch-target-retry-state"
cp "$fixture/committed-release" "$fixture/release.json"
cp "$fixture/committed-release" "$fixture/github-latest.json"
require_success prepare-native --platform linux/arm64
require_success record-native --platform linux/arm64 --records "$fixture/records"
registry_state >"$fixture/retried-state"
cmp "$fixture/retried-state" "$fixture/committed-state"
export RELEASE_FIXTURE_DIFFERENT_LOCAL=true
require_rejected_without_writes record-native --platform linux/arm64 --records "$fixture/records"
unset RELEASE_FIXTURE_DIFFERENT_LOCAL

# Release authority comes from an existing tag, committed VERSION, and main
# ancestry. A checkout edit or another tag target cannot authorize promotion.
rm "$fixture/tag.json"
require_rejected_without_writes publish --records "$fixture/records"
jq -cn --arg sha "$other" '{object:{type:"commit",sha:$sha}}' >"$fixture/tag.json"
require_rejected_without_writes publish --records "$fixture/records"
jq -cn --arg sha "$revision" '{object:{type:"commit",sha:$sha}}' >"$fixture/tag.json"
printf '%s\n' 1.2.4 >"$fixture/checkout/VERSION"
require_rejected_without_writes publish --records "$fixture/records"
cp "$fixture/checkout/VERSION" "$fixture/committed-version"
printf '%s\n' 1.2.3 >"$fixture/checkout/VERSION"
require_rejected_without_writes publish --records "$fixture/records"
printf '%s\n' 1.2.3 >"$fixture/committed-version"
cp "$fixture/committed-version" "$fixture/checkout/VERSION"

# Annotated tags resolve to the same release identity. A moving main remains
# authorized only when the selected tagged commit is still its ancestor.
jq -cn --arg sha "$other" '{object:{type:"tag",sha:$sha}}' >"$fixture/tag.json"
jq -cn --arg sha "$revision" '{object:{type:"commit",sha:$sha}}' >"$fixture/tag-object.json"
require_success publish --records "$fixture/records"
jq -cn --arg sha "$revision" '{object:{type:"commit",sha:$sha}}' >"$fixture/tag.json"
printf '%s\n' "$other" >"$fixture/main"
printf '%s\n' ahead >"$fixture/main-comparison"
require_success publish --records "$fixture/records"
registry_state >"$fixture/descendant-state"
cmp "$fixture/committed-state" "$fixture/descendant-state"
printf '%s\n' diverged >"$fixture/main-comparison"
require_rejected_without_writes publish --records "$fixture/records"
cmp "$fixture/registry/latest.json" "$fixture/committed-latest"
printf '%s\n' "$revision" >"$fixture/main"
printf '%s\n' identical >"$fixture/main-comparison"

# An older release may finish after a newer release, preserving both latest
# pointers and the already published immutable version bytes.
jq --arg revision "$other" '.annotations["org.opencontainers.image.version"]="9.0.0" | .annotations["org.opencontainers.image.revision"]=$revision | .digest="sha256:9999999999999999999999999999999999999999999999999999999999999999"' "$fixture/committed-latest" >"$fixture/registry/latest.json"
cp "$fixture/registry/latest.json" "$fixture/newer-latest"
jq '.tag_name="9.0.0"' "$fixture/committed-release" >"$fixture/github-latest.json"
cp "$fixture/github-latest.json" "$fixture/newer-github-latest"
registry_state >"$fixture/newer-state"
require_success publish --records "$fixture/records"
registry_state >"$fixture/older-retry-state"
cmp "$fixture/newer-state" "$fixture/older-retry-state"
rm "$fixture/release.json"
require_success publish --records "$fixture/records"
cmp "$fixture/release.json" "$fixture/committed-release"
cmp "$fixture/registry/latest.json" "$fixture/newer-latest"
cmp "$fixture/github-latest.json" "$fixture/newer-github-latest"

# If the release already exists but its latest promotion was lost, retry can
# repair the pointer without changing the release's immutable image bytes.
cp "$fixture/committed-latest" "$fixture/registry/latest.json"
jq '.tag_name="1.2.2"' "$fixture/committed-release" >"$fixture/github-latest.json"
require_success publish --records "$fixture/records"
registry_state >"$fixture/repaired-state"
cmp "$fixture/committed-state" "$fixture/repaired-state"

rm "$fixture/registry/version.json" "$fixture/registry/latest.json" "$fixture/release.json" "$fixture/github-latest.json"
promotion_state >"$fixture/before-promotion"
printf '%s\n' after-version >"$fixture/fault"
if release publish --records "$fixture/records"; then printf 'lost version response unexpectedly succeeded\n' >&2; exit 1; fi
cmp "$fixture/registry/version.json" "$fixture/committed-latest"
promotion_state >"$fixture/after-promotion"
cmp "$fixture/before-promotion" "$fixture/after-promotion"
require_success publish --records "$fixture/records"
cmp "$fixture/registry/latest.json" "$fixture/committed-latest"
rm "$fixture/registry/latest.json" "$fixture/release.json" "$fixture/github-latest.json"
stored_state "$fixture/registry/latest.json" >"$fixture/before-promotion"
printf '%s\n' after-release >"$fixture/fault"
if release publish --records "$fixture/records"; then printf 'lost release response unexpectedly succeeded\n' >&2; exit 1; fi
cmp "$fixture/release.json" "$fixture/committed-release"
stored_state "$fixture/registry/latest.json" >"$fixture/after-promotion"
cmp "$fixture/before-promotion" "$fixture/after-promotion"
require_success publish --records "$fixture/records"
registry_state >"$fixture/recovered-state"
cmp "$fixture/recovered-state" "$fixture/committed-state"
printf '%s\n' registry-auth >"$fixture/fault"
require_rejected_without_writes plan
rm "$fixture/fault"
jq '.manifests += [.manifests[0]]' "$fixture/registry/version.json" >"$fixture/duplicate-index"
mv "$fixture/duplicate-index" "$fixture/registry/version.json"
require_rejected_without_writes plan

# Containerd image stores expose manifest/index Id values; classic stores expose
# the config Id. Exercise immutable-byte preservation through all actual formats.
export RELEASE_FIXTURE_PLATFORM=linux/amd64
for storage in manifest index; do
  export RELEASE_FIXTURE_IMAGE_ID_KIND=$storage
  rm -f "$fixture/registry/amd64.json" "$fixture/local-amd64-kind"
  require_success record-native --platform linux/amd64 --records "$fixture/records"
  registry_state >"$fixture/recorded-state"
  require_success record-native --platform linux/amd64 --records "$fixture/records"
  registry_state >"$fixture/recorded-retry-state"
  cmp "$fixture/recorded-state" "$fixture/recorded-retry-state"
  export RELEASE_FIXTURE_DIFFERENT_LOCAL=true
  require_rejected_without_writes record-native --platform linux/amd64 --records "$fixture/records"
  unset RELEASE_FIXTURE_DIFFERENT_LOCAL
  require_success prepare-native --platform linux/amd64
  require_success record-native --platform linux/amd64 --records "$fixture/records"
  registry_state >"$fixture/pulled-retry-state"
  cmp "$fixture/recorded-state" "$fixture/pulled-retry-state"
  rm -f "$fixture/registry/amd64.json" "$fixture/local-amd64-kind"
  cp "$fixture/records/amd64.json" "$fixture/retained-proof"
  export RELEASE_FIXTURE_PUSH_DIFFERENT=true
  if release record-native --platform linux/amd64 --records "$fixture/records"; then
    printf 'different bytes after push acquired native verification authority\n' >&2
    exit 1
  fi
  unset RELEASE_FIXTURE_PUSH_DIFFERENT
  cmp "$fixture/records/amd64.json" "$fixture/retained-proof"
done

# A successful native pair can be published in either registry format. A lost
# publish response must not require overwriting the already accepted version.
unset RELEASE_FIXTURE_IMAGE_ID_KIND
for format in oci docker; do
  export RELEASE_FIXTURE_INDEX_FORMAT=$format
  rm -f "$fixture/registry/"*.json "$fixture/records/"*.json "$fixture/release.json" "$fixture/github-latest.json" "$fixture/local-"*-kind
  export RELEASE_FIXTURE_PLATFORM=linux/amd64
  require_success record-native --platform linux/amd64 --records "$fixture/records"
  require_rejected_without_writes publish --records "$fixture/records"
  export RELEASE_FIXTURE_PLATFORM=linux/arm64 RELEASE_FIXTURE_FAIL_NATIVE=true
  require_rejected_without_writes record-native --platform linux/arm64 --records "$fixture/records"
  unset RELEASE_FIXTURE_FAIL_NATIVE
  require_success record-native --platform linux/arm64 --records "$fixture/records"
  promotion_state >"$fixture/before-promotion"
  printf '%s\n' after-version >"$fixture/fault"
  if release publish --records "$fixture/records"; then printf 'lost version response unexpectedly succeeded\n' >&2; exit 1; fi
  promotion_state >"$fixture/after-promotion"
  cmp "$fixture/before-promotion" "$fixture/after-promotion"
  cp "$fixture/registry/version.json" "$fixture/format-version"
  require_success publish --records "$fixture/records"
  cmp "$fixture/format-version" "$fixture/registry/version.json"
  cmp "$fixture/format-version" "$fixture/registry/latest.json"
  registry_state >"$fixture/format-committed"
  require_success publish --records "$fixture/records"
  registry_state >"$fixture/format-retried"
  cmp "$fixture/format-committed" "$fixture/format-retried"

  # Metadata conflicts and changed children cannot gain promotion authority,
  # including when the Docker root has no version/revision annotations.
  for change in \
    '.annotations["org.opencontainers.image.version"]="9.0.0"' \
    '.annotations["org.opencontainers.image.revision"]="2222222222222222222222222222222222222222"' \
    '.manifests += [.manifests[0]]' \
    '.manifests[1].digest=.manifests[0].digest'; do
    jq "$change" "$fixture/format-version" >"$fixture/registry/version.json"
    require_rejected_without_writes publish --records "$fixture/records"
  done
  if [[ $format == oci ]]; then
    jq 'del(.annotations)' "$fixture/format-version" >"$fixture/registry/version.json"
    require_rejected_without_writes publish --records "$fixture/records"
  fi
  cp "$fixture/format-version" "$fixture/registry/version.json"
  docker buildx imagetools inspect fixture/runtime@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb --format '{{json .Image}}' >"$fixture/format-arm64"
  for change in \
    '.config.Labels["org.opencontainers.image.version"]="9.0.0"' \
    '.config.Labels["org.opencontainers.image.revision"]="2222222222222222222222222222222222222222"' \
    '.config.Labels["io.multica.controller-abi"]="1"' \
    '.architecture="amd64"'; do
    jq "$change" "$fixture/format-arm64" >"$fixture/remote-arm64.json"
    require_rejected_without_writes publish --records "$fixture/records"
  done
  rm "$fixture/remote-arm64.json"
  require_success plan
done
printf '%s\n' 'PASS: tagged source authority, immutable release guards, failed/partial native results, lost-response retries, monotonic GitHub/registry latest, Docker/OCI metadata authority, and Docker config/manifest/index image identities (local fixture emulation).'
