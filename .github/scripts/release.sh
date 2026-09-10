#!/usr/bin/env bash
# Publish only matching, verified native candidates; preserve immutable bytes.
set -euo pipefail
export LC_ALL=C
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=scripts/lib/version.sh
source "$repository/scripts/lib/version.sh"

usage() {
  cat <<'USAGE'
Usage: release.sh --root ABSOLUTE_CHECKOUT --image REPOSITORY --revision FULL_COMMIT COMMAND
  plan
  prepare-native --platform linux/amd64|linux/arm64
  record-native --platform linux/amd64|linux/arm64 --records DIRECTORY
  publish --records DIRECTORY
External effects use gh and docker; PATH can provide local fixture commands.
USAGE
}
fail() { printf 'release blocked: %s\n' "$*" >&2; exit 1; }
supported_platform() { [[ $1 == linux/amd64 || $1 == linux/arm64 ]]; }
valid_digest() { [[ $1 =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'invalid registry digest'; printf '%s\n' "$1"; }

root='' image='' revision='' release_command='' platform='' records=''
while [[ $# -gt 0 ]]; do
  case $1 in
    --root|--image|--revision)
      [[ $# -ge 2 ]] || { usage >&2; exit 1; }
      case $1 in --root) root=$2 ;; --image) image=$2 ;; --revision) revision=$2 ;; esac
      shift 2 ;;
    plan|prepare-native|record-native|publish) release_command=$1; shift; break ;;
    --help|-h) usage; exit 0 ;;
    *) usage >&2; exit 1 ;;
  esac
done
[[ $root == /* && $image =~ ^[a-z0-9][a-z0-9./_-]+$ && $revision =~ ^[0-9a-f]{40}$ && -n $release_command ]] ||
  { usage >&2; exit 1; }
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || { usage >&2; exit 1; }
  case "$release_command:$1" in
    prepare-native:--platform|record-native:--platform) platform=$2 ;;
    record-native:--records|publish:--records) records=$2 ;;
    *) usage >&2; exit 1 ;;
  esac
  shift 2
done
case $release_command in
  prepare-native|record-native) supported_platform "$platform" || fail 'a supported --platform is required' ;;
esac
case $release_command in
  record-native|publish) [[ -n $records ]] || fail '--records is required' ;;
esac
case $release_command in
  plan|publish) [[ -n ${GH_REPO:-} ]] || fail 'GH_REPO is required' ;;
esac
version=$(version_read "$root/VERSION")
version_build_args "$root" "$revision" "$version" >/dev/null
scratch=$(mktemp -d "${TMPDIR:-/tmp}/controller-release.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
revision_label=org.opencontainers.image.revision
version_label=org.opencontainers.image.version

run_command() {
  "$@" >"$scratch/stdout" 2>"$scratch/stderr" || fail "$1 $2 failed; no release promotion performed"
  cat "$scratch/stdout" || fail 'cannot read command output'
}

github() {
  local response header body status code
  if response=$(gh api --include "repos/$GH_REPO/$1" 2>"$scratch/stderr"); then code=0; else code=$?; fi
  response=${response//$'\r'/}
  [[ $response == *$'\n\n'* ]] || fail 'GitHub returned an unreadable response'
  header=${response%%$'\n\n'*} body=${response#*$'\n\n'}
  [[ $header =~ ^HTTP/[0-9.]+\ ([0-9]{3}) ]] || fail 'GitHub returned an unreadable response'
  status=${BASH_REMATCH[1]}
  if [[ $status == 404 ]]; then printf 'null\n'; return; fi
  [[ $status == 200 && $code == 0 ]] || fail 'GitHub lookup failed'
  jq -ce 'if type == "object" then . else error("invalid GitHub object") end' <<<"$body" || fail 'invalid GitHub object'
}

inspect() {
  local ref=$1 field=${2:-Manifest} optional=${3:-false} result error
  if ! result=$(docker buildx imagetools inspect "$ref" --format "{{json .$field}}" 2>"$scratch/stderr"); then
    error=$(cat "$scratch/stderr") || fail 'cannot read registry error'
    # An auth, transport or server failure never means the artifact is absent.
    if [[ $optional == true ]] && jq -en --arg error "$error" --arg ref "$ref" '
      ($error | ascii_downcase) as $message |
      ($message | test("manifest unknown|manifest_unknown")) or
      ($message | split($ref + ": not found")[1:] | any(. == "" or test("^\\s")))
    ' >/dev/null; then printf 'null\n'; return; fi
    fail 'registry lookup failed'
  fi
  jq -ce 'if type == "object" then . else error("invalid registry object") end' <<<"$result" || fail 'invalid registry object'
}

image_metadata() {
  local metadata
  metadata=$(inspect "$1" Image) || return 1
  jq -e --arg platform "$2" --arg revision "$revision" --arg version "$version" '
    .os + "/" + .architecture == $platform and
    .config.Labels["org.opencontainers.image.revision"] == $revision and
    .config.Labels["org.opencontainers.image.version"] == $version and
    .config.Labels["io.multica.controller-abi"] == "2"
  ' <<<"$metadata" >/dev/null || fail 'native image platform or source/build mismatch'
}

native_digest() {
  local pin
  pin=$(jq -er --arg platform "$2" '
    if has("manifests") then
      [.manifests[] | select((.platform.os == "unknown" and .platform.architecture == "unknown" and
        .annotations["vnd.docker.reference.type"] == "attestation-manifest") | not)] |
      if length == 1 and (.[0].platform.os + "/" + .[0].platform.architecture) == $platform
      then .[0].digest else error("native candidate must contain one executable of the requested platform") end
    else .digest end
  ' <<<"$1") || fail 'invalid native candidate'
  valid_digest "$pin"
}

index_entries() {
  jq -ce '
    reduce (.manifests[] | select((.platform.os == "unknown" and .platform.architecture == "unknown" and
      .annotations["vnd.docker.reference.type"] == "attestation-manifest") | not)) as $entry
      ({}; ($entry.platform.os + "/" + $entry.platform.architecture) as $name |
        if ($name == "linux/amd64" or $name == "linux/arm64") and (has($name) | not) and
           (($entry.platform.variant // "") == "" or ($name == "linux/arm64" and $entry.platform.variant == "v8")) and
           ($entry.digest | type == "string" and test("^sha256:[0-9a-f]{64}$"))
        then . + {($name):$entry.digest} else error("invalid executable index entry") end) |
    if length == 2 then . else error("both native platforms are required") end
  ' <<<"$1" || fail 'release index must contain exactly one executable per supported platform'
}

guard() {
  local main tag target sha nested existing_release visited=' '
  main=$(github git/ref/heads/main) || return 1
  [[ $(jq -r '.object.sha // ""' <<<"$main") == "$revision" ]] || fail 'a newer or different main revision exists'
  tag=$(github "git/ref/tags/$version") || return 1
  if [[ $tag != null ]]; then
    target=$(jq -c '.object' <<<"$tag") || fail 'invalid release tag'
    while [[ $(jq -r '.type' <<<"$target") == tag ]]; do
      sha=$(jq -r '.sha' <<<"$target") || fail 'invalid annotated release tag'
      [[ $sha =~ ^[0-9a-f]{40}$ && $visited != *" $sha "* ]] || fail 'invalid annotated release tag'
      visited="$visited$sha "
      nested=$(github "git/tags/$sha") || return 1
      target=$(jq -c '.object' <<<"$nested") || fail 'invalid annotated release tag'
    done
    jq -e --arg revision "$revision" '.type == "commit" and .sha == $revision' <<<"$target" >/dev/null ||
      fail 'release version belongs to another revision'
  fi
  existing_release=$(github "releases/tags/$version") || return 1
  if [[ $existing_release != null ]]; then
    jq -e --arg revision "$revision" '.target_commitish == $revision and (.draft | not) and (.prerelease | not)' \
      <<<"$existing_release" >/dev/null || fail 'GitHub release belongs to another revision or is incomplete'
  fi
}

published() {
  local manifest entries native_platform pin
  manifest=$(inspect "$image:$version" Manifest true) || return 1
  if [[ $manifest == null ]]; then printf 'null\n'; return; fi
  # Buildx only retains index annotations for OCI. Docker schema-2 lists rely
  # on both native image labels below; any supplied root metadata must agree.
  jq -e --arg revision "$revision" --arg version "$version" '
    (if has("annotations") then .annotations else {} end) as $annotations |
    .schemaVersion == 2 and ($annotations | type == "object") and
    if .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json" then
      (($annotations | has("org.opencontainers.image.revision") | not) or $annotations["org.opencontainers.image.revision"] == $revision) and
      (($annotations | has("org.opencontainers.image.version") | not) or $annotations["org.opencontainers.image.version"] == $version)
    elif .mediaType == "application/vnd.oci.image.index.v1+json" then
      $annotations["org.opencontainers.image.revision"] == $revision and
      $annotations["org.opencontainers.image.version"] == $version
    else false end
  ' <<<"$manifest" >/dev/null || fail 'published version belongs to another build'
  entries=$(index_entries "$manifest") || return 1
  for native_platform in linux/amd64 linux/arm64; do
    pin=$(jq -r --arg platform "$native_platform" '.[$platform]' <<<"$entries") || fail 'invalid release index'
    image_metadata "$image@$pin" "$native_platform" || return 1
  done
  valid_digest "$(jq -r '.digest' <<<"$manifest")" >/dev/null || return 1
  printf '%s\n' "$manifest"
}

prior_latest_version() {
  local manifest=$1 prior_version prior_revision entries pin metadata build='' candidate native_platform
  prior_version=$(jq -r '.annotations["org.opencontainers.image.version"] // ""' <<<"$manifest") || fail 'invalid latest metadata'
  prior_revision=$(jq -r '.annotations["org.opencontainers.image.revision"] // ""' <<<"$manifest") || fail 'invalid latest metadata'
  if [[ -z $prior_version || -z $prior_revision ]]; then
    # Older indexes kept metadata in image labels. Compare the actual labels
    # from both platforms before ordering the first schema-2 release.
    entries=$(index_entries "$manifest") || return 1
    for native_platform in linux/amd64 linux/arm64; do
      pin=$(jq -r --arg platform "$native_platform" '.[$platform]' <<<"$entries") || fail 'invalid latest index'
      metadata=$(inspect "$image@$pin" Image) || return 1
      candidate=$(jq -c '[.config.Labels["org.opencontainers.image.version"],.config.Labels["org.opencontainers.image.revision"]]' <<<"$metadata") || fail 'invalid latest image metadata'
      [[ -z $build || $candidate == "$build" ]] || fail 'latest platforms do not identify one source build'
      build=$candidate
    done
    prior_version=$(jq -r '.[0]' <<<"$build") prior_revision=$(jq -r '.[1]' <<<"$build")
  fi
  [[ $prior_revision =~ ^[0-9a-f]{40}$ ]] || fail 'latest source metadata cannot be ordered safely'
  version_stable "$prior_version" || return 1
  printf '%s\n' "$prior_version"
}

native_ref() { printf '%s:build-%s-%s-%s\n' "$image" "$version" "$revision" "${1#linux/}"; }

# Docker's classic image store reports a config digest as Id. The containerd
# store can report the top-level OCI index or selected manifest instead. Compare
# the digest's actual media type, never an assumed config-digest representation.
verify_local_bytes() {
  local local_image=$1 manifest=$2 native_pin=$3 local_id media expected raw
  local_id=$(jq -er 'if length == 1 then .[0].Id else error("invalid local image") end' <<<"$local_image") || fail 'invalid local native image'
  valid_digest "$local_id" >/dev/null || return 1
  media=$(jq -er '.[0].Descriptor.mediaType // ""' <<<"$local_image") || fail 'invalid local descriptor'
  case "$media" in
    application/vnd.oci.image.index.v1+json|application/vnd.docker.distribution.manifest.list.v2+json)
      expected=$(jq -er '.digest' <<<"$manifest") || fail 'native index has no digest' ;;
    application/vnd.oci.image.manifest.v1+json|application/vnd.docker.distribution.manifest.v2+json)
      expected=$native_pin ;;
    '')
      raw=$(run_command docker buildx imagetools inspect "$image@$native_pin" --raw) || return 1
      expected=$(jq -er '.config.digest' <<<"$raw") || fail 'native manifest has no config digest' ;;
    *) fail 'unrecognized local image descriptor media type' ;;
  esac
  valid_digest "$expected" >/dev/null || return 1
  [[ $local_id == "$expected" ]] || fail 'native candidate bytes differ from the locally verified image'
}

prepare_native() {
  local ref manifest pin reuse=false
  ref=$(native_ref "$platform")
  manifest=$(inspect "$ref" Manifest true) || return 1
  if [[ $manifest != null ]]; then
    pin=$(native_digest "$manifest" "$platform") || return 1
    image_metadata "$image@$pin" "$platform" || return 1
    run_command docker pull "$image@$pin" >/dev/null || return 1
    run_command docker tag "$image@$pin" "$ref" >/dev/null || return 1
    reuse=true
  fi
  jq -cnS --arg image "$ref" --argjson reuse "$reuse" '{image:$image,reuse:$reuse}'
}

record_native() {
  local ref local_image existing pin manifest record
  ref=$(native_ref "$platform")
  run_command bash "$root/.github/scripts/verify-base.sh" "$ref" "$platform" "$version" "$revision" >/dev/null || return 1
  local_image=$(run_command docker image inspect "$ref") || return 1
  existing=$(inspect "$ref" Manifest true) || return 1
  if [[ $existing != null ]]; then
    pin=$(native_digest "$existing" "$platform") || return 1
    verify_local_bytes "$local_image" "$existing" "$pin" || return 1
  else
    run_command docker push "$ref" >/dev/null || return 1
  fi
  manifest=$(inspect "$ref") || return 1
  pin=$(native_digest "$manifest" "$platform") || return 1
  verify_local_bytes "$local_image" "$manifest" "$pin" || return 1
  image_metadata "$image@$pin" "$platform" || return 1
  record=$(jq -cnS --arg platform "$platform" --arg pin "$pin" --arg version "$version" --arg revision "$revision" \
    '{platform:$platform,digest:$pin,version:$version,revision:$revision,verification:"native-controller-base-v2"}') || fail 'cannot encode native result'
  mkdir -p -- "$records" || fail 'cannot create result directory'
  printf '%s\n' "$record" >"$records/${platform#linux/}.json" || fail 'cannot record native result'
  printf '%s\n' "$record"
}

publish_index() {
  local manifest entries='{}' path record native_platform pin actual
  manifest=$(published) || return 1
  if [[ $manifest != null ]]; then printf '%s\n' "$manifest"; return; fi
  for path in "$records"/*.json; do
    [[ -e $path ]] || continue
    record=$(cat -- "$path") || fail 'cannot read native result'
    native_platform=$(jq -er '.platform' <<<"$record") || fail 'invalid native result'
    supported_platform "$native_platform" || fail 'unsupported native result platform'
    jq -e --arg version "$version" --arg revision "$revision" '
      .version == $version and .revision == $revision and .verification == "native-controller-base-v2"
    ' <<<"$record" >/dev/null || fail 'native result does not belong to this verified build'
    jq -e --arg platform "$native_platform" 'has($platform) | not' <<<"$entries" >/dev/null || fail 'duplicate native result'
    pin=$(valid_digest "$(jq -r '.digest' <<<"$record")") || return 1
    image_metadata "$image@$pin" "$native_platform" || return 1
    entries=$(jq -c --arg platform "$native_platform" --arg pin "$pin" '. + {($platform):$pin}' <<<"$entries") || fail 'invalid native results'
  done
  [[ $(jq 'length' <<<"$entries") == 2 ]] || fail 'both successful native results are required'
  guard || return 1
  run_command docker buildx imagetools create --tag "$image:$version" \
    --annotation "index:$revision_label=$revision" --annotation "index:$version_label=$version" \
    "$image@$(jq -r '.["linux/amd64"]' <<<"$entries")" "$image@$(jq -r '.["linux/arm64"]' <<<"$entries")" >/dev/null || return 1
  manifest=$(published) || return 1
  [[ $manifest != null ]] || fail 'published index is missing'
  actual=$(index_entries "$manifest") || return 1
  jq -e --argjson expected "$entries" '. == $expected' <<<"$actual" >/dev/null || fail 'published index does not match verified native results'
  printf '%s\n' "$manifest"
}

publish_release() {
  local manifest pin existing_release latest latest_version compared latest_pin
  guard || return 1
  manifest=$(publish_index) || return 1
  pin=$(valid_digest "$(jq -r '.digest' <<<"$manifest")") || return 1
  guard || return 1
  existing_release=$(github "releases/tags/$version") || return 1
  if [[ $existing_release == null ]]; then
    mkdir -p -- "$records" || fail 'cannot create release result directory'
    # shellcheck disable=SC2016
    printf 'Controller base: `%s@%s`\n\nNative platforms: linux/amd64, linux/arm64.\n' "$image" "$pin" >"$records/release-notes.md" || fail 'cannot write release notes'
    run_command gh release create "$version" --repo "$GH_REPO" --target "$revision" --title "$version" \
      --notes-file "$records/release-notes.md" >/dev/null || return 1
  fi
  # Workflow concurrency serializes publishers; guard again before promotion.
  guard || return 1
  latest=$(inspect "$image:latest" Manifest true) || return 1
  latest_pin=''
  if [[ $latest != null ]]; then
    latest_version=$(prior_latest_version "$latest") || return 1
    compared=$(version_compare "$latest_version" "$version") || return 1
    [[ $compared != 1 ]] || fail 'latest is newer or cannot be ordered safely'
    latest_pin=$(jq -r '.digest' <<<"$latest") || fail 'invalid latest digest'
    [[ $compared != 0 || $latest_pin == "$pin" ]] || fail 'latest version has different immutable bytes'
  fi
  if [[ $latest_pin != "$pin" ]]; then
    guard || return 1
    run_command docker buildx imagetools create --tag "$image:latest" "$image@$pin" >/dev/null || return 1
    latest=$(inspect "$image:latest") || return 1
    [[ $(jq -r '.digest' <<<"$latest") == "$pin" ]] || fail 'latest promotion did not preserve the verified index'
  fi
  jq -cnS --arg version "$version" --arg revision "$revision" --arg image "$image@$pin" \
    '{version:$version,revision:$revision,image:$image}'
}

case $release_command in
  plan)
    guard
    manifest=$(published)
    is_published=false
    [[ $manifest == null ]] || is_published=true
    result=$(jq -cnS --arg version "$version" --argjson published "$is_published" '{version:$version,published:$published}')
    ;;
  prepare-native) result=$(prepare_native) ;;
  record-native) result=$(record_native) ;;
  publish) result=$(publish_release) ;;
esac
printf '%s\n' "$result"
[[ -z ${GITHUB_OUTPUT:-} ]] || version_github_output "$result"
