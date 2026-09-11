#!/usr/bin/env bash
# Move only :develop after both native results and publication order are proven.
set +x
set -euo pipefail
export LC_ALL=C
umask 077

fail() { printf 'develop publication blocked: %s\n' "$*" >&2; exit 1; }
notice() {
  printf '%s\n' "$1"
  if [[ -n ${GITHUB_STEP_SUMMARY:-} ]]; then printf '%s\n' "$1" >>"$GITHUB_STEP_SUMMARY"; fi
}
positive_integer() { [[ $1 =~ ^[1-9][0-9]*$ ]]; }
decimal_less() {
  # Run identifiers are decimal strings; avoid shell/jq integer overflow.
  [[ ${#1} -lt ${#2} ]] || { [[ ${#1} -eq ${#2} ]] && [[ $1 < $2 ]]; }
}

[[ $# == 0 ]] || fail 'this command takes its inputs from workflow environment variables'
[[ ${GITHUB_REF:-} == refs/heads/develop && ${GITHUB_SHA:-} =~ ^[0-9a-f]{40}$ ]] || fail 'an exact develop revision is required'
[[ ${GITHUB_REPOSITORY:-} =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || fail 'a GitHub repository identity is required'
repository=$(printf '%s' "$GITHUB_REPOSITORY" | tr '[:upper:]' '[:lower:]')
[[ ${IMAGE:-} == "ghcr.io/$repository" ]] || fail 'the image must belong to this workflow repository'
for variable in GITHUB_RUN_ID GITHUB_RUN_NUMBER GITHUB_RUN_ATTEMPT; do
  positive_integer "${!variable:-}" || fail 'positive workflow run identifiers are required'
done
[[ -n ${GH_TOKEN:-} && -d ${RESULTS_DIR:-} && ! -L $RESULTS_DIR ]] || fail 'GitHub authentication and the native results directory are required'
for command in docker gh jq; do command -v "$command" >/dev/null || fail 'a required publication tool is unavailable'; done
scratch=$(mktemp -d "${TMPDIR:-/tmp}/controller-develop.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT

inspect() {
  local ref=$1 field=${2:-Manifest} optional=${3:-false}
  if ! docker buildx imagetools inspect "$ref" --format "{{json .$field}}" >"$scratch/inspect.json" 2>"$scratch/stderr"; then
    # Only an explicit missing manifest is absence. Authentication, transport,
    # throttling and server failures never authorize replacing an unknown tag.
    if [[ $optional == true ]] && jq -en --rawfile error "$scratch/stderr" --arg ref "$ref" '
      ($error | ascii_downcase | gsub("^\\s+|\\s+$"; "") | sub("^error: "; "")) as $message |
      if $message == ($ref + ": not found") then true
      else ($message | ltrimstr($ref + ": ")) |
        . == "manifest unknown" or . == "manifest_unknown" or
        . == "manifest unknown: manifest unknown" or . == "manifest_unknown: manifest unknown"
      end
    ' >/dev/null 2>"$scratch/json-error"; then printf 'null\n'; return; fi
    fail 'registry lookup failed; inspect remote state before retrying'
  fi
  jq -ces 'if length == 1 and (.[0] | type == "object") then .[0] else error("invalid registry object") end' \
    "$scratch/inspect.json" 2>"$scratch/json-error" || fail 'registry metadata is invalid'
}

index_entries() {
  jq -ce '
    if (.manifests | type == "array" and length == 2) then
      reduce .manifests[] as $entry
        ({}; ($entry.platform.os + "/" + $entry.platform.architecture) as $platform |
          if ($platform == "linux/amd64" or $platform == "linux/arm64") and (has($platform) | not) and
             $entry.mediaType == "application/vnd.oci.image.manifest.v1+json" and
             (($entry.platform.variant // "") == "" or ($platform == "linux/arm64" and $entry.platform.variant == "v8")) and
             ($entry.digest | type == "string" and test("^sha256:[0-9a-f]{64}$"))
          then . + {($platform):$entry.digest} else error("invalid platform entry") end)
    else error("two native platforms are required") end
  ' <<<"$1" 2>"$scratch/json-error" || fail 'develop index must identify exactly the two supported native platforms'
}

managed_index() {
  jq -e --arg repository "$repository" '
    . as $root |
    .schemaVersion == 2 and .mediaType == "application/vnd.oci.image.index.v1+json" and
    (.digest | type == "string" and test("^sha256:[0-9a-f]{64}$")) and
    (.annotations | type == "object") and
    .annotations["org.opencontainers.image.version"] == "develop" and
    (.annotations["org.opencontainers.image.revision"] | type == "string" and test("^[0-9a-f]{40}$")) and
    .annotations["io.multica.develop.repository"] == $repository and
    all(["io.multica.develop.run-number", "io.multica.develop.run-id", "io.multica.develop.run-attempt"][];
      . as $key | $root.annotations[$key] | type == "string" and test("^[1-9][0-9]*$"))
  ' <<<"$1" >/dev/null 2>"$scratch/json-error" || fail 'existing develop index has missing or inconsistent publication ownership'
  index_entries "$1" >/dev/null
}

count=0
: >"$scratch/results.jsonl"
for path in "$RESULTS_DIR"/*.json; do
  [[ -e $path || -L $path ]] || continue
  [[ -f $path && ! -L $path ]] || fail 'native results must be regular JSON files'
  size=$(wc -c <"$path") || fail 'native result could not be read'
  [[ $size -gt 0 && $size -le 16384 ]] || fail 'native result exceeds its size limit or is empty'
  jq -ces 'if length == 1 and (.[0] | type == "object") then .[0] else error("one native result required") end' \
    "$path" >>"$scratch/results.jsonl" 2>"$scratch/json-error" || fail 'native result is invalid'
  count=$((count + 1))
done
[[ $count == 2 ]] || fail 'both verified native results are required'
entries=$(jq -cs --arg image "$IMAGE" --arg revision "$GITHUB_SHA" --arg run_id "$GITHUB_RUN_ID" '
  if length == 2 and (map(.platform) | sort) == ["linux/amd64", "linux/arm64"] and
     all(.[]; .image == $image and .revision == $revision and .run_id == $run_id and
       (.digest | type == "string" and test("^sha256:[0-9a-f]{64}$")))
  then reduce .[] as $result ({}; . + {($result.platform):$result.digest})
  else error("native result ownership mismatch") end
' "$scratch/results.jsonl" 2>"$scratch/json-error") || fail 'native results do not belong to this verified workflow run'

current=$(inspect "$IMAGE:develop" Manifest true)
if [[ $current != null ]]; then
  managed_index "$current"
  previous_number=$(jq -r '.annotations["io.multica.develop.run-number"]' <<<"$current")
  if decimal_less "$GITHUB_RUN_NUMBER" "$previous_number"; then
    notice 'Skipping publication: a newer workflow run already owns develop.'
    exit 0
  fi
  if [[ $GITHUB_RUN_NUMBER == "$previous_number" ]]; then
    jq -e --arg run_id "$GITHUB_RUN_ID" --arg revision "$GITHUB_SHA" '
      .annotations["io.multica.develop.run-id"] == $run_id and
      .annotations["org.opencontainers.image.revision"] == $revision
    ' <<<"$current" >/dev/null || fail 'this publication generation belongs to another workflow run or revision'
    previous_attempt=$(jq -r '.annotations["io.multica.develop.run-attempt"]' <<<"$current")
    if decimal_less "$GITHUB_RUN_ATTEMPT" "$previous_attempt"; then
      notice 'Skipping publication: a newer attempt of this run already owns develop.'
      exit 0
    fi
  fi
fi

sources=()
for platform in linux/amd64 linux/arm64; do
  digest=$(jq -r --arg platform "$platform" '.[$platform]' <<<"$entries")
  native=$(inspect "$IMAGE@$digest")
  jq -e --arg digest "$digest" '
    .mediaType == "application/vnd.oci.image.manifest.v1+json" and .digest == $digest and (has("manifests") | not)
  ' <<<"$native" >/dev/null 2>"$scratch/json-error" || fail 'a verified result does not identify one native OCI manifest'
  metadata=$(inspect "$IMAGE@$digest" Image)
  jq -e --arg platform "$platform" --arg revision "$GITHUB_SHA" '
    .os + "/" + .architecture == $platform and
    .config.Labels["org.opencontainers.image.revision"] == $revision and
    .config.Labels["org.opencontainers.image.version"] == "develop" and
    .config.Labels["io.multica.controller-abi"] == "2"
  ' <<<"$metadata" >/dev/null 2>"$scratch/json-error" || fail 'native image metadata differs from its verified result'
  sources+=("$IMAGE@$digest")
done

# The workflow serializes publishers. The branch check is deliberately the last
# remote lookup before the one mutable tag write, including publication reruns.
if ! gh api "repos/$repository/git/ref/heads/develop" --jq .object.sha >"$scratch/head" 2>"$scratch/stderr"; then
  fail 'develop HEAD lookup failed; no tag was moved'
fi
head=$(cat "$scratch/head")
[[ $head =~ ^[0-9a-f]{40}$ ]] || fail 'develop HEAD did not identify one commit'
if [[ $head != "$GITHUB_SHA" ]]; then
  notice 'Skipping publication: develop advanced while this image was building.'
  exit 0
fi
if ! docker buildx imagetools create --tag "$IMAGE:develop" \
  --annotation "index:org.opencontainers.image.revision=$GITHUB_SHA" \
  --annotation 'index:org.opencontainers.image.version=develop' \
  --annotation "index:io.multica.develop.run-number=$GITHUB_RUN_NUMBER" \
  --annotation "index:io.multica.develop.run-id=$GITHUB_RUN_ID" \
  --annotation "index:io.multica.develop.run-attempt=$GITHUB_RUN_ATTEMPT" \
  --annotation "index:io.multica.develop.repository=$repository" \
  "${sources[@]}" >"$scratch/stdout" 2>"$scratch/stderr"; then
  fail 'publication result is uncertain; inspect develop before retrying'
fi

published=$(inspect "$IMAGE:develop")
managed_index "$published"
jq -e --arg revision "$GITHUB_SHA" --arg run_number "$GITHUB_RUN_NUMBER" --arg run_id "$GITHUB_RUN_ID" --arg attempt "$GITHUB_RUN_ATTEMPT" '
  .annotations["org.opencontainers.image.revision"] == $revision and
  .annotations["io.multica.develop.run-number"] == $run_number and
  .annotations["io.multica.develop.run-id"] == $run_id and
  .annotations["io.multica.develop.run-attempt"] == $attempt
' <<<"$published" >/dev/null 2>"$scratch/json-error" || fail 'develop readback differs from this publication; inspect the registry before retrying'
actual=$(index_entries "$published")
jq -e --argjson expected "$entries" '. == $expected' <<<"$actual" >/dev/null 2>"$scratch/json-error" ||
  fail 'develop does not reference the verified native bytes; inspect the registry before retrying'
notice "Published $IMAGE:develop from $GITHUB_SHA for amd64 and arm64."
