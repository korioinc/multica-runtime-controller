#!/usr/bin/env bash
# Build a controller-only contract from the actual executable bytes.
set -euo pipefail
export LC_ALL=C

usage() {
  printf '%s\n' 'Usage: core-contract.sh DIRECTORY --platform linux/amd64|linux/arm64 --version VERSION --commit FULL_COMMIT --go-version goX.Y.Z [--root /opt/multica/controller]'
}
fail() { printf 'controller contract error: %s\n' "$*" >&2; exit 1; }
sha256() {
  if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi | cut -d ' ' -f 1
}

[[ $# -gt 0 ]] || { usage >&2; exit 1; }
[[ $1 != --help && $1 != -h ]] || { usage; exit 0; }
directory=$1
shift
root=/opt/multica/controller platform='' version='' commit='' go_version=''
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || { usage >&2; exit 1; }
  case $1 in
    --root) root=$2 ;;
    --platform) platform=$2 ;;
    --version) version=$2 ;;
    --commit) commit=$2 ;;
    --go-version) go_version=$2 ;;
    *) usage >&2; exit 1 ;;
  esac
  shift 2
done
[[ $platform == linux/amd64 || $platform == linux/arm64 ]] || fail 'unsupported platform'
[[ $version =~ ^([0-9]+\.[0-9]+\.[0-9]+|dev|ci|develop)$ ]] || fail 'invalid controller version'
[[ $commit =~ ^[0-9a-f]{40}$ ]] || fail 'commit must be a full source revision'
[[ $go_version =~ ^go[1-9][0-9]*\.[0-9]+\.[0-9]+$ ]] || fail "go-version must be the SDK's actual version"
[[ $root == /* && $root != / && $root != *//* && $root != */./* && $root != */../* && $root != */. && $root != */.. && $root != */ ]] ||
  fail 'controller root must be canonical and absolute'
runtime=$directory/runtime
[[ -f $runtime && ! -L $runtime && -x $runtime ]] || fail 'controller is not a regular executable'
chmod 0555 "$runtime"
runtime_hash=$(sha256 <"$runtime")
mkdir -m 0755 -- "$directory/shims"
mkdir -m 0555 -- "$directory/disabled"
for alias in pi codex copilot agy; do ln -- "$runtime" "$directory/shims/$alias"; done
image_root=${root%/}
build=$(jq -cnS --arg version "$version" --arg commit "$commit" --arg platform "$platform" \
  --arg runtime_hash "$runtime_hash" --arg go_version "$go_version" \
  '{version:$version,commit:$commit,platform:$platform,runtimeSHA256:$runtime_hash,goVersion:$go_version,controllerABI:2}')
build_id=$(printf '%s' "$build" | sha256)
contract=$(jq -cnS --arg build_id "$build_id" --arg platform "$platform" --arg root "$image_root" \
  --arg runtime_hash "$runtime_hash" --arg go_version "$go_version" \
  '{schemaVersion:2,controllerABI:2,buildID:$build_id,platform:$platform,runtimePath:($root+"/runtime"),runtimeSHA256:$runtime_hash,
    shimPaths:(["pi","codex","copilot","agy"] | map({key:.,value:($root+"/shims/"+.)}) | from_entries),goVersion:$go_version}')
printf '%s' "$contract" >"$directory/build.json"
chmod 0444 "$directory/build.json"
