#!/usr/bin/env bash
# Validate literal Go pins and prepare the independent controller VERSION.
set -euo pipefail
export LC_ALL=C
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=scripts/lib/version.sh
source "$script_dir/lib/version.sh"

usage() {
  cat <<'USAGE'
Usage: runtime-versions.sh --root ABSOLUTE_CHECKOUT COMMAND
  validate
  build-args [--revision FULL_COMMIT] [--version VERSION] [--format json|github-output]
  prepare-release --base-ref REF
USAGE
}

root='' command='' revision='' version='' format=json base_ref=''
while [[ $# -gt 0 ]]; do
  case $1 in
    --root) [[ $# -ge 2 ]] || { usage >&2; exit 1; }; root=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    validate|build-args|prepare-release) command=$1; shift; break ;;
    *) usage >&2; exit 1 ;;
  esac
done
[[ $root == /* && -n $command ]] || { usage >&2; exit 1; }
while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || { usage >&2; exit 1; }
  case "$command:$1" in
    build-args:--revision) revision=$2 ;;
    build-args:--version) version=$2 ;;
    build-args:--format) format=$2 ;;
    prepare-release:--base-ref) base_ref=$2 ;;
    *) usage >&2; exit 1 ;;
  esac
  shift 2
done

case $command in
  validate)
    version=$(version_read "$root/VERSION")
    pin=$(version_go_pin "$root")
    jq -cnS --arg version "$version" --arg pin "$pin" '{version:$version,goVersion:$pin}'
    ;;
  build-args)
    [[ $format == json || $format == github-output ]] || { usage >&2; exit 1; }
    result=$(version_build_args "$root" "$revision" "$version")
    if [[ $format == github-output ]]; then version_github_output "$result"; else printf '%s\n' "$result"; fi
    ;;
  prepare-release)
    [[ -n $base_ref ]] || { usage >&2; exit 1; }
    base=$(git -C "$root" show "$base_ref:VERSION")
    desired=$(version_next_patch "$base")
    current=$(version_read "$root/VERSION")
    [[ $current == "$base" || $current == "$desired" ]] ||
      { version_error 'VERSION must equal the base version or its next patch release'; exit 1; }
    changed=false
    if [[ $current == "$base" ]]; then
      changed=true
      mode=$(stat -c '%a' "$root/VERSION" 2>/dev/null || stat -f '%Lp' "$root/VERSION")
      printf -v mode '%o' "$((8#$mode & 0777))"
      temporary=$(mktemp "$root/.VERSION.XXXXXX")
      trap 'rm -f -- "$temporary"' EXIT
      # Write before restoring the source mode, including a read-only VERSION.
      printf '%s\n' "$desired" >"$temporary"
      chmod "$mode" "$temporary"
      sync
      mv -f -- "$temporary" "$root/VERSION"
      sync
    fi
    jq -cnS --arg base_ref "$base_ref" --arg from "$base" --arg to "$desired" \
      --argjson changed "$changed" '{base_ref:$base_ref,changed:$changed,release_version:{from:$from,to:$to}}'
    ;;
esac
