#!/usr/bin/env bash
# Validate literal Go pins and the independent controller VERSION.
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
USAGE
}

root='' command='' revision='' version='' format=json
while [[ $# -gt 0 ]]; do
  case $1 in
    --root) [[ $# -ge 2 ]] || { usage >&2; exit 1; }; root=$2; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    validate|build-args) command=$1; shift; break ;;
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
esac
