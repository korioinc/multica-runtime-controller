#!/usr/bin/env bash
# Turn a committed VERSION change into an immutable tag and release request.
set -euo pipefail
export LC_ALL=C
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=scripts/lib/version.sh
source "$repository/scripts/lib/version.sh"

usage() { echo 'Usage: tag-version.sh --root ABSOLUTE_CHECKOUT --before FULL_COMMIT --revision FULL_COMMIT'; }
fail() { printf 'release blocked: %s\n' "$*" >&2; exit 1; }
root='' before='' revision=''
while (($#)); do
  case $1 in --help|-h) usage; exit 0 ;; esac
  (($# >= 2)) || { usage >&2; exit 1; }
  case $1 in
    --root) root=$2 ;;
    --before) before=$2 ;;
    --revision) revision=$2 ;;
    *) usage >&2; exit 1 ;;
  esac
  shift 2
done
[[ $root == /* && $before =~ ^[0-9a-f]{40}$ && $revision =~ ^[0-9a-f]{40}$ ]] || { usage >&2; exit 1; }
[[ -n ${GH_REPO:-} ]] || fail 'GH_REPO is required'
[[ $(git -C "$root" rev-parse HEAD) == "$revision" ]] || fail 'revision differs from checked-out source'
scratch=$(mktemp -d "${TMPDIR:-/tmp}/controller-tag-version.XXXXXX")
trap 'rm -rf -- "$scratch"' EXIT
# shellcheck source=.github/scripts/github-lib.sh
source "$repository/.github/scripts/github-lib.sh"

# Read committed blobs so a dirty checkout or a later main push cannot select
# another version. An unavailable before commit is not an absent VERSION file.
git -C "$root" show "$revision:VERSION" > "$scratch/version" || fail 'the pushed commit must contain VERSION'
version=$(version_read "$scratch/version")
previous=''
if [[ $before != 0000000000000000000000000000000000000000 ]]; then
  git -C "$root" cat-file -e "$before^{commit}" || fail 'the before commit is unavailable'
  previous_path=$(git -C "$root" ls-tree --name-only "$before" -- VERSION) || fail 'cannot inspect the before commit'
  if [[ -n $previous_path ]]; then
    git -C "$root" show "$before:VERSION" > "$scratch/previous-version" || fail 'cannot read the previous VERSION'
    previous=$(version_read "$scratch/previous-version")
  fi
fi
if [[ $version == "$previous" ]]; then
  printf 'VERSION is unchanged (%s); no release requested.\n' "$version"
  exit 0
fi
if [[ -n $previous ]]; then
  [[ $(version_compare "$version" "$previous") == 1 ]] || fail 'VERSION must increase for an automatic release'
fi

github_require_main_revision "$revision"
owner=$(github_tag_revision "$version")
if [[ -z $owner ]]; then
  jq -n --arg ref "refs/tags/$version" --arg sha "$revision" '{ref:$ref,sha:$sha}' > "$scratch/tag.json"
  run_command gh api --method POST "repos/$GH_REPO/git/refs" --input "$scratch/tag.json" >/dev/null
fi
owner=$(github_tag_revision "$version" true)
[[ $owner == "$revision" ]] || fail 'release tag already identifies another commit'

# Run release control from protected main, keeping the tag/SHA as source data.
# Re-dispatch even when the matching tag exists to recover creation/dispatch gaps.
jq -n --arg tag "$version" --arg revision "$revision" \
  '{ref:"main",inputs:{tag:$tag,expected_revision:$revision}}' > "$scratch/dispatch.json"
run_command gh api --method POST \
  "repos/$GH_REPO/actions/workflows/release.yml/dispatches" --input "$scratch/dispatch.json" >/dev/null
printf 'Requested controller release %s at %s.\n' "$version" "$revision"
