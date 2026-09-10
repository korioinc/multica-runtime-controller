#!/usr/bin/env bash
# Shared GitHub source identity. Callers provide scratch, GH_REPO and fail().
: "${scratch:?}"

run_command() {
  "$@" >"$scratch/stdout" 2>"$scratch/stderr" || fail "$1 $2 failed; inspect remote state before retrying"
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

github_tag_revision() {
  local requested_version=$1 require_tag=${2:-false} tag target sha nested visited=' '
  tag=$(github "git/ref/tags/$requested_version") || return 1
  [[ $require_tag != true || $tag != null ]] || fail 'pushed release tag is missing'
  if [[ $tag == null ]]; then return; fi
  target=$(jq -c '.object' <<<"$tag") || fail 'invalid release tag'
  while [[ $(jq -r '.type' <<<"$target") == tag ]]; do
    sha=$(jq -r '.sha' <<<"$target") || fail 'invalid annotated release tag'
    [[ $sha =~ ^[0-9a-f]{40}$ && $visited != *" $sha "* ]] || fail 'invalid annotated release tag'
    visited="$visited$sha "
    nested=$(github "git/tags/$sha") || return 1
    target=$(jq -c '.object' <<<"$nested") || fail 'invalid annotated release tag'
  done
  jq -e '.type == "commit" and (.sha | type == "string" and test("^[0-9a-f]{40}$"))' <<<"$target" >/dev/null ||
    fail 'invalid release tag target'
  jq -r '.sha' <<<"$target"
}

github_require_main_revision() {
  local revision=$1 main main_revision comparison
  main=$(github git/ref/heads/main) || return 1
  main_revision=$(jq -r '.object.sha // ""' <<<"$main") || fail 'invalid main revision'
  [[ $main_revision =~ ^[0-9a-f]{40}$ ]] || fail 'main must identify a commit'
  comparison=$(github "compare/$revision...$main_revision") || return 1
  jq -e '.status == "ahead" or .status == "identical"' <<<"$comparison" >/dev/null ||
    fail 'release revision is not in main history'
}
