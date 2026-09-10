#!/usr/bin/env bash
# Shared literal VERSION/Go pin parsing. This file never evaluates env contents.

version_error() {
  printf 'controller version error: %s\n' "$*" >&2
  return 1
}

version_stable() {
  [[ $1 =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    version_error 'expected a stable semantic version'
}

version_read() {
  local value
  value=$(cat -- "$1") || return 1
  value=${value%$'\r'}
  version_stable "$value" || return 1
  printf '%s\n' "$value"
}

version_go_pin() {
  local line pin='' number=0
  while IFS= read -r line || [[ -n $line ]]; do
    number=$((number + 1))
    line=${line%$'\r'}
    [[ $line =~ ^[[:space:]]*$ || $line == \#* ]] && continue
    if [[ ! $line =~ ^GO_VERSION=([0-9]+\.[0-9]+\.[0-9]+)$ || -n $pin ]]; then
      version_error "invalid controller build assignment on line $number"
      return 1
    fi
    pin=${BASH_REMATCH[1]}
  done <"$1/build/runtime-versions.env"
  [[ -n $pin ]] || { version_error 'missing Go SDK pin'; return 1; }
  version_stable "$pin" || return 1
  printf '%s\n' "$pin"
}

version_build_args() {
  local root=$1 revision=$2 version=$3 actual actual_version pin
  actual=$(git -C "$root" rev-parse HEAD) || return 1
  revision=${revision:-$actual}
  [[ $revision =~ ^[0-9a-f]{40}$ && $revision == "$actual" ]] ||
    { version_error 'COMMIT must identify the checked-out full source revision'; return 1; }
  if [[ $version != dev && $version != ci && $version != develop ]]; then
    actual_version=$(version_read "$root/VERSION") || return 1
    version=${version:-$actual_version}
    version_stable "$version" || return 1
    [[ $version == "$actual_version" ]] ||
      { version_error 'release version differs from the checked-out VERSION'; return 1; }
  fi
  pin=$(version_go_pin "$root") || return 1
  jq -cnS --arg pin "$pin" --arg version "$version" --arg revision "$revision" \
    '{GO_VERSION:$pin, VERSION:$version, COMMIT:$revision}'
}

version_next_patch() {
  local major minor patch digit index carry=1 result=''
  version_stable "$1" || return 1
  IFS=. read -r major minor patch <<<"$1"
  # Work one digit at a time; release versions need not fit shell integers.
  for ((index=${#patch}-1; index >= 0; index--)); do
    digit=${patch:index:1}
    if [[ $carry == 1 ]]; then
      if [[ $digit == 9 ]]; then digit=0; else digit=$((digit + 1)); carry=0; fi
    fi
    result=$digit$result
  done
  [[ $carry == 0 ]] || result=1$result
  printf '%s.%s.%s\n' "$major" "$minor" "$result"
}

version_compare() {
  local left right index
  local -a left_parts right_parts
  version_stable "$1" && version_stable "$2" || return 1
  IFS=. read -r -a left_parts <<<"$1"
  IFS=. read -r -a right_parts <<<"$2"
  for index in 0 1 2; do
    left=${left_parts[index]} right=${right_parts[index]}
    if [[ ${#left} -gt ${#right} || (${#left} -eq ${#right} && $left > $right) ]]; then
      printf '1\n'; return
    fi
    if [[ ${#left} -lt ${#right} || (${#left} -eq ${#right} && $left < $right) ]]; then
      printf '%s\n' -1; return
    fi
  done
  printf '0\n'
}

version_github_output() {
  [[ -n ${GITHUB_OUTPUT:-} ]] || { version_error 'GITHUB_OUTPUT is required'; return 1; }
  jq -er 'to_entries[] | if (.value | tostring | test("[\r\n]"))
    then error("multiline GitHub output is not permitted") else "\(.key)=\(.value)" end' <<<"$1" >>"$GITHUB_OUTPUT"
}
