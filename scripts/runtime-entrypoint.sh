#!/bin/sh

set -eu

: "${HOME:=/home/multica}"
export HOME

if [ "${HOME}" = "/home/multica/agents" ]; then
  CBM_CACHE_DIR="${CBM_CACHE_DIR:-${HOME}/.cache/codebase-memory-mcp}"
  export CBM_CACHE_DIR
  chmod 0700 "${HOME}"
  mkdir -p "${HOME}/.cache" "${CBM_CACHE_DIR}"
  chmod 0700 "${HOME}/.cache" "${CBM_CACHE_DIR}"
  codebase-memory-mcp config set auto_index true >/dev/null
fi

exec /usr/local/bin/multica-runtime "$@"
