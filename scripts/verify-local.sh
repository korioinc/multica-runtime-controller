#!/usr/bin/env bash
# Cluster access is confined to the disposable Docker/K3s instance below.
set -euo pipefail
repo_root=$(cd -- "$(dirname -- "$0")/.." && pwd)
exec python3 "$repo_root/scripts/localverify/run.py" "$@"
