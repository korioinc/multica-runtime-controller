#!/usr/bin/env bash
# Native execution against the actual candidate; never downloads a test daemon.
set -euo pipefail
image=$1
platform=$2
version=$3
revision=$4
case "$(docker info --format '{{.Architecture}}')" in
  x86_64|amd64) native_platform=linux/amd64 ;;
  aarch64|arm64) native_platform=linux/arm64 ;;
  *) echo 'Unsupported native Docker engine architecture.' >&2; exit 1 ;;
esac
[ "$platform" = "$native_platform" ]
docker image inspect "$image" | jq -e \
  --arg platform "$platform" --arg version "$version" --arg revision "$revision" '
    length == 1 and (.[0] |
      .Os + "/" + .Architecture == $platform and
      .Config.Labels["org.opencontainers.image.version"] == $version and
      .Config.Labels["org.opencontainers.image.revision"] == $revision and
      .Config.User == "65532:65532")
  ' >/dev/null
docker run --rm -i --network none --read-only --user 65532:65532 \
  --cap-drop ALL --security-opt no-new-privileges --entrypoint /bin/sh "$image" -eu <<'VERIFY'
packages=$(dpkg-query -W -f='${binary:Package}\t${db:Status-Status}\n')
if printf '%s\n' "$packages" | awk '$1 ~ /^tini(-static)?(:[^[:space:]]+)?$/ && $2 != "not-installed" && $2 != "config-files" { found=1 } END { exit !found }' ||
   command -v tini >/dev/null 2>&1 || command -v tini-static >/dev/null 2>&1; then
  echo 'Controller base still contains Tini; rebuild the matching controller base.' >&2
  exit 1
fi
for candidate in /usr/bin/tini /usr/bin/tini-static /bin/tini /bin/tini-static; do
  if [ -e "$candidate" ] || [ -L "$candidate" ]; then
    echo 'Controller base still contains a Tini executable or link.' >&2
    exit 1
  fi
done
exec /opt/multica/controller/runtime version
VERIFY
