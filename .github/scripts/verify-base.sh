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
      .Config.Labels["io.multica.controller-abi"] == "2" and
      .Config.User == "65532:65532" and
      all(.Config.Env[]; startswith("MULTICA_CLI_VERSION=") | not))
  ' >/dev/null
docker run --rm --network none --read-only "$image" version
docker run --rm --network none --read-only \
  --tmpfs /home/multica/agents:rw,mode=0700,uid=65532,gid=65532 \
  --tmpfs /tmp:rw,exec,mode=0700,uid=65532,gid=65532 \
  --entrypoint /bin/sh "$image" -ec '
    test "$(id -u)" = 65532
    ! command -v multica
    test ! -e /opt/multica/controller/multica
    test ! -e /artifact
    test -x /usr/local/go/bin/go
    go version
    printf "package main\nfunc main() {}\n" > /tmp/hello.go
    CGO_ENABLED=0 go run /tmp/hello.go
  '
