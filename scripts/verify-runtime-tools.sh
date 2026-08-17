#!/bin/sh

set -eu

failures=0

pass() {
  printf 'PASS %s\n' "$1"
}

fail() {
  printf 'FAIL %s\n' "$1" >&2
  failures=$((failures + 1))
}

require_command() {
  name="$1"
  if command -v "${name}" >/dev/null 2>&1; then
    pass "command:${name}"
  else
    fail "command:${name}"
  fi
}

require_runnable() {
  name="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    pass "runnable:${name}"
  else
    fail "runnable:${name}"
  fi
}

require_debian_package() {
  name="$1"
  if dpkg-query --show --showformat='${db:Status-Status}' "${name}" 2>/dev/null | grep -qx 'installed'; then
    pass "debian-package:${name}"
  else
    fail "debian-package:${name}"
  fi
}

require_php_extension() {
  name="$1"
  if php --modules | grep -qx "${name}"; then
    pass "php-extension:${name}"
  else
    fail "php-extension:${name}"
  fi
}

for command_name in \
  git git-lfs gh k9s kubectx kubens kubectl aws oci gcloud codebase-memory-mcp \
  php composer node npm python3 python go \
  rustc cargo rustup \
  gcc g++ make pkg-config cmake ninja jq yq rg fdfind fd patch rsync zip xz \
  file tree ps lsof ip dig nc shellcheck shfmt uv uvx corepack; do
  require_command "${command_name}"
done

require_runnable git git --version
require_runnable git-lfs git lfs version
require_runnable gh gh --version
require_runnable k9s k9s version --short
require_runnable kubectx kubectx --version
require_runnable kubens kubens --version
require_runnable kubectl kubectl version --client=true
require_runnable aws aws --version
require_runnable oci oci --version
require_runnable gcloud gcloud --version
require_runnable codebase-memory-mcp codebase-memory-mcp --version
require_runnable php php --version
require_runnable composer composer --version
require_runnable node node --version
require_runnable npm npm --version
require_runnable python3 python3 --version
require_runnable python python --version
require_runnable go go version
require_runnable rustc rustc --version
require_runnable cargo cargo --version
require_runnable rustup rustup --version
require_runnable gcc gcc --version
require_runnable g++ g++ --version
require_runnable make make --version
require_runnable pkg-config pkg-config --version
require_runnable cmake cmake --version
require_runnable ninja ninja --version
require_runnable jq jq --version
require_runnable yq yq --version
require_runnable ripgrep rg --version
require_runnable fdfind fdfind --version
require_runnable fd fd --version
require_runnable patch patch --version
require_runnable rsync rsync --version
require_runnable zip zip -v
require_runnable xz xz --version
require_runnable file file --version
require_runnable tree tree --version
require_runnable procps ps --version
require_runnable lsof lsof -v
require_runnable iproute2 ip -Version
require_runnable dnsutils dig -v
require_runnable netcat nc -h
require_runnable shellcheck shellcheck --version
require_runnable shfmt shfmt --version
require_runnable uv uv --version
require_runnable uvx uvx --version
require_runnable corepack corepack --version

for package_name in \
  zlib1g zlib1g-dev libsasl2-2 libsasl2-dev libsasl2-modules sasl2-bin \
  build-essential pkg-config cmake ninja-build jq ripgrep fd-find patch rsync \
  zip xz-utils file tree procps lsof iproute2 dnsutils netcat-openbsd shellcheck; do
  require_debian_package "${package_name}"
done

if php -r 'exit(PHP_MAJOR_VERSION === 8 && PHP_MINOR_VERSION === 5 ? 0 : 1);'; then
  pass 'php-version:8.5'
else
  fail 'php-version:8.5'
fi

for extension_name in mongodb redis zstd; do
  require_php_extension "${extension_name}"
done

if node -e 'process.exit(Number(process.versions.node.split(".")[0]) === 26 ? 0 : 1)'; then
  pass 'node-version:26'
else
  fail 'node-version:26'
fi

if [ "$(python --version 2>&1)" = "$(python3 --version 2>&1)" ]; then
  pass 'python-alias:python3'
else
  fail 'python-alias:python3'
fi

if [ "${failures}" -ne 0 ]; then
  printf 'RESULT failed checks=%s\n' "${failures}" >&2
  exit 1
fi

printf 'RESULT passed\n'
