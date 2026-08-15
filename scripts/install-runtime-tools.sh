#!/bin/sh

set -eu

: "${GH_VERSION:?}"
: "${K9S_VERSION:?}"
: "${KUBECTX_VERSION:?}"
: "${KUBECTL_VERSION:?}"
: "${AWS_CLI_VERSION:?}"
: "${OCI_CLI_VERSION:?}"
: "${GCLOUD_CLI_VERSION:?}"
: "${UV_VERSION:?}"
: "${YQ_VERSION:?}"
: "${SHFMT_VERSION:?}"
: "${MONGODB_PHP_EXTENSION_VERSION:?}"
: "${PHPREDIS_VERSION:?}"
: "${ZSTD_PHP_EXTENSION_VERSION:?}"

case "$(dpkg --print-architecture)" in
  amd64)
    github_arch=amd64
    kubectx_arch=x86_64
    kubectl_arch=amd64
    aws_arch=x86_64
    gcloud_arch=x86_64
    uv_arch=x86_64
    ;;
  arm64)
    github_arch=arm64
    kubectx_arch=arm64
    kubectl_arch=arm64
    aws_arch=aarch64
    gcloud_arch=arm
    uv_arch=aarch64
    ;;
  *)
    printf 'unsupported runtime architecture: %s\n' "$(dpkg --print-architecture)" >&2
    exit 1
    ;;
esac

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT HUP INT TERM
cd "${workdir}"

download() {
  url="$1"
  output="$2"
  curl --fail --location --silent --show-error --output "${output}" "${url}"
}

apt-get update
# PHPIZE_DEPS is an intentional package list supplied by the official PHP image.
# shellcheck disable=SC2086
apt-get install --yes --no-install-recommends \
  ${PHPIZE_DEPS} \
  build-essential \
  ca-certificates \
  cmake \
  curl \
  dnsutils \
  fd-find \
  file \
  git \
  git-lfs \
  iproute2 \
  jq \
  less \
  lsof \
  libatomic1 \
  libsasl2-2 \
  libsasl2-dev \
  libsasl2-modules \
  libzstd-dev \
  netcat-openbsd \
  ninja-build \
  openssh-client \
  patch \
  pkg-config \
  procps \
  python-is-python3 \
  python3 \
  python3-pip \
  python3-venv \
  ripgrep \
  rsync \
  sasl2-bin \
  shellcheck \
  tini \
  tree \
  unzip \
  xz-utils \
  zip \
  zlib1g \
  zlib1g-dev
apt-mark manual libatomic1

pecl install "mongodb-${MONGODB_PHP_EXTENSION_VERSION}"
printf '\n\n\n\n\n\n' | pecl install "redis-${PHPREDIS_VERSION}"
pecl install "zstd-${ZSTD_PHP_EXTENSION_VERSION}"
docker-php-ext-enable mongodb redis zstd
pecl clear-cache

gh_archive="gh_${GH_VERSION}_linux_${github_arch}.tar.gz"
download \
  "https://github.com/cli/cli/releases/download/v${GH_VERSION}/${gh_archive}" \
  "${gh_archive}"
tar -xzf "${gh_archive}"
install -m 0755 "gh_${GH_VERSION}_linux_${github_arch}/bin/gh" /usr/local/bin/gh

k9s_archive="k9s_Linux_${github_arch}.tar.gz"
download \
  "https://github.com/derailed/k9s/releases/download/v${K9S_VERSION}/${k9s_archive}" \
  "${k9s_archive}"
tar -xzf "${k9s_archive}"
install -m 0755 k9s /usr/local/bin/k9s

for executable in kubectx kubens; do
  archive="${executable}_v${KUBECTX_VERSION}_linux_${kubectx_arch}.tar.gz"
  download \
    "https://github.com/ahmetb/kubectx/releases/download/v${KUBECTX_VERSION}/${archive}" \
    "${archive}"
  tar -xzf "${archive}"
  install -m 0755 "${executable}" "/usr/local/bin/${executable}"
done

download \
  "https://dl.k8s.io/release/v${KUBECTL_VERSION}/bin/linux/${kubectl_arch}/kubectl" \
  kubectl
install -m 0755 kubectl /usr/local/bin/kubectl

# Debian bookworm does not package these utilities at the required current
# versions, so install the upstream artifacts selected by version and architecture.
yq_binary="yq_linux_${github_arch}"
download \
  "https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/${yq_binary}" \
  "${yq_binary}"
install -m 0755 "${yq_binary}" /usr/local/bin/yq

shfmt_binary="shfmt_v${SHFMT_VERSION}_linux_${github_arch}"
download \
  "https://github.com/mvdan/sh/releases/download/v${SHFMT_VERSION}/${shfmt_binary}" \
  "${shfmt_binary}"
install -m 0755 "${shfmt_binary}" /usr/local/bin/shfmt

uv_archive="uv-${uv_arch}-unknown-linux-gnu.tar.gz"
download \
  "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/${uv_archive}" \
  "${uv_archive}"
tar -xzf "${uv_archive}"
install -m 0755 "uv-${uv_arch}-unknown-linux-gnu/uv" /usr/local/bin/uv
install -m 0755 "uv-${uv_arch}-unknown-linux-gnu/uvx" /usr/local/bin/uvx

aws_archive="awscli-exe-linux-${aws_arch}-${AWS_CLI_VERSION}.zip"
download \
  "https://awscli.amazonaws.com/${aws_archive}" \
  "${aws_archive}"
unzip -q "${aws_archive}" -d aws-installer
aws-installer/aws/install --bin-dir /usr/local/bin --install-dir /opt/aws-cli

python3 -m venv /opt/oci-cli
/opt/oci-cli/bin/pip install --no-cache-dir --disable-pip-version-check \
  "oci-cli==${OCI_CLI_VERSION}"
ln -s /opt/oci-cli/bin/oci /usr/local/bin/oci

gcloud_archive="google-cloud-cli-${GCLOUD_CLI_VERSION}-linux-${gcloud_arch}.tar.gz"
download \
  "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${gcloud_archive}" \
  "${gcloud_archive}"
tar -xzf "${gcloud_archive}" -C /opt
ln -s /opt/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud
ln -s /opt/google-cloud-sdk/bin/gsutil /usr/local/bin/gsutil
ln -s /opt/google-cloud-sdk/bin/bq /usr/local/bin/bq

git lfs install --system
ln -s /usr/bin/fdfind /usr/local/bin/fd

# build-essential keeps the compiler, make, libc headers, and dpkg-dev. Remove
# only PHP extension generators that are no longer needed at runtime.
apt-get purge --yes --auto-remove autoconf re2c
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/pear
