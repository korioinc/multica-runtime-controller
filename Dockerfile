# syntax=docker/dockerfile:1.7

ARG NODE_VERSION
ARG PHP_VERSION
ARG GO_VERSION
ARG COMPOSER_VERSION

FROM node:${NODE_VERSION}-bookworm-slim AS node-runtime

FROM composer:${COMPOSER_VERSION} AS composer-runtime

FROM golang:${GO_VERSION}-bookworm AS go-runtime

FROM go-runtime AS controller-build
WORKDIR /src
COPY src/go.mod src/go.sum ./
RUN go mod download
COPY src/cmd/ ./cmd/
COPY src/internal/ ./internal/
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/multica-runtime ./cmd/runtime \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/multica-provider-shim ./cmd/provider-shim

FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS cli-download-base
RUN apt-get update \
 && apt-get install --yes --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

FROM php:${PHP_VERSION}-cli-bookworm AS runtime-base
ARG COREPACK_VERSION
ARG MONGODB_PHP_EXTENSION_VERSION
ARG PHPREDIS_VERSION
ARG ZSTD_PHP_EXTENSION_VERSION
ARG GH_VERSION
ARG K9S_VERSION
ARG KUBECTX_VERSION
ARG KUBECTL_VERSION
ARG AWS_CLI_VERSION
ARG OCI_CLI_VERSION
ARG GCLOUD_CLI_VERSION
ARG UV_VERSION
ARG YQ_VERSION
ARG SHFMT_VERSION
ENV PATH="/usr/local/go/bin:/opt/oci-cli/bin:/opt/google-cloud-sdk/bin:${PATH}" \
    CLOUDSDK_PYTHON=/usr/bin/python3
COPY --from=node-runtime /usr/local/ /usr/local/
COPY --from=go-runtime /usr/local/go /usr/local/go
COPY --from=composer-runtime /usr/bin/composer /usr/local/bin/composer
COPY scripts/install-runtime-tools.sh /usr/local/sbin/install-runtime-tools
RUN /usr/local/sbin/install-runtime-tools \
 && rm /usr/local/sbin/install-runtime-tools \
 && groupadd --gid 65532 multica \
 && useradd --uid 65532 --gid 65532 --home-dir /home/multica --create-home --shell /usr/sbin/nologin multica \
 && npm install --global --ignore-scripts --no-audit --no-fund "corepack@${COREPACK_VERSION}" \
 && corepack --version | grep --fixed-strings "${COREPACK_VERSION}" \
 && npm cache clean --force
RUN install -d /opt/codebase-memory-mcp-install/home \
 && curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.sh \
      | HOME=/opt/codebase-memory-mcp-install/home \
        bash -s -- --dir=/opt/codebase-memory-mcp-install --skip-config \
 && install -m 0555 /opt/codebase-memory-mcp-install/codebase-memory-mcp \
      /usr/local/bin/codebase-memory-mcp \
 && rm -rf /opt/codebase-memory-mcp-install \
 && install -d -o 65532 -g 65532 -m 0700 /home/multica/.cache/codebase-memory-mcp \
 && runuser --user multica -- env \
      CBM_CACHE_DIR=/home/multica/.cache/codebase-memory-mcp \
      codebase-memory-mcp config set auto_index true \
 && codebase-memory-mcp --version
COPY scripts/runtime-entrypoint.sh /usr/local/bin/runtime-entrypoint
COPY scripts/verify-runtime-tools.sh /usr/local/bin/verify-runtime-tools
RUN chmod 0555 /usr/local/bin/runtime-entrypoint /usr/local/bin/verify-runtime-tools

# These provider versions change most often, so their layer starts after the stable runtime base.
FROM runtime-base AS provider-runtime
ARG CODEX_VERSION
ARG COPILOT_VERSION
ARG PI_VERSION
RUN npm install --global --ignore-scripts --no-audit --no-fund \
      "@openai/codex@${CODEX_VERSION}" \
      "@github/copilot@${COPILOT_VERSION}" \
      "@earendil-works/pi-coding-agent@${PI_VERSION}" \
 && npm cache clean --force \
 && codex --version | grep --fixed-strings "${CODEX_VERSION}" \
 && copilot --version | grep --fixed-strings "${COPILOT_VERSION}" \
 && pi --version | grep --fixed-strings "${PI_VERSION}" \
 && mkdir -p /opt/multica/providers/bin \
 && ln -s "$(readlink -f /usr/local/bin/codex)" /opt/multica/providers/bin/codex \
 && ln -s "$(readlink -f /usr/local/bin/copilot)" /opt/multica/providers/bin/copilot \
 && ln -s "$(readlink -f /usr/local/bin/pi)" /opt/multica/providers/bin/pi \
 && rm /usr/local/bin/codex /usr/local/bin/copilot /usr/local/bin/pi

FROM cli-download-base AS multica-download
ARG TARGETARCH
ARG MULTICA_CLI_VERSION
RUN case "${TARGETARCH}" in \
      amd64|arm64) ;; \
      *) echo "unsupported Multica CLI architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && curl --fail --location --silent --show-error \
      --output /tmp/multica.tar.gz \
      "https://github.com/multica-ai/multica/releases/download/v${MULTICA_CLI_VERSION}/multica-cli-${MULTICA_CLI_VERSION}-linux-${TARGETARCH}.tar.gz" \
 && mkdir -p /out \
 && tar -xzf /tmp/multica.tar.gz -C /out multica \
 && chmod 0555 /out/multica \
 && /out/multica version | grep --fixed-strings "multica ${MULTICA_CLI_VERSION}"

FROM cli-download-base AS antigravity-download
ARG TARGETARCH
ARG ANTIGRAVITY_VERSION
RUN case "${TARGETARCH}" in \
      amd64) archive_arch=x64 ;; \
      arm64) archive_arch=arm64 ;; \
      *) echo "unsupported Antigravity architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && curl --fail --location --silent --show-error \
      --output /tmp/antigravity.tar.gz \
      "https://github.com/google-antigravity/antigravity-cli/releases/download/${ANTIGRAVITY_VERSION}/agy_cli_linux_${archive_arch}.tar.gz" \
 && mkdir -p /out \
 && tar -xzf /tmp/antigravity.tar.gz -C /out \
 && mv /out/antigravity /out/agy \
 && chmod 0555 /out/agy

FROM provider-runtime AS runtime
COPY --from=controller-build /out/multica-runtime /usr/local/bin/multica-runtime
COPY --from=controller-build /out/multica-provider-shim /usr/local/bin/multica-provider-shim
COPY --from=multica-download /out/multica /usr/local/bin/multica
COPY --from=antigravity-download /out/agy /opt/multica/providers/bin/agy
RUN chmod 0555 \
      /usr/local/bin/multica-runtime \
      /usr/local/bin/multica-provider-shim \
      /usr/local/bin/multica \
      /opt/multica/providers/bin/agy \
 && ln /usr/local/bin/multica-provider-shim /usr/local/bin/agy \
 && ln /usr/local/bin/multica-provider-shim /usr/local/bin/codex \
 && ln /usr/local/bin/multica-provider-shim /usr/local/bin/copilot \
 && ln /usr/local/bin/multica-provider-shim /usr/local/bin/pi \
 && multica version >/dev/null
USER 65532:65532
RUN /usr/local/bin/verify-runtime-tools
WORKDIR /workspace

# Keep version-only metadata at the end so metadata changes do not invalidate runtime layers.
ARG VERSION=dev
ARG COMMIT=unknown
ARG NODE_VERSION
ARG PHP_VERSION
ARG GO_VERSION
ARG COMPOSER_VERSION
ARG MONGODB_PHP_EXTENSION_VERSION
ARG PHPREDIS_VERSION
ARG ZSTD_PHP_EXTENSION_VERSION
ARG GH_VERSION
ARG K9S_VERSION
ARG KUBECTX_VERSION
ARG KUBECTL_VERSION
ARG AWS_CLI_VERSION
ARG OCI_CLI_VERSION
ARG GCLOUD_CLI_VERSION
ARG UV_VERSION
ARG YQ_VERSION
ARG SHFMT_VERSION
ARG COREPACK_VERSION
ARG MULTICA_CLI_VERSION
ARG CODEX_VERSION
ARG COPILOT_VERSION
ARG PI_VERSION
ARG ANTIGRAVITY_VERSION
LABEL org.opencontainers.image.title="Multica Runtime Controller" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      io.multica.cli-version="${MULTICA_CLI_VERSION}" \
      io.multica.execution-modes="official-daemon,kubernetes-provider-intercept" \
      io.multica.providers="antigravity@${ANTIGRAVITY_VERSION},codex@${CODEX_VERSION},copilot@${COPILOT_VERSION},pi@${PI_VERSION}" \
      io.multica.toolchain="aws-cli@${AWS_CLI_VERSION},composer@${COMPOSER_VERSION},corepack@${COREPACK_VERSION},gcloud@${GCLOUD_CLI_VERSION},gh@${GH_VERSION},go@${GO_VERSION},k9s@${K9S_VERSION},kubectx@${KUBECTX_VERSION},kubectl@${KUBECTL_VERSION},node@${NODE_VERSION},oci-cli@${OCI_CLI_VERSION},php@${PHP_VERSION},shfmt@${SHFMT_VERSION},uv@${UV_VERSION},yq@${YQ_VERSION}" \
      io.multica.php-extensions="mongodb@${MONGODB_PHP_EXTENSION_VERSION},redis@${PHPREDIS_VERSION},zstd@${ZSTD_PHP_EXTENSION_VERSION}"
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/runtime-entrypoint"]
