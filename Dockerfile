# syntax=docker/dockerfile:1.7
ARG GO_VERSION
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS core-build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY src/go.mod src/go.sum ./
RUN go mod download
COPY src/cmd/ ./cmd/
COPY src/internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags '-s -w' -o /out/runtime ./cmd/runtime

FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS artifact
RUN apt-get update \
 && apt-get install --yes --no-install-recommends ca-certificates curl python3 \
 && rm -rf /var/lib/apt/lists/*
ARG TARGETARCH
ARG MULTICA_CLI_VERSION
RUN case "${TARGETARCH}" in amd64|arm64) ;; *) exit 1 ;; esac \
 && archive="multica-cli-${MULTICA_CLI_VERSION}-linux-${TARGETARCH}.tar.gz" \
 && release_url="https://github.com/multica-ai/multica/releases/download/v${MULTICA_CLI_VERSION}" \
 && curl --fail --location --silent --show-error --output "/tmp/${archive}" "${release_url}/${archive}" \
 && curl --fail --location --silent --show-error --output /tmp/checksums.txt "${release_url}/checksums.txt" \
 && cd /tmp \
 && grep --fixed-strings "  ${archive}" checksums.txt | sha256sum --check --strict \
 && mkdir -p /artifact \
 && tar -xzf "/tmp/${archive}" -C /artifact multica \
 && chmod 0555 /artifact/multica
COPY --from=core-build /out/runtime /artifact/runtime
COPY scripts/core_contract.py /tmp/core_contract.py
ARG VERSION=dev
ARG COMMIT=unknown
RUN python3 /tmp/core_contract.py /artifact --platform "linux/${TARGETARCH}" \
      --version "${VERSION}" --commit "${COMMIT}" --official-version "${MULTICA_CLI_VERSION}"

FROM scratch AS runtime
COPY --from=artifact --chown=65532:65532 /artifact /artifact
USER 65532:65532
ARG VERSION=dev
ARG COMMIT=unknown
ARG MULTICA_CLI_VERSION
LABEL org.opencontainers.image.title="Multica Runtime Core" \
      org.opencontainers.image.source="https://github.com/korioinc/multica-runtime-controller" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      io.multica.core-contract="1" \
      io.multica.cli-version="${MULTICA_CLI_VERSION}"
ENTRYPOINT ["/artifact/runtime"]
CMD ["materialize"]
