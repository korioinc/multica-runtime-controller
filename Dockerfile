# syntax=docker/dockerfile:1.7
ARG GO_VERSION
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS core-build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY src/go.mod src/go.sum ./
RUN go mod download
RUN apt-get update \
 && apt-get install --yes --no-install-recommends jq \
 && rm -rf /var/lib/apt/lists/*
COPY src/cmd/ ./cmd/
COPY src/internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags '-s -w' -o /out/runtime ./cmd/runtime
COPY scripts/core-contract.sh /scripts/core-contract.sh
ARG VERSION=dev
ARG COMMIT
RUN /scripts/core-contract.sh /out --platform "linux/${TARGETARCH}" \
      --version "${VERSION}" --commit "${COMMIT}" --go-version "$(go env GOVERSION)" \
 && mkdir -p /layout/home/multica/agents /layout/run/multica \
 && chmod 0755 /layout/home /layout/home/multica /layout/run \
 && chmod 0700 /layout/home/multica/agents /layout/run/multica \
 && chown 65532:65532 /layout/home/multica/agents /layout/run/multica

FROM golang:${GO_VERSION}-bookworm AS task-sdk
FROM debian:bookworm-slim AS runtime
RUN groupadd --gid 65532 multica \
 && useradd --uid 65532 --gid multica --no-create-home --no-log-init \
      --home-dir /home/multica/agents --shell /bin/bash multica
COPY --from=task-sdk /usr/local/go /usr/local/go
COPY --from=core-build /etc/ssl/certs /etc/ssl/certs
COPY --from=core-build /out /opt/multica/controller
COPY --from=core-build /layout/ /
ENV PATH="/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    HOME="/home/multica/agents" \
    GOPATH="/home/multica/agents/go" \
    GOCACHE="/home/multica/agents/.cache/go-build"
USER 65532:65532
WORKDIR /home/multica/agents
ARG VERSION=dev
ARG COMMIT
LABEL org.opencontainers.image.title="Multica Runtime Controller Base" \
      org.opencontainers.image.source="https://github.com/korioinc/multica-runtime-controller" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      io.multica.controller-abi="2"
ENTRYPOINT ["/opt/multica/controller/runtime"]
CMD ["version"]
