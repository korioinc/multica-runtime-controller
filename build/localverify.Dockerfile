# syntax=docker/dockerfile:1.7
ARG RUNTIME_IMAGE
FROM ${RUNTIME_IMAGE} AS prepared
USER 0:0
COPY --chmod=0555 verifyruntime /usr/local/bin/verifyruntime
RUN mkdir -p /opt/multica/fixture /git /evidence \
 && printf '#!/bin/sh\nexec /usr/local/bin/verifyruntime provider "$@"\n' > /opt/multica/fixture/pi \
 && chmod 0555 /opt/multica/fixture/pi \
 && chown 65532:65532 /git /evidence
ARG IMAGE_BUILD_ID
ARG SOURCE_SHA256
RUN seed=$(jq -r '.homeSeed // "/opt/multica/fixture/seed"' /opt/multica/runtime/image.json) \
 && mkdir -p "$seed/.fixture" \
 && printf 'image-seed' > "$seed/.fixture/operator.txt" \
 && jq --arg seed "$seed" --arg id "$IMAGE_BUILD_ID" --arg sha "$(sha256sum /opt/multica/fixture/pi | cut -d ' ' -f 1)" \
      '.imageBuildID=$id | .homeSeed=$seed | .providers={pi:{path:"/opt/multica/fixture/pi",version:"0.85.0",sha256:$sha}} | .env.FIXTURE_CACHE="${WORKSPACE}/.cache" | .env.LOCALVERIFY_DISPOSABLE_CLUSTER="true"' \
      /opt/multica/runtime/image.json > /opt/multica/runtime/image.next.json \
 && mv /opt/multica/runtime/image.next.json /opt/multica/runtime/image.json \
 && chmod 0444 /opt/multica/runtime/image.json \
 && rm /opt/multica/runtime/verification.json
LABEL io.multica.image-build-id="${IMAGE_BUILD_ID}" \
      io.multica.local-fixture-source="${SOURCE_SHA256}" \
      io.multica.verification-fixture="true"
USER 65532:65532

FROM prepared AS verify
USER 0:0
COPY --chmod=0555 verifyofficial /usr/local/bin/verifyofficial
RUN mkdir -p /out /workspace /run/multica /home/multica/agents \
 && chown 65532:65532 /out /workspace /run/multica /home/multica/agents \
 && chmod 0700 /out /workspace /run/multica /home/multica/agents
USER 65532:65532
RUN LOCALVERIFY_DISPOSABLE_CONTAINER=true /usr/local/bin/verifyofficial --image \
      --verification-output /out/verification.json --evidence /evidence/official

FROM prepared AS final
COPY --from=verify --chown=0:0 --chmod=0444 /out/verification.json /opt/multica/runtime/verification.json
USER 65532:65532
ENTRYPOINT ["/opt/multica/controller/runtime"]
CMD ["controller"]
