ARG ENVIRONMENT_IMAGE
FROM ${ENVIRONMENT_IMAGE}
COPY verifyofficial verifyruntime /usr/local/bin/
RUN mkdir -p /git /evidence /home/multica/agents \
 && chown -R 65532:65532 /git /evidence /home/multica/agents
USER 65532:65532
ENV HOME=/home/multica/agents
ENTRYPOINT ["/usr/local/bin/verifyruntime"]
