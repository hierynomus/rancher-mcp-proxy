# syntax=docker/dockerfile:1.4
# Runtime stage - binaries are pre-compiled via cross and passed via build context
FROM registry.suse.com/bci/bci-minimal:15.7

ARG TARGETARCH
ARG BINARY=rancher-mcp-proxy

COPY --from=binaries linux/${TARGETARCH}/rancher-mcp-proxy /usr/local/bin/rancher-mcp-proxy

USER 1001

ENTRYPOINT ["/usr/local/bin/rancher-mcp-proxy"]
