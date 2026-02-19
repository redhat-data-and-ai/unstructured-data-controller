# Build the manager binary
FROM registry.access.redhat.com/ubi9/go-toolset:1.24 AS builder

USER root
WORKDIR /workspace

# Build arguments for multi-platform support
ARG TARGETOS=linux
ARG TARGETARCH=amd64

# Copy the Go Modules manifests (for dependency caching)
COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/

# Copy the go source
COPY cmd/ cmd/
COPY api/ api/
COPY pkg/ pkg/
COPY internal/ internal/

# Build
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -a -o manager cmd/main.go

# Use Red Hat UBI minimal as runtime image
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest

WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]