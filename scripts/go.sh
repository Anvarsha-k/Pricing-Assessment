#!/bin/sh
# Run the Go toolchain in a container, for machines without a local Go install.
#   scripts/go.sh test ./...
#   scripts/go.sh mod tidy
set -e
export MSYS_NO_PATHCONV=1 # Git Bash on Windows: keep /src as a container path
cd "$(dirname "$0")/.."
exec docker run --rm -i \
  -v "$(pwd -W 2>/dev/null || pwd)":/src -w /src \
  -v pricing-gomod:/go/pkg/mod -v pricing-gocache:/root/.cache/go-build \
  -e CGO_ENABLED="${CGO_ENABLED:-0}" -e GOFLAGS="${GOFLAGS:-}" \
  golang:1.26-alpine go "$@"
