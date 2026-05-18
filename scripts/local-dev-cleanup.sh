#!/usr/bin/env bash
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CACHE_DIR="${CACHE_DIR:-tmp/cache}"
if [[ "${CACHE_DIR}" != /* ]]; then
    CACHE_DIR="${REPO_ROOT}/${CACHE_DIR}"
fi

# Configuration
LOCAL_KIND_CLUSTER="${LOCAL_KIND_CLUSTER:-unstructured-data-controller-local}"

echo "Cleaning up local development environment..."

# Stop Docling if it was started by setup script
if [ -f /tmp/docling.pid ]; then
    DOCLING_PID=$(cat /tmp/docling.pid)
    if ps -p "$DOCLING_PID" > /dev/null 2>&1; then
        kill "$DOCLING_PID" 2>/dev/null || true
    fi
    rm -f /tmp/docling.pid /tmp/docling.log
fi

# Remove local cache created by setup / controller
if [ -d "${CACHE_DIR}" ]; then
    rm -rf "${CACHE_DIR}"
    echo "✓ Removed cache directory ${CACHE_DIR}"
fi

# Delete Kind cluster
kind delete cluster --name "${LOCAL_KIND_CLUSTER}" 2>/dev/null || true
echo "✓ Local development environment removed"
