#!/usr/bin/env bash
# Cleanup for local-dev-setup.sh
# Usage: make local-dev-cleanup
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_KIND_CLUSTER="${LOCAL_KIND_CLUSTER:-unstructured-data-controller-local}"

echo "Cleaning up local development environment..."

if [ -f /tmp/docling.pid ]; then
    DOCLING_PID=$(cat /tmp/docling.pid)
    if ps -p "${DOCLING_PID}" > /dev/null 2>&1; then
        kill "${DOCLING_PID}" 2>/dev/null || true
    fi
    rm -f /tmp/docling.pid /tmp/docling.log
    echo "✓ Stopped Docling"
fi

if [ -f /tmp/ollama.pid ]; then
    OLLAMA_PID=$(cat /tmp/ollama.pid)
    if ps -p "${OLLAMA_PID}" > /dev/null 2>&1; then
        kill "${OLLAMA_PID}" 2>/dev/null || true
    fi
    rm -f /tmp/ollama.pid /tmp/ollama.log
    echo "✓ Stopped Ollama"
fi

LOCALSTACK_CONTAINER="${LOCALSTACK_CONTAINER:-localstack-dev}"

if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "${LOCALSTACK_CONTAINER}"; then
    docker rm -f "${LOCALSTACK_CONTAINER}" >/dev/null
    echo "✓ Stopped and removed LocalStack container '${LOCALSTACK_CONTAINER}'"
else
    echo "✓ No LocalStack container '${LOCALSTACK_CONTAINER}' found"
fi

CACHE_DIR="${REPO_ROOT}/$(
    yq -r '.spec.cacheDirectory' \
        "${REPO_ROOT}/config/samples/operator_v1alpha1_controllerconfig.yaml" 2>/dev/null \
    | sed 's|/$||' || echo "tmp/cache"
)"
if [ -d "${CACHE_DIR}" ]; then
    rm -rf "${CACHE_DIR}"
    echo "✓ Removed cache directory ${CACHE_DIR}"
fi

kind delete cluster --name "${LOCAL_KIND_CLUSTER}" 2>/dev/null || true
echo "✓ Local development environment removed"
