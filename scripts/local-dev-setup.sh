#!/usr/bin/env bash
# Usage: make local-dev-setup
#
# Before running, edit:
#   config/samples/unstructured-secret.yaml       — credentials
#   config/samples/operator_v1alpha1_controllerconfig.yaml — operator config
set -e

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

LOCAL_KIND_CLUSTER="${LOCAL_KIND_CLUSTER:-unstructured-data-controller-local}"
LOCAL_NAMESPACE="${LOCAL_NAMESPACE:-unstructured-controller-namespace}"

CONTROLLER_CONFIG_YAML="${REPO_ROOT}/config/samples/operator_v1alpha1_controllerconfig.yaml"
SOURCE_CRAWLER_YAML="${REPO_ROOT}/config/samples/operator_v1alpha1_sourcecrawler.yaml"
DEST_SYNCER_YAML="${REPO_ROOT}/config/samples/operator_v1alpha1_destinationsyncer.yaml"
PIPELINE_YAML="${REPO_ROOT}/config/samples/operator_v1alpha1_unstructureddatapipeline.yaml"

echo "Setting up local development environment..."

# 1. Create Kind cluster
echo ""
echo "1. Creating Kind cluster '${LOCAL_KIND_CLUSTER}'..."
if kind get clusters 2>/dev/null | grep -q "^${LOCAL_KIND_CLUSTER}$"; then
    echo "✓ Kind cluster '${LOCAL_KIND_CLUSTER}' already exists"
else
    kind create cluster --name "${LOCAL_KIND_CLUSTER}"
    echo "✓ Kind cluster '${LOCAL_KIND_CLUSTER}' created"
fi

# 2. Create namespace
echo ""
echo "2. Creating namespace '${LOCAL_NAMESPACE}'..."
if kubectl get namespace "${LOCAL_NAMESPACE}" &>/dev/null; then
    echo "✓ Namespace '${LOCAL_NAMESPACE}' already exists"
else
    kubectl create namespace "${LOCAL_NAMESPACE}"
    echo "✓ Namespace '${LOCAL_NAMESPACE}' created"
fi

# 3. Create cache directory (from ControllerConfig sample)
echo ""
echo "3. Creating local cache directory..."
mkdir -p "${REPO_ROOT}/$(
    yq -r '.spec.cacheDirectory' "${CONTROLLER_CONFIG_YAML}" \
    | sed 's|/$||'
)"
echo "✓ Cache directory created"

# 4. Check/Start Docling
echo ""
echo "4. Checking Docling service..."
if command -v docling-serve &>/dev/null; then
    if curl -s http://localhost:5001/health &>/dev/null; then
        echo "✓ Docling is already running"
    else
        echo "Starting Docling in the background..."
        nohup docling-serve run --enable-ui > /tmp/docling.log 2>&1 &
        echo $! > /tmp/docling.pid
        sleep 2
        if curl -s http://localhost:5001/health &>/dev/null; then
            echo "✓ Docling started (log: /tmp/docling.log)"
        else
            echo "Warning: Docling may still be starting. Check /tmp/docling.log"
        fi
    fi
else
    echo "⚠ Docling is not installed. Start it manually or install docling-serve."
fi

# 5. Check/Start Ollama (for embedding models)
echo ""
echo "5. Checking Ollama service..."
if command -v ollama &>/dev/null; then
    if curl -s http://localhost:11434/api/tags &>/dev/null; then
        echo "✓ Ollama is already running"
    else
        echo "Starting Ollama in the background..."
        nohup ollama serve > /tmp/ollama.log 2>&1 &
        echo $! > /tmp/ollama.pid
        sleep 2
        if curl -s http://localhost:11434/api/tags &>/dev/null; then
            echo "✓ Ollama started (log: /tmp/ollama.log)"
        else
            echo "Warning: Ollama may still be starting. Check /tmp/ollama.log"
        fi
    fi
    echo "Pulling embedding model..."
    ollama pull nomic-embed-text:latest 2>/dev/null || true
    ollama cp nomic-embed-text:latest nomic-ai/nomic-embed-text-v1.5 2>/dev/null || true
    echo "✓ Embedding model ready"
else
    echo "⚠ Ollama is not installed. Install from https://ollama.com/ for embeddings."
fi

# 6. Start LocalStack in Docker on localhost:4566
echo ""
echo "6. Starting LocalStack..."
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
LOCALSTACK_CONTAINER="${LOCALSTACK_CONTAINER:-localstack-dev}"
LOCALSTACK_IMAGE="${LOCALSTACK_IMAGE:-localstack/localstack:4.14.0}"
LOCALSTACK_ENDPOINT="http://127.0.0.1:4566"

if curl -sf "${LOCALSTACK_ENDPOINT}/_localstack/health" &>/dev/null; then
    echo "✓ LocalStack already running at ${LOCALSTACK_ENDPOINT}"
elif docker ps -a --format '{{.Names}}' | grep -qx "${LOCALSTACK_CONTAINER}"; then
    echo "Starting existing container '${LOCALSTACK_CONTAINER}'..."
    docker start "${LOCALSTACK_CONTAINER}" >/dev/null
else
    echo "Creating container '${LOCALSTACK_CONTAINER}' from ${LOCALSTACK_IMAGE}..."
    docker run -d \
        --name "${LOCALSTACK_CONTAINER}" \
        -p 127.0.0.1:4566:4566 \
        "${LOCALSTACK_IMAGE}" >/dev/null
fi

echo "Waiting for LocalStack at ${LOCALSTACK_ENDPOINT}..."
for _ in $(seq 1 30); do
    if curl -sf "${LOCALSTACK_ENDPOINT}/_localstack/health" &>/dev/null; then
        echo "✓ LocalStack is healthy"
        break
    fi
    sleep 1
done
if ! curl -sf "${LOCALSTACK_ENDPOINT}/_localstack/health" &>/dev/null; then
    echo "Error: LocalStack did not become healthy in time" >&2
    docker logs "${LOCALSTACK_CONTAINER}" --tail=40 >&2 || true
    exit 1
fi

# 7. Create S3 buckets
echo ""
echo "7. Creating S3 buckets..."
INGESTION_BUCKET="$(yq -r '.spec.sourceCrawlerConfig.s3Config.bucket' "${SOURCE_CRAWLER_YAML}")"
STORAGE_BUCKET="$(yq -r '.spec.dataStorageBucket' "${CONTROLLER_CONFIG_YAML}")"
OUTPUT_BUCKET="$(yq -r '.spec.destinationSyncerConfig.s3DestinationConfig.bucket' "${DEST_SYNCER_YAML}")"

for bucket in "${INGESTION_BUCKET}" "${STORAGE_BUCKET}" "${OUTPUT_BUCKET}"; do
    if awslocal s3 ls "s3://${bucket}" &>/dev/null; then
        echo "✓ Bucket '${bucket}' already exists"
    else
        awslocal s3 mb "s3://${bucket}"
        echo "✓ Bucket '${bucket}' created"
    fi
done

# 8. Install CRDs
echo ""
echo "8. Installing CRDs..."
cd "${REPO_ROOT}" && make install
echo "✓ CRDs installed"

# 9. Apply secrets (operator-secret + pipeline-secret)
echo ""
echo "9. Creating secrets..."
kubectl apply -f "${REPO_ROOT}/config/samples/unstructured-secret.yaml" \
    -n "${LOCAL_NAMESPACE}"
echo "✓ Secrets applied"

# 10. Apply ControllerConfig
echo ""
echo "10. Creating ControllerConfig..."
kubectl apply -f "${CONTROLLER_CONFIG_YAML}" -n "${LOCAL_NAMESPACE}"
echo "✓ ControllerConfig applied"

# 11. Apply UnstructuredDataPipeline (creates all stage CRs automatically)
echo ""
echo "11. Creating UnstructuredDataPipeline..."
kubectl apply -f "${PIPELINE_YAML}" -n "${LOCAL_NAMESPACE}"
echo "✓ UnstructuredDataPipeline applied"

echo ""
echo "✓ Local development environment setup complete"
echo ""
echo "Starting controller (Ctrl+C to stop)..."
cd "${REPO_ROOT}" && make run
