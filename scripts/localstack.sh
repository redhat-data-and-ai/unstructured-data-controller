#!/usr/bin/env bash
# LocalStack helper for local development.
# Usage:
#   ./scripts/localstack.sh start   # start container (no-op if healthy)
#   ./scripts/localstack.sh stop    # stop and remove container
#   ./scripts/localstack.sh setup   # start + create S3 buckets
#   ./scripts/localstack.sh status  # show health and resource summary
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROLLER_CONFIG_YAML="${REPO_ROOT}/config/samples/operator_v1alpha1_controllerconfig.yaml"
SOURCE_CRAWLER_YAML="${REPO_ROOT}/config/samples/operator_v1alpha1_sourcecrawler.yaml"
DEST_SYNCER_YAML="${REPO_ROOT}/config/samples/operator_v1alpha1_destinationsyncer.yaml"

CONTAINER="${LOCALSTACK_CONTAINER:-localstack-dev}"
IMAGE="${LOCALSTACK_IMAGE:-localstack/localstack:4.14.0}"
ENDPOINT="${LOCALSTACK_ENDPOINT:-http://127.0.0.1:4566}"
AWS_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

INGESTION_BUCKET="$(yq -r '.spec.sourceCrawlerConfig.s3Config.bucket' "${SOURCE_CRAWLER_YAML}")"
DATA_STORAGE_BUCKET="$(yq -r '.spec.dataStorageBucket' "${CONTROLLER_CONFIG_YAML}")"
OUTPUT_BUCKET="$(yq -r '.spec.destinationSyncerConfig.s3DestinationConfig.bucket' "${DEST_SYNCER_YAML}")"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  start   Start LocalStack in Docker (skips if already healthy)
  stop    Stop and remove the LocalStack container
  setup   Start LocalStack and create S3 buckets
  status  Print LocalStack health and configured resources

Environment overrides:
  LOCALSTACK_CONTAINER   Docker container name (default: localstack-dev)
  LOCALSTACK_IMAGE       Docker image (default: localstack/localstack:4.14.0)
  LOCALSTACK_ENDPOINT    LocalStack URL (default: http://127.0.0.1:4566)
EOF
}

require_command() {
    if ! command -v "$1" &>/dev/null; then
        echo "Error: '$1' is required but not installed." >&2
        exit 1
    fi
}

aws_local() {
    AWS_ACCESS_KEY_ID=test \
    AWS_SECRET_ACCESS_KEY=test \
    AWS_DEFAULT_REGION="${AWS_REGION}" \
        aws --endpoint-url="${ENDPOINT}" "$@"
}

localstack_healthy() {
    curl -sf "${ENDPOINT}/_localstack/health" &>/dev/null
}

wait_for_localstack() {
    echo "Waiting for LocalStack at ${ENDPOINT}..."
    for _ in $(seq 1 30); do
        if localstack_healthy; then
            echo "✓ LocalStack is healthy"
            return 0
        fi
        sleep 1
    done
    echo "Error: LocalStack did not become healthy in time" >&2
    docker logs "${CONTAINER}" --tail=40 >&2 || true
    return 1
}

cmd_start() {
    require_command docker
    require_command curl

    if localstack_healthy; then
        echo "✓ LocalStack already running at ${ENDPOINT}"
        return 0
    fi

    if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
        echo "Starting existing container '${CONTAINER}'..."
        docker start "${CONTAINER}" >/dev/null
    else
        echo "Creating container '${CONTAINER}' from ${IMAGE}..."
        docker run -d \
            --name "${CONTAINER}" \
            -p 127.0.0.1:4566:4566 \
            "${IMAGE}" >/dev/null
    fi

    wait_for_localstack
}

cmd_stop() {
    require_command docker

    if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
        docker rm -f "${CONTAINER}" >/dev/null
        echo "✓ Stopped and removed '${CONTAINER}'"
    else
        echo "✓ No '${CONTAINER}' container found"
    fi
}

cmd_setup() {
    require_command aws
    require_command yq

    cmd_start

    echo ""
    echo "Creating S3 buckets..."
    for bucket in "${INGESTION_BUCKET}" "${DATA_STORAGE_BUCKET}" "${OUTPUT_BUCKET}"; do
        if aws_local s3 ls "s3://${bucket}" &>/dev/null; then
            echo "✓ Bucket '${bucket}' already exists"
        else
            aws_local s3 mb "s3://${bucket}" >/dev/null
            echo "✓ Bucket '${bucket}' created"
        fi
    done

    echo ""
    echo "✓ LocalStack setup complete"
    echo "  Endpoint: ${ENDPOINT}"
}

cmd_status() {
    require_command aws

    if localstack_healthy; then
        echo "✓ LocalStack healthy at ${ENDPOINT}"
        curl -s "${ENDPOINT}/_localstack/health" | sed 's/^/  /'
    else
        echo "✗ LocalStack not reachable at ${ENDPOINT}"
        return 1
    fi

    echo ""
    echo "S3 buckets:"
    aws_local s3 ls 2>/dev/null | sed 's/^/  /' || echo "  (none)"
}

main() {
    local command="${1:-}"
    case "${command}" in
        start)  cmd_start ;;
        stop)   cmd_stop ;;
        setup)  cmd_setup ;;
        status) cmd_status ;;
        -h|--help|help|"") usage ;;
        *)
            echo "Error: unknown command '${command}'" >&2
            usage >&2
            exit 1
            ;;
    esac
}

main "$@"
