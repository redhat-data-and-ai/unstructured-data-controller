#!/usr/bin/env bash
set -e

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Configuration
LOCAL_KIND_CLUSTER="${LOCAL_KIND_CLUSTER:-unstructured-data-controller-local}"
LOCAL_NAMESPACE="${LOCAL_NAMESPACE:-unstructured-controller-namespace}"
CACHE_DIR="${CACHE_DIR:-tmp/cache}"
DATA_STORAGE_BUCKET="${DATA_STORAGE_BUCKET:-data-storage-bucket}"
DATA_INGESTION_BUCKET="${DATA_INGESTION_BUCKET:-data-ingestion-bucket}"
OUTPUT_RESULT_BUCKET="${OUTPUT_RESULT_BUCKET:-output-result-bucket}"
SQS_QUEUE_NAME="${SQS_QUEUE_NAME:-unstructured-s3-queue}"

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

# 3. Create cache directory
echo ""
echo "3. Creating local cache directory..."
mkdir -p "${CACHE_DIR}"
echo "✓ Cache directory '${CACHE_DIR}' created"

# 4. Check/Start Docling
echo ""
echo "4. Checking Docling service..."
if command -v docling-serve &> /dev/null; then
    if curl -s http://localhost:5001/health &>/dev/null; then
        echo "✓ Docling is already running"
    else
        echo "Starting Docling in the background..."
        nohup docling-serve run --enable-ui > /tmp/docling.log 2>&1 &
        DOCLING_PID=$!
        echo $DOCLING_PID > /tmp/docling.pid
        sleep 2
        if curl -s http://localhost:5001/health &>/dev/null; then
            echo "✓ Docling started successfully (PID: $DOCLING_PID)"
            echo "  Log: /tmp/docling.log"
        else
            echo "Warning: Docling may still be starting. Check /tmp/docling.log"
        fi
    fi
else
    echo "⚠ Docling is not installed. You'll need to start it manually."
fi

# 5. Setting up LocalStack resources
# Create S3 buckets
echo ""
echo "5a. Creating S3 buckets..."
for bucket in "${DATA_STORAGE_BUCKET}" "${DATA_INGESTION_BUCKET}"; do
    if awslocal s3 ls "s3://${bucket}" &>/dev/null; then
        echo "✓ Bucket '${bucket}' already exists"
    else
        awslocal s3 mb "s3://${bucket}"
        echo "✓ Bucket '${bucket}' created"
    fi
done

# Create SQS queue
echo ""
echo "5b. Creating SQS queue..."
if awslocal sqs get-queue-url --queue-name "${SQS_QUEUE_NAME}" &>/dev/null; then
    echo "✓ SQS queue '${SQS_QUEUE_NAME}' already exists"
else
    awslocal sqs create-queue --queue-name "${SQS_QUEUE_NAME}"
    echo "✓ SQS queue '${SQS_QUEUE_NAME}' created"
fi

# Configure S3 bucket notification
echo ""
echo "5c. Configuring S3 bucket notification..."
QUEUE_URL="$(awslocal sqs get-queue-url --queue-name "${SQS_QUEUE_NAME}" --query 'QueueUrl' --output text)"
QUEUE_ARN="$(awslocal sqs get-queue-attributes --queue-url "${QUEUE_URL}" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)"
NOTIFICATION_FILE="$(mktemp)"
trap "rm -f \"${NOTIFICATION_FILE}\"" EXIT
cat >"${NOTIFICATION_FILE}" <<EOF
{
  "QueueConfigurations": [
    {
      "Id": "S3ToSQSNotification",
      "QueueArn": "${QUEUE_ARN}",
      "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
    }
  ]
}
EOF
awslocal s3api put-bucket-notification-configuration \
    --bucket "${DATA_INGESTION_BUCKET}" \
    --notification-configuration "file://${NOTIFICATION_FILE}"
echo "✓ S3 bucket notification configured (queue ARN from LocalStack)"

echo ""
echo "✓ Local development environment setup complete!"
