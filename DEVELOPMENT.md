# Local Development Setup

This guide walks you through setting up the Unstructured Data Controller for local development.

## Prerequisites

- Docker
- kubectl
- Go
- Docling

## Step 1: Start LocalStack

Start LocalStack in a separate terminal to simulate AWS services locally:

```bash
sudo docker run --rm -it \
  -p 127.0.0.1:4566:4566 \
  -p 127.0.0.1:4510-4559:4510-4559 \
  docker.io/localstack/localstack:4.14.0
```

Keep this terminal running throughout your development session.

## Step 2: Run Local Development Setup

Execute the make target to set up the local development environment. This creates S3 buckets, configures SQS policies, and sets up Docling:

```bash
make local-dev-setup
```

This command will:
- Create required S3 buckets in LocalStack
- Configure SQS queues and policies
- Set up Docling service for document processing

## Step 3: Create the Unstructured Secret

The controller requires AWS credentials, Snowflake private key, and Docling authentication stored in a Kubernetes secret.

### Apply the Secret

A template exists at `test/resources/unstructured/unstructured-secret.yaml`. Apply it with:

```bash
kubectl apply -f test/resources/unstructured/unstructured-secret.yaml -n unstructured-controller-namespace
```

### Configuration Notes

Edit the YAML file before applying to customize values:

- Add your real AWS credentials if needed
- For production AWS deployments, you can omit `AWS_SESSION_TOKEN` and `AWS_ENDPOINT`

## Step 4: Install Custom Resource Definitions

Install the CRDs required by the controller:

```bash
make install
```

## Step 5: Run the Controller

Launch the controller locally for development:

```bash
make run
```

The controller will start and begin watching for custom resources.

## Step 6: Create ControllerConfig Custom Resource

Create this resource to provide the controller with secret references and configuration parameters. The ingestion bucket name must match what you established in LocalStack.

### Sample ControllerConfig

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: ControllerConfig
metadata:
  labels:
    app.kubernetes.io/name: unstructured-data-controller
    app.kubernetes.io/managed-by: kustomize
  name: controllerconfig-sample
spec:
  unstructuredSecret: unstructured-secret
  snowflakeConfig:
    name: "testing"
    account: "account-identifier"
    user: "SNOWFLAKE_USER"
    role: "TESTING_ROLE"
    region: "us-west-2"
    warehouse: "default"
  unstructuredDataProcessingConfig:
    ingestionBucket: data-ingestion-bucket
    dataStorageBucket: data-storage-bucket
    doclingServeURL: "http://localhost:5001"
    cacheDirectory: "tmp/cache/"
    maxConcurrentDoclingTasks: 5
    maxConcurrentLangchainTasks: 10
```

### Apply the ControllerConfig

```bash
kubectl apply -f config/samples/operator_v1alpha1_controllerconfig.yaml -n unstructured-controller-namespace
```

## Step 7: Create SQSInformer Custom Resource

This resource enables the controller to listen to message queue events. The queue URL must align with your LocalStack setup.

### Sample SQSInformer

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: SQSInformer
metadata:
  labels:
    app.kubernetes.io/name: unstructured-data-controller
    app.kubernetes.io/managed-by: kustomize
  name: sqsinformer-sample
spec:
  queueURL: http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/unstructured-s3-queue
```

Set `queueURL` to the URL returned by LocalStack for your default region, for example:

```bash
awslocal sqs get-queue-url --queue-name unstructured-s3-queue --output text
```

Keep the region in that URL aligned with `SOURCE_AWS_REGION` / `AWS_DEFAULT_REGION` (the sample uses `us-east-1`).

### Apply the SQSInformer

```bash
kubectl apply -f config/samples/operator_v1alpha1_sqsinformer.yaml -n unstructured-controller-namespace
```

## Verification

Monitor controller logs for reconciliation events and verify that resources are ready:

### Check ControllerConfig Status

```bash
kubectl get controllerconfig controllerconfig-sample -n unstructured-controller-namespace -o yaml
```

Should display "ConfigReady" condition as true.

### Check SQSInformer Status

```bash
kubectl get sqsinformer sqsinformer-sample -n unstructured-controller-namespace -o yaml
```

Should show "SQSInformerReady" as true upon successful reconciliation.

## Next Steps

Once your local environment is set up and verified:

1. **Try the Quick Start Guide**: Follow the [Creating Sample File Guide](docs/creating-sample-file.md) to process unstructured files and see the controller in action
   - Upload test documents to the S3 bucket
   - Create an UnstructuredDataProduct custom resource
   - Monitor controller logs to see document processing
   - Verify data is being ingested into Snowflake

## Running Tests

To run the end-to-end tests, you need to set up the required environment variables and build/push the Docker image.

### Export Environment Variables

```bash
  export IMG=<your-image-registry>/<image-name>:<tag>
  export SNOWFLAKE_ACCOUNT=<your-snowflake-account>
  export SNOWFLAKE_USER=<your-snowflake-user>
  export SNOWFLAKE_ROLE=<your-snowflake-role>
  export SNOWFLAKE_WAREHOUSE=<your-snowflake-warehouse>
export SNOWFLAKE_PRIVATE_KEY=<your-base64-encoded-private-key>
```

### Run Tests

Once the environment variables are set, build the Docker image, push it to the registry, and run the end-to-end tests:

```bash
make docker-build docker-push test-e2e
```

This command will:
- Build the controller Docker image
- Push the image to the specified registry
- Execute the end-to-end test suite
