# Unstructured Data Controller with LocalStack

Simple guide to get the Unstructured Data Controller up and running. This guide walks you through setting up a ControllerConfig using [LocalStack](https://localstack.cloud/) for local development and testing.

The ControllerConfig supplies the controller with AWS credentials and the name of the S3 ingestion bucket. snowflake and docling.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or Podman with Docker socket)
- Kubernetes cluster (or Kind for local dev)
- [AWS CLI](https://aws.amazon.com/cli/) with [awslocal](https://github.com/localstack/awscli-local) (or configure AWS CLI to target LocalStack)
- [Docling-server] using `pip install "docling-serve[ui]"`
- `kubectl` and access to a Kubernetes cluster where the unstructured-data-controller is deployed
- snowflake account key pair and hosted/local docling url
- **Unstructured secret and ControllerConfig:** The controller uses AWS credentials and the ingestion bucket. If you have not created localstack, follow [Setup LocalStack](setup-localstack.md) first, then continue with this guide.

### 1. Create Namespace

Run kind cluster locally and create namespace

```bash
kubectl create namespace unstructured-controller-namespace
```

## 2. Run docling locally

Run docling serve locally if you dont have or want to use hosted docling url

```bash
docling-serve run --enable-ui
```

### 3. Create the Unstructured secret

The controller reads AWS credentials and endpoint, snowflake private key and docling key from a Kubernetes secret. A sample secret is provided in `test/resources/unstructured/unstructured-secret.yaml` with the following keys:

Specify the docling user key if required, base64 snowflake private key and local stack details

```bash
kubectl apply -f test/resources/unstructured/unstructured-secret.yaml -n unstructured-controller-namespace
```

Edit `test/resources/unstructured/unstructured-secret.yaml` if you need different values (e.g. real AWS credentials or a different endpoint). For real AWS, you can remove or leave empty `AWS_SESSION_TOKEN` and omit `AWS_ENDPOINT`.

**In-cluster access to LocalStack:** If the controller runs inside the cluster, `localhost` from the pod will not reach LocalStack on your machine. In `unstructured-secret.yaml`, set `AWS_ENDPOINT` to a URL reachable from the cluster, for example:

- Linux: `http://host.docker.internal:4566` (if the cluster allows it)
- Or your host machine’s IP and port 4566, or a port-forward to the LocalStack port

### 4. Setup local cache directory or provide bucket url for local cache

```bash
mkdir -p tmp/cache/
```

### 5. Deploy Controller

**Install CRD:**
```bash
make install
```

**Run Controller (local dev):**
```bash
make run
```

## 6. Create the ControllerConfig custom resource

Create the ControllerConfig CR so the controller can use the unstructured secret and ingestion bucket name. The bucket name in the CR must match the bucket you created in localstack.

Example (from the sample manifest):

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
    doclingServeURL: "http://localhost:5001"
    cacheDirectory: "tmp/cache/"
    maxConcurrentDoclingTasks: 5
    maxConcurrentLangchainTasks: 10
```

Apply the sample or your own manifest (use the same namespace where you created the secret):

```bash
kubectl apply -f config/samples/operator_v1alpha1_controllerconfig.yaml -n unstructured-controller-namespace
```

## 7. Create the SQSConsumer custom resource

Create the SQSConsumer CR so the controller can consume messages from the queue. The queue URL must match the one used with LocalStack (same region and host).

Example (from the sample manifest):

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: SQSConsumer
metadata:
  labels:
    app.kubernetes.io/name: unstructured-data-controller
    app.kubernetes.io/managed-by: kustomize
  name: sqsconsumer-sample
spec:
  queueURL: http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/unstructured-s3-queue
```

Apply the sample or your own manifest:

```bash
kubectl apply -f config/samples/operator_v1alpha1_sqsconsumer.yaml
```

## Verifying

- **Controller**: Ensure the controller is running and check its logs for reconciliation of the SQSConsumer.
- **CR status**: Inspect the SQSConsumer status:

  ```bash
  kubectl get controllerconfig controllerconfig-sample -o yaml
  ```

  The status should show `ConfigReady` with condition `status: "True"` and message "Config is healthy" when reconciliation succeeds.

  ```bash
  kubectl get sqsconsumer sqsconsumer-sample -o yaml
  ```

  The status should show `SQSConsumerReady` with condition `status: "True"` when reconciliation succeeds.
