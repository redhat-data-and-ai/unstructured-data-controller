# ControllerConfig with LocalStack

This guide walks you through setting up a ControllerConfig using [LocalStack](https://localstack.cloud/) for local development and testing. 

The ControllerConfig supplies the controller with AWS credentials and the name of the S3 ingestion bucket. 

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or Podman with Docker socket)
- [AWS CLI](https://aws.amazon.com/cli/) with [awslocal](https://github.com/localstack/awscli-local) (or configure AWS CLI to target LocalStack)
- `kubectl` and access to a Kubernetes cluster where the unstructured-data-controller is deployed

## 1. Start LocalStack

Run LocalStack so that S3 (and optionally SQS) are available on `localhost`:

```bash
sudo docker run --rm -it \
  -p 127.0.0.1:4566:4566 \
  -p 127.0.0.1:4510-4559:4510-4559 \
  -v /run/podman/podman.sock:/var/run/docker.sock \
  localstack/localstack
```

Leave this terminal running. In a new terminal, continue with the steps below.

## 2. Create the S3 ingestion bucket

Create the S3 bucket that will be used as the ingestion bucket. Its name must match `spec.unstructuredDataProcessingConfig.ingestionBucket` in your ControllerConfig (e.g. `data-ingestion-bucket`).

```bash
awslocal s3 mb s3://data-ingestion-bucket
```

## 3. Create the AWS secret

The controller reads AWS credentials and endpoint from a Kubernetes secret. A sample secret is provided in `test/resources/unstructured/aws-secret.yaml` with the following keys:

Create the secret in the same namespace where you will create the ControllerConfig. Apply the sample file:

```bash
kubectl apply -f test/resources/unstructured/aws-secret.yaml -n <your-operator-namespace>
```

Example if the operator runs in `default`:

```bash
kubectl apply -f test/resources/unstructured/aws-secret.yaml -n default
```

Edit `test/resources/unstructured/aws-secret.yaml` if you need different values (e.g. real AWS credentials or a different endpoint). For real AWS, you can remove or leave empty `AWS_SESSION_TOKEN` and omit `AWS_ENDPOINT`.

**In-cluster access to LocalStack:** If the controller runs inside the cluster, `localhost` from the pod will not reach LocalStack on your machine. In `aws-secret.yaml`, set `AWS_ENDPOINT` to a URL reachable from the cluster, for example:

- Linux: `http://host.docker.internal:4566` (if the cluster allows it)
- Or your host machine’s IP and port 4566, or a port-forward to the LocalStack port

## 4. Create the ControllerConfig custom resource

Create the ControllerConfig CR so the controller can use the AWS secret and ingestion bucket name. The bucket name in the CR must match the bucket you created in step 2.

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
  awsSecret: aws-secret
  unstructuredDataProcessingConfig:
    ingestionBucket: data-ingestion-bucket
```

Apply the sample or your own manifest (use the same namespace where you created the secret):

```bash
kubectl apply -f config/samples/operator_v1alpha1_controllerconfig.yaml -n <your-operator-namespace>
```

Or apply all samples:

```bash
kubectl apply -k config/samples/ -n <your-operator-namespace>
```


## Verifying

- **Controller**: Ensure the controller is running and check its logs for successful reconciliation of the ControllerConfig (e.g. "SQS client created ...", "S3 client created ...", "Presign client created ...").
- **CR status**: Inspect the ControllerConfig status:

  ```bash
  kubectl get controllerconfig controllerconfig-sample -o yaml
  ```

  The status should show `ConfigReady` with condition `status: "True"` and message "Config is healthy" when reconciliation succeeds.
