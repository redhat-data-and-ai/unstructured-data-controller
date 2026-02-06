# SQSConsumer with LocalStack

This guide walks you through setting up an SQSConsumer using [LocalStack](https://localstack.cloud/)

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or Podman with Docker socket)
- [AWS CLI](https://aws.amazon.com/cli/) with [awslocal](https://github.com/localstack/awscli-local) (or configure AWS CLI to target LocalStack)
- `kubectl` and access to a Kubernetes cluster where the unstructured-data-controller is deployed
- **AWS secret and ControllerConfig:** The controller uses AWS credentials and the ingestion bucket from [ControllerConfig](controllerconfig.md). If you have not created the AWS secret and ControllerConfig yet, follow [ControllerConfig with LocalStack](controllerconfig.md) first (steps 1–4: LocalStack, S3 bucket, AWS secret, ControllerConfig CR), then continue with this guide.

## 1. Start LocalStack

Run LocalStack so that S3 and SQS are available on `localhost`:

```bash
sudo docker run --rm -it \
  -p 127.0.0.1:4566:4566 \
  -p 127.0.0.1:4510-4559:4510-4559 \
  -v /run/podman/podman.sock:/var/run/docker.sock \
  localstack/localstack
```

Leave this terminal running. In a new terminal, continue with the steps below.

## 2. Create S3 bucket and SQS queue

Create the S3 bucket and the SQS queue that will receive S3 event notifications:

```bash
# Create the S3 bucket
awslocal s3 mb s3://data-ingestion-bucket

# Create the SQS queue
awslocal sqs create-queue --queue-name unstructured-s3-queue
```

## 3. Get the SQS queue ARN

You need the queue ARN to configure S3 bucket notifications. Use the same region as in your queue URL (LocalStack often uses `us-east-1`; adjust the region in the URL if you use another):

```bash
awslocal sqs get-queue-attributes \
  --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/unstructured-s3-queue \
  --attribute-names QueueArn
```

Example output:

```json
{
  "Attributes": {
    "QueueArn": "arn:aws:sqs:us-east-1:000000000000:unstructured-s3-queue"
  }
}
```

Copy the `QueueArn` value (e.g. `arn:aws:sqs:us-east-1:000000000000:unstructured-s3-queue`). You will use it in the next step.

## 4. Configure S3 bucket notifications

Create a notification configuration file that sends S3 events to your SQS queue. A template is provided at `test/resources/unstructured/notification.json`.

**Important:** Open `test/resources/unstructured/notification.json` and replace the placeholder `REPLACE_WITH_QUEUE_ARN_FROM_GET_QUEUE_ATTRIBUTES` in `QueueArn` with the ARN you obtained in the previous step. The file should look like this (with your actual ARN):

```json
{
  "QueueConfigurations": [
    {
      "Id": "S3ToSQSNotification",
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:unstructured-s3-queue",
      "Events": [
        "s3:ObjectCreated:*"
      ]
    }
  ]
}
```

Then apply the notification configuration to the bucket:

```bash
awslocal s3api put-bucket-notification-configuration \
  --bucket data-ingestion-bucket \
  --notification-configuration file://test/resources/unstructured/notification.json
```

From the project root, the path `test/resources/unstructured/notification.json` is relative to the current directory; adjust the path if you run the command from somewhere else.

## 5. Create the SQSConsumer custom resource

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

Or apply all samples:

```bash
kubectl apply -k config/samples/
```


## Verifying

- **LocalStack**: Check the LocalStack logs for S3/SQS requests.
- **Controller**: Ensure the controller is running and check its logs for reconciliation of the SQSConsumer.
- **CR status**: Inspect the SQSConsumer status:

  ```bash
  kubectl get sqsconsumer sqsconsumer-sample -o yaml
  ```

