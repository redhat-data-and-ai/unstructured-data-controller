# Setting up LocalStack

This guide walks you through setting up [LocalStack](https://localstack.cloud/)

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or Podman with Docker socket)
- [AWS CLI](https://aws.amazon.com/cli/) with [awslocal](https://github.com/localstack/awscli-local) (or configure AWS CLI to target LocalStack)

## 1. Start LocalStack

Run LocalStack so that S3 and SQS are available on `localhost`:

```bash
sudo docker run --rm -it \
  -p 127.0.0.1:4566:4566 \
  -p 127.0.0.1:4510-4559:4510-4559 \
  docker.io/localstack/localstack
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

This will provide the queue url

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

## Verifying

- **LocalStack**: Check the LocalStack logs for S3/SQS requests.
