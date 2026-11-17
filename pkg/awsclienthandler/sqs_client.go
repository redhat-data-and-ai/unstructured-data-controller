/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package awsclienthandler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// NewSQSClientFromConfig returns a new Amazon SQS client for awsConfig. Each
// call builds its own, since crawlers can use different credentials and queues.
func NewSQSClientFromConfig(ctx context.Context, awsConfig *AWSConfig) (*sqs.Client, error) {
	if awsConfig == nil {
		return nil, errors.New("cannot create an SQS client without an AWS config")
	}
	cfg, err := getAWSConfig(ctx, awsConfig)
	if err != nil {
		return nil, err
	}

	sqsOptions := func(o *sqs.Options) {
		if awsConfig.Endpoint != "" {
			o.BaseEndpoint = aws.String(awsConfig.Endpoint)
		}
	}
	return sqs.NewFromConfig(cfg, sqsOptions), nil
}

const sqsLongPollSeconds = 20

type s3EventMessage struct {
	Records []s3EventRecord `json:"Records"`
}

type s3EventRecord struct {
	S3 s3EventData `json:"s3"`
}

type s3EventData struct {
	Bucket s3BucketInfo `json:"bucket"`
	Object s3ObjectInfo `json:"object"`
}

type s3BucketInfo struct {
	Name string `json:"name"`
}

type s3ObjectInfo struct {
	Key string `json:"key"`
}

// DrainSQSQueue long-polls SQS (up to 20s per receive) and deletes only
// messages whose S3 event matches the given bucket and prefix. Unrelated
// messages are left in the queue for other consumers.
// Returns true if any matching messages were found.
func DrainSQSQueue(ctx context.Context, sqsClient *sqs.Client, queueURL, bucket, prefix string) (bool, error) {
	logger := log.FromContext(ctx)
	hasMessages := false

	for {
		output, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     sqsLongPollSeconds,
		})
		if err != nil {
			return hasMessages, err
		}
		if len(output.Messages) == 0 {
			break
		}

		for _, msg := range output.Messages {
			if msg.Body == nil {
				continue
			}
			var event s3EventMessage
			if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
				logger.Error(err, "failed to parse SQS message body, skipping", "messageId", *msg.MessageId)
				continue
			}

			matches := false
			for _, record := range event.Records {
				if record.S3.Bucket.Name == bucket && strings.HasPrefix(record.S3.Object.Key, prefix) {
					matches = true
					break
				}
			}

			if !matches {
				continue
			}

			hasMessages = true
			if _, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				logger.Error(err, "failed to delete SQS message", "messageId", *msg.MessageId)
			}
		}
	}
	return hasMessages, nil
}
