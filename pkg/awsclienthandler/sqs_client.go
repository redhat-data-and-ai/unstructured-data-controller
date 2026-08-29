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
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type SQSClientCache struct {
	mu      sync.Mutex
	clients map[string]*sqsEntry
}

type sqsEntry struct {
	client    *sqs.Client
	awsConfig AWSConfig
	lastUsed  time.Time
}

var sqsClientCache = &SQSClientCache{
	clients: make(map[string]*sqsEntry),
}

func InitSQSCache(ctx context.Context, ttl time.Duration) {
	sqsClientCache.startCleanup(ctx, ttl)
}

func (c *SQSClientCache) getClient(pipelineName string) (*sqsEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.clients[pipelineName]
	if ok {
		entry.lastUsed = time.Now()
	}
	return entry, ok
}

func (c *SQSClientCache) setClient(pipelineName string, entry *sqsEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clients == nil {
		c.clients = make(map[string]*sqsEntry)
	}
	c.clients[pipelineName] = entry
}

func (c *SQSClientCache) startCleanup(ctx context.Context, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	ticker := time.NewTicker(ttl / 2)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.evictStale(ttl)
			}
		}
	}()
}

func (c *SQSClientCache) evictStale(ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for key, entry := range c.clients {
		if now.Sub(entry.lastUsed) > ttl {
			delete(c.clients, key)
		}
	}
}

// NewSQSClientFromConfig creates and returns an Amazon SQS client using the provided context and AWS configuration.
func NewSQSClientFromConfig(ctx context.Context, awsConfig *AWSConfig, pipelineName string) (*sqs.Client, error) {
	logger := log.FromContext(ctx)

	entry, ok := sqsClientCache.getClient(pipelineName)
	if ok && awsConfig != nil && entry.awsConfig == *awsConfig {
		logger.Info("Using existing SQS client for the pipeline", "pipelineName", pipelineName)
		return entry.client, nil
	}

	logger.Info("Creating new SQS client for the pipeline", "pipelineName", pipelineName)
	cfg, err := getAWSConfig(ctx, awsConfig)
	if err != nil {
		return nil, err
	}

	sqsOptions := func(o *sqs.Options) {
		if awsConfig.Endpoint != "" {
			o.BaseEndpoint = aws.String(awsConfig.Endpoint)
		}
	}

	sqsClient := sqs.NewFromConfig(cfg, sqsOptions)

	var cfgVal AWSConfig
	if awsConfig != nil {
		cfgVal = *awsConfig
	}
	newEntry := &sqsEntry{client: sqsClient, awsConfig: cfgVal, lastUsed: time.Now()}
	sqsClientCache.setClient(pipelineName, newEntry)

	return sqsClient, nil
}

func DeleteSQSClient(pipelineName string) {
	sqsClientCache.mu.Lock()
	defer sqsClientCache.mu.Unlock()
	delete(sqsClientCache.clients, pipelineName)
}

// GetSQSClient returns the initialized Amazon SQS client instance.
func GetSQSClient(pipelineName string) (*sqs.Client, bool) {
	entry, ok := sqsClientCache.getClient(pipelineName)
	if !ok {
		return nil, false
	}
	return entry.client, true
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
