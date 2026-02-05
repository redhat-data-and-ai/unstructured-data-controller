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
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	SQSClient *sqs.Client
)

// NewSQSClientFromConfig creates and returns an Amazon SQS client using the provided context and AWS configuration.
func NewSQSClientFromConfig(ctx context.Context, awsConfig *AWSConfig) (*sqs.Client, error) {
	logger := log.FromContext(ctx)
	if SQSClient != nil {
		return SQSClient, nil
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
	SQSClient = sqs.NewFromConfig(cfg, sqsOptions)
	logger.Info("SQS client initialized ...")
	return SQSClient, nil
}

// GetSQSClient returns the initialized Amazon SQS client instance.
func GetSQSClient() (*sqs.Client, error) {
	if SQSClient == nil {
		return nil, errors.New("SQS client not initialized yet")
	}
	return SQSClient, nil
}
