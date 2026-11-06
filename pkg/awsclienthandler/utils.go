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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type AWSConfig struct {
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	SessionToken    string `json:"sessionToken"`
	Endpoint        string `json:"endpoint"`
}

// DestinationSecretPrefix is what a pipeline's destination credentials are
// keyed by inside its secret.
const DestinationSecretPrefix = "DESTINATION_S3_"

// ConfigFromSecretData reads credentials out of a secret's data, where each
// key is the prefix followed by the field name. A missing key reads as empty,
// and an empty AccessKeyID or SecretAccessKey makes every request fail: the
// SDK is given these as static credentials, so it never falls back to the
// ambient environment. Only Region and Endpoint are safe to leave unset.
func ConfigFromSecretData(data map[string][]byte, prefix string) *AWSConfig {
	return &AWSConfig{
		Region:          string(data[prefix+"REGION"]),
		AccessKeyID:     string(data[prefix+"ACCESS_KEY_ID"]),
		SecretAccessKey: string(data[prefix+"SECRET_ACCESS_KEY"]),
		SessionToken:    string(data[prefix+"SESSION_TOKEN"]),
		Endpoint:        string(data[prefix+"ENDPOINT"]),
	}
}

// getAWSConfig initializes and returns an AWS configuration using the provided context and AWSConfig details.
func getAWSConfig(ctx context.Context, awsConfig *AWSConfig) (aws.Config, error) {
	credentialsProvider := credentials.NewStaticCredentialsProvider(
		awsConfig.AccessKeyID,
		awsConfig.SecretAccessKey,
		awsConfig.SessionToken,
	)
	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(awsConfig.Region),
		config.WithCredentialsProvider(credentialsProvider),
	}

	return config.LoadDefaultConfig(ctx, loadOptions...)
}
