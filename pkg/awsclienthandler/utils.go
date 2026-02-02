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
