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
	loadOptions := []func(*config.LoadOptions) error{
		config.WithRegion(awsConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(awsConfig.AccessKeyID,
			awsConfig.SecretAccessKey, awsConfig.SessionToken)),
	}

	return config.LoadDefaultConfig(ctx, loadOptions...)
}
