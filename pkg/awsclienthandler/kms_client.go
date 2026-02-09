package awsclienthandler

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	KMSClient *kms.Client
)

func NewKMSClientFromConfig(ctx context.Context, awsConfig *AWSConfig) (*kms.Client, error) {
	logger := log.FromContext(ctx)
	if KMSClient != nil {
		return KMSClient, nil
	}

	cfg, err := getAWSConfig(ctx, awsConfig)
	if err != nil {
		return nil, err
	}

	kmsOptions := func(o *kms.Options) {
		if awsConfig.Endpoint != "" {
			o.BaseEndpoint = aws.String(awsConfig.Endpoint)
		}
	}
	KMSClient = kms.NewFromConfig(cfg, kmsOptions)
	logger.Info("KMS client initialized ...")
	return KMSClient, nil
}

func GetKMSClient() (*kms.Client, error) {
	if KMSClient == nil {
		return nil, errors.New("KMS client is not initialized")
	}
	return KMSClient, nil
}
