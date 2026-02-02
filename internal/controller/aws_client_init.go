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

package controller

import (
	"context"
	"fmt"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// InitializeAWSClients initializes the AWS clients (S3 and SQS) if they are not already initialized.
// It fetches AWS credentials from a Kubernetes secret using the provided client.
func InitializeAWSClients(ctx context.Context, k8sClient client.Client, secretName, namespace string) error {
	logger := log.FromContext(ctx)

	// Check if clients are already initialized
	if _, err := awsclienthandler.GetSQSClient(); err == nil {
		// SQS client already initialized
		logger.Info("AWS clients already initialized, skipping initialization")
		return nil
	}

	// Fetch AWS secret from Kubernetes
	awsSecret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: namespace,
	}, awsSecret); err != nil {
		return fmt.Errorf("error fetching AWS secret %s/%s: %w", namespace, secretName, err)
	}

	awsConfig := &awsclienthandler.AWSConfig{
		Region:          string(awsSecret.Data["AWS_REGION"]),
		AccessKeyID:     string(awsSecret.Data["AWS_ACCESS_KEY_ID"]),
		SecretAccessKey: string(awsSecret.Data["AWS_SECRET_ACCESS_KEY"]),
		SessionToken:    string(awsSecret.Data["AWS_SESSION_TOKEN"]),
		Endpoint:        string(awsSecret.Data["AWS_ENDPOINT"]),
	}

	// Create SQS client
	if _, err := awsclienthandler.NewSQSClientFromConfig(ctx, awsConfig); err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}
	logger.Info("SQS client created")

	// Create S3 client
	if _, err := awsclienthandler.NewS3ClientFromConfig(ctx, awsConfig); err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}
	logger.Info("S3 client created")

	// Create presign client
	if _, err := awsclienthandler.NewPresignClient(ctx); err != nil {
		return fmt.Errorf("failed to create Presign client: %w", err)
	}
	logger.Info("Presign client created")

	return nil
}
