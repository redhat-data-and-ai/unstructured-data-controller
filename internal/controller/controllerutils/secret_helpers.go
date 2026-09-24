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

package controllerutils

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
)

// ParentPipelineNameFromOwnerReference extracts the pipeline name from the controller owner reference.
func ParentPipelineNameFromOwnerReference(obj client.Object) (string, error) {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return ref.Name, nil
		}
	}
	return "", fmt.Errorf("no controller owner reference found on %s/%s", obj.GetNamespace(), obj.GetName())
}

// AWSConfigFromSecret reads a K8s secret and returns an AWSConfig.
// The prefix selects which set of keys to read (e.g. "SOURCE_S3_" or
// "DESTINATION_S3_"), so the same secret can carry separate credentials
// for source and destination.
func AWSConfigFromSecret(ctx context.Context, c client.Client, secretName, namespace, prefix string) (*awsclienthandler.AWSConfig, error) {
	if secretName == "" {
		return &awsclienthandler.AWSConfig{}, nil
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to fetch secret %s: %w", secretName, err)
	}
	return &awsclienthandler.AWSConfig{
		Region:          string(secret.Data[prefix+"REGION"]),
		AccessKeyID:     string(secret.Data[prefix+"ACCESS_KEY_ID"]),
		SecretAccessKey: string(secret.Data[prefix+"SECRET_ACCESS_KEY"]),
		SessionToken:    string(secret.Data[prefix+"SESSION_TOKEN"]),
		Endpoint:        string(secret.Data[prefix+"ENDPOINT"]),
	}, nil
}

// SQSQueueURLFromSecret reads the per-dataproduct SQS queue URL from the same
// secret used for AWS credentials, keyed by <prefix>SQS_QUEUE_URL (e.g.
// SOURCE_S3_SQS_QUEUE_URL). Returns an empty string if the secret or key is
// absent, so callers can fall back to the default polling interval.
func SQSQueueURLFromSecret(ctx context.Context, c client.Client, secretName, namespace, prefix string) (string, error) {
	if secretName == "" {
		return "", nil
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return "", fmt.Errorf("failed to fetch secret %s: %w", secretName, err)
	}
	return string(secret.Data[prefix+"SQS_QUEUE_URL"]), nil
}

// GDriveCredentialsFromSecret reads the Google service account JSON
// from a K8s Secret. The secret must contain a key named
// "SOURCE_GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON".
func GDriveCredentialsFromSecret(ctx context.Context, c client.Client, secretName, namespace string) ([]byte, error) {
	if secretName == "" {
		return nil, errors.New("secretRef is required for gdrive source type")
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to fetch secret %s: %w", secretName, err)
	}
	const key = "SOURCE_GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON"
	credentialsJSON, ok := secret.Data[key]
	if !ok || len(credentialsJSON) == 0 {
		return nil, fmt.Errorf("secret %s does not contain key %s", secretName, key)
	}
	return credentialsJSON, nil
}
