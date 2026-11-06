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

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
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
	return awsclienthandler.ConfigFromSecretData(secret.Data, prefix), nil
}

const (
	sourceCrawlerGitLabTokenKey = "SOURCE_CRAWLER_GITLAB_TOKEN"
)

// GitTokenFromSecret reads the git access token from a K8s Secret.
// The provider determines which key to read (e.g. SOURCE_CRAWLER_GITLAB_TOKEN).
func GitTokenFromSecret(ctx context.Context, c client.Client, secretName, namespace string, provider operatorv1alpha1.GitProvider) (string, error) {
	if secretName == "" {
		return "", nil
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return "", fmt.Errorf("failed to fetch secret %s: %w", secretName, err)
	}
	tokenKey := providerTokenKey(provider)
	if tokenKey == "" {
		return "", nil
	}
	token, ok := secret.Data[tokenKey]
	if !ok || len(token) == 0 {
		return "", fmt.Errorf("token key %s not found or empty in secret %s", tokenKey, secretName)
	}
	return string(token), nil
}

func providerTokenKey(provider operatorv1alpha1.GitProvider) string {
	switch provider {
	case operatorv1alpha1.GitProviderGitLab:
		return sourceCrawlerGitLabTokenKey
	default:
		return ""
	}
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
