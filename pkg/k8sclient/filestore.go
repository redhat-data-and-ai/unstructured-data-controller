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

package k8sclient

import (
	"context"
	"errors"
	"fmt"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// controllerConfigName is the fixed name every controller reads its
// operator-level settings from.
const controllerConfigName = "controllerconfig"

// fileStoreSecretPrefix is what the file store credentials are keyed by
// inside the operator secret.
const fileStoreSecretPrefix = "FILE_STORE_AWS_"

// FileStoreConfig is the shared bucket the source crawler downloads into,
// and the credentials to read it back.
type FileStoreConfig struct {
	Bucket string
	AWS    *awsclienthandler.AWSConfig
}

// GetFileStore resolves the operator's file store from the ControllerConfig
// and its secret. Errors when either is missing or names no bucket.
func (c *Client) GetFileStore(ctx context.Context) (*FileStoreConfig, error) {
	namespace := pipelineNamespace()

	config := &operatorv1alpha1.ControllerConfig{}
	key := client.ObjectKey{Namespace: namespace, Name: controllerConfigName}
	if err := c.client.Get(ctx, key, config); err != nil {
		return nil, fmt.Errorf("failed to get ControllerConfig: %w", err)
	}

	if config.Spec.DataStorageBucket == "" {
		return nil, errors.New("ControllerConfig names no dataStorageBucket")
	}
	// Refused rather than defaulted: the SDK takes these as static
	// credentials, so an empty set fails every request instead of falling
	// back to the pod's own role.
	if config.Spec.SecretRef == "" {
		return nil, errors.New("ControllerConfig names no secret to read the file store with")
	}

	secret := &corev1.Secret{}
	secretKey := client.ObjectKey{Namespace: namespace, Name: config.Spec.SecretRef}
	if err := c.client.Get(ctx, secretKey, secret); err != nil {
		return nil, fmt.Errorf("failed to get ControllerConfig secret: %w", err)
	}

	awsConfig := awsclienthandler.ConfigFromSecretData(secret.Data, fileStoreSecretPrefix)
	// The region and endpoint live on the spec rather than the secret, so
	// ConfigFromSecretData leaves them empty.
	awsConfig.Region = config.Spec.DataStorageBucketRegion
	awsConfig.Endpoint = config.Spec.DataStorageBucketEndpoint

	return &FileStoreConfig{Bucket: config.Spec.DataStorageBucket, AWS: awsConfig}, nil
}

// GDriveCrawlStage returns the name of the pipeline's Google Drive
// SourceCrawler stage, which is the directory segment its crawled files are
// stored under. Errors when the pipeline crawls no Drive folders, which is
// what stops a caller asking for Drive files from an S3 or git pipeline.
func (c *Client) GDriveCrawlStage(ctx context.Context, name string) (string, error) {
	pipeline := &operatorv1alpha1.UnstructuredDataPipeline{}
	key := client.ObjectKey{Namespace: pipelineNamespace(), Name: name}
	if err := c.client.Get(ctx, key, pipeline); err != nil {
		return "", fmt.Errorf("failed to get pipeline %q: %w", name, err)
	}

	stage := gdriveCrawlStage(pipeline)
	if stage == "" {
		return "", fmt.Errorf("pipeline %q has no Google Drive source crawler", name)
	}
	return stage, nil
}

// gdriveCrawlStage returns the name of the first Google Drive SourceCrawler
// stage in a pipeline, or "" when it has none.
func gdriveCrawlStage(pipeline *operatorv1alpha1.UnstructuredDataPipeline) string {
	for _, stage := range pipeline.Spec.Stages {
		if stage.Type != operatorv1alpha1.StageTypeSourceCrawler ||
			stage.SourceCrawlerConfig == nil ||
			stage.SourceCrawlerConfig.Type != operatorv1alpha1.TypeGoogleDrive {
			continue
		}
		return stage.Name
	}
	return ""
}
