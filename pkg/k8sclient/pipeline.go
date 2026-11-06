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
	"fmt"
	"os"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultPipelineNamespace = "unstructured-controller-namespace"

type PipelineInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Guidance    string `json:"guidance,omitempty"`
	Database    string `json:"-"`
}

type QueryConfig struct {
	Database string
	Schema   string
	Table    string
}

func pipelineNamespace() string {
	if ns := os.Getenv("UNSTRUCTURED_DATA_CONTROLLER_NAMESPACE"); ns != "" {
		return ns
	}
	return defaultPipelineNamespace
}

func snowflakeQueryConfig(
	pipeline *operatorv1alpha1.UnstructuredDataPipeline, stageType operatorv1alpha1.StageType,
) *QueryConfig {
	for _, stage := range pipeline.Spec.Stages {
		if stage.Type == stageType &&
			stage.QueryConfig != nil && stage.QueryConfig.Snowflake != nil {
			return &QueryConfig{
				Database: stage.QueryConfig.Snowflake.Database,
				Schema:   stage.QueryConfig.Snowflake.Schema,
				Table:    stage.QueryConfig.Snowflake.Table,
			}
		}
	}
	return nil
}

func (c *Client) ListPipelines(ctx context.Context) ([]PipelineInfo, error) {
	pipelineList := &operatorv1alpha1.UnstructuredDataPipelineList{}

	err := c.client.List(ctx, pipelineList, &client.ListOptions{
		Namespace: pipelineNamespace(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pipelines: %w", err)
	}

	result := make([]PipelineInfo, len(pipelineList.Items))
	for i := range pipelineList.Items {
		pipeline := &pipelineList.Items[i]
		var db string
		if qc := snowflakeQueryConfig(pipeline, operatorv1alpha1.StageTypeVectorEmbeddingsGenerator); qc != nil {
			db = qc.Database
		}
		result[i] = PipelineInfo{
			Name:        pipeline.Name,
			Description: pipeline.Spec.Description,
			Guidance:    pipeline.Spec.Guidance,
			Database:    db,
		}
	}

	return result, nil
}

func (c *Client) GetPipelineQueryConfig(
	ctx context.Context, name string, stageType operatorv1alpha1.StageType,
) (*QueryConfig, error) {
	pipeline := &operatorv1alpha1.UnstructuredDataPipeline{}
	err := c.client.Get(ctx, client.ObjectKey{
		Namespace: pipelineNamespace(),
		Name:      name,
	}, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to get pipeline %q: %w", name, err)
	}

	qc := snowflakeQueryConfig(pipeline, stageType)
	if qc == nil {
		return nil, fmt.Errorf("pipeline %q has no Snowflake query config for stage type %q", name, stageType)
	}
	return qc, nil
}

// DestinationConfig is where a pipeline's output lands and how to read it.
type DestinationConfig struct {
	Bucket string
	Prefix string
	AWS    *awsclienthandler.AWSConfig
}

// GetPipelineDestination resolves where a pipeline writes its output and the
// credentials to read it back.
//
// The location comes from the destination syncer stage and the credentials
// from the pipeline's own secret, so a path handling mistake cannot reach
// another product's bucket.
func (c *Client) GetPipelineDestination(ctx context.Context, name string) (*DestinationConfig, error) {
	namespace := pipelineNamespace()
	pipeline := &operatorv1alpha1.UnstructuredDataPipeline{}
	if err := c.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pipeline); err != nil {
		return nil, fmt.Errorf("failed to get pipeline %q: %w", name, err)
	}

	var s3Config *operatorv1alpha1.S3Config
	for _, stage := range pipeline.Spec.Stages {
		if stage.Type == operatorv1alpha1.StageTypeDestinationSyncer &&
			stage.DestinationSyncerConfig != nil &&
			stage.DestinationSyncerConfig.Type == operatorv1alpha1.TypeS3 {
			s3Config = &stage.DestinationSyncerConfig.S3DestinationConfig
			break
		}
	}
	if s3Config == nil || s3Config.Bucket == "" {
		return nil, fmt.Errorf("pipeline %q has no S3 destination", name)
	}

	destination := &DestinationConfig{
		Bucket: s3Config.Bucket,
		Prefix: s3Config.Prefix,
		AWS:    &awsclienthandler.AWSConfig{},
	}

	// Read at request time rather than held, so rotating the secret takes
	// effect without a restart.
	if pipeline.Spec.SecretRef != "" {
		secret := &corev1.Secret{}
		key := client.ObjectKey{Namespace: namespace, Name: pipeline.Spec.SecretRef}
		if err := c.client.Get(ctx, key, secret); err != nil {
			return nil, fmt.Errorf("failed to get secret for pipeline %q: %w", name, err)
		}
		destination.AWS = awsclienthandler.ConfigFromSecretData(
			secret.Data, awsclienthandler.DestinationSecretPrefix)
	}

	return destination, nil
}
