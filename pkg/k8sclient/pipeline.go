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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive"
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
// credentials to read it back. Errors if the pipeline has no S3 destination
// syncer stage or names no secret.
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

	s3Config := s3Destination(pipeline)
	if s3Config == nil {
		return nil, fmt.Errorf("pipeline %q has no S3 destination", name)
	}

	// Refused rather than defaulted: the SDK is handed these as static
	// credentials, so an empty set fails every request instead of falling
	// back to the pod's own role.
	if pipeline.Spec.SecretRef == "" {
		return nil, fmt.Errorf("pipeline %q names no secret to read its destination with", name)
	}
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: namespace, Name: pipeline.Spec.SecretRef}
	if err := c.client.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("failed to get secret for pipeline %q: %w", name, err)
	}

	return &DestinationConfig{
		Bucket: s3Config.Bucket,
		Prefix: s3Config.Prefix,
		AWS: awsclienthandler.ConfigFromSecretData(
			secret.Data, awsclienthandler.DestinationSecretPrefix),
	}, nil
}

// DriveSearchCredentialKey is the secret key holding the Google service
// account JSON a pipeline crawls and searches Drive with.
const DriveSearchCredentialKey = "SOURCE_GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON"

// DriveSearchConfig is what the MCP server needs to run a live Drive search for
// a pipeline.
type DriveSearchConfig struct {
	// CredentialsJSON is the pipeline's own Google service account key. The
	// search runs as that account.
	CredentialsJSON []byte
	// FolderIDs are the roots declared by the pipeline. The search is confined
	// to these and their subfolders, so it covers the pipeline's corpus rather
	// than everything the service account happens to be able to see.
	FolderIDs []string
	// SkipFolderNames are folder names the crawler skips, applied to the search
	// too so both agree on what the corpus is.
	SkipFolderNames []string
	// StageName is the crawler stage that wrote the files. Needed to locate a
	// file in the destination bucket, whose keys are laid out per stage.
	StageName string
	// Access names the Snowflake table whose row access policy decides which
	// results the caller may see. It comes from the same stage as the folders,
	// so the search and the check always describe one corpus.
	Access QueryConfig
}

// GetPipelineDriveSearchConfig resolves a pipeline to everything one Drive
// search needs: the folders, the credential, and the table that gates access.
//
// Requires a stage that crawls Google Drive and records its files in
// Snowflake. A pipeline over any other source has no Drive corpus, and one
// without the table has no way to decide who may see a result, so refusing is
// the correct answer rather than an empty one.
func (c *Client) GetPipelineDriveSearchConfig(ctx context.Context, name string) (*DriveSearchConfig, error) {
	namespace := pipelineNamespace()
	pipeline := &operatorv1alpha1.UnstructuredDataPipeline{}
	if err := c.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pipeline); err != nil {
		return nil, fmt.Errorf("failed to get pipeline %q: %w", name, err)
	}

	gdriveConfig, stage := driveSearchStage(pipeline)
	if gdriveConfig == nil {
		return nil, fmt.Errorf("pipeline %q has no Google Drive source to search", name)
	}
	if stage.QueryConfig == nil || stage.QueryConfig.Snowflake == nil {
		return nil, fmt.Errorf(
			"pipeline %q does not record which files you may read, so its Drive search "+
				"cannot be filtered: add queryConfig.snowflake to stage %q", name, stage.Name)
	}

	if pipeline.Spec.SecretRef == "" {
		return nil, fmt.Errorf("pipeline %q names no secret to read its Drive credentials from", name)
	}
	secret := &corev1.Secret{}
	key := client.ObjectKey{Namespace: namespace, Name: pipeline.Spec.SecretRef}
	if err := c.client.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("failed to get secret for pipeline %q: %w", name, err)
	}
	credentialsJSON, ok := secret.Data[DriveSearchCredentialKey]
	if !ok || len(credentialsJSON) == 0 {
		return nil, fmt.Errorf("secret for pipeline %q has no %s", name, DriveSearchCredentialKey)
	}

	// Refused rather than skipped: a folder we cannot resolve would quietly
	// shrink the searched corpus, and the caller would see fewer results with
	// no indication why.
	folderIDs := make([]string, 0, len(gdriveConfig.Folders))
	for _, folder := range gdriveConfig.Folders {
		folderID, err := gdrive.FolderIDFromURL(folder.URL)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q has an unreadable folder URL: %w", name, err)
		}
		folderIDs = append(folderIDs, folderID)
	}

	skipNames := make([]string, 0, len(gdriveConfig.SkipFolders))
	for _, skip := range gdriveConfig.SkipFolders {
		skipNames = append(skipNames, skip.Pattern)
	}

	return &DriveSearchConfig{
		CredentialsJSON: credentialsJSON,
		FolderIDs:       folderIDs,
		SkipFolderNames: skipNames,
		StageName:       stage.Name,
		Access: QueryConfig{
			Database: stage.QueryConfig.Snowflake.Database,
			Schema:   stage.QueryConfig.Snowflake.Schema,
			Table:    stage.QueryConfig.Snowflake.Table,
		},
	}, nil
}

// driveSearchStage returns the Google Drive source config of the first stage
// that crawls Drive, with the stage it came from, or nil when there is none.
//
// No opt-in marker: a stage that crawls Drive is searchable by definition.
// Returning the stage lets the caller read the rest of it without searching
// again, which is what keeps the folders and the access table in step.
func driveSearchStage(
	pipeline *operatorv1alpha1.UnstructuredDataPipeline,
) (*operatorv1alpha1.GoogleDriveConfig, operatorv1alpha1.PipelineStage) {
	for _, stage := range pipeline.Spec.Stages {
		if stage.SourceCrawlerConfig == nil ||
			stage.SourceCrawlerConfig.Type != operatorv1alpha1.TypeGoogleDrive ||
			stage.SourceCrawlerConfig.GoogleDriveConfig == nil {
			continue
		}
		return stage.SourceCrawlerConfig.GoogleDriveConfig, stage
	}
	return nil, operatorv1alpha1.PipelineStage{}
}

// s3Destination returns the bucket a pipeline's first usable S3 destination
// syncer writes to, or nil when it has none.
func s3Destination(pipeline *operatorv1alpha1.UnstructuredDataPipeline) *operatorv1alpha1.S3Config {
	for _, stage := range pipeline.Spec.Stages {
		if stage.Type != operatorv1alpha1.StageTypeDestinationSyncer ||
			stage.DestinationSyncerConfig == nil ||
			stage.DestinationSyncerConfig.Type != operatorv1alpha1.TypeS3 ||
			stage.DestinationSyncerConfig.S3DestinationConfig.Bucket == "" {
			continue
		}
		return &stage.DestinationSyncerConfig.S3DestinationConfig
	}
	return nil
}
