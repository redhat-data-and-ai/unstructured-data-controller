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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

func pipelineWithStages(stages ...operatorv1alpha1.PipelineStage) *operatorv1alpha1.UnstructuredDataPipeline {
	return &operatorv1alpha1.UnstructuredDataPipeline{
		Spec: operatorv1alpha1.UnstructuredDataPipelineSpec{Stages: stages},
	}
}

func syncerStage(name, bucket, prefix string, stageType operatorv1alpha1.UnstructuredDataType,
) operatorv1alpha1.PipelineStage {
	return operatorv1alpha1.PipelineStage{
		Name: name,
		Type: operatorv1alpha1.StageTypeDestinationSyncer,
		DestinationSyncerConfig: &operatorv1alpha1.DestinationSyncerConfig{
			Type:                stageType,
			S3DestinationConfig: operatorv1alpha1.S3Config{Bucket: bucket, Prefix: prefix},
		},
	}
}

// The bucket a citation image is read from comes off this stage, so picking
// the wrong one reads another product's data.
func TestS3Destination(t *testing.T) {
	pipeline := pipelineWithStages(
		operatorv1alpha1.PipelineStage{Name: "convert", Type: operatorv1alpha1.StageTypeDocumentProcessor},
		syncerStage("sync", "output-bucket", "processed/", operatorv1alpha1.TypeS3),
	)

	destination := s3Destination(pipeline)

	require.NotNil(t, destination)
	assert.Equal(t, "output-bucket", destination.Bucket)
	assert.Equal(t, "processed/", destination.Prefix)
}

func TestS3Destination_NoneToPick(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *operatorv1alpha1.UnstructuredDataPipeline
	}{
		{name: "no stages at all", pipeline: pipelineWithStages()},
		{
			name: "no syncer stage",
			pipeline: pipelineWithStages(operatorv1alpha1.PipelineStage{
				Name: "convert", Type: operatorv1alpha1.StageTypeDocumentProcessor,
			}),
		},
		{
			name: "a syncer with no config",
			pipeline: pipelineWithStages(operatorv1alpha1.PipelineStage{
				Name: "sync", Type: operatorv1alpha1.StageTypeDestinationSyncer,
			}),
		},
		{
			name:     "a syncer that is not s3",
			pipeline: pipelineWithStages(syncerStage("sync", "output-bucket", "", "gcs")),
		},
		{
			name:     "a syncer with no bucket",
			pipeline: pipelineWithStages(syncerStage("sync", "", "processed/", operatorv1alpha1.TypeS3)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Nil(t, s3Destination(test.pipeline))
		})
	}
}

// A syncer with no bucket is skipped rather than ending the search, so a
// usable one later in the list is still found.
func TestS3Destination_SkipsAnUnusableSyncer(t *testing.T) {
	pipeline := pipelineWithStages(
		syncerStage("sync-nowhere", "", "", operatorv1alpha1.TypeS3),
		syncerStage("sync", "output-bucket", "processed/", operatorv1alpha1.TypeS3),
	)

	destination := s3Destination(pipeline)

	require.NotNil(t, destination)
	assert.Equal(t, "output-bucket", destination.Bucket)
}

// driveStage builds a crawler stage of the given source type.
func driveStage(
	sourceType operatorv1alpha1.UnstructuredDataType, folderURL string,
) operatorv1alpha1.PipelineStage {
	stage := operatorv1alpha1.PipelineStage{
		Name: "crawl",
		Type: operatorv1alpha1.StageTypeSourceCrawler,
		SourceCrawlerConfig: &operatorv1alpha1.SourceCrawlerConfig{
			Type: sourceType,
		},
	}
	if folderURL != "" {
		stage.SourceCrawlerConfig.GoogleDriveConfig = &operatorv1alpha1.GoogleDriveConfig{
			Folders: []operatorv1alpha1.GoogleDriveFolders{{URL: folderURL}},
		}
	}
	return stage
}

func TestDriveSearchStage(t *testing.T) {
	const folder = "https://drive.google.com/drive/folders/abc123"

	tests := []struct {
		name          string
		pipeline      *operatorv1alpha1.UnstructuredDataPipeline
		wantNil       bool
		wantStageName string
	}{
		{
			// No marker needed: crawling Drive is what makes it searchable.
			name: "a Drive crawler",
			pipeline: pipelineWithStages(
				driveStage(operatorv1alpha1.TypeGoogleDrive, folder),
			),
			wantStageName: "crawl",
		},
		{
			// The corpus and the credential both come from the Drive crawler,
			// so another source offers neither.
			name: "an S3 crawler",
			pipeline: pipelineWithStages(
				driveStage(operatorv1alpha1.TypeS3, ""),
			),
			wantNil: true,
		},
		{
			name: "a Drive crawler with no googleDriveConfig",
			pipeline: pipelineWithStages(
				driveStage(operatorv1alpha1.TypeGoogleDrive, ""),
			),
			wantNil: true,
		},
		{
			name:     "a pipeline with no stages",
			pipeline: pipelineWithStages(),
			wantNil:  true,
		},
		{
			// The stage name locates the files in the destination bucket, so
			// the right one has to come back, not just a non-nil config.
			name: "the Drive stage is found among others",
			pipeline: pipelineWithStages(
				operatorv1alpha1.PipelineStage{Name: "convert", Type: operatorv1alpha1.StageTypeDocumentProcessor},
				driveStage(operatorv1alpha1.TypeGoogleDrive, folder),
			),
			wantStageName: "crawl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, stage := driveSearchStage(tt.pipeline)

			if tt.wantNil {
				assert.Nil(t, got)
				assert.Empty(t, stage.Name)
				return
			}
			require.NotNil(t, got)
			require.Len(t, got.Folders, 1)
			assert.Equal(t, folder, got.Folders[0].URL)
			// The whole stage comes back so the caller reads its query config
			// from the same place the folders came from.
			assert.Equal(t, tt.wantStageName, stage.Name)
		})
	}
}
