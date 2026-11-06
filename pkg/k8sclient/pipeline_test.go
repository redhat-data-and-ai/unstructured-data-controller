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
