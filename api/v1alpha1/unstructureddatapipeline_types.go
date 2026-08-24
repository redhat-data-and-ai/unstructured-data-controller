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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// sample spec (S3 source):
//
//	spec:
//	  secretRef: pipeline-secret             # k8s secret with source/destination AWS credentials
//	  stages:
//	    - name: crawl
//	      type: SourceCrawler
//	      sourceCrawlerConfig:
//	        type: s3
//	        s3Config:
//	          bucket: data-ingestion-bucket
//	          prefix: documents/
//
// sample spec (Google Drive source):
//
//	spec:
//	  secretRef: pipeline-secret             # k8s secret with GOOGLE_SERVICE_ACCOUNT_JSON + destination AWS credentials
//	  stages:
//	    - name: crawl
//	      type: SourceCrawler
//	      sourceCrawlerConfig:
//	        type: googleDrive
//	        googleDriveConfig:
//	          folders:
//	            - url: "https://drive.google.com/drive/folders/1ABCdef_example_folder_id"
//	          skipFolders:
//	            - pattern: ".archive"
//	    - name: convert
//	      type: DocumentProcessor
//	      dependsOn:
//	        - name: crawl
//	    - name: chunk
//	      type: ChunksGenerator
//	      dependsOn: [convert]
//	    - name: embed
//	      type: VectorEmbeddingsGenerator
//	      dependsOn: [chunk]
//	    - name: sync
//	      type: DestinationSyncer
//	      dependsOn: [embed]
//	      destinationSyncerConfig:
//	        type: s3
//	        s3DestinationConfig:
//	          bucket: output-bucket
//	status:
//	  stages:                                # tracks child CR creation
//	    - name: crawl
//	      created: true
//	    - name: convert
//	      created: true
//	  conditions:
//	    - type: UnstructuredDataPipelineReady
//	      status: "True"

type (
	UnstructuredDataType string
	ChunkingStrategy     string
)

const (
	TypeS3                             UnstructuredDataType = "s3"
	TypeGoogleDrive                    UnstructuredDataType = "googleDrive"
	ChunkingStrategyRecursiveCharacter ChunkingStrategy     = "recursiveCharacterTextSplitter"
	ChunkingStrategyMarkdown           ChunkingStrategy     = "markdownTextSplitter"
	ChunkingStrategyToken              ChunkingStrategy     = "tokenTextSplitter"

	UnstructuredDataPipelineCondition = "UnstructuredDataPipelineReady"
)

// StageType is the discriminator for the stage union config.
// +kubebuilder:validation:Enum=SourceCrawler;DocumentProcessor;ChunksGenerator;VectorEmbeddingsGenerator;DestinationSyncer
type StageType string

const (
	StageTypeSourceCrawler             StageType = "SourceCrawler"
	StageTypeDocumentProcessor         StageType = "DocumentProcessor"
	StageTypeChunksGenerator           StageType = "ChunksGenerator"
	StageTypeVectorEmbeddingsGenerator StageType = "VectorEmbeddingsGenerator"
	StageTypeDestinationSyncer         StageType = "DestinationSyncer"
)

// +kubebuilder:object:generate=false
type StageMapping struct {
	Type       StageType
	Object     client.Object
	ObjectList client.ObjectList
}

func ListStages() []StageMapping {
	return []StageMapping{
		{StageTypeSourceCrawler, &SourceCrawler{}, &SourceCrawlerList{}},
		{StageTypeDocumentProcessor, &DocumentProcessor{}, &DocumentProcessorList{}},
		{StageTypeChunksGenerator, &ChunksGenerator{}, &ChunksGeneratorList{}},
		{StageTypeVectorEmbeddingsGenerator, &VectorEmbeddingsGenerator{}, &VectorEmbeddingsGeneratorList{}},
		{StageTypeDestinationSyncer, &DestinationSyncer{}, &DestinationSyncerList{}},
	}
}

// PipelineStage defines a single step in the pipeline DAG.
// +kubebuilder:validation:XValidation:rule="self.type == 'SourceCrawler' ? has(self.sourceCrawlerConfig) : true",message="sourceCrawlerConfig is required when type is SourceCrawler"
// +kubebuilder:validation:XValidation:rule="self.type == 'DestinationSyncer' ? has(self.destinationSyncerConfig) : true",message="destinationSyncerConfig is required when type is DestinationSyncer"
type PipelineStage struct {
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// +kubebuilder:validation:Required
	Type StageType `json:"type"`

	// +optional
	DependsOn []StageDependency `json:"dependsOn,omitempty"`

	// +optional
	SourceCrawlerConfig *SourceCrawlerConfig `json:"sourceCrawlerConfig,omitempty"`
	// +optional
	DocumentProcessorConfig *DocumentProcessorConfig `json:"documentProcessorConfig,omitempty"`
	// +optional
	ChunksGeneratorConfig *ChunksGeneratorConfig `json:"chunksGeneratorConfig,omitempty"`
	// +optional
	VectorEmbeddingsGeneratorConfig *VectorEmbeddingsGeneratorConfig `json:"vectorEmbeddingsGeneratorConfig,omitempty"`
	// +optional
	DestinationSyncerConfig *DestinationSyncerConfig `json:"destinationSyncerConfig,omitempty"`

	// +optional
	QueryConfig *QueryConfig `json:"queryConfig,omitempty"`
}

// SourceCrawlerConfig configures where to read unstructured data from.
type SourceCrawlerConfig struct {
	Type              UnstructuredDataType `json:"type,omitempty"`
	S3Config          S3Config             `json:"s3Config,omitempty"`
	GoogleDriveConfig *GoogleDriveConfig   `json:"googleDriveConfig,omitempty"`
}

// GDriveConfig configures Google Drive folder crawling at the pipeline level.
// Controller-level settings (maxRetries, concurrency, LDAP) are in ControllerConfig.
type GoogleDriveConfig struct {
	// FolderIDs is the list of Google Drive folder IDs to crawl recursively.
	// +kubebuilder:validation:MinItems=1
	Folders []GoogleDriveFolders `json:"folders"`
	// SkipFolderNames is an optional list of folder names to skip during crawling.
	// +optional
	SkipFolders []SkipFolders `json:"skipFolders,omitempty"`
}

type GoogleDriveFolders struct {
	URL string `json:"url"`
}

type SkipFolders struct {
	Pattern string `json:"pattern"`
}

// DestinationSyncerConfig configures where to write processed data.
type DestinationSyncerConfig struct {
	Type                UnstructuredDataType `json:"type,omitempty"`
	S3DestinationConfig S3Config             `json:"s3DestinationConfig,omitempty"`
}

// StageDependency identifies an upstream stage by name.
type StageDependency struct {
	Name string `json:"name"`
}

// QueryEndpointType identifies the type of query endpoint.
// +kubebuilder:validation:Enum=snowflake
type QueryEndpointType string

const (
	QueryEndpointTypeSnowflake QueryEndpointType = "snowflake"
)

// QueryConfig describes where a stage's output data can be queried.
// This is purely informational metadata for the MCP server — the pipeline
// controller does not act on it or create the query endpoint.
// +kubebuilder:validation:XValidation:rule="self.type == 'snowflake' ? has(self.snowflake) : true",message="snowflake is required when type is snowflake"
type QueryConfig struct {
	// +kubebuilder:validation:Required
	Type QueryEndpointType `json:"type"`
	// +optional
	Snowflake *SnowflakeQueryConfig `json:"snowflake,omitempty"`
}

// SnowflakeQueryConfig contains the connection details for a Snowflake query endpoint.
type SnowflakeQueryConfig struct {
	Account  string `json:"account"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
}

// UnstructuredDataPipelineSpec defines the desired state of UnstructuredDataPipeline
type UnstructuredDataPipelineSpec struct {
	// Description is a human-readable summary of what the pipeline does.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Description string `json:"description"`
	// Guidance is instructions for the AI agent on how to use this pipeline's data.
	// +optional
	Guidance string `json:"guidance,omitempty"`
	// +optional
	SecretRef string `json:"secretRef,omitempty"`
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	Stages []PipelineStage `json:"stages"`
}

// S3Config configures an S3 bucket and optional prefix.
type S3Config struct {
	Bucket      string `json:"bucket"`
	Prefix      string `json:"prefix,omitempty"`
	SQSQueueURL string `json:"sqsQueueURL,omitempty"`
}

// StageCreationStatus tracks whether a child CR has been created for a stage.
type StageCreationStatus struct {
	Name    string `json:"name"`
	Created bool   `json:"created"`
}

// UnstructuredDataPipelineStatus defines the observed state of UnstructuredDataPipeline
type UnstructuredDataPipelineStatus struct {
	LastAppliedGeneration int64                 `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition    `json:"conditions,omitempty"`
	Stages                []StageCreationStatus `json:"stages,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="UnstructuredDataPipelineReady")].status`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.conditions[?(@.type=="UnstructuredDataPipelineReady")].message`

// UnstructuredDataPipeline is the Schema for the unstructureddatapipelines API
type UnstructuredDataPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UnstructuredDataPipelineSpec   `json:"spec,omitempty"`
	Status UnstructuredDataPipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// UnstructuredDataPipelineList contains a list of UnstructuredDataPipeline
type UnstructuredDataPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UnstructuredDataPipeline `json:"items"`
}

func (u *UnstructuredDataPipeline) SetWaiting() {
	condition := metav1.Condition{
		Type:               UnstructuredDataPipelineCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "UnstructuredDataPipeline is getting reconciled",
		Reason:             "Waiting",
	}
	for i, currentCondition := range u.Status.Conditions {
		if currentCondition.Type == condition.Type {
			u.Status.Conditions[i] = condition
			return
		}
	}
	u.Status.Conditions = append(u.Status.Conditions, condition)
}

func (u *UnstructuredDataPipeline) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               UnstructuredDataPipelineCondition,
		LastTransitionTime: metav1.Now(),
	}
	if err == nil {
		condition.Status = metav1.ConditionTrue
		condition.Message = message
		condition.Reason = SuccessfullyReconciled
		u.Status.LastAppliedGeneration = u.Generation
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Message = message + ", error: " + err.Error()
		condition.Reason = ReconcileFailed
	}

	for i, currentCondition := range u.Status.Conditions {
		if currentCondition.Type == condition.Type {
			u.Status.Conditions[i] = condition
			return
		}
	}
	u.Status.Conditions = append(u.Status.Conditions, condition)
}

func init() {
	SchemeBuilder.Register(&UnstructuredDataPipeline{}, &UnstructuredDataPipelineList{})
}
