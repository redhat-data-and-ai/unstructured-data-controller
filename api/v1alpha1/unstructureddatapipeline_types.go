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

// sample spec:
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
//	    - name: convert
//	      type: DocumentProcessor
//	      dependsOn:
//	        - name: crawl
//	      documentProcessorConfig:
//	        type: docling
//	    - name: chunk
//	      type: ChunksGenerator
//	      dependsOn: [convert]
//	      chunksGeneratorConfig:
//	        strategy: recursiveCharacterTextSplitter
//	    - name: embed
//	      type: VectorEmbeddingsGenerator
//	      dependsOn: [chunk]
//	      vectorEmbeddingsGeneratorConfig:
//	        modelName: nomic-embed-text-v1.5
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
	TypeGDrive                         UnstructuredDataType = "gdrive"
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
// +kubebuilder:validation:XValidation:rule="self.type == 'DocumentProcessor' ? has(self.documentProcessorConfig) : true",message="documentProcessorConfig is required when type is DocumentProcessor"
// +kubebuilder:validation:XValidation:rule="self.type == 'ChunksGenerator' ? has(self.chunksGeneratorConfig) : true",message="chunksGeneratorConfig is required when type is ChunksGenerator"
// +kubebuilder:validation:XValidation:rule="self.type == 'VectorEmbeddingsGenerator' ? has(self.vectorEmbeddingsGeneratorConfig) : true",message="vectorEmbeddingsGeneratorConfig is required when type is VectorEmbeddingsGenerator"
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
	Type         UnstructuredDataType `json:"type,omitempty"`
	S3Config     S3Config             `json:"s3Config,omitempty"`
	GDriveConfig *GDriveConfig        `json:"gdriveConfig,omitempty"`
}

// GDriveConfig configures Google Drive folder crawling at the pipeline level.
// Controller-level settings (maxRetries, concurrency, LDAP) are in ControllerConfig.
type GDriveConfig struct {
	// FolderIDs is the list of Google Drive folder IDs to crawl recursively.
	// +kubebuilder:validation:MinItems=1
	FolderIDs []string `json:"folderIDs"`
	// SkipFolderNames is an optional list of folder names to skip during crawling.
	// +optional
	SkipFolderNames []string `json:"skipFolderNames,omitempty"`
}

// LDAPConfig holds configuration for connecting to an LDAP server for identity resolution.
type LDAPConfig struct {
	// Server is the LDAP server URL (e.g., "ldap://ldap.example.com:389").
	Server string `json:"server"`
	// GroupDN is the base DN for group searches.
	// +optional
	GroupDN string `json:"groupDN,omitempty"`
	// UserDN is the DN template for user lookups (must contain %s for the user ID).
	// +optional
	UserDN string `json:"userDN,omitempty"`
	// BaseUserDN is the base DN for user searches.
	// +optional
	BaseUserDN string `json:"baseUserDN,omitempty"`
	// UserSearchFilter is the LDAP filter for user searches (e.g., "(objectClass=person)").
	// +optional
	UserSearchFilter string `json:"userSearchFilter,omitempty"`
	// EmailAttribute is the LDAP attribute containing user email addresses (e.g., "mail").
	// +optional
	EmailAttribute string `json:"emailAttribute,omitempty"`
	// Attributes is the list of LDAP attributes to retrieve.
	// +optional
	Attributes []string `json:"attributes,omitempty"`
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

type DocumentProcessorConfig struct {
	Type          string        `json:"type,omitempty"`
	DoclingConfig DoclingConfig `json:"doclingConfig,omitempty"`
}

type DoclingConfig struct {
	FromFormats     []string `json:"from_formats,omitempty"`
	ToFormats       []string `json:"to_formats,omitempty"`
	ImageExportMode string   `json:"image_export_mode,omitempty"`
	DoOCR           bool     `json:"do_ocr,omitempty"`
	ForceOCR        bool     `json:"force_ocr,omitempty"`
	OCREngine       string   `json:"ocr_engine,omitempty"`
	OCRLang         []string `json:"ocr_lang,omitempty"`
	PDFBackend      string   `json:"pdf_backend,omitempty"`
	TableMode       string   `json:"table_mode,omitempty"`
	AbortOnError    bool     `json:"abort_on_error,omitempty"`
}

type ChunksGeneratorConfig struct {
	Strategy                         ChunkingStrategy                 `json:"strategy"`
	RecursiveCharacterSplitterConfig RecursiveCharacterSplitterConfig `json:"recursiveCharacterSplitterConfig,omitempty"`
	MarkdownSplitterConfig           MarkdownSplitterConfig           `json:"markdownSplitterConfig,omitempty"`
	TokenSplitterConfig              TokenSplitterConfig              `json:"tokenSplitterConfig,omitempty"`
}

type RecursiveCharacterSplitterConfig struct {
	Separators    []string `json:"separators,omitempty"`
	ChunkSize     int      `json:"chunkSize,omitempty"`
	ChunkOverlap  int      `json:"chunkOverlap,omitempty"`
	KeepSeparator bool     `json:"keepSeparator,omitempty"`
}

type MarkdownSplitterConfig struct {
	ChunkSize        int  `json:"chunkSize,omitempty"`
	ChunkOverlap     int  `json:"chunkOverlap,omitempty"`
	CodeBlocks       bool `json:"codeBlocks,omitempty"`
	ReferenceLinks   bool `json:"referenceLinks,omitempty"`
	HeadingHierarchy bool `json:"headingHierarchy,omitempty"`
	JoinTableRows    bool `json:"joinTableRows,omitempty"`
}

type TokenSplitterConfig struct {
	ChunkSize         int      `json:"chunkSize,omitempty"`
	ChunkOverlap      int      `json:"chunkOverlap,omitempty"`
	ModelName         string   `json:"modelName,omitempty"`
	EncodingName      string   `json:"encodingName,omitempty"`
	AllowedSpecial    []string `json:"allowedSpecial,omitempty"`
	DisallowedSpecial []string `json:"disallowedSpecial,omitempty"`
}

type VectorEmbeddingsGeneratorConfig struct {
	ModelName               string                  `json:"modelName,omitempty"`
	NomicEmbedTextV15Config NomicEmbedTextV15Config `json:"nomicEmbedTextV15Config,omitempty"`
}

type NomicEmbedTextV15Config struct {
	EncodingFormat string `json:"encodingformat,omitempty"`
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
