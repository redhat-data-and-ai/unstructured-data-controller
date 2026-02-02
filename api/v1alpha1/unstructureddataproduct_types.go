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
)

type (
	UnstructuredDataSourceType      string
	UnstructuredDataDestinationType string
	ChunkingStrategy                string
)

const (
	SourceTypeS3                       UnstructuredDataSourceType      = "s3"
	DestinationTypeInternalStage       UnstructuredDataDestinationType = "snowflakeInternalStage"
	ChunkingStrategyRecursiveCharacter ChunkingStrategy                = "recursiveCharacterTextSplitter"
	ChunkingStrategyMarkdown           ChunkingStrategy                = "markdownTextSplitter"
	ChunkingStrategyToken              ChunkingStrategy                = "tokenTextSplitter"

	UnstructuredDataProductCondition = "UnstructuredDataProductReady"
)

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
	ReturnAsFile    bool     `json:"return_as_file,omitempty"`
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

// UnstructuredDataProductSpec defines the desired state of UnstructuredDataProduct
type UnstructuredDataProductSpec struct {
	SourceConfig            SourceConfig            `json:"sourceConfig,omitempty"`
	DestinationConfig       DestinationConfig       `json:"destinationConfig,omitempty"`
	DocumentProcessorConfig DocumentProcessorConfig `json:"documentProcessorConfig,omitempty"`
	ChunksGeneratorConfig   ChunksGeneratorConfig   `json:"chunksGeneratorConfig,omitempty"`
}

type SourceConfig struct {
	Type     UnstructuredDataSourceType `json:"type,omitempty"`
	S3Config S3Config                   `json:"s3Config,omitempty"`
}

type S3Config struct {
	Bucket string `json:"bucket,omitempty"`
	Prefix string `json:"prefix,omitempty"`
}

type DestinationConfig struct {
	Type                         UnstructuredDataDestinationType `json:"type,omitempty"`
	SnowflakeInternalStageConfig SnowflakeInternalStageConfig    `json:"snowflakeInternalStageConfig,omitempty"`
}

type SnowflakeInternalStageConfig struct {
	Stage    string `json:"stage,omitempty"`
	Database string `json:"database,omitempty"`
	Schema   string `json:"schema,omitempty"`
}

// UnstructuredDataProductStatus defines the observed state of UnstructuredDataProduct
type UnstructuredDataProductStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="UnstructuredDataProductReady")].status`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.conditions[?(@.type=="UnstructuredDataProductReady")].message`

// UnstructuredDataProduct is the Schema for the unstructureddataproducts API
type UnstructuredDataProduct struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UnstructuredDataProductSpec   `json:"spec,omitempty"`
	Status UnstructuredDataProductStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// UnstructuredDataProductList contains a list of UnstructuredDataProduct
type UnstructuredDataProductList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UnstructuredDataProduct `json:"items"`
}

func (u *UnstructuredDataProduct) SetWaiting() {
	condition := metav1.Condition{
		Type:               UnstructuredDataProductCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "UnstructuredDataProduct is getting reconciled",
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

func (u *UnstructuredDataProduct) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               UnstructuredDataProductCondition,
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
	SchemeBuilder.Register(&UnstructuredDataProduct{}, &UnstructuredDataProductList{})
}
