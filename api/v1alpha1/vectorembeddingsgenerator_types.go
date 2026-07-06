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

type EmbeddingProvider string

// sample spec:
//
//	spec:
//	  stageName: embed
//	  dependsOn:
//	    - name: chunk
//	  embeddingGeneratorConfig:
//	    modelName: nomic-embed-text-v1.5
//	status:
//	  conditions:
//	    - type: VectorEmbeddingGenerationReady
//	      status: "True"
//	      message: successfully reconciled

const (
	VectorEmbeddingGenerationConditionType = "VectorEmbeddingGenerationReady"
	DefaultEmbeddingModelName              = "nomic-ai/nomic-embed-text-v1.5"
	DefaultEmbeddingBatchSize              = 1000
)

// VectorEmbeddingsGeneratorSpec defines the desired state of VectorEmbeddingsGenerator.
type VectorEmbeddingsGeneratorSpec struct {
	StageName                       string                          `json:"stageName,omitempty"`
	DependsOn                       []StageDependency               `json:"dependsOn,omitempty"`
	VectorEmbeddingsGeneratorConfig VectorEmbeddingsGeneratorConfig `json:"embeddingGeneratorConfig,omitempty"`
	// Deprecated: use StageName and DependsOn instead.
	// +optional
	DataProduct string `json:"dataProduct,omitempty"`
}

// VectorEmbeddingsGeneratorStatus defines the observed state of VectorEmbeddingsGenerator.
type VectorEmbeddingsGeneratorStatus struct {
	LastAppliedGeneration int64                            `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition               `json:"conditions,omitempty"`
	FilesProcessed        int64                            `json:"filesProcessed,omitempty"`
	AppliedConfig         *VectorEmbeddingsGeneratorConfig `json:"appliedConfig,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"VectorEmbeddingGenerationReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"VectorEmbeddingGenerationReady\")].message"
// +kubebuilder:printcolumn:name="Files",type="integer",JSONPath=".status.filesProcessed"

// VectorEmbeddingsGenerator is the Schema for the vectorembeddingsgenerators API.
type VectorEmbeddingsGenerator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VectorEmbeddingsGeneratorSpec   `json:"spec,omitempty"`
	Status VectorEmbeddingsGeneratorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VectorEmbeddingsGeneratorList contains a list of VectorEmbeddingsGenerator.
type VectorEmbeddingsGeneratorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VectorEmbeddingsGenerator `json:"items"`
}

func (c *VectorEmbeddingsGenerator) GetFilesProcessed() int64 {
	return c.Status.FilesProcessed
}

func (c *VectorEmbeddingsGenerator) SetWaiting() {
	condition := metav1.Condition{
		Type:               VectorEmbeddingGenerationConditionType,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "VectorEmbeddingsGenerator is getting reconciled",
		Reason:             "Waiting",
	}
	for i, currentCondition := range c.Status.Conditions {
		if currentCondition.Type == condition.Type {
			c.Status.Conditions[i] = condition
			return
		}
	}
	c.Status.Conditions = append(c.Status.Conditions, condition)
}

func (c *VectorEmbeddingsGenerator) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               VectorEmbeddingGenerationConditionType,
		LastTransitionTime: metav1.Now(),
	}
	if err == nil {
		condition.Status = metav1.ConditionTrue
		condition.Message = message
		condition.Reason = SuccessfullyReconciled
		c.Status.LastAppliedGeneration = c.Generation
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Message = message + ", error: " + err.Error()
		condition.Reason = ReconcileFailed
	}

	for i, currentCondition := range c.Status.Conditions {
		if currentCondition.Type == condition.Type {
			c.Status.Conditions[i] = condition
			return
		}
	}
	c.Status.Conditions = append(c.Status.Conditions, condition)
}

type VectorEmbeddingsGeneratorConfig struct {
	ModelName string `json:"modelName,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +optional
	BatchSize               int                     `json:"batchSize,omitempty"`
	NomicEmbedTextV15Config NomicEmbedTextV15Config `json:"nomicEmbedTextV15Config,omitempty"`
}

type NomicEmbedTextV15Config struct {
	EncodingFormat string `json:"encodingformat,omitempty"`
}

// SetDefaults fills in sane defaults for any unset fields.
func (c *VectorEmbeddingsGeneratorConfig) SetDefaults() {
	if c.ModelName == "" {
		c.ModelName = DefaultEmbeddingModelName
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultEmbeddingBatchSize
	}
}

func init() {
	SchemeBuilder.Register(&VectorEmbeddingsGenerator{}, &VectorEmbeddingsGeneratorList{})
}
