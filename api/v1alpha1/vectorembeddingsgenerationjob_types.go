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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	VectorEmbeddingGenerationConditionType = "VectorEmbeddingGenerationReady"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VectorEmbeddingsGenerationJobSpec defines the desired state of VectorEmbeddingsGenerationJob.
type VectorEmbeddingsGenerationJobSpec struct {
	DataProduct              string                             `json:"dataProduct,omitempty"`
	EmbeddingGeneratorConfig embedding.EmbeddingGeneratorConfig `json:"embeddingGeneratorConfig,omitempty"`
	ApiKey                   string                             `json:"apiKey,omitempty"`
	AwsSecret                string                             `json:"awsSecret,omitempty"`
}

// VectorEmbeddingsGenerationJobStatus defines the observed state of VectorEmbeddingsGenerationJob.
type VectorEmbeddingsGenerationJobStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// VectorEmbeddingsGenerationJob is the Schema for the vectorembeddingsgenerationjobs API.
type VectorEmbeddingsGenerationJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VectorEmbeddingsGenerationJobSpec   `json:"spec,omitempty"`
	Status VectorEmbeddingsGenerationJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VectorEmbeddingsGenerationJobList contains a list of VectorEmbeddingsGenerationJob.
type VectorEmbeddingsGenerationJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VectorEmbeddingsGenerationJob `json:"items"`
}

func (c *VectorEmbeddingsGenerationJob) SetWaiting() {
	condition := metav1.Condition{
		Type:               VectorEmbeddingGenerationConditionType,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "VectorEmbeddingsGenerationJob is getting reconciled",
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

func (c *VectorEmbeddingsGenerationJob) UpdateStatus(message string, err error) {
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

func init() {
	SchemeBuilder.Register(&VectorEmbeddingsGenerationJob{}, &VectorEmbeddingsGenerationJobList{})
}
