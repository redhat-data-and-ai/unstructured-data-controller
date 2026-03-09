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

const (
	VectorEmbeddingGenerationConditionType = "VectorEmbeddingGenerationReady"
)

// VectorEmbeddingsGeneratorSpec defines the desired state of VectorEmbeddingsGenerator.
type VectorEmbeddingsGeneratorSpec struct {
	DataProduct                     string                          `json:"dataProduct,omitempty"`
	VectorEmbeddingsGeneratorConfig VectorEmbeddingsGeneratorConfig `json:"embeddingGeneratorConfig,omitempty"`
}

// VectorEmbeddingsGeneratorStatus defines the observed state of VectorEmbeddingsGenerator.
type VectorEmbeddingsGeneratorStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"VectorEmbeddingGenerationReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"VectorEmbeddingGenerationReady\")].message"

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

func init() {
	SchemeBuilder.Register(&VectorEmbeddingsGenerator{}, &VectorEmbeddingsGeneratorList{})
}
