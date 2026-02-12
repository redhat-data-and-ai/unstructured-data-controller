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

const (
	ChunksGeneratorCondition = "ChunksGeneratorReady"
)

// ChunksGeneratorSpec defines the desired state of ChunksGenerator
type ChunksGeneratorSpec struct {
	DataProduct           string                `json:"dataProduct,omitempty"`
	ChunksGeneratorConfig ChunksGeneratorConfig `json:"config,omitempty"`
}

// ChunksGeneratorStatus defines the observed state of ChunksGenerator
type ChunksGeneratorStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"ChunksGeneratorReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"ChunksGeneratorReady\")].message"

// ChunksGenerator is the Schema for the chunksgenerators API
type ChunksGenerator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ChunksGeneratorSpec   `json:"spec,omitempty"`
	Status ChunksGeneratorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ChunksGeneratorList contains a list of ChunksGenerator
type ChunksGeneratorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ChunksGenerator `json:"items"`
}

func (c *ChunksGenerator) SetWaiting() {
	condition := metav1.Condition{
		Type:               ChunksGeneratorCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "ChunksGenerator is getting reconciled",
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

func (c *ChunksGenerator) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               ChunksGeneratorCondition,
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
	SchemeBuilder.Register(&ChunksGenerator{}, &ChunksGeneratorList{})
}
