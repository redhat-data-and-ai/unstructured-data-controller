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
	SQSInformerCondition = "SQSInformerReady"
)

// SQSInformerSpec defines the desired state of SQSInformer.
type SQSInformerSpec struct {
	QueueURL string `json:"queueURL,omitempty"`
}

// SQSInformerStatus defines the observed state of SQSInformer.
type SQSInformerStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="SQSInformerReady")].status`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.conditions[?(@.type=="SQSInformerReady")].message`

// SQSInformer is the Schema for the sqsinformers API.
type SQSInformer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SQSInformerSpec   `json:"spec,omitempty"`
	Status SQSInformerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SQSInformerList contains a list of SQSInformer.
type SQSInformerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SQSInformer `json:"items"`
}

func (c *SQSInformer) SetWaiting() {
	condition := metav1.Condition{
		Type:               SQSInformerCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "SQSInformer is getting reconciled",
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

// UpdateStatus updates the status of the SQSInformer CR
// It takes a message and an error, and updates the status of the CR accordingly
// If the error is nil, the status is set to True and the message is set to the message
// If the error is not nil, the status is set to False and the message is set to the message and the error
func (c *SQSInformer) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               SQSInformerCondition,
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
	SchemeBuilder.Register(&SQSInformer{}, &SQSInformerList{})
}
