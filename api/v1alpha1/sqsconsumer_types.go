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
	SQSConsumerCondition = "SQSConsumerReady"
)

// SQSConsumerSpec defines the desired state of SQSConsumer
type SQSConsumerSpec struct {
	QueueURL           string `json:"queueURL,omitempty"`
	AWSSecret          string `json:"awsSecret,omitempty"`
	AWSSecretNamespace string `json:"awsSecretNamespace,omitempty"` // Optional: defaults to CR's namespace if not specified
}

// SQSConsumerStatus defines the observed state of SQSConsumer
type SQSConsumerStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="SQSConsumerReady")].status`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.conditions[?(@.type=="SQSConsumerReady")].message`

// SQSConsumer is the Schema for the sqsconsumers API
type SQSConsumer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SQSConsumerSpec   `json:"spec,omitempty"`
	Status SQSConsumerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SQSConsumerList contains a list of SQSConsumer
type SQSConsumerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SQSConsumer `json:"items"`
}

func (c *SQSConsumer) SetWaiting() {
	condition := metav1.Condition{
		Type:               SQSConsumerCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "SQSConsumer is getting reconciled",
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

// UpdateStatus updates the status of the SQSConsumer CR
// It takes a message and an error, and updates the status of the CR accordingly
// If the error is nil, the status is set to True and the message is set to the message
// If the error is not nil, the status is set to False and the message is set to the message and the error
func (c *SQSConsumer) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               SQSConsumerCondition,
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
	SchemeBuilder.Register(&SQSConsumer{}, &SQSConsumerList{})
}
