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
	GoogleDriveInformerCondition = "GoogleDriveInformerReady"
)

type Folder struct {
	ID string `json:"id"`
}

// GoogleDriveInformerSpec defines the desired state of GoogleDriveInformer.
type GoogleDriveInformerSpec struct {
	DataProduct string   `json:"dataProduct"`
	Folders     []Folder `json:"folders"`
}

// GoogleDriveInformerStatus defines the observed state of GoogleDriveInformer.
type GoogleDriveInformerStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	PageToken             string             `json:"pageToken,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="GoogleDriveInformerReady")].status`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.conditions[?(@.type=="GoogleDriveInformerReady")].message`

// GoogleDriveInformer is the Schema for the googledriveinformers API.
type GoogleDriveInformer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GoogleDriveInformerSpec   `json:"spec,omitempty"`
	Status GoogleDriveInformerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GoogleDriveInformerList contains a list of GoogleDriveInformer.
type GoogleDriveInformerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GoogleDriveInformer `json:"items"`
}

func (g *GoogleDriveInformer) SetWaiting() {
	condition := metav1.Condition{
		Type:               GoogleDriveInformerCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "GoogleDriveInformer is getting reconciled",
		Reason:             "Waiting",
	}
	for i, currentCondition := range g.Status.Conditions {
		if currentCondition.Type == condition.Type {
			g.Status.Conditions[i] = condition
			return
		}
	}
	g.Status.Conditions = append(g.Status.Conditions, condition)
}

func (g *GoogleDriveInformer) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               GoogleDriveInformerCondition,
		LastTransitionTime: metav1.Now(),
	}
	if err == nil {
		condition.Status = metav1.ConditionTrue
		condition.Message = message
		condition.Reason = SuccessfullyReconciled
		g.Status.LastAppliedGeneration = g.Generation
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Message = message + ", error: " + err.Error()
		condition.Reason = ReconcileFailed
	}

	for i, currentCondition := range g.Status.Conditions {
		if currentCondition.Type == condition.Type {
			g.Status.Conditions[i] = condition
			return
		}
	}
	g.Status.Conditions = append(g.Status.Conditions, condition)
}

func init() {
	SchemeBuilder.Register(&GoogleDriveInformer{}, &GoogleDriveInformerList{})
}
