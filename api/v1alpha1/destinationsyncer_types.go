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

// sample spec:
//
//	spec:
//	  stageName: sync
//	  dependsOn:
//	    - name: embed
//	  destinationSyncerConfig:
//	    type: s3
//	    s3DestinationConfig:
//	      bucket: output-bucket
//	      prefix: processed/
//	status:
//	  conditions:
//	    - type: DestinationSyncerReady
//	      status: "True"
//	      message: successfully reconciled

const (
	DestinationSyncerCondition = "DestinationSyncerReady"
)

// DestinationSyncerSpec defines the desired state of DestinationSyncer.
type DestinationSyncerSpec struct {
	StageName               string                  `json:"stageName,omitempty"`
	SecretRef               string                  `json:"secretRef,omitempty"`
	DependsOn               []StageDependency       `json:"dependsOn,omitempty"`
	DestinationSyncerConfig DestinationSyncerConfig `json:"destinationSyncerConfig,omitempty"`
}

// DestinationSyncerStatus defines the observed state of DestinationSyncer.
type DestinationSyncerStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
	FilesProcessed        int64              `json:"filesProcessed,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"DestinationSyncerReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"DestinationSyncerReady\")].message"
// +kubebuilder:printcolumn:name="Files",type="integer",JSONPath=".status.filesProcessed"

// DestinationSyncer is the Schema for the destinationsyncers API.
type DestinationSyncer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DestinationSyncerSpec   `json:"spec,omitempty"`
	Status DestinationSyncerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DestinationSyncerList contains a list of DestinationSyncer.
type DestinationSyncerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DestinationSyncer `json:"items"`
}

func (d *DestinationSyncer) GetFilesProcessed() int64 {
	return d.Status.FilesProcessed
}

func (d *DestinationSyncer) SetWaiting() {
	condition := metav1.Condition{
		Type:               DestinationSyncerCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "DestinationSyncer is getting reconciled",
		Reason:             "Waiting",
	}
	for i, currentCondition := range d.Status.Conditions {
		if currentCondition.Type == condition.Type {
			d.Status.Conditions[i] = condition
			return
		}
	}
	d.Status.Conditions = append(d.Status.Conditions, condition)
}

func (d *DestinationSyncer) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               DestinationSyncerCondition,
		LastTransitionTime: metav1.Now(),
	}
	if err == nil {
		condition.Status = metav1.ConditionTrue
		condition.Message = message
		condition.Reason = SuccessfullyReconciled
		d.Status.LastAppliedGeneration = d.Generation
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Message = message + ", error: " + err.Error()
		condition.Reason = ReconcileFailed
	}

	for i, currentCondition := range d.Status.Conditions {
		if currentCondition.Type == condition.Type {
			d.Status.Conditions[i] = condition
			return
		}
	}
	d.Status.Conditions = append(d.Status.Conditions, condition)
}

func (d *DestinationSyncer) GetLastAppliedGeneration() int64         { return d.Status.LastAppliedGeneration }
func (d *DestinationSyncer) GetStatusConditions() []metav1.Condition { return d.Status.Conditions }

func init() {
	SchemeBuilder.Register(&DestinationSyncer{}, &DestinationSyncerList{})
}
