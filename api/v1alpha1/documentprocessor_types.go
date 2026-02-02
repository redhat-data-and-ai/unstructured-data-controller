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
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DocumentProcessorCondition = "DocumentProcessorReady"
)

// DocumentProcessorSpec defines the desired state of DocumentProcessor
type DocumentProcessorSpec struct {
	DataProduct             string                  `json:"dataProduct,omitempty"`
	DocumentProcessorConfig DocumentProcessorConfig `json:"config,omitempty"`
}

// DocumentProcessorStatus defines the observed state of DocumentProcessor
type DocumentProcessorStatus struct {
	LastAppliedGeneration   int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions              []metav1.Condition `json:"conditions,omitempty"`
	Jobs                    []Job              `json:"jobs,omitempty"`
	PermanentlyFailingFiles []string           `json:"permanentlyFailingFiles,omitempty"`
}

type Job struct {
	FilePath          string        `json:"filePath,omitempty"`
	DocumentConverter string        `json:"documentConverter,omitempty"`
	DoclingConfig     DoclingConfig `json:"doclingConfig,omitempty"`
	TaskID            string        `json:"taskID,omitempty"`
	Status            string        `json:"status,omitempty"`
	CreatedOn         string        `json:"createdOn,omitempty"`
	Attempts          int           `json:"attempts,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"DocumentProcessorReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"DocumentProcessorReady\")].message"

// DocumentProcessor is the Schema for the documentprocessors API
type DocumentProcessor struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DocumentProcessorSpec   `json:"spec,omitempty"`
	Status DocumentProcessorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DocumentProcessorList contains a list of DocumentProcessor
type DocumentProcessorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DocumentProcessor `json:"items"`
}

func (d *DocumentProcessor) SetWaiting() {
	condition := metav1.Condition{
		Type:               DocumentProcessorCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "DocumentProcessor is getting reconciled",
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

func (d *DocumentProcessor) AddOrUpdateJob(newJob Job) {
	for i, job := range d.Status.Jobs {
		if job.FilePath == newJob.FilePath {
			d.Status.Jobs[i] = newJob
			return
		}
	}
	d.Status.Jobs = append(d.Status.Jobs, newJob)
}

func (d *DocumentProcessor) GetJobByFilePath(filePath string) *Job {
	for _, job := range d.Status.Jobs {
		if job.FilePath == filePath {
			return &job
		}
	}
	return nil
}

func (d *DocumentProcessor) DeleteJobByFilePath(filePath string) {
	newJobs := []Job{}
	for _, job := range d.Status.Jobs {
		if job.FilePath != filePath {
			newJobs = append(newJobs, job)
		}
	}
	d.Status.Jobs = newJobs
}

func (d *DocumentProcessor) AddPermanentlyFailingFile(filePath string) {
	if slices.Contains(d.Status.PermanentlyFailingFiles, filePath) {
		return
	}
	d.Status.PermanentlyFailingFiles = append(d.Status.PermanentlyFailingFiles, filePath)
}

func (d *DocumentProcessor) IsFilePermanentlyFailing(filePath string) bool {
	return slices.Contains(d.Status.PermanentlyFailingFiles, filePath)
}

func (d *DocumentProcessor) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               DocumentProcessorCondition,
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

func init() {
	SchemeBuilder.Register(&DocumentProcessor{}, &DocumentProcessorList{})
}
