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

// sample spec (S3):
//
//	spec:
//	  stageName: crawl
//	  sourceCrawlerConfig:
//	    type: s3
//	    s3Config:
//	      bucket: data-ingestion-bucket
//	      prefix: documents/
//	      sqsQueueURL: https://sqs...        # optional, enables real-time S3 notifications
//
// sample spec (Google Drive):
//
//	spec:
//	  stageName: crawl
//	  sourceCrawlerConfig:
//	    type: googleDrive
//	    googleDriveConfig:
//	      folders:
//	        - url: "https://drive.google.com/drive/folders/1ABCdef_example_folder_id"
//	      skipFolders:
//	        - pattern: ".archive"
//	status:
//	  conditions:
//	    - type: SourceCrawlerReady
//	      status: "True"                     # True | False | Unknown
//	      message: successfully reconciled

const (
	SourceCrawlerCondition = "SourceCrawlerReady"
)

// SourceCrawlerSpec defines the desired state of SourceCrawler.
type SourceCrawlerSpec struct {
	StageName           string              `json:"stageName,omitempty"`
	SecretRef           string              `json:"secretRef,omitempty"`
	DependsOn           []StageDependency   `json:"dependsOn,omitempty"`
	SourceCrawlerConfig SourceCrawlerConfig `json:"sourceCrawlerConfig,omitempty"`
}

type GDriveFolderStatus struct {
	URL   string `json:"url"`
	Error string `json:"error,omitempty"`
}

// SourceCrawlerStatus defines the observed state of SourceCrawler.
type SourceCrawlerStatus struct {
	LastAppliedGeneration int64                `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition   `json:"conditions,omitempty"`
	FilesProcessed        int64                `json:"filesProcessed,omitempty"`
	GDriveStatus          []GDriveFolderStatus `json:"gdriveStatus,omitempty"`
	GitHeadHash           string               `json:"gitHeadHash,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"SourceCrawlerReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"SourceCrawlerReady\")].message"
// +kubebuilder:printcolumn:name="Files",type="integer",JSONPath=".status.filesProcessed"

// SourceCrawler is the Schema for the sourcecrawlers API.
type SourceCrawler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SourceCrawlerSpec   `json:"spec,omitempty"`
	Status SourceCrawlerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SourceCrawlerList contains a list of SourceCrawler.
type SourceCrawlerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SourceCrawler `json:"items"`
}

func (c *SourceCrawler) GetFilesProcessed() int64 {
	return c.Status.FilesProcessed
}

func (c *SourceCrawler) SetWaiting() {
	condition := metav1.Condition{
		Type:               SourceCrawlerCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "SourceCrawler is getting reconciled",
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

func (c *SourceCrawler) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               SourceCrawlerCondition,
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
	SchemeBuilder.Register(&SourceCrawler{}, &SourceCrawlerList{})
}
