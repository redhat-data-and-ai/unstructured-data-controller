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
	ConfigCondition = "ConfigReady"
)

// ControllerConfigSpec defines the desired state of ControllerConfig.
type ControllerConfigSpec struct {
	SnowflakeConfig                  SnowflakeConfig                      `json:"snowflakeConfig,omitempty"`
	AWSSecret                        string                               `json:"awsSecret,omitempty"`
	UnstructuredDataProcessingConfig UnstructuredDataProcessingConfigSpec `json:"unstructuredDataProcessingConfig,omitempty"`
}

type SnowflakeConfig struct {
	Name             string `json:"name"`
	Account          string `json:"account"`
	User             string `json:"user"`
	Role             string `json:"role"`
	Region           string `json:"region,omitempty"`
	DefaultDatabase  string `json:"defaultDatabase,omitempty"`
	Warehouse        string `json:"warehouse"`
	PrivateKeySecret string `json:"privateKeySecret"`
}

type UnstructuredDataProcessingConfigSpec struct {
	MaxConcurrentDoclingTasks   int `json:"maxConcurrentDoclingTasks,omitempty"`
	MaxConcurrentLangchainTasks int `json:"maxConcurrentLangchainTasks,omitempty"`

	IngestionBucket string `json:"ingestionBucket,omitempty"`
	DoclingServeURL string `json:"doclingServeURL,omitempty"`

	CacheDirectory    string `json:"cacheDirectory,omitempty"`
	DataStorageBucket string `json:"dataStorageBucket,omitempty"`
}

// ControllerConfigStatus defines the observed state of ControllerConfig.
type ControllerConfigStatus struct {
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="ConfigReady")].status`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.conditions[?(@.type=="ConfigReady")].message`

// ControllerConfig is the Schema for the controllerconfigs API.
type ControllerConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ControllerConfigSpec   `json:"spec,omitempty"`
	Status ControllerConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ControllerConfigList contains a list of ControllerConfig.
type ControllerConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ControllerConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ControllerConfig{}, &ControllerConfigList{})
}

func (c *ControllerConfig) UpdateStatus(err error) {
	condition := metav1.Condition{
		Type:               ConfigCondition,
		LastTransitionTime: metav1.Now(),
	}
	if err == nil {
		condition.Status = metav1.ConditionTrue
		condition.Message = "Config is healthy"
		condition.Reason = "SuccessfullyReconciled"
		c.Status.LastAppliedGeneration = c.Generation
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Message = err.Error()
		condition.Reason = "ReconcileFailed"
	}

	for i, currentCondition := range c.Status.Conditions {
		if currentCondition.Type == condition.Type {
			c.Status.Conditions[i] = condition
			return
		}
	}
	c.Status.Conditions = append(c.Status.Conditions, condition)
}

func (c *ControllerConfig) SetWaiting() {
	condition := metav1.Condition{
		Type:               ConfigCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "Config is getting reconciled",
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

func (c *ControllerConfig) IsHealthy() bool {
	for _, condition := range c.Status.Conditions {
		if condition.Type == ConfigCondition && condition.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}
