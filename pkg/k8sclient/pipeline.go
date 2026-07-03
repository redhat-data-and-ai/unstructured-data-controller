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

package k8sclient

import (
	"context"
	"fmt"
	"os"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const defaultPipelineNamespace = "unstructured-controller-namespace"

type PipelineInfo struct {
	Name        string `json:"name"`
	Namespace   string `json:"namespace"`
	Description string `json:"description"`
	Database    string `json:"database,omitempty"`
	Schema      string `json:"schema,omitempty"`
	Table       string `json:"table,omitempty"`
	Status      string `json:"status,omitempty"`
	Message     string `json:"message,omitempty"`
}

func (c *Client) ListPipelines(ctx context.Context) ([]PipelineInfo, error) {
	pipelineList := &operatorv1alpha1.UnstructuredDataPipelineList{}

	namespace := os.Getenv("UNSTRUCTURED_DATA_CONTROLLER_NAMESPACE")
	if namespace == "" {
		namespace = defaultPipelineNamespace
	}

	err := c.client.List(ctx, pipelineList, &client.ListOptions{
		Namespace: namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pipelines: %w", err)
	}

	result := make([]PipelineInfo, len(pipelineList.Items))
	for i, pipeline := range pipelineList.Items {
		info := PipelineInfo{
			Name:        pipeline.Name,
			Namespace:   pipeline.Namespace,
			Description: pipeline.Spec.Description,
		}

		for _, stage := range pipeline.Spec.Stages {
			if stage.QueryConfig != nil && stage.QueryConfig.Snowflake != nil {
				info.Database = stage.QueryConfig.Snowflake.Database
				info.Schema = stage.QueryConfig.Snowflake.Schema
				info.Table = stage.QueryConfig.Snowflake.Table
				break
			}
		}

		for _, condition := range pipeline.Status.Conditions {
			if condition.Type == "UnstructuredDataPipelineReady" {
				info.Status = string(condition.Status)
				info.Message = condition.Message
				break
			}
		}

		result[i] = info
	}

	return result, nil
}
