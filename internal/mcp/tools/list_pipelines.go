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

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

// RegisterListPipelines registers the list_unstructured_data_pipelines_for_user MCP tool
func RegisterListPipelines(s *mcp.Server, k8sClient *k8sclient.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "list_unstructured_data_pipelines_for_user",
		Description: `List the UnstructuredDataPipelines the authenticated user has access to. Returns an array of {name, description}.
If EXACTLY ONE pipeline matches the user's question, use it.
If MORE THAN ONE pipeline could match, STOP and ask the user which one to use. Do NOT pick one yourself.
If NONE match, tell the user. Do NOT try all pipelines.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "list_unstructured_data_pipelines_for_user", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked")

		oauthToken, ok := auth.AccessTokenFromContext(ctx)
		if !ok {
			log.Error("oauth token not found in context")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{
					Text: "Error: oauth token not found in context",
				}},
				IsError: true,
			}, nil, nil
		}

		pipelines := []k8sclient.PipelineInfo{}
		if k8sClient != nil {
			var err error
			pipelines, err = k8sClient.ListPipelines(ctx)
			if err != nil {
				log.Error("failed to list pipelines from kubernetes", "error", err)
				return &mcp.CallToolResult{
					Content: []mcp.Content{&mcp.TextContent{
						Text: fmt.Sprintf("Error listing pipelines: %v", err),
					}},
					IsError: true,
				}, nil, nil
			}
			log.Info("listed pipelines from kubernetes", "count", len(pipelines))
		} else {
			log.Warn("kubernetes client is nil, skipping pipeline listing")
		}

		databases, err := snowflake.ShowDatabases(ctx, oauthToken)
		if err != nil {
			log.Error("failed to list databases from snowflake", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{
					Text: fmt.Sprintf("Error querying Snowflake: %v", err),
				}},
				IsError: true,
			}, nil, nil
		}
		log.Info("listed databases from snowflake", "count", len(databases))

		userDBs := make(map[string]bool, len(databases))
		for _, db := range databases {
			userDBs[strings.ToUpper(db.Name)] = true
		}

		accessible := []k8sclient.PipelineInfo{}
		for _, p := range pipelines {
			dbKey := strings.ToUpper(strings.ReplaceAll(p.Database, "-", "_"))
			if p.Database != "" && userDBs[dbKey] {
				accessible = append(accessible, p)
			}
		}

		jsonBytes, err := json.Marshal(accessible)
		if err != nil {
			log.Error("failed to marshal result", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{
					Text: fmt.Sprintf("Error marshaling result: %v", err),
				}},
				IsError: true,
			}, nil, nil
		}

		log.Info("completed successfully", "total_pipelines", len(pipelines), "accessible_pipelines", len(accessible))
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{
				Text: string(jsonBytes),
			}},
		}, nil, nil
	})
}
