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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestatus"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
)

type listFilesInPipelineArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first."`
	Limit        int    `json:"limit,omitempty" jsonschema:"Max number of files to return. Defaults to 300 if not specified."`
}

func RegisterListFilesInPipeline(s *mcp.Server, k8sClient *k8sclient.Client, newQuerier func(filestatus.StatusQuerierType) (filestatus.StatusQuerier, error)) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_files_in_pipeline",
		Description: `List all files in a pipeline's first stage. Returns file_id, file_path, file_name, and file_url.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args listFilesInPipelineArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "list_files_in_pipeline", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName)

		if args.PipelineName == "" {
			log.Error("missing required parameter pipeline_name")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: pipeline_name is required. Call list_unstructured_data_pipelines_for_user first to get the pipeline name."}},
				IsError: true,
			}, nil, nil
		}

		if _, ok := auth.AccessTokenFromContext(ctx); !ok {
			log.Error("oauth token not found in context")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: errOAuthTokenNotFound}},
				IsError: true,
			}, nil, nil
		}

		if k8sClient == nil {
			log.Error("kubernetes client is nil")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: kubernetes client is not initialized"}},
				IsError: true,
			}, nil, nil
		}

		qc, err := k8sClient.GetFileStatusQueryConfig(ctx, args.PipelineName)
		if err != nil {
			log.Error("failed to get pipeline query config", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error resolving pipeline %q: %v", args.PipelineName, err)}},
				IsError: true,
			}, nil, nil
		}

		querier, err := newQuerier(qc.ProviderType)
		if err != nil {
			log.Error("unsupported status provider", "provider", qc.ProviderType, "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		database := strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_"))
		schema := strings.ToUpper(qc.Schema)
		table := strings.ToUpper(qc.Stages[0].Table)

		limit := args.Limit
		if limit <= 0 {
			limit = filestatus.MCPMaxResults
		}

		log.Info("querying pipeline files", "database", database, "schema", schema, "table", table, "limit", limit)

		files, err := querier.ListPipelineFiles(ctx, database, schema, table, limit)
		if err != nil {
			log.Error("failed to list pipeline files", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error listing files: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		jsonBytes, err := json.Marshal(files)
		if err != nil {
			log.Error("failed to marshal result", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error marshaling result: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		log.Info("completed successfully", "pipeline", args.PipelineName, "files_count", len(files))
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("Found %d files in pipeline %q:\n%s", len(files), args.PipelineName, string(jsonBytes)),
			}},
		}, nil, nil
	})
}
