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
	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

const (
	errOAuthTokenNotFound = "Error: oauth token not found in context"
	maxDocuments          = 20
)

type getProcessedDocumentArgs struct {
	PipelineName   string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first and pick the matching pipeline based on its description."`
	FileID         string `json:"file_id,omitempty" jsonschema:"The exact file identifier to look up. Takes priority over file_path_filter if both are provided."`
	FilePathFilter string `json:"file_path_filter,omitempty" jsonschema:"Glob pattern to retrieve processed documents from files matching this path. Use * to match any path segment. Examples: '*/README.md', '*/aemv2/*'."`
}

func RegisterGetProcessedDocument(s *mcp.Server, k8sClient *k8sclient.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "get_processed_document",
		Description: `Retrieve processed document output from a pipeline's DocumentProcessor stage. Supports exact lookup via file_id or glob pattern matching via file_path_filter (e.g. "*/README.md"). At least one of file_id or file_path_filter is required.
If pipeline_name is not known, call list_unstructured_data_pipelines_for_user first and follow the instructions in its response.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args getProcessedDocumentArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "get_processed_document", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName, "file_id", args.FileID, "file_path_filter", args.FilePathFilter)

		if args.PipelineName == "" || (args.FileID == "" && args.FilePathFilter == "") {
			log.Error("missing required parameters", "pipeline_name", args.PipelineName, "file_id_empty", args.FileID == "", "file_path_filter_empty", args.FilePathFilter == "")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: pipeline_name and at least one of file_id or file_path_filter are required. Call list_unstructured_data_pipelines_for_user first to get the pipeline name."}},
				IsError: true,
			}, nil, nil
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			log.Error("pipeline not in per-client allow-list", "pipeline_name", args.PipelineName)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName)}},
				IsError: true,
			}, nil, nil
		}

		oauthToken, ok := auth.AccessTokenFromContext(ctx)
		if !ok {
			log.Error("oauth token not found in context")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: errOAuthTokenNotFound}},
				IsError: true,
			}, nil, nil
		}

		if k8sClient == nil {
			log.Error("kubernetes client is nil")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: unable to fetch pipeline information"}},
				IsError: true,
			}, nil, nil
		}

		qc, err := k8sClient.GetPipelineQueryConfig(ctx, args.PipelineName, operatorv1alpha1.StageTypeDocumentProcessor)
		if err != nil {
			log.Error("failed to get pipeline query config", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: unable to resolve configuration for pipeline %q. Please verify the pipeline name is correct.", args.PipelineName)}},
				IsError: true,
			}, nil, nil
		}

		if qc.Database == "" || qc.Schema == "" || qc.Table == "" {
			log.Error("pipeline query config has empty fields", "database", qc.Database, "schema", qc.Schema, "table", qc.Table)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Unable to retrieve the document, please check again in some time"}},
				IsError: true,
			}, nil, nil
		}

		databaseName := strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_"))
		schemaName := strings.ToUpper(qc.Schema)
		tableName := strings.ToUpper(qc.Table)

		var result any
		var responseMsg string

		if args.FileID != "" {
			log.Info("querying snowflake by file_id", "database", databaseName, "schema", schemaName, "table", tableName, "file_id", args.FileID)
			doc, queryErr := snowflake.GetProcessedDocument(ctx, oauthToken, databaseName, schemaName, tableName, args.FileID)
			if queryErr != nil {
				log.Error("failed to get processed document", "error", queryErr)
				return &mcp.CallToolResult{
					Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: unable to retrieve the processed document for file_id %q. Please verify the file_id is correct.", args.FileID)}},
					IsError: true,
				}, nil, nil
			}
			result = doc
			responseMsg = fmt.Sprintf("Processed document for file_id %q in pipeline %q", args.FileID, args.PipelineName)
		} else {
			filePattern := convertFileGlobToSQL(args.FilePathFilter)
			log.Info("querying snowflake by file path pattern", "database", databaseName, "schema", schemaName, "table", tableName, "filePattern", filePattern)
			count, countErr := snowflake.CountProcessedDocumentsByFilePath(ctx, oauthToken, databaseName, schemaName, tableName, filePattern)
			if countErr != nil {
				log.Error("failed to count matching documents", "error", countErr)
				return &mcp.CallToolResult{
					Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: unable to retrieve processed documents matching %q.", args.FilePathFilter)}},
					IsError: true,
				}, nil, nil
			}
			log.Info("counted matching documents", "count", count, "max", maxDocuments, "filePattern", filePattern, "fileID", args.FileID, "pipeline_name", args.PipelineName)
			if count == 0 {
				return &mcp.CallToolResult{
					Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("No processed documents found matching %q in pipeline %q.", args.FilePathFilter, args.PipelineName)}},
				}, nil, nil
			}

			if count > maxDocuments {
				log.Info("pattern matched too many documents, returning file IDs only", "count", count, "max", maxDocuments)
				fileIDs, listErr := snowflake.ListFileIDsByFilePath(ctx, oauthToken, databaseName, schemaName, tableName, filePattern)
				if listErr != nil {
					log.Error("failed to list matching file IDs", "error", listErr)
					return &mcp.CallToolResult{
						Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: unable to retrieve processed documents matching %q.", args.FilePathFilter)}},
						IsError: true,
					}, nil, nil
				}
				result = fileIDs
				responseMsg = fmt.Sprintf("Pattern %q matched %d documents in pipeline %q, which exceeds the limit of %d. Narrow your pattern or use file_id with one of these paths", args.FilePathFilter, count, args.PipelineName, maxDocuments)
			} else {
				docs, queryErr := snowflake.GetProcessedDocumentsByFilePath(ctx, oauthToken, databaseName, schemaName, tableName, filePattern, maxDocuments)
				if queryErr != nil {
					log.Error("failed to get processed documents", "error", queryErr)
					return &mcp.CallToolResult{
						Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: unable to retrieve processed documents matching %q.", args.FilePathFilter)}},
						IsError: true,
					}, nil, nil
				}
				result = docs
				responseMsg = fmt.Sprintf("Found %d processed document(s) matching %q in pipeline %q", len(docs), args.FilePathFilter, args.PipelineName)
			}
		}

		jsonBytes, err := json.Marshal(result)
		if err != nil {
			log.Error("failed to marshal result", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: unable to process the document result"}},
				IsError: true,
			}, nil, nil
		}

		log.Info("completed successfully", "pipeline", args.PipelineName)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("%s:\n%s", responseMsg, string(jsonBytes)),
			}},
		}, nil, nil
	})
}
