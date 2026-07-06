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
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

type getChunksArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first and pick the matching pipeline based on its description."`
	Query        string `json:"query" jsonschema:"The search query to find relevant chunks"`
}

func RegisterGetChunksForEmbeddings(s *mcp.Server, k8sClient *k8sclient.Client, embeddingClient *embedding.HTTPClient) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "get_chunks_for_embeddings",
		Description: `Search for relevant text chunks in a pipeline's data product using vector cosine similarity. Returns top 5 matching chunks for the given query.
If pipeline_name is not known, call list_unstructured_data_pipelines_for_user first and follow the instructions in its response.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines.
On follow-up: if the user is not satisfied, ask them which pipeline to search. Do NOT automatically try other pipelines.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args getChunksArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "get_chunks_for_embeddings", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName)

		if args.PipelineName == "" || args.Query == "" {
			log.Error("missing required parameters", "pipeline_name", args.PipelineName, "query_empty", args.Query == "")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: pipeline_name and query are required. Call list_unstructured_data_pipelines_for_user first to get the pipeline name."}},
				IsError: true,
			}, nil, nil
		}

		oauthToken, ok := auth.AccessTokenFromContext(ctx)
		if !ok {
			log.Error("oauth token not found in context")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: oauth token not found in context"}},
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

		qc, err := k8sClient.GetPipelineQueryConfig(ctx, args.PipelineName)
		if err != nil {
			log.Error("failed to get pipeline query config", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error resolving pipeline %q: %v", args.PipelineName, err)}},
				IsError: true,
			}, nil, nil
		}

		if qc.Database == "" || qc.Schema == "" || qc.Table == "" {
			log.Error("pipeline query config has empty fields", "database", qc.Database, "schema", qc.Schema, "table", qc.Table)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error: pipeline %q has incomplete Snowflake query configuration", args.PipelineName)}},
				IsError: true,
			}, nil, nil
		}

		log.Info("generating embedding for query")
		result, err := embeddingClient.GenerateEmbeddings(ctx, []string{args.Query}, "float")
		if err != nil {
			log.Error("failed to generate embedding", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error generating embedding: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		if result == nil || result.Count == 0 || len(result.Embeddings) == 0 {
			log.Error("embedding API returned no vectors")
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: embedding API returned no vectors"}},
				IsError: true,
			}, nil, nil
		}
		log.Info("embedding generated", "vector_dimensions", len(result.Embeddings[0]))

		vectorLiteral := formatVectorLiteral(result.Embeddings[0])
		databaseName := strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_"))
		schemaName := strings.ToUpper(qc.Schema)
		tableName := strings.ToUpper(qc.Table)

		log.Info("searching snowflake", "database", databaseName, "schema", schemaName, "table", tableName)
		chunks, err := snowflake.SearchChunks(ctx, oauthToken, databaseName, schemaName, tableName, vectorLiteral)
		if err != nil {
			log.Error("failed to search chunks in snowflake", "error", err, "database", databaseName, "schema", schemaName, "table", tableName)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error searching chunks: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		jsonBytes, err := json.Marshal(chunks)
		if err != nil {
			log.Error("failed to marshal result", "error", err)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error marshaling result: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		log.Info("completed successfully", "pipeline", args.PipelineName, "chunks_found", len(chunks))
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("Found %d chunks for query in pipeline %q:\n%s", len(chunks), args.PipelineName, string(jsonBytes)),
			}},
		}, nil, nil
	})
}

func formatVectorLiteral(vec []float64) string {
	parts := make([]string, len(vec))
	for i, v := range vec {
		parts[i] = strconv.FormatFloat(v, 'f', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
