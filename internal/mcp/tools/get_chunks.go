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
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/reranker"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/rrf"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

type getChunksArgs struct {
	PipelineName   string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first and pick the matching pipeline based on its description."`
	Query          string `json:"query" jsonschema:"The search query to find relevant chunks using vector cosine similarity."`
	FilePathFilter string `json:"file_path_filter,omitempty" jsonschema:"Optional glob pattern to scope the search to files matching this path. Use * to match any path segment. Examples: '*/README.md', 'repo-name/docs/*', '*/aemv2/*'."`
	Limit          int    `json:"limit,omitempty" jsonschema:"Number of chunks to return. Use a lower value (1-5) for specific factual questions. Use a higher value for broad questions like summaries or 'tell me everything about X'. Defaults to 10 if not specified."`
}

// rerankerOverFetchMultiplier controls how many more candidates to retrieve from
// Snowflake when reranking is enabled, giving the cross-encoder a broader set to
// re-score before truncating to the requested limit.
const rerankerOverFetchMultiplier = 3

func RegisterGetChunksForEmbeddings(s *mcp.Server, k8sClient *k8sclient.Client, embeddingClient *embedding.HTTPClient, rerankerClient reranker.Reranker, hybridSearchEnabled bool) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "get_chunks_for_embeddings",
		Description: `Search for relevant text chunks in a pipeline's data product using vector cosine similarity. Optionally scope the search to specific files using file_path_filter with glob patterns (e.g. "*/README.md", "*/aemv2/*").
If pipeline_name is not known, call list_unstructured_data_pipelines_for_user first and follow the instructions in its response.
If the returned chunks are not sufficient to answer the user's question, you may call get_processed_document with the same pipeline_name and the file_id from the top matching chunk to retrieve the full processed document for more context.
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
				Content: []mcp.Content{&mcp.TextContent{Text: "Error: kubernetes client is not initialized"}},
				IsError: true,
			}, nil, nil
		}

		qc, err := k8sClient.GetPipelineQueryConfig(ctx, args.PipelineName, operatorv1alpha1.StageTypeVectorEmbeddingsGenerator)
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

		limit := args.Limit
		if limit <= 0 {
			limit = 10
		}

		fetchLimit := limit
		if rerankerClient != nil {
			fetchLimit = limit * rerankerOverFetchMultiplier
		}

		var vectorChunks []snowflake.ChunkResult
		if args.FilePathFilter != "" {
			filePattern := convertFileGlobToSQL(args.FilePathFilter)
			log.Info("searching snowflake with file filter", "database", databaseName, "schema", schemaName, "table", tableName, "filePattern", filePattern, "limit", fetchLimit)
			vectorChunks, err = snowflake.SearchChunksWithFileFilter(ctx, oauthToken, databaseName, schemaName, tableName, vectorLiteral, filePattern, fetchLimit)
		} else {
			log.Info("searching snowflake", "database", databaseName, "schema", schemaName, "table", tableName, "limit", fetchLimit)
			vectorChunks, err = snowflake.SearchChunks(ctx, oauthToken, databaseName, schemaName, tableName, vectorLiteral, fetchLimit)
		}
		if err != nil {
			log.Error("failed to search chunks in snowflake", "error", err, "database", databaseName, "schema", schemaName, "table", tableName)
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("Error searching chunks: %v", err)}},
				IsError: true,
			}, nil, nil
		}

		chunks := vectorChunks

		if hybridSearchEnabled {
			var keywordChunks []snowflake.ChunkResult
			if args.FilePathFilter != "" {
				filePattern := convertFileGlobToSQL(args.FilePathFilter)
				log.Info("running keyword search with file filter", "limit", fetchLimit)
				keywordChunks, err = snowflake.SearchChunksByKeywordWithFileFilter(ctx, oauthToken, databaseName, schemaName, tableName, args.Query, filePattern, fetchLimit)
			} else {
				log.Info("running keyword search", "limit", fetchLimit)
				keywordChunks, err = snowflake.SearchChunksByKeyword(ctx, oauthToken, databaseName, schemaName, tableName, args.Query, fetchLimit)
			}
			if err != nil {
				log.Error("keyword search failed, using vector results only", "error", err)
			} else {
				chunks = rrf.Fuse(vectorChunks, keywordChunks, rrf.DefaultK)
				log.Info("hybrid search fused", "vector_count", len(vectorChunks), "keyword_count", len(keywordChunks), "fused_count", len(chunks))
				if len(chunks) > fetchLimit {
					chunks = chunks[:fetchLimit]
				}
			}
		}

		if rerankerClient != nil && len(chunks) > 0 {
			chunks, err = rerankChunks(ctx, rerankerClient, args.Query, chunks, limit)
			if err != nil {
				log.Error("reranking failed, returning pre-rerank results", "error", err)
			}
		}

		if len(chunks) > limit {
			chunks = chunks[:limit]
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

		var nextStep string
		if len(chunks) > 0 {
			nextStep = fmt.Sprintf(
				"\n\nTIP: To retrieve the full processed document, call get_processed_document with pipeline_name=%q and the file_id from the most relevant chunk above.",
				args.PipelineName,
			)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("Found %d chunks for query in pipeline %q:\n%s%s", len(chunks), args.PipelineName, string(jsonBytes), nextStep),
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

// convertFileGlobToSQL converts a file path glob pattern to a SQL LIKE pattern,
// escaping literal SQL wildcards (% and _) before converting glob * to %.
func convertFileGlobToSQL(glob string) string {
	if glob == "" {
		return ""
	}
	pattern := strings.ReplaceAll(glob, "%", "\\%")
	pattern = strings.ReplaceAll(pattern, "_", "\\_")
	return strings.ReplaceAll(pattern, "*", "%")
}

// rerankChunks sends the chunk texts through a cross-encoder reranker,
// assigns reranker scores, re-sorts by relevance, and truncates to limit.
func rerankChunks(ctx context.Context, rerankerClient reranker.Reranker, query string, chunks []snowflake.ChunkResult, limit int) ([]snowflake.ChunkResult, error) {
	log := logger.FromContext(ctx)

	documents := make([]string, len(chunks))
	for i, chunk := range chunks {
		documents[i] = chunk.ChunkText
	}

	results, err := rerankerClient.Rerank(ctx, query, documents, limit)
	if err != nil {
		return chunks, fmt.Errorf("calling reranker: %w", err)
	}

	log.Info("reranking applied", "candidates", len(chunks), "results", len(results))

	reranked := make([]snowflake.ChunkResult, 0, len(results))
	for _, result := range results {
		if result.Index < 0 || result.Index >= len(chunks) {
			continue
		}
		chunk := chunks[result.Index]
		score := result.RelevanceScore
		chunk.RerankerScore = &score
		reranked = append(reranked, chunk)
	}

	sort.Slice(reranked, func(i, j int) bool {
		return *reranked[i].RerankerScore > *reranked[j].RerankerScore
	})

	if len(reranked) > limit {
		reranked = reranked[:limit]
	}

	return reranked, nil
}
