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

type getChunkArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first and pick the matching pipeline based on its description."`
	ChunkID      string `json:"chunk_id" jsonschema:"The exact chunk identifier, as returned in the chunk_id field of a get_chunks_for_embeddings result."`
}

// RegisterGetChunk registers the get_chunk MCP tool.
func RegisterGetChunk(s *mcp.Server, k8sClient *k8sclient.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "get_chunk",
		Description: `Retrieve a single chunk in full by its chunk_id, including its page number, block class, section header and bounding box. Use this after get_chunks_for_embeddings to inspect one result in detail, or to re-read a chunk you have cited before.
If the chunk has a crop image, call get_crop with the same chunk_id to see the region of the page it came from.
If pipeline_name is not known, call list_unstructured_data_pipelines_for_user first and follow the instructions in its response.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args getChunkArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "get_chunk", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName, "chunk_id", args.ChunkID)

		if args.PipelineName == "" || args.ChunkID == "" {
			log.Error("missing required parameters",
				"pipeline_name", args.PipelineName, "chunk_id_empty", args.ChunkID == "")
			return toolError("Error: pipeline_name and chunk_id are required. Call get_chunks_for_embeddings first to obtain a chunk_id.")
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			log.Error("pipeline not in per-client allow-list", "pipeline_name", args.PipelineName)
			return toolError(fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName))
		}

		oauthToken, ok := auth.AccessTokenFromContext(ctx)
		if !ok {
			log.Error("oauth token not found in context")
			return toolError(errOAuthTokenNotFound)
		}

		if k8sClient == nil {
			log.Error("kubernetes client is nil")
			return toolError("Error: kubernetes client is not initialized")
		}

		qc, err := k8sClient.GetPipelineQueryConfig(ctx, args.PipelineName,
			operatorv1alpha1.StageTypeVectorEmbeddingsGenerator)
		if err != nil {
			log.Error("failed to get pipeline query config", "error", err)
			return toolError(fmt.Sprintf("Error resolving pipeline %q: %v", args.PipelineName, err))
		}
		if qc.Database == "" || qc.Schema == "" || qc.Table == "" {
			log.Error("pipeline query config has empty fields",
				"database", qc.Database, "schema", qc.Schema, "table", qc.Table)
			return toolError(fmt.Sprintf(
				"Error: pipeline %q has incomplete Snowflake query configuration", args.PipelineName))
		}

		chunk, err := snowflake.GetChunkByID(ctx, oauthToken,
			strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_")),
			strings.ToUpper(qc.Schema), strings.ToUpper(qc.Table), args.ChunkID)
		if err != nil {
			log.Error("failed to fetch chunk", "error", err)
			return toolError(fmt.Sprintf("Error: unable to retrieve chunk %q.", args.ChunkID))
		}
		if chunk == nil {
			// Either no such chunk, or the caller cannot see it. The two are
			// deliberately indistinguishable: saying which would leak the
			// existence of a document the caller has no access to.
			log.Info("chunk not found or not visible to caller", "chunk_id", args.ChunkID)
			return toolError(fmt.Sprintf(
				"No chunk found with chunk_id %q in pipeline %q.", args.ChunkID, args.PipelineName))
		}

		payload, err := json.Marshal(chunk)
		if err != nil {
			log.Error("failed to marshal chunk", "error", err)
			return toolError("Error: unable to format the chunk")
		}

		log.Info("chunk retrieved", "chunk_id", chunk.ChunkID, "has_crop", chunk.CropURI != "")

		text := fmt.Sprintf("Chunk %s from pipeline %q:\n\n%s",
			chunk.ChunkID, args.PipelineName, string(payload))
		if chunk.CropURI != "" {
			text += fmt.Sprintf(
				"\n\nTIP: this chunk has a crop image. Call get_crop with pipeline_name %q and chunk_id %q to see it.",
				args.PipelineName, chunk.ChunkID)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: text}},
		}, nil, nil
	})
}

// toolError builds the error result shape every tool in this package returns:
// a text message flagged as an error, with a nil Go error so the model sees
// the message rather than a transport failure.
func toolError(message string) (*mcp.CallToolResult, any, error) {
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: message}},
		IsError: true,
	}, nil, nil
}
