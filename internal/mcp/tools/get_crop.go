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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

// maxCropBytes bounds what is read into a response. A block crop is tens to
// hundreds of kilobytes; anything far larger is a sign the stored path points
// at something unexpected, and base64 in a tool response costs a third again
// on top.
const maxCropBytes = 8 << 20

// cropURLTTL bounds how long a crop link stays usable.
//
// A presigned URL is a bearer capability: anyone holding it can fetch the
// image until it expires, with no further authorization. It is minted only
// after the caller has been shown to have access to the chunk, and kept
// short because it ends up pasted into a chat transcript. The shared
// GetPresignedURL helper uses an hour, which is right for handing a source
// document to a parser and wrong for this.
const cropURLTTL = 10 * time.Minute

// CropStore reads crop images from object storage.
type CropStore struct {
	Client *s3.Client
	Bucket string
}

// presignCrop returns a short-lived HTTPS link to a crop image.
//
// Chat clients fetch markdown images with a plain unauthenticated GET, so a
// link is the only way the picture can reach the UI. The signature carries
// the authorization in place of a header.
func (s *CropStore) presignCrop(ctx context.Context, cropURI string) (string, error) {
	presigned, err := s3.NewPresignClient(s.Client).PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(cropURI),
	}, func(options *s3.PresignOptions) {
		options.Expires = cropURLTTL
	})
	if err != nil {
		return "", fmt.Errorf("presigning %s: %w", cropURI, err)
	}
	return presigned.URL, nil
}

type getCropArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first and pick the matching pipeline based on its description."`
	ChunkID      string `json:"chunk_id" jsonschema:"The exact chunk identifier, as returned in the chunk_id field of a get_chunks_for_embeddings or get_chunk result."`
}

// RegisterGetCrop registers the get_crop MCP tool.
//
// cropStore may be nil, in which case the tool reports that image retrieval
// is not configured rather than failing at call time.
func RegisterGetCrop(s *mcp.Server, k8sClient *k8sclient.Client, cropStore *CropStore) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "get_crop",
		Description: `Retrieve the cropped image of the page region a chunk came from, as a visual citation. Use this after get_chunks_for_embeddings or get_chunk when the user asks to see a table, chart or figure, or wants to verify a figure against the source document.
Only chunks whose crop_uri is set have an image. Text chunks usually do; page headers, page footers and pictures do not.
If pipeline_name is not known, call list_unstructured_data_pipelines_for_user first and follow the instructions in its response.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args getCropArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "get_crop", username)
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

		if cropStore == nil || cropStore.Client == nil || cropStore.Bucket == "" {
			log.Error("crop store is not configured")
			return toolError("Error: image retrieval is not configured on this server.")
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

		// Resolve through the governed view FIRST, under the caller's token.
		// The image in object storage carries no row access policy, so going
		// straight from a caller-supplied chunk_id to a read would hand out
		// any document's imagery regardless of who may see it.
		crop, err := snowflake.GetCropReference(ctx, oauthToken,
			strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_")),
			strings.ToUpper(qc.Schema), strings.ToUpper(qc.Table), args.ChunkID)
		if err != nil {
			log.Error("failed to resolve crop reference", "error", err)
			return toolError(fmt.Sprintf("Error: unable to look up chunk %q.", args.ChunkID))
		}
		if crop == nil {
			// No row means either no such chunk or no access, and the two are
			// deliberately indistinguishable.
			log.Info("chunk not found or not visible to caller", "chunk_id", args.ChunkID)
			return toolError(fmt.Sprintf(
				"No chunk found with chunk_id %q in pipeline %q.", args.ChunkID, args.PipelineName))
		}
		if crop.CropURI == "" {
			log.Info("chunk has no crop image", "chunk_id", args.ChunkID, "block_class", crop.BlockClass)
			return toolError(fmt.Sprintf(
				"Chunk %q has no crop image. Blocks of class %q are not cropped.",
				args.ChunkID, crop.BlockClass))
		}

		image, err := readCrop(ctx, cropStore, crop.CropURI)
		if err != nil {
			log.Error("failed to read crop image", "error", err, "crop_uri", crop.CropURI)
			return toolError(fmt.Sprintf("Error: unable to retrieve the image for chunk %q.", args.ChunkID))
		}

		caption := fmt.Sprintf("%s from page %s of %s (chunk %s)",
			crop.BlockClass, crop.PageNumber, crop.FileName, crop.ChunkID)
		text := caption

		// The image is returned twice on purpose, for two different readers.
		//
		// ImageContent is what the model sees, and is the point of the tool:
		// it can read the rendered region rather than trusting parsed text.
		// Several chat clients, Cursor among them, currently receive that
		// block but do not display it, so a markdown link is added for the
		// human. It costs one extra signature and no extra bytes in the
		// response.
		if url, presignErr := cropStore.presignCrop(ctx, crop.CropURI); presignErr != nil {
			// Not fatal: the model still has the image.
			log.Error("failed to presign crop image", "error", presignErr, "crop_uri", crop.CropURI)
		} else {
			text = fmt.Sprintf("%s\n\n![%s](%s)\n\nShow the markdown image above to the user so they can see the region.",
				caption, caption, url)
		}

		log.Info("crop retrieved", "chunk_id", crop.ChunkID, "bytes", len(image))

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
				&mcp.ImageContent{Data: image, MIMEType: "image/png"},
			},
		}, nil, nil
	})
}

// readCrop fetches a crop image from object storage with the server's own
// credentials. The caller must already have established that the requesting
// user may see the chunk this image belongs to.
func readCrop(ctx context.Context, store *CropStore, cropURI string) ([]byte, error) {
	object, err := awsclienthandler.GetObject(ctx, store.Client, store.Bucket, cropURI)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", cropURI, err)
	}
	defer func() { _ = object.Body.Close() }()

	image, err := io.ReadAll(io.LimitReader(object.Body, maxCropBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", cropURI, err)
	}
	if len(image) > maxCropBytes {
		return nil, fmt.Errorf("%s exceeds the %d byte limit for a crop image", cropURI, maxCropBytes)
	}
	return image, nil
}
