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
	"io"
	"log/slog"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

type getCitationImageArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline the chunk came from. Use the same value passed to get_chunks_for_embeddings."`
	ChunkID      string `json:"chunk_id" jsonschema:"The chunk_id of a chunk returned by get_chunks_for_embeddings."`
}

// maxCitationPages bounds how many images one call returns. A chunk spans a
// handful of pages at most, so a larger number means the regions are wrong.
const maxCitationPages = 10

// errCitationNotFound is returned for a chunk that does not exist and for one
// the caller may not read, which must be indistinguishable: telling them
// apart would make the tool an existence oracle over other tenants' data.
const errCitationNotFound = "Error: no chunk with that chunk_id is available in this pipeline."

// destinationClients hands out an S3 client per pipeline, built from that
// pipeline's own credentials.
//
// Cached because the alternative is reading a Secret and handshaking TLS on
// every citation. Keyed by pipeline so a path handling mistake cannot reach
// another product's bucket.
type destinationClients struct {
	k8sClient *k8sclient.Client

	mu      sync.Mutex
	clients map[string]*destinationClient
}

type destinationClient struct {
	s3Client *s3.Client
	bucket   string
	prefix   string
}

func newDestinationClients(k8sClient *k8sclient.Client) *destinationClients {
	return &destinationClients{k8sClient: k8sClient, clients: map[string]*destinationClient{}}
}

func (d *destinationClients) get(ctx context.Context, pipelineName string) (*destinationClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if cached, ok := d.clients[pipelineName]; ok {
		return cached, nil
	}

	destination, err := d.k8sClient.GetPipelineDestination(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	s3Client, err := awsclienthandler.NewS3Client(ctx, destination.AWS)
	if err != nil {
		return nil, fmt.Errorf("creating destination client for pipeline %q: %w", pipelineName, err)
	}

	client := &destinationClient{s3Client: s3Client, bucket: destination.Bucket, prefix: destination.Prefix}
	d.clients[pipelineName] = client
	return client, nil
}

// fetch reads one object, named relative to the destination prefix.
func (d *destinationClient) fetch(ctx context.Context, key string) ([]byte, error) {
	fullKey := path.Join(d.prefix, key)
	object, err := d.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(fullKey),
	})
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", fullKey, err)
	}
	defer object.Body.Close() //nolint:errcheck // read-only body

	data, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", fullKey, err)
	}
	return data, nil
}

func RegisterGetCitationImage(s *mcp.Server, k8sClient *k8sclient.Client) {
	destinations := newDestinationClients(k8sClient)

	mcp.AddTool(s, &mcp.Tool{
		Name: "get_citation_image",
		Description: `Return the region of the source page that a chunk was taken from, cropped, as an image.

Use this to show a reader where an answer came from, after get_chunks_for_embeddings has given you a chunk_id. One image is returned per page the chunk covers, so a chunk spanning a page break returns two.
Not every chunk has one. Only PDFs and images have pages to render, so a chunk from a spreadsheet or a web page has text and no picture, and the tool says so rather than failing.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines or other chunk ids.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args getCitationImageArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "get_citation_image", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName, "chunk_id", args.ChunkID)

		if args.PipelineName == "" || args.ChunkID == "" {
			return toolError(log, "missing required parameters",
				"Error: pipeline_name and chunk_id are required. Call get_chunks_for_embeddings first."), nil, nil
		}

		// Validated rather than split: it is caller supplied, and it names a
		// file that will be fetched.
		_, parseErr := unstructured.ParseChunkID(args.ChunkID)
		if parseErr != nil {
			return toolError(log, "malformed chunk id",
				fmt.Sprintf("Error: %q is not a valid chunk_id: %v", args.ChunkID, parseErr)), nil, nil
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			return toolError(log, "pipeline not in per-client allow-list",
				fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName)), nil, nil
		}

		oauthToken, ok := auth.AccessTokenFromContext(ctx)
		if !ok {
			return toolError(log, "oauth token not found in context", errOAuthTokenNotFound), nil, nil
		}

		if k8sClient == nil {
			return toolError(log, "kubernetes client is nil",
				"Error: kubernetes client is not initialized"), nil, nil
		}

		qc, err := k8sClient.GetPipelineQueryConfig(ctx, args.PipelineName,
			operatorv1alpha1.StageTypeVectorEmbeddingsGenerator)
		if err != nil {
			return toolError(log, "failed to get pipeline query config",
				fmt.Sprintf("Error resolving pipeline %q: %v", args.PipelineName, err)), nil, nil
		}

		// The authorization check. Runs as the caller, so a chunk their row
		// access policy hides simply is not there.
		chunk, err := snowflake.GetChunkByID(ctx, oauthToken,
			strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_")),
			strings.ToUpper(qc.Schema), strings.ToUpper(qc.Table), args.ChunkID)
		if err != nil {
			return toolError(log, "failed to look up chunk",
				fmt.Sprintf("Error looking up chunk: %v", err)), nil, nil
		}
		if chunk == nil {
			return toolError(log, "chunk not visible to caller", errCitationNotFound), nil, nil
		}

		regions, regionErr := decodeRegions(chunk.Regions)
		if regionErr != nil {
			return toolError(log, "chunk regions could not be read",
				fmt.Sprintf("Error: this chunk's page regions are unreadable: %v", regionErr)), nil, nil
		}

		pages := citedPages(regions)
		if len(pages) == 0 {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("No page image is available for chunk %q. Its source format has no pages to render, or it was processed before page images were stored.", args.ChunkID),
			}}}, nil, nil
		}

		destination, err := destinations.get(ctx, args.PipelineName)
		if err != nil {
			return toolError(log, "failed to resolve destination",
				fmt.Sprintf("Error resolving the destination for pipeline %q: %v", args.PipelineName, err)), nil, nil
		}

		content := []mcp.Content{&mcp.TextContent{
			Text: fmt.Sprintf("Citation for chunk %q from %s, %d page(s): %s",
				args.ChunkID, chunk.FileID, len(pages), pageList(pages)),
		}}
		for _, page := range pages {
			cropped, cropErr := cropPage(ctx, destination, regions, page)
			if cropErr != nil {
				log.Error("could not produce the citation image", "page", page.pageNo, "error", cropErr)
				return toolError(log, "failed to produce citation image",
					fmt.Sprintf("Error reading the page image for page %d: %v", page.pageNo, cropErr)), nil, nil
			}
			content = append(content, &mcp.ImageContent{Data: cropped, MIMEType: "image/png"})
		}

		log.Info("completed successfully", "pipeline", args.PipelineName,
			"chunk_id", args.ChunkID, "images", len(pages))
		return &mcp.CallToolResult{Content: content}, nil, nil
	})
}

// citedPage is one page of a chunk and where its image lives.
type citedPage struct {
	pageNo int
	image  string
}

// citedPages returns the distinct pages a chunk covers that have an image,
// in page order. A region with no image is skipped: most input formats
// render no pages, and the writer leaves the field empty for those.
func citedPages(regions []docling.Region) []citedPage {
	seen := map[int]string{}
	for _, region := range regions {
		if region.Image == "" {
			continue
		}
		if _, ok := seen[region.PageNo]; !ok {
			seen[region.PageNo] = region.Image
		}
	}

	pages := make([]citedPage, 0, len(seen))
	for pageNo, image := range seen {
		pages = append(pages, citedPage{pageNo: pageNo, image: image})
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].pageNo < pages[j].pageNo })

	if len(pages) > maxCitationPages {
		pages = pages[:maxCitationPages]
	}
	return pages
}

// cropPage fetches one page's image and cuts the cited part out of it.
func cropPage(ctx context.Context, destination *destinationClient,
	regions []docling.Region, page citedPage) ([]byte, error) {
	pageImage, err := destination.fetch(ctx, page.image)
	if err != nil {
		return nil, err
	}
	return unstructured.CropCitation(pageImage, regions, page.pageNo)
}

// decodeRegions reads the regions column, which is absent for a chunk
// written before citations existed.
func decodeRegions(raw json.RawMessage) ([]docling.Region, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var regions []docling.Region
	if err := json.Unmarshal(raw, &regions); err != nil {
		return nil, fmt.Errorf("decoding regions: %w", err)
	}
	return regions, nil
}

func pageList(pages []citedPage) string {
	numbers := make([]string, len(pages))
	for i, page := range pages {
		numbers[i] = fmt.Sprintf("%d", page.pageNo)
	}
	return strings.Join(numbers, ", ")
}

// toolError logs and renders the same message, so a caller and the logs
// never disagree about what went wrong.
func toolError(log *slog.Logger, logMessage, userMessage string) *mcp.CallToolResult {
	log.Error(logMessage)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: userMessage}},
		IsError: true,
	}
}
