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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
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

// maxPageImageBytes bounds one page read. The server runs on 512Mi and
// decoding costs several times the encoded size, so an unexpectedly large
// object at that key has to be refused rather than read.
const maxPageImageBytes = 32 << 20

// errCitationNotFound is returned for a chunk that does not exist and for one
// the caller may not read, which must be indistinguishable: telling them
// apart would make the tool an existence oracle over other tenants' data.
const errCitationNotFound = "Error: no chunk with that chunk_id is available in this pipeline."

// destinationClientTTL is how long a built client is reused. The credentials
// inside it are static, so without an expiry a rotated secret would break
// every citation until the pod restarts.
const destinationClientTTL = 5 * time.Minute

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
	builtAt  time.Time
}

func newDestinationClients(k8sClient *k8sclient.Client) *destinationClients {
	return &destinationClients{k8sClient: k8sClient, clients: map[string]*destinationClient{}}
}

func (d *destinationClients) get(ctx context.Context, pipelineName string) (*destinationClient, error) {
	if cached := d.cached(pipelineName); cached != nil {
		return cached, nil
	}

	// Built outside the lock: it reads a pipeline and a Secret from the API
	// server, and holding a process-wide lock across that would make one slow
	// pipeline stall every other pipeline's citations. A lost race just
	// rebuilds a client.
	destination, err := d.k8sClient.GetPipelineDestination(ctx, pipelineName)
	if err != nil {
		return nil, err
	}
	s3Client, err := awsclienthandler.NewS3Client(ctx, destination.AWS)
	if err != nil {
		return nil, fmt.Errorf("creating destination client for pipeline %q: %w", pipelineName, err)
	}

	client := &destinationClient{
		s3Client: s3Client,
		bucket:   destination.Bucket,
		prefix:   destination.Prefix,
		builtAt:  time.Now(),
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.clients[pipelineName] = client
	return client, nil
}

func (d *destinationClients) cached(pipelineName string) *destinationClient {
	d.mu.Lock()
	defer d.mu.Unlock()

	cached, ok := d.clients[pipelineName]
	if !ok || time.Since(cached.builtAt) > destinationClientTTL {
		return nil
	}
	return cached
}

// errPageImageMissing reports a key that is not in the destination, which is
// an ordinary outcome rather than a failure: the syncer only uploads the
// stages a pipeline asked it to.
var errPageImageMissing = errors.New("page image is not in the destination")

// fetch reads one object, named relative to the destination prefix. The key
// comes out of a warehouse column, so a key that escapes the prefix or an
// object larger than maxPageImageBytes is refused rather than read.
func (d *destinationClient) fetch(ctx context.Context, key string) ([]byte, error) {
	// path.Join resolves ".." rather than refusing it, which would walk out
	// of this pipeline's prefix and into another product's data in the same
	// bucket, so the key is checked before it is joined.
	// "." and ".." also bypass the prefix guard: path.Clean returns them unchanged
	// and strings.HasPrefix(key, "../") misses ".." (no trailing slash).
	if key == "" || key == "." || key == ".." || path.IsAbs(key) || key != path.Clean(key) || strings.HasPrefix(key, "../") {
		return nil, fmt.Errorf("page image key %q is not a plain relative path", key)
	}
	fullKey := path.Join(d.prefix, key)

	object, err := awsclienthandler.GetObject(ctx, d.s3Client, d.bucket, fullKey)
	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return nil, fmt.Errorf("%w: %s", errPageImageMissing, fullKey)
		}
		return nil, fmt.Errorf("reading %s: %w", fullKey, err)
	}
	defer object.Body.Close() //nolint:errcheck // read-only body

	data, err := io.ReadAll(io.LimitReader(object.Body, maxPageImageBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", fullKey, err)
	}
	if len(data) > maxPageImageBytes {
		return nil, fmt.Errorf("page image %s is larger than %d bytes", fullKey, maxPageImageBytes)
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
			return toolError(log, "missing required parameters", nil,
				"Error: pipeline_name and chunk_id are required. Call get_chunks_for_embeddings first."), nil, nil
		}

		// Validated rather than split: it is caller supplied, and it is bound
		// into a query against another tenant's warehouse.
		_, parseErr := unstructured.ParseChunkID(args.ChunkID)
		if parseErr != nil {
			return toolError(log, "malformed chunk id", parseErr,
				fmt.Sprintf("Error: %q is not a valid chunk_id.", args.ChunkID)), nil, nil
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			return toolError(log, "pipeline not in per-client allow-list", nil,
				fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName)), nil, nil
		}

		oauthToken, ok := auth.AccessTokenFromContext(ctx)
		if !ok {
			return toolError(log, "oauth token not found in context", nil, errOAuthTokenNotFound), nil, nil
		}

		if k8sClient == nil {
			return toolError(log, "kubernetes client is nil", nil,
				"Error: kubernetes client is not initialized"), nil, nil
		}

		// Messages from here on name nothing the caller did not supply, since
		// the model is told to repeat them to a user. They do still say which
		// step failed: the allow-list above already decided this caller may
		// ask about this pipeline, so a configuration fault is theirs to see.
		// Only the chunk itself is hidden, below.
		qc, err := k8sClient.GetPipelineQueryConfig(ctx, args.PipelineName,
			operatorv1alpha1.StageTypeVectorEmbeddingsGenerator)
		if err != nil {
			return toolError(log, "failed to get pipeline query config", err,
				fmt.Sprintf("Error: could not resolve pipeline %q.", args.PipelineName)), nil, nil
		}
		if qc.Database == "" || qc.Schema == "" || qc.Table == "" {
			return toolError(log, "pipeline query config has empty fields",
				fmt.Errorf("database %q, schema %q, table %q", qc.Database, qc.Schema, qc.Table),
				fmt.Sprintf("Error: pipeline %q has incomplete Snowflake query configuration",
					args.PipelineName)), nil, nil
		}

		// The authorization check. Runs as the caller, so a chunk their row
		// access policy hides simply is not there.
		chunk, err := snowflake.GetChunkByID(ctx, oauthToken,
			strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_")),
			strings.ToUpper(qc.Schema), strings.ToUpper(qc.Table), args.ChunkID)
		if err != nil {
			if snowflake.IsOAuthTokenExpiredError(err) {
				return handleOAuthExpiry(ctx, log)
			}
			// A failed query is not an absent chunk. Reporting it as one would
			// have the model stop and tell the user their citation does not
			// exist, when the warehouse was briefly unreachable.
			return toolError(log, "failed to look up chunk", err,
				fmt.Sprintf("Error: the lookup for chunk %q failed. This is not the same as the chunk being absent, so it is worth retrying.",
					args.ChunkID)), nil, nil
		}
		if chunk == nil {
			return toolError(log, "chunk not visible to caller", nil, errCitationNotFound), nil, nil
		}

		regions, regionErr := decodeRegions(chunk.Regions)
		if regionErr != nil {
			return toolError(log, "chunk regions could not be read", regionErr,
				"Error: this chunk's page regions are unreadable."), nil, nil
		}

		pages, dropped := citedPages(regions)
		if len(pages) == 0 {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("No page image is available for chunk %q. Its source format has no pages to render, or it was processed before page images were stored.", args.ChunkID),
			}}}, nil, nil
		}

		destination, err := destinations.get(ctx, args.PipelineName)
		if err != nil {
			return toolError(log, "failed to resolve destination", err,
				fmt.Sprintf("Error: unable to resolve the destination for pipeline %q.",
					args.PipelineName)), nil, nil
		}

		summary := fmt.Sprintf("Citation for chunk %q from %s, %d page(s): %s",
			args.ChunkID, chunk.FileID, len(pages), pageList(pages))
		if dropped > 0 {
			summary += fmt.Sprintf(" (%d further page(s) omitted, which means this chunk's regions are wrong)",
				dropped)
		}
		images := []mcp.Content{}
		var missing []int
		for _, page := range pages {
			cropped, cropErr := cropPage(ctx, destination, regions, page)
			if errors.Is(cropErr, errPageImageMissing) {
				// The writer recorded an image the syncer never uploaded, most
				// often because the destination does not sync the convert
				// stage. Skipped rather than fatal, because the other pages of
				// this chunk are still worth showing.
				log.Info("page image is not in the destination", "page", page.pageNo, "error", cropErr)
				missing = append(missing, page.pageNo)
				continue
			}
			if cropErr != nil {
				return toolError(log, "failed to produce citation image", cropErr,
					fmt.Sprintf("Error: unable to read the page image for page %d.", page.pageNo)), nil, nil
			}
			images = append(images, &mcp.ImageContent{Data: cropped, MIMEType: "image/png"})
		}

		if len(images) == 0 {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("No page image is available for chunk %q. It was not found in this pipeline's destination.", args.ChunkID),
			}}}, nil, nil
		}
		if len(missing) > 0 {
			summary += fmt.Sprintf(" (page(s) %s are cited but not in the destination)", pageNumbers(missing))
		}
		content := append([]mcp.Content{&mcp.TextContent{Text: summary}}, images...)

		log.Info("completed successfully", "pipeline", args.PipelineName,
			"chunk_id", args.ChunkID, "images", len(images), "missing", len(missing))
		return &mcp.CallToolResult{Content: content}, nil, nil
	})
}

// citedPage is one page of a chunk and where its image lives.
type citedPage struct {
	pageNo int
	image  string
}

// citedPages returns the distinct pages a chunk covers that have an image, in
// page order, capped at maxCitationPages, and how many the cap dropped. A
// region with no image is skipped: most input formats render no pages, and
// the writer leaves the field empty for those.
func citedPages(regions []docling.Region) (pages []citedPage, dropped int) {
	seen := map[int]string{}
	for _, region := range regions {
		if region.Image == "" {
			continue
		}
		if _, ok := seen[region.PageNo]; !ok {
			seen[region.PageNo] = region.Image
		}
	}

	pages = make([]citedPage, 0, len(seen))
	for pageNo, image := range seen {
		pages = append(pages, citedPage{pageNo: pageNo, image: image})
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].pageNo < pages[j].pageNo })

	if len(pages) > maxCitationPages {
		dropped = len(pages) - maxCitationPages
		pages = pages[:maxCitationPages]
	}
	return pages, dropped
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

func pageNumbers(pages []int) string {
	numbers := make([]string, len(pages))
	for i, pageNo := range pages {
		numbers[i] = fmt.Sprintf("%d", pageNo)
	}
	return strings.Join(numbers, ", ")
}

func pageList(pages []citedPage) string {
	numbers := make([]string, len(pages))
	for i, page := range pages {
		numbers[i] = fmt.Sprintf("%d", page.pageNo)
	}
	return strings.Join(numbers, ", ")
}

// toolError logs the failure with its cause and renders the message the
// caller sees, which carries no infrastructure detail: the caller is a model
// that was told to repeat the error verbatim to a user.
func toolError(log *slog.Logger, logMessage string, cause error, userMessage string) *mcp.CallToolResult {
	if cause != nil {
		log.Error(logMessage, "error", cause)
	} else {
		log.Error(logMessage)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: userMessage}},
		IsError: true,
	}
}
