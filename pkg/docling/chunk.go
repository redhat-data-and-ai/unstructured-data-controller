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

package docling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ChunkType represents the type of Docling chunker to use.
type ChunkType string

const (
	ChunkTypeHierarchical ChunkType = "hierarchical"
	ChunkTypeHybrid       ChunkType = "hybrid"

	chunkingHTTPTimeout = 300 * time.Second
)

// HierarchicalChunkingOptions are options for the Docling HierarchicalChunker.
type HierarchicalChunkingOptions struct {
	MergeListItems *bool `json:"merge_list_items,omitempty"`
}

// HybridChunkingOptions are options for the Docling HybridChunker.
type HybridChunkingOptions struct {
	Tokenizer  string `json:"tokenizer,omitempty"`
	MaxTokens  int    `json:"max_tokens,omitempty"`
	MergePeers *bool  `json:"merge_peers,omitempty"`
}

// DoclingChunkRequestPayload is the request body for the Docling chunk endpoint.
type DoclingChunkRequestPayload struct {
	Sources         []DoclingSource `json:"sources"`
	Options         *DoclingConfig  `json:"options,omitempty"`
	ChunkingOptions any             `json:"chunking_options,omitempty"`
}

// DoclingChunk represents a single chunk in the Docling chunk response.
type DoclingChunk struct {
	Text string `json:"text"`
}

// DoclingChunkResponse is the response from the Docling chunk endpoint.
type DoclingChunkResponse struct {
	Chunks []DoclingChunk `json:"chunks"`
}

func (c *Client) chunkSourceEndpoint(chunkType ChunkType) (string, error) {
	return url.JoinPath(c.ClientConfig.URL, "/v1/chunk", string(chunkType), "source")
}

// ChunkFile sends a document to the Docling chunk endpoint and returns the chunk texts.
// It uses TryAcquire on the semaphore to avoid blocking the reconciliation loop.
func (c *Client) ChunkFile(
	ctx context.Context,
	fileURL string,
	chunkType ChunkType,
	chunkingOptions any,
) ([]string, error) {
	logger := log.FromContext(ctx)

	acquired := c.ClientConfig.sem.TryAcquire(1)
	if !acquired {
		return nil, errors.New(SemaphoreAcquireError)
	}
	defer c.ClientConfig.sem.Release(1)

	endpoint, err := c.chunkSourceEndpoint(chunkType)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunk endpoint: %w", err)
	}

	payload, err := json.Marshal(DoclingChunkRequestPayload{
		Sources: []DoclingSource{
			{URL: fileURL, Kind: "http"},
		},
		ChunkingOptions: chunkingOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal chunk request payload: %w", err)
	}

	logger.Info("sending chunk request to docling", "url", endpoint, "source", fileURL, "chunkType", string(chunkType))

	responseBody, err := c.createDoclingChunkRequest(ctx, http.MethodPost, endpoint, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to send chunk request: %w", err)
	}
	defer func() {
		if closeErr := responseBody.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close chunk response body")
		}
	}()

	body, err := io.ReadAll(responseBody)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk response body: %w", err)
	}

	var chunkResponse DoclingChunkResponse
	if err = json.Unmarshal(body, &chunkResponse); err != nil {
		return nil, fmt.Errorf("failed to decode chunk response: %w", err)
	}

	chunks := make([]string, 0, len(chunkResponse.Chunks))
	for _, chunk := range chunkResponse.Chunks {
		chunks = append(chunks, chunk.Text)
	}

	logger.Info("successfully chunked file via docling", "chunkCount", len(chunks))
	return chunks, nil
}

// createDoclingChunkRequest is like createDoclingRequest but with a longer timeout
// suitable for synchronous chunking (which includes conversion + chunking).
func (c *Client) createDoclingChunkRequest(ctx context.Context, method, endpoint string, payload []byte) (
	io.ReadCloser, error) {
	logger := log.FromContext(ctx)
	client := &http.Client{
		Timeout: chunkingHTTPTimeout,
	}

	req, err := c.createHTTPRequest(ctx, method, endpoint, payload, "Bearer %s")
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	logger.Info("sending chunk request to docling service", "url", endpoint)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode == http.StatusForbidden && c.ClientConfig.Key != "" {
		req, err = c.createHTTPRequest(ctx, method, endpoint, payload, "%s")
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		resp, err = client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
	}

	if resp.StatusCode != http.StatusOK {
		logger.Error(errors.New("received non-200 OK response from chunk endpoint"),
			"status code", resp.StatusCode, "url", endpoint)
		return nil, fmt.Errorf("failed to chunk file: status code %d", resp.StatusCode)
	}

	return resp.Body, nil
}
