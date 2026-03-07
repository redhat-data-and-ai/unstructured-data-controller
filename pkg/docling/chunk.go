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
	"net/url"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ChunkType represents the type of Docling chunker to use.
type ChunkType string

const (
	ChunkTypeHierarchical ChunkType = "hierarchical"
	ChunkTypeHybrid       ChunkType = "hybrid"

	chunkPollInterval = 2 * time.Second
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

// ChunkedDocumentResultItem represents a single chunk returned by the Docling chunk API.
type ChunkedDocumentResultItem struct {
	Text string `json:"text"`
}

// ChunkDocumentResponse is the response from GET /v1/result/{task_id} for chunk tasks.
type ChunkDocumentResponse struct {
	Chunks         []ChunkedDocumentResultItem `json:"chunks"`
	ProcessingTime float64                     `json:"processing_time"`
}

func (c *Client) chunkSourceAsyncEndpoint(chunkType ChunkType) (string, error) {
	return url.JoinPath(c.ClientConfig.URL, "/v1/chunk", string(chunkType), "source", "async")
}

// ChunkFile sends a document to the Docling async chunk endpoint and returns the chunk texts.
// It submits the task, polls for completion, then fetches the result.
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

	// Step 1: Submit async chunk request
	endpoint, err := c.chunkSourceAsyncEndpoint(chunkType)
	if err != nil {
		return nil, fmt.Errorf("failed to get async chunk endpoint: %w", err)
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

	logger.Info("submitting async chunk request to docling", "url", endpoint, "source", fileURL, "chunkType", string(chunkType))

	responseBody, err := c.createDoclingRequest(ctx, "POST", endpoint, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to submit async chunk request: %w", err)
	}

	body, err := io.ReadAll(responseBody)
	if err != nil {
		responseBody.Close()
		return nil, fmt.Errorf("failed to read async chunk response body: %w", err)
	}
	responseBody.Close()

	var taskStatus TaskStatusResponse
	if err = json.Unmarshal(body, &taskStatus); err != nil {
		return nil, fmt.Errorf("failed to decode async chunk response: %w", err)
	}

	taskID := taskStatus.TaskID
	logger.Info("async chunk task submitted", "taskID", taskID, "status", taskStatus.TaskStatus)

	// Step 2: Poll for completion
	if err := c.pollUntilComplete(ctx, taskID); err != nil {
		return nil, fmt.Errorf("chunk task failed: %w", err)
	}

	// Step 3: Fetch result
	return c.getChunkResult(ctx, taskID)
}

func (c *Client) pollUntilComplete(ctx context.Context, taskID string) error {
	logger := log.FromContext(ctx)

	for {
		_, taskStatus, err := c.getTaskStatus(ctx, taskID)
		if err != nil {
			return fmt.Errorf("failed to poll task status: %w", err)
		}

		switch taskStatus.TaskStatus {
		case TaskStatusSuccess:
			logger.Info("chunk task completed successfully", "taskID", taskID)
			return nil
		case TaskStatusFailure:
			return fmt.Errorf("task failed: task id: %s", taskID)
		case TaskStatusPending, TaskStatusStarted:
			logger.Info("chunk task still in progress, polling again", "taskID", taskID, "status", taskStatus.TaskStatus)
		default:
			return fmt.Errorf("unexpected task status %q for task id: %s", taskStatus.TaskStatus, taskID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(chunkPollInterval):
		}
	}
}

func (c *Client) getChunkResult(ctx context.Context, taskID string) ([]string, error) {
	logger := log.FromContext(ctx)

	taskResultURL, err := c.getTaskResultEndpoint(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task result endpoint: %w", err)
	}

	logger.Info("fetching chunk result", "url", taskResultURL)
	responseBody, err := c.createDoclingRequest(ctx, "GET", taskResultURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunk result: %w", err)
	}

	body, err := io.ReadAll(responseBody)
	if err != nil {
		responseBody.Close()
		return nil, fmt.Errorf("failed to read chunk result body: %w", err)
	}
	responseBody.Close()

	var chunkResponse ChunkDocumentResponse
	if err = json.Unmarshal(body, &chunkResponse); err != nil {
		return nil, fmt.Errorf("failed to decode chunk result: %w", err)
	}

	chunks := make([]string, 0, len(chunkResponse.Chunks))
	for _, chunk := range chunkResponse.Chunks {
		chunks = append(chunks, chunk.Text)
	}

	logger.Info("successfully chunked file via docling", "chunkCount", len(chunks))
	return chunks, nil
}
