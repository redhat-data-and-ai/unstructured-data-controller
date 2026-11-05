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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// InputFormatDoclingDocument tells docling the file it is being handed is
// already a DoclingDocument, so it reads it back instead of parsing it. It is
// an input format, not a source kind: the only kinds the server accepts are
// "file" and "http".
const InputFormatDoclingDocument = "json_docling"

// ChunkerHybrid splits on the document's structure rather than on a character
// count. The endpoint is chosen by path and this field has to agree with it.
const ChunkerHybrid = "hybrid"

// ErrNoChunksReturned reports a chunk task that finished successfully and
// produced nothing.
//
// This is how the chunker reports being unable to load its tokenizer: success
// status, empty errors, empty chunks. Nothing in the response tells that apart
// from a document that really had no content, so the chunks are returned
// alongside this error rather than instead of it.
var ErrNoChunksReturned = errors.New("the chunk task succeeded and returned no chunks")

// ErrChunkTaskEnded marks every error from GetChunks that means the task is
// over and its semaphore slot has been given back.
//
// A task that is over must not be polled again: a second release is a bare
// subtraction on the weighted semaphore and would take a slot another task
// holds. Errors that leave the task running, such as an unreadable status,
// carry no such marker and should be retried against the same task id.
var ErrChunkTaskEnded = errors.New("the chunk task has ended")

// ChunkingOptions is what the chunk endpoints read as "chunking_options".
//
// Every value is set explicitly: an unset tokenizer reaches for a model docling
// has to download, an unset MaxTokens falls back to the tokenizer's own limit
// which is several times an embedding window, and both flags default off, which
// returns no text to display and serialises tables as value triplets.
type ChunkingOptions struct {
	Chunker           string `json:"chunker"`
	Tokenizer         string `json:"tokenizer"`
	MaxTokens         int    `json:"max_tokens"`
	IncludeRawText    bool   `json:"include_raw_text"`
	UseMarkdownTables bool   `json:"use_markdown_tables"`
}

// ChunkConvertOptions is the "convert_options" block of a chunk request. Mind
// the key: the convert endpoints read "options" instead, and docling drops an
// unrecognised key and still returns 200, so the wrong one is discarded
// silently.
type ChunkConvertOptions struct {
	FromFormats []string `json:"from_formats"`
}

// ChunkFileSource carries the document inline. "file" is the source kind that
// takes bytes we hold rather than a URL something else must fetch.
type ChunkFileSource struct {
	Kind         string `json:"kind"`
	Filename     string `json:"filename"`
	Base64String string `json:"base64_string"`
}

type ChunkRequestPayload struct {
	Sources         []ChunkFileSource    `json:"sources"`
	ChunkingOptions *ChunkingOptions     `json:"chunking_options"`
	ConvertOptions  *ChunkConvertOptions `json:"convert_options"`
}

// Chunk is one chunk as docling returns it.
//
// Text is the contextualised form, with the heading trail prepended, and is
// what gets embedded; RawText is the body alone and is what gets displayed.
// Geometry is not included: DocItems names the elements, and the rectangles
// live in the DoclingDocument under those addresses.
type Chunk struct {
	Filename   string   `json:"filename"`
	ChunkIndex int      `json:"chunk_index"`
	Text       string   `json:"text"`
	RawText    string   `json:"raw_text"`
	NumTokens  int      `json:"num_tokens"`
	Headings   []string `json:"headings"`
	Captions   []string `json:"captions"`

	// DocItems are self-references into the document that was chunked, and
	// resolve against that document only.
	DocItems []string `json:"doc_items"`

	// PageNumbers are the pages the chunk's elements sit on, so a chunk that
	// straddles a page break lists both.
	PageNumbers []int `json:"page_numbers"`
}

// ChunkResponse is the body of a finished chunk task.
type ChunkResponse struct {
	Chunks         []Chunk `json:"chunks"`
	ProcessingTime float64 `json:"processing_time"`
}

func (c *Client) chunkHybridSourceAsyncEndpoint() (string, error) {
	return url.JoinPath(c.ClientConfig.URL, "/v1/chunk/hybrid/source/async")
}

// ChunkDoclingDocument hands a stored DoclingDocument back to docling to be
// chunked, and returns the task it accepted.
//
// The stored document is sent rather than the original file because
// converting the same file twice produces different element addresses, and a
// chunk names its elements by address. The returned task is polled through
// GetTaskStatus and GetChunks.
func (c *Client) ChunkDoclingDocument(
	ctx context.Context, filename string, doclingDocument json.RawMessage, options ChunkingOptions,
) (*AsyncDoclingResponse, error) {
	logger := log.FromContext(ctx)

	// Docling writes the JSON literal null when it ran and produced nothing, so
	// a length test alone lets four bytes of "null" through as a document.
	if trimmed := bytes.TrimSpace(doclingDocument); len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, errors.New("cannot chunk an empty docling document")
	}
	// The server rejects neither of these, and both waste a submit, a poll and
	// a semaphore slot to learn something knowable here.
	if strings.TrimSpace(options.Tokenizer) == "" {
		return nil, errors.New("cannot chunk without a tokenizer: docling's default has to be " +
			"downloaded, and a download it cannot make returns success with zero chunks")
	}
	if options.MaxTokens <= 0 {
		return nil, fmt.Errorf("cannot chunk with max_tokens %d: unset falls back to the "+
			"tokenizer's own limit, which is far larger than an embedding window", options.MaxTokens)
	}

	// TryAcquire rather than Acquire, so a full queue returns to the
	// reconciler instead of parking a worker on it.
	if !c.ClientConfig.sem.TryAcquire(1) {
		return nil, ErrSemaphoreUnavailable
	}
	// Held until the task is collected. Released here only if docling never
	// accepts it, because nothing else will know to.
	accepted := false
	defer func() {
		if !accepted {
			c.ClientConfig.sem.Release(1)
		}
	}()

	endpoint, err := c.chunkHybridSourceAsyncEndpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get chunk source async endpoint: %w", err)
	}

	options.Chunker = ChunkerHybrid
	payload, err := json.Marshal(ChunkRequestPayload{
		Sources: []ChunkFileSource{{
			Kind:         "file",
			Filename:     filename,
			Base64String: base64.StdEncoding.EncodeToString(doclingDocument),
		}},
		ChunkingOptions: &options,
		ConvertOptions: &ChunkConvertOptions{
			FromFormats: []string{InputFormatDoclingDocument},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal chunk payload: %w", err)
	}

	logger.Info("sending request to chunk document", "urlToSendRequest", endpoint, "filename", filename)

	responseBody, err := c.createDoclingRequest(ctx, http.MethodPost, endpoint, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to get response body: %w", err)
	}
	defer func() {
		if closeErr := responseBody.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close the chunk submit response body")
		}
	}()

	body, err := io.ReadAll(responseBody)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var asyncResponse AsyncDoclingResponse
	if err := json.Unmarshal(body, &asyncResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal async chunk response: %w", err)
	}

	// A 200 carrying something that is not a task envelope still unmarshals and
	// leaves TaskID empty. Treating that as accepted would hold the slot until
	// the process restarts, since only a poll ever frees it.
	if asyncResponse.TaskID == "" {
		return nil, errors.New("docling accepted the chunk request but returned no task id")
	}

	accepted = true
	return &asyncResponse, nil
}

// GetChunks reports where a chunk task has got to, and returns its chunks once
// it is finished.
//
// A task still running returns its status and no chunks, which is the normal
// answer and not an error. The semaphore slot taken at submission is released
// here on every outcome that ends the task, because this is the only place
// that learns the task has ended.
func (c *Client) GetChunks(ctx context.Context, taskID string) (TaskStatus, *ChunkResponse, error) {
	logger := log.FromContext(ctx)

	// The slot is NOT released on either of these: an unreadable status is no
	// evidence the task ended, and releasing again on the next attempt would
	// take a slot another task holds rather than panicking.
	isValidStatus, taskStatus, err := c.getTaskStatus(ctx, taskID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get task status: %w", err)
	}

	if !isValidStatus {
		return "", nil, fmt.Errorf("invalid task status received for task id: %s", taskID)
	}

	if taskStatus.TaskStatus == TaskStatusFailure {
		c.safeRelease()
		return "", nil, fmt.Errorf("%w: chunk task failed: task id: %s", ErrChunkTaskEnded, taskID)
	}

	// Still working. Leave the slot held, the caller will ask again.
	if taskStatus.TaskStatus == TaskStatusStarted || taskStatus.TaskStatus == TaskStatusPending {
		return taskStatus.TaskStatus, nil, nil
	}

	taskResultURL, err := c.getTaskResultEndpoint(taskID)
	if err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("%w: failed to get task result endpoint: %w", ErrChunkTaskEnded, err)
	}

	logger.Info("sending request to get chunks", "url", taskResultURL)
	bodyResponse, err := c.createDoclingRequest(ctx, http.MethodGet, taskResultURL, nil)
	if err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("%w: failed to get the chunk result: %w", ErrChunkTaskEnded, err)
	}
	defer func() {
		if closeErr := bodyResponse.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close the chunk result response body", "task id", taskID)
		}
	}()

	var chunkResponse ChunkResponse
	if err := json.NewDecoder(bodyResponse).Decode(&chunkResponse); err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("%w: failed to decode chunk response: %w", ErrChunkTaskEnded, err)
	}

	// The result body carries no status of its own, unlike a conversion
	// result. Reaching here means the poll said the task had finished, so the
	// slot is released whatever the body turned out to hold.
	c.safeRelease()
	logger.Info("chunk task completed", "task id", taskID, "chunks", len(chunkResponse.Chunks))

	// A chunker that could not load its tokenizer answers with
	// {"chunks": [], "status": "success", "errors": []}, so without this an
	// empty artifact would be stored and the cache key would never retry it.
	if len(chunkResponse.Chunks) == 0 {
		return taskStatus.TaskStatus, &chunkResponse, fmt.Errorf("%w: %w", ErrChunkTaskEnded, ErrNoChunksReturned)
	}

	return taskStatus.TaskStatus, &chunkResponse, nil
}
