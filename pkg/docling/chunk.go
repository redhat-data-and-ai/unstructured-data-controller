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
// already a DoclingDocument, so it reads it back instead of parsing it.
//
// It is an input *format* and not a source kind. The only kinds the server
// accepts are "file" and "http", so a stored document is sent as a file whose
// contents happen to be the document.
const InputFormatDoclingDocument = "json_docling"

// ChunkerHybrid splits on the document's structure rather than on a character
// count. It is the only chunker this client asks for; the endpoint is chosen
// by path, and the field has to agree with it.
const ChunkerHybrid = "hybrid"

// ErrNoChunksReturned reports a chunk task that finished successfully and
// produced nothing.
//
// This is how the chunker reports being unable to load its tokenizer: the
// task status is success, the errors list is empty, and the chunks list is
// empty too. Nothing else in the response distinguishes it from a document
// that really had no content, which is why the chunks are returned alongside
// this error rather than instead of it.
var ErrNoChunksReturned = errors.New("the chunk task succeeded and returned no chunks")

// ChunkingOptions is what the chunk endpoints read as "chunking_options".
//
// Every value here is set explicitly because not one of docling's own
// defaults suits this use. Leaving tokenizer out reaches for a model name it
// has to download, and a download it cannot make returns success with zero
// chunks. Leaving MaxTokens out falls back to the tokenizer's own limit,
// several times an embedding window. The two flags default off, which returns
// no text to display and serialises tables as value triplets.
type ChunkingOptions struct {
	Chunker           string `json:"chunker"`
	Tokenizer         string `json:"tokenizer"`
	MaxTokens         int    `json:"max_tokens"`
	IncludeRawText    bool   `json:"include_raw_text"`
	UseMarkdownTables bool   `json:"use_markdown_tables"`
}

// ChunkConvertOptions is the "convert_options" block of a chunk request.
//
// Note the key. The convert endpoints read "options" and the chunk endpoints
// read "convert_options", and docling drops a key it does not recognise and
// still returns 200, so the wrong one here means every option is silently
// discarded.
type ChunkConvertOptions struct {
	FromFormats []string `json:"from_formats"`
}

// ChunkFileSource carries the document inline. The chunk endpoints take the
// same source kinds as the convert endpoints, and "file" is the one that can
// carry bytes we hold rather than a URL something else must fetch.
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
// It already carries almost everything a stored chunk needs. Text is the
// contextualised form, with the heading trail prepended, and is what gets
// embedded so a passage is findable under its section. RawText is the body
// alone and is what gets displayed.
//
// What it does not carry is geometry. DocItems holds the addresses of the
// elements the chunk was built from, for example "#/texts/4", and the
// rectangles live in the DoclingDocument under those addresses.
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
// Sending the stored document rather than the original file is not an
// optimisation. Converting the same file twice produces a different document,
// and a chunk names its elements by address, so addresses from one parse
// resolve against another parse to the wrong elements. Reusing the stored
// document is also the difference between seconds and milliseconds, since
// nothing is parsed again.
//
// Asynchronous because a synchronous call holds the connection for as long as
// the work takes, which on a real document is minutes, and on a misconfigured
// tokenizer is a minute of waiting to be told nothing. The task it returns is
// polled through GetTaskStatus and GetChunks, the same endpoints conversion
// already uses.
func (c *Client) ChunkDoclingDocument(
	ctx context.Context, filename string, doclingDocument json.RawMessage, options ChunkingOptions,
) (*AsyncDoclingResponse, error) {
	logger := log.FromContext(ctx)

	// The same rule JSONDocument applies, for the same reason: the field is
	// absent when nothing asked for it and the JSON literal null when docling
	// ran and produced nothing, and null is four bytes, so a length test
	// alone lets it through. Sending it would base64 "null" as a document and
	// hold a slot until the resulting failure is collected.
	if trimmed := bytes.TrimSpace(doclingDocument); len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, errors.New("cannot chunk an empty docling document")
	}
	// Checked here for the same reason the document is, and with a stronger
	// case. Neither of these is rejected by the server. An empty tokenizer
	// falls back to a model name docling has to download, and where it cannot
	// the request returns HTTP 200 with zero chunks after the download times
	// out, which was 58 seconds when measured. A MaxTokens of zero falls back
	// to the tokenizer's own limit, several times an embedding window, so the
	// chunks come back too large to embed. Both cost a submit, a poll and a
	// semaphore slot to learn something knowable before any of them.
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
		return nil, errors.New(SemaphoreAcquireError)
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

	// Unmarshalling into this struct succeeds for any JSON object, so a 200
	// carrying something that is not a task envelope leaves TaskID empty.
	// Treating that as accepted would hold the slot for a task nobody can
	// poll, and the only path that frees it runs on a poll, so it would be
	// held until the process restarts.
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
// here, on every outcome that ends the task, because this is the only place
// that learns the task has ended.
//
// It polls the same endpoints conversion does. Docling does not separate them
// by task type, so the only thing specific to chunking is how the result is
// decoded.
func (c *Client) GetChunks(ctx context.Context, taskID string) (TaskStatus, *ChunkResponse, error) {
	logger := log.FromContext(ctx)

	// The slot is NOT released on either of these. Failing to read a status
	// is not evidence that the task ended, and the task carries on holding
	// docling's own capacity either way. Releasing here would also release
	// again on the next attempt, and Release on a weighted semaphore is a
	// bare subtraction with no record of who holds what, so while other
	// tasks hold slots the extra release takes theirs instead of panicking.
	// Real concurrency against docling then quietly exceeds the configured
	// limit.
	isValidStatus, taskStatus, err := c.getTaskStatus(ctx, taskID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get task status: %w", err)
	}

	if !isValidStatus {
		return "", nil, fmt.Errorf("invalid task status received for task id: %s", taskID)
	}

	if taskStatus.TaskStatus == TaskStatusFailure {
		c.safeRelease()
		return "", nil, fmt.Errorf("chunk task failed: task id: %s", taskID)
	}

	// Still working. Leave the slot held, the caller will ask again.
	if taskStatus.TaskStatus == TaskStatusStarted || taskStatus.TaskStatus == TaskStatusPending {
		return taskStatus.TaskStatus, nil, nil
	}

	taskResultURL, err := c.getTaskResultEndpoint(taskID)
	if err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("failed to get task result endpoint: %w", err)
	}

	logger.Info("sending request to get chunks", "url", taskResultURL)
	bodyResponse, err := c.createDoclingRequest(ctx, http.MethodGet, taskResultURL, nil)
	if err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("failed to get response body: %w", err)
	}
	defer func() {
		if closeErr := bodyResponse.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close the chunk result response body", "task id", taskID)
		}
	}()

	var chunkResponse ChunkResponse
	if err := json.NewDecoder(bodyResponse).Decode(&chunkResponse); err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("failed to decode chunk response: %w", err)
	}

	// The result body carries no status of its own, unlike a conversion
	// result. Reaching here means the poll said the task had finished, so the
	// slot is released whatever the body turned out to hold.
	c.safeRelease()
	logger.Info("chunk task completed", "task id", taskID, "chunks", len(chunkResponse.Chunks))

	// Docling's documented silent failure, and the reason this is an error
	// rather than a log line. A chunker that could not load its tokenizer
	// answers with {"chunks": [], "status": "success", "errors": []}. Every
	// signal above says the task succeeded, so a caller reading only the
	// error would store an empty artifact, and the cache key would then match
	// on the next reconcile and never retry it.
	//
	// The chunks are returned alongside, because this client cannot tell that
	// case apart from a document that genuinely holds nothing. A caller that
	// knows the input was empty can look at the error and carry on.
	if len(chunkResponse.Chunks) == 0 {
		return taskStatus.TaskStatus, &chunkResponse, ErrNoChunksReturned
	}

	return taskStatus.TaskStatus, &chunkResponse, nil
}
