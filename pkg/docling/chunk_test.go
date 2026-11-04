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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// convertOptionsKey is the key the chunk endpoints read. The convert
// endpoints read "options", and docling answers 200 while discarding
// everything sent under a key it does not know, so sending the wrong one
// means the request silently loses from_formats and parses the document
// again instead of reading it back.
const convertOptionsKey = "convert_options"

const storedDocument = `{"schema_name":"DoclingDocument","version":"1.10.0",` +
	`"texts":[{"self_ref":"#/texts/0","text":"hello"}]}`

// newChunkTestClient returns a client pointed at srv with one semaphore slot,
// which is the smallest setting that still lets a leak show up as a second
// call failing.
func newChunkTestClient(srv *httptest.Server) *Client {
	return NewClientFromURL(&ClientConfig{URL: srv.URL, MaxConcurrentRequests: 1})
}

// TestChunkDoclingDocumentSendsTheRightRequest pins the wire format, because
// every part of it has a near-miss that fails silently rather than loudly.
func TestChunkDoclingDocumentSendsTheRightRequest(t *testing.T) {
	var gotPath string
	var body map[string]json.RawMessage

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		_, _ = w.Write([]byte(`{"task_id":"t-1","task_status":"pending"}`))
	}))
	defer srv.Close()

	resp, err := newChunkTestClient(srv).ChunkDoclingDocument(
		context.Background(), "doc.json", json.RawMessage(storedDocument),
		ChunkingOptions{Tokenizer: "/models/tok", MaxTokens: 2048, IncludeRawText: true, UseMarkdownTables: true},
	)
	require.NoError(t, err)
	assert.Equal(t, "t-1", resp.TaskID)
	assert.Equal(t, "/v1/chunk/hybrid/source/async", gotPath)

	// from_formats has to arrive under convert_options. Under "options" it is
	// dropped, and docling then parses the file rather than reading it back.
	require.Contains(t, body, convertOptionsKey)
	require.NotContains(t, body, "options")
	var convertOptions struct {
		FromFormats []string `json:"from_formats"`
	}
	require.NoError(t, json.Unmarshal(body[convertOptionsKey], &convertOptions))
	assert.Equal(t, []string{"json_docling"}, convertOptions.FromFormats,
		"json_docling is an input format; without it the document is parsed again")

	// The chunker name has to agree with the endpoint the path selected.
	var chunkingOptions struct {
		Chunker           string `json:"chunker"`
		Tokenizer         string `json:"tokenizer"`
		MaxTokens         int    `json:"max_tokens"`
		IncludeRawText    bool   `json:"include_raw_text"`
		UseMarkdownTables bool   `json:"use_markdown_tables"`
	}
	require.NoError(t, json.Unmarshal(body["chunking_options"], &chunkingOptions))
	assert.Equal(t, "hybrid", chunkingOptions.Chunker)
	assert.Equal(t, "/models/tok", chunkingOptions.Tokenizer)
	assert.Equal(t, 2048, chunkingOptions.MaxTokens)
	assert.True(t, chunkingOptions.IncludeRawText, "without this there is no text to display")
	assert.True(t, chunkingOptions.UseMarkdownTables)

	// The document goes inline as a file. The chunk endpoints take only
	// "file" and "http" kinds, and nothing else is holding these bytes.
	var sources []struct {
		Kind         string `json:"kind"`
		Filename     string `json:"filename"`
		Base64String string `json:"base64_string"`
	}
	require.NoError(t, json.Unmarshal(body["sources"], &sources))
	require.Len(t, sources, 1)
	assert.Equal(t, "file", sources[0].Kind)
	assert.Equal(t, "doc.json", sources[0].Filename)

	decoded, err := base64.StdEncoding.DecodeString(sources[0].Base64String)
	require.NoError(t, err)
	assert.JSONEq(t, storedDocument, string(decoded),
		"the document has to arrive unchanged, or its addresses move")
}

// An empty document must not be sent. Docling would accept the request and
// fail the task, which costs a submit, a poll and a slot to learn something
// knowable here.
func TestChunkDoclingDocumentRejectsAnEmptyDocument(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("no request should have been sent")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := newChunkTestClient(srv).ChunkDoclingDocument(
		context.Background(), "doc.json", nil, ChunkingOptions{})
	require.Error(t, err)
}

// A rejected submit has to give the slot back. Holding it would leak one
// permit per failure until the stage stops chunking anything, reporting
// success the whole time.
func TestChunkDoclingDocumentReleasesTheSlotWhenTheSubmitFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := newChunkTestClient(srv)
	for range 3 {
		_, err := client.ChunkDoclingDocument(
			context.Background(), "doc.json", json.RawMessage(storedDocument), ChunkingOptions{})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), SemaphoreAcquireError,
			"the slot from the previous failure was never released")
	}
}

// GetChunks on a task still running returns its status and no chunks. That is
// the normal answer and not an error, and the slot stays held.
func TestGetChunksWhileTheTaskIsStillRunning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status/poll/t-1" {
			_, _ = w.Write([]byte(`{"task_id":"t-1","task_status":"started"}`))
			return
		}
		t.Errorf("the result should not have been fetched for a running task, got %s", r.URL.Path)
	}))
	defer srv.Close()

	status, chunks, err := newChunkTestClient(srv).GetChunks(context.Background(), "t-1")
	require.NoError(t, err)
	assert.Equal(t, TaskStatusStarted, status)
	assert.Nil(t, chunks)
}

// A finished task returns its chunks, carrying the two things the stored
// chunk cannot be built without: the addresses of its elements and the pages
// they sit on.
func TestGetChunksReturnsFinishedChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status/poll/t-1":
			_, _ = w.Write([]byte(`{"task_id":"t-1","task_status":"success"}`))
		case "/v1/result/t-1":
			_, _ = w.Write([]byte(`{"chunks":[{"chunk_index":0,"text":"Heading\nbody",` +
				`"raw_text":"body","num_tokens":3,"headings":["Heading"],` +
				`"doc_items":["#/texts/0","#/texts/1"],"page_numbers":[1,2]}],` +
				`"processing_time":0.8}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	status, response, err := newChunkTestClient(srv).GetChunks(context.Background(), "t-1")
	require.NoError(t, err)
	assert.Equal(t, TaskStatusSuccess, status)
	require.NotNil(t, response)
	require.Len(t, response.Chunks, 1)

	chunk := response.Chunks[0]
	assert.Equal(t, "Heading\nbody", chunk.Text, "the contextualised text is what gets embedded")
	assert.Equal(t, "body", chunk.RawText, "the raw text is what gets displayed")
	assert.Equal(t, []string{"Heading"}, chunk.Headings)
	assert.Equal(t, []string{"#/texts/0", "#/texts/1"}, chunk.DocItems)
	assert.Equal(t, []int{1, 2}, chunk.PageNumbers, "a chunk can straddle a page break")
}

// A failed task has to give the slot back too, and say so rather than
// returning an empty result that reads as a document with no chunks.
func TestGetChunksReleasesTheSlotWhenTheTaskFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"task_id":"t-1","task_status":"failure"}`))
	}))
	defer srv.Close()

	client := newChunkTestClient(srv)
	_, _, err := client.GetChunks(context.Background(), "t-1")
	require.Error(t, err)

	// The slot is free, so a submit can still be made.
	submitted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"task_id":"t-2","task_status":"pending"}`))
	}))
	defer submitted.Close()
	client.ClientConfig.URL = submitted.URL

	_, err = client.ChunkDoclingDocument(
		context.Background(), "doc.json", json.RawMessage(storedDocument), ChunkingOptions{})
	require.NoError(t, err, "the slot from the failed task was never released")
}
