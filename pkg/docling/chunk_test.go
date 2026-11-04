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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

const (
	pollPath   = "/v1/status/poll/t-1"
	resultPath = "/v1/result/t-1"
)

const storedDocument = `{"schema_name":"DoclingDocument","version":"1.10.0",` +
	`"texts":[{"self_ref":"#/texts/0","text":"hello"}]}`

// writeJSON replies from a test handler and reports a failed write.
//
// A discarded write error leaves the client with a truncated body, so the
// test fails on a decode error somewhere else and never says the write was
// the problem. require must not be used here: handlers run on the server
// goroutine, and a failed require there does not stop the test.
func writeJSON(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("writing the test response: %v", err)
	}
}

// validOptions is what every test that is not about validation sends, since
// ChunkDoclingDocument now rejects a request that would reach docling without
// a tokenizer or a token budget.
func validOptions() ChunkingOptions {
	return ChunkingOptions{Tokenizer: "/models/tok", MaxTokens: 2048, IncludeRawText: true}
}

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
		writeJSON(t, w, `{"task_id":"t-1","task_status":"pending"}`)
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
	// The JSON literal null is the one worth spelling out. It is what the
	// convert response carries when docling ran and produced no document,
	// it is four bytes, so a length test alone lets it through, and
	// JSONDocument in this package already treats it as absent.
	for _, document := range []json.RawMessage{
		nil,
		json.RawMessage(``),
		json.RawMessage(`   `),
		json.RawMessage(`null`),
		json.RawMessage(`  null  `),
	} {
		t.Run(string(document), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Error("no request should have been sent")
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer srv.Close()

			_, err := newChunkTestClient(srv).ChunkDoclingDocument(
				context.Background(), "doc.json", document, validOptions())
			require.Error(t, err)
		})
	}
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
			context.Background(), "doc.json", json.RawMessage(storedDocument), validOptions())
		require.Error(t, err)
		assert.NotContains(t, err.Error(), SemaphoreAcquireError,
			"the slot from the previous failure was never released")
	}
}

// GetChunks on a task still running returns its status and no chunks. That is
// the normal answer and not an error, and the slot stays held.
func TestGetChunksWhileTheTaskIsStillRunning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pollPath {
			writeJSON(t, w, `{"task_id":"t-1","task_status":"started"}`)
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
		case pollPath:
			writeJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
		case resultPath:
			writeJSON(t, w, `{"chunks":[{"chunk_index":0,"text":"Heading\nbody",`+
				`"raw_text":"body","num_tokens":3,"headings":["Heading"],`+
				`"doc_items":["#/texts/0","#/texts/1"],"page_numbers":[1,2]}],`+
				`"processing_time":0.8}`)
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
		writeJSON(t, w, `{"task_id":"t-1","task_status":"failure"}`)
	}))
	defer srv.Close()

	client := newChunkTestClient(srv)

	// Take the only slot first. Without this the test passes whether or not
	// the release happens, because the submit below finds a free slot
	// either way.
	accepted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, `{"task_id":"t-1","task_status":"pending"}`)
	}))
	client.ClientConfig.URL = accepted.URL
	_, err := client.ChunkDoclingDocument(
		context.Background(), "doc.json", json.RawMessage(storedDocument), validOptions())
	require.NoError(t, err)
	accepted.Close()

	client.ClientConfig.URL = srv.URL
	_, _, err = client.GetChunks(context.Background(), "t-1")
	require.Error(t, err)

	// The slot came back, so a submit can take it again.
	submitted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, `{"task_id":"t-2","task_status":"pending"}`)
	}))
	defer submitted.Close()
	client.ClientConfig.URL = submitted.URL

	_, err = client.ChunkDoclingDocument(
		context.Background(), "doc.json", json.RawMessage(storedDocument), validOptions())
	require.NoError(t, err, "the slot from the failed task was never released")
}

// Neither of these is rejected by the server, and both cost a submit, a poll
// and a slot to learn something knowable before any of them. An empty
// tokenizer returns success with zero chunks after a long timeout; a zero
// budget falls back to the tokenizer's own limit and produces chunks too
// large to embed.
func TestChunkDoclingDocumentRejectsOptionsThatCannotWork(t *testing.T) {
	tests := []struct {
		name    string
		options ChunkingOptions
		wantErr string
	}{
		{
			name:    "no tokenizer",
			options: ChunkingOptions{MaxTokens: 2048},
			wantErr: "tokenizer",
		},
		{
			name:    "a tokenizer of only whitespace",
			options: ChunkingOptions{Tokenizer: "   ", MaxTokens: 2048},
			wantErr: "tokenizer",
		},
		{
			name:    "no token budget",
			options: ChunkingOptions{Tokenizer: "/models/tok"},
			wantErr: "max_tokens",
		},
		{
			name:    "a negative token budget",
			options: ChunkingOptions{Tokenizer: "/models/tok", MaxTokens: -1},
			wantErr: "max_tokens",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Error("no request should have been sent")
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer srv.Close()

			_, err := newChunkTestClient(srv).ChunkDoclingDocument(
				context.Background(), "doc.json", json.RawMessage(storedDocument), tt.options)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// A 200 carrying something that is not a task envelope has to be refused.
// Unmarshalling succeeds for any JSON object, so the task id comes back
// empty, and a slot held for a task nobody can poll is never given back:
// the only release path runs on a poll.
func TestChunkDoclingDocumentRejectsAResponseWithNoTaskID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, `{"detail":"something else entirely"}`)
	}))
	defer srv.Close()

	client := newChunkTestClient(srv)
	_, err := client.ChunkDoclingDocument(
		context.Background(), "doc.json", json.RawMessage(storedDocument), validOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no task id")

	// And the slot came back, so the next submit can still take one.
	accepted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, `{"task_id":"t-2","task_status":"pending"}`)
	}))
	defer accepted.Close()
	client.ClientConfig.URL = accepted.URL

	_, err = client.ChunkDoclingDocument(
		context.Background(), "doc.json", json.RawMessage(storedDocument), validOptions())
	require.NoError(t, err, "the slot was held for a task that was never accepted")
}

// A poll that cannot be read is not evidence the task ended, so the slot
// stays held.
//
// Releasing it would also release again on the next attempt, and Release on
// a weighted semaphore is a bare subtraction with no record of who holds
// what. While another task holds a slot, the extra release takes that one
// rather than panicking, and real concurrency against docling quietly
// exceeds the configured limit.
func TestGetChunksKeepsTheSlotWhenTheStatusCannotBeRead(t *testing.T) {
	polls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/status/poll/") {
			polls++
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		t.Errorf("unexpected path %s", r.URL.Path)
	}))
	defer srv.Close()

	// Two slots, so one submit can still be made if exactly one leaks, and
	// the assertion below can tell the difference.
	client := NewClientFromURL(&ClientConfig{URL: srv.URL, MaxConcurrentRequests: 2})

	// Take both slots.
	for range 2 {
		accepted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(t, w, `{"task_id":"t-1","task_status":"pending"}`)
		}))
		url := client.ClientConfig.URL
		client.ClientConfig.URL = accepted.URL
		_, err := client.ChunkDoclingDocument(
			context.Background(), "doc.json", json.RawMessage(storedDocument), validOptions())
		require.NoError(t, err)
		client.ClientConfig.URL = url
		accepted.Close()
	}

	// Three failed polls. Each one used to hand a slot back.
	for range 3 {
		_, _, err := client.GetChunks(context.Background(), "t-1")
		require.Error(t, err)
	}
	assert.Equal(t, 3, polls)

	// Both tasks are still running, so both slots are still held and a third
	// submit must be refused.
	_, err := client.ChunkDoclingDocument(
		context.Background(), "doc.json", json.RawMessage(storedDocument), validOptions())
	require.Error(t, err)
	assert.Contains(t, err.Error(), SemaphoreAcquireError,
		"a failed poll released a slot for a task that is still running")
}

// The silent failure this client exists to catch. A chunker that could not
// load its tokenizer answers with a successful task, an empty errors list and
// no chunks, so every other signal says it worked.
//
// The chunks come back alongside the error, because nothing here can tell
// that case apart from a document that genuinely held nothing.
func TestGetChunksReportsASuccessfulTaskThatProducedNothing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case pollPath:
			writeJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
		case resultPath:
			writeJSON(t, w, `{"chunks":[],"processing_time":58.2}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	status, response, err := newChunkTestClient(srv).GetChunks(context.Background(), "t-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoChunksReturned)
	assert.Equal(t, TaskStatusSuccess, status, "docling reported success, and that is worth seeing")
	require.NotNil(t, response, "the caller decides whether an empty document is acceptable")
	assert.Empty(t, response.Chunks)
}

// Whether a task has ended cannot be read from an error message, and the
// caller has to know: polling a task that has ended releases its slot a
// second time, and a weighted semaphore keeps no record of who holds what, so
// the second release takes a slot from a task that is still running.
func TestGetChunksMarksTheErrorsThatMeanTheTaskHasEnded(t *testing.T) {
	tests := []struct {
		name     string
		handler  http.HandlerFunc
		hasEnded bool
	}{
		{
			name: "docling reported the task as failed",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, `{"task_id":"t-1","task_status":"failure"}`)
			},
			hasEnded: true,
		},
		{
			name: "the result could not be fetched",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == pollPath {
					writeJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
			},
			hasEnded: true,
		},
		{
			name: "the result was not a chunk response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == pollPath {
					writeJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
					return
				}
				writeJSON(t, w, `not json`)
			},
			hasEnded: true,
		},
		{
			name: "the task succeeded and produced nothing",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == pollPath {
					writeJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
					return
				}
				writeJSON(t, w, `{"chunks":[]}`)
			},
			hasEnded: true,
		},
		{
			// The one case that leaves the task alive. Nothing was learned
			// about it, so the same id has to be polled again.
			name: "the status could not be read",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
			},
			hasEnded: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := httptest.NewServer(test.handler)
			defer srv.Close()

			_, _, err := newChunkTestClient(srv).GetChunks(context.Background(), "t-1")
			require.Error(t, err)
			if test.hasEnded {
				assert.ErrorIs(t, err, ErrChunkTaskEnded)
				return
			}
			assert.NotErrorIs(t, err, ErrChunkTaskEnded)
		})
	}
}
