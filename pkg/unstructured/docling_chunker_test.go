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

package unstructured

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// A page 1000 points square, so a normalised coordinate is the raw value with
// the decimal point moved and the arithmetic can be read at a glance.
const chunkerTestDocument = `{
  "texts":[
    {"self_ref":"#/texts/0","label":"text",
     "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
              "coord_origin":"TOPLEFT"}}]},
    {"self_ref":"#/texts/1","label":"text","parent":{"$ref":"#/texts/0"},"prov":[]}
  ],
  "pages":{"1":{"page_no":1,"size":{"width":1000.0,"height":1000.0}}}}`

// pollPath is where a task's status is read from, for the one task id every
// test in this file uses.
const pollPath = "/v1/status/poll/t-1"

func convertedDocument(doclingDocument string) *ConvertedDocument {
	content := &Content{Markdown: "some text"}
	if doclingDocument != "" {
		content.DoclingDocument = json.RawMessage(doclingDocument)
	}
	return &ConvertedDocument{
		Metadata: &ConvertedFileMetadata{RawFilePath: "raw/report.pdf", FileIdentifier: "uid-1"},
		Content:  content,
	}
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("writing the test response: %v", err)
	}
}

func testChunker(srv *httptest.Server) *DoclingChunker {
	return &DoclingChunker{
		Client: docling.NewClientFromURL(&docling.ClientConfig{URL: srv.URL, MaxConcurrentRequests: 2}),
		Options: docling.ChunkingOptions{
			Tokenizer: "/models/tok", MaxTokens: 2048, IncludeRawText: true,
		},
	}
}

// The interface's whole point: this chunker answers with a receipt rather
// than with chunks, and the caller reads that from the result instead of
// from the configuration that produced the chunker.
func TestDoclingChunkerChunkReturnsAReceiptAndNoChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{"task_id":"t-1","task_status":"pending"}`)
	}))
	defer srv.Close()

	result, err := testChunker(srv).Chunk(context.Background(), convertedDocument(chunkerTestDocument))

	require.NoError(t, err)
	assert.True(t, result.Pending(), "the work has only started")
	assert.Equal(t, "t-1", result.TaskID)
	assert.Empty(t, result.Chunks)
}

// Expected for anything converted before structured output was asked for.
// Retrying cannot help while the converted file stays as it is.
func TestDoclingChunkerChunkRefusesAMarkdownOnlyDocument(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("nothing should be submitted for a document that cannot be chunked")
	}))
	defer srv.Close()

	_, err := testChunker(srv).Chunk(context.Background(), convertedDocument(""))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoStructuredDocument)
}

// A running task is a normal answer, not a failure, and it comes back in the
// same pending shape Chunk used so the caller has one thing to check.
func TestDoclingChunkerCollectReportsATaskStillRunning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{"task_id":"t-1","task_status":"started"}`)
	}))
	defer srv.Close()

	result, err := testChunker(srv).Collect(context.Background(), "t-1", convertedDocument(chunkerTestDocument))

	require.NoError(t, err)
	assert.True(t, result.Pending())
	assert.Equal(t, "t-1", result.TaskID)
	assert.Empty(t, result.Chunks)
}

func TestDoclingChunkerCollectResolvesChunks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pollPath {
			writeTestJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
			return
		}
		writeTestJSON(t, w, `{"chunks":[{"chunk_index":0,
		  "text":"Heading\nbody","raw_text":"body",
		  "doc_items":["#/texts/0","#/texts/1"],"page_numbers":[3,1,2]}]}`)
	}))
	defer srv.Close()

	result, err := testChunker(srv).Collect(context.Background(), "t-1", convertedDocument(chunkerTestDocument))

	require.NoError(t, err)
	assert.False(t, result.Pending())
	require.Len(t, result.Chunks, 1)

	chunk := result.Chunks[0]
	assert.Equal(t, "Heading\nbody", chunk.Text, "the contextualised text is what gets embedded")
	assert.Equal(t, "body", chunk.RawText, "the body alone is what gets displayed")
	assert.Equal(t, []string{"#/texts/0", "#/texts/1"}, chunk.BlockIDs)
	assert.Equal(t, 1, chunk.PageFirst, "the lowest page the chunk touches")
	assert.Equal(t, 3, chunk.PageLast, "the highest, whatever order they arrived in")

	// Both addresses resolve to the same box, the second through its parent,
	// so a reader sees one rectangle rather than the same one twice.
	assert.Equal(t, []docling.Region{{PageNo: 1, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}}}, chunk.Regions)
}

// The stored artifact is read by position, so an out of order response would
// pair one chunk's text with another chunk's provenance, and nothing
// downstream could detect it.
func TestDoclingChunkerCollectOrdersChunksByIndex(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pollPath {
			writeTestJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
			return
		}
		writeTestJSON(t, w, `{"chunks":[
		  {"chunk_index":2,"text":"third"},
		  {"chunk_index":0,"text":"first"},
		  {"chunk_index":1,"text":"second"}]}`)
	}))
	defer srv.Close()

	result, err := testChunker(srv).Collect(context.Background(), "t-1", convertedDocument(chunkerTestDocument))

	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second", "third"}, ChunkTexts(result.Chunks))
}

// This is both how the chunker reports being unable to load its tokenizer and
// what a document with no content legitimately produces. Only the document
// tells them apart, so the chunker hands back nothing and lets the caller
// decide rather than failing here.
func TestDoclingChunkerCollectPassesAnEmptyResultToTheCaller(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pollPath {
			writeTestJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
			return
		}
		writeTestJSON(t, w, `{"chunks":[]}`)
	}))
	defer srv.Close()

	result, err := testChunker(srv).Collect(context.Background(), "t-1", convertedDocument(chunkerTestDocument))

	require.NoError(t, err)
	assert.False(t, result.Pending())
	assert.Empty(t, result.Chunks)
}

// The caller has to know whether a task is over, because collecting one that
// is releases its semaphore slot a second time, which silently takes a slot
// from a task that is still running.
func TestDoclingChunkerCollectMarksAnEndedTask(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{"task_id":"t-1","task_status":"failure"}`)
	}))
	defer srv.Close()

	_, err := testChunker(srv).Collect(context.Background(), "t-1", convertedDocument(chunkerTestDocument))

	require.Error(t, err)
	assert.ErrorIs(t, err, docling.ErrChunkTaskEnded)
}

// A task that finished but whose chunks cannot be placed is still a task that
// has ended. Reporting it as anything else would have the caller collect it
// again and release its slot twice.
func TestDoclingChunkerCollectMarksAnEndedTaskWhenResolutionFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pollPath {
			writeTestJSON(t, w, `{"task_id":"t-1","task_status":"success"}`)
			return
		}
		writeTestJSON(t, w, `{"chunks":[{"chunk_index":0,"text":"body"}]}`)
	}))
	defer srv.Close()

	_, err := testChunker(srv).Collect(context.Background(), "t-1", convertedDocument(`not a document`))

	require.Error(t, err)
	assert.ErrorIs(t, err, docling.ErrChunkTaskEnded)
}

// A transient failure to read a status leaves the task running and holding
// its slot, so it must not be marked as ended.
func TestDoclingChunkerCollectDoesNotMarkATaskItCouldNotRead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	_, err := testChunker(srv).Collect(context.Background(), "t-1", convertedDocument(chunkerTestDocument))

	require.Error(t, err)
	assert.NotErrorIs(t, err, docling.ErrChunkTaskEnded)
}

// A format with no pages produces chunks with no page numbers, and zero is
// the honest answer rather than something invented.
func TestPageRange(t *testing.T) {
	tests := []struct {
		name        string
		pageNumbers []int
		first, last int
	}{
		{name: "one page", pageNumbers: []int{4}, first: 4, last: 4},
		{name: "a chunk across a page break", pageNumbers: []int{7, 8}, first: 7, last: 8},
		{name: "arriving out of order", pageNumbers: []int{9, 2, 5}, first: 2, last: 9},
		{name: "no pages at all", pageNumbers: nil, first: 0, last: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first, last := pageRange(test.pageNumbers)
			assert.Equal(t, test.first, first)
			assert.Equal(t, test.last, last)
		})
	}
}
