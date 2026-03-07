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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

func newTestDoclingClient(serverURL string) *docling.Client {
	return docling.NewClientFromURL(&docling.ClientConfig{
		URL:                   serverURL,
		MaxConcurrentRequests: 5,
	})
}

// asyncChunkServer creates a test server that handles the async chunk flow:
// POST /v1/chunk/{type}/source/async -> TaskStatusResponse
// GET /v1/status/poll/{taskID} -> TaskStatusResponse (success)
// GET /v1/result/{taskID} -> ChunkDocumentResponse
func asyncChunkServer(t *testing.T, expectedPath string, taskID string, chunks []docling.ChunkedDocumentResultItem) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == expectedPath:
			_ = json.NewEncoder(w).Encode(docling.TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: docling.TaskStatusSuccess,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status/poll/"+taskID:
			_ = json.NewEncoder(w).Encode(docling.TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: docling.TaskStatusSuccess,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/result/"+taskID:
			_ = json.NewEncoder(w).Encode(docling.ChunkDocumentResponse{
				Chunks: chunks,
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestDoclingHierarchicalChunker_ImplementsChunker(t *testing.T) {
	server := asyncChunkServer(t, "/v1/chunk/hierarchical/source/async", "hier-task-1",
		[]docling.ChunkedDocumentResultItem{
			{Text: "chunk one"},
			{Text: "chunk two"},
		})
	defer server.Close()

	mergeListItems := true
	var chunker Chunker = &DoclingHierarchicalChunker{
		DoclingClient: newTestDoclingClient(server.URL),
		Options: &docling.HierarchicalChunkingOptions{
			MergeListItems: &mergeListItems,
		},
	}

	chunks, err := chunker.Chunk(context.Background(), "https://example.com/doc.pdf")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[0] != "chunk one" {
		t.Errorf("unexpected first chunk: %s", chunks[0])
	}
	if chunks[1] != "chunk two" {
		t.Errorf("unexpected second chunk: %s", chunks[1])
	}
}

func TestDoclingHybridChunker_ImplementsChunker(t *testing.T) {
	server := asyncChunkServer(t, "/v1/chunk/hybrid/source/async", "hybrid-task-1",
		[]docling.ChunkedDocumentResultItem{
			{Text: "hybrid one"},
			{Text: "hybrid two"},
			{Text: "hybrid three"},
		})
	defer server.Close()

	mergePeers := false
	var chunker Chunker = &DoclingHybridChunker{
		DoclingClient: newTestDoclingClient(server.URL),
		Options: &docling.HybridChunkingOptions{
			Tokenizer:  "sentence-transformers/all-MiniLM-L6-v2",
			MaxTokens:  512,
			MergePeers: &mergePeers,
		},
	}

	chunks, err := chunker.Chunk(context.Background(), "https://example.com/doc.pdf")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if chunks[0] != "hybrid one" {
		t.Errorf("unexpected first chunk: %s", chunks[0])
	}
}
