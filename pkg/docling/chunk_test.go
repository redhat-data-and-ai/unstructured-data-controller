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
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/sync/semaphore"
)

func newTestClient(serverURL string, maxConcurrent int64) *Client {
	return &Client{
		ClientConfig: &ClientConfig{
			URL:                   serverURL,
			MaxConcurrentRequests: maxConcurrent,
			sem:                   semaphore.NewWeighted(maxConcurrent),
		},
	}
}

func TestChunkFile_Hierarchical(t *testing.T) {
	response := DoclingChunkResponse{
		Chunks: []DoclingChunk{
			{Text: "This is the first chunk."},
			{Text: "This is the second chunk."},
			{Text: "This is the third chunk."},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/v1/chunk/hierarchical/source" {
			t.Errorf("expected /v1/chunk/hierarchical/source, got %s", r.URL.Path)
		}

		var payload DoclingChunkRequestPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}
		if len(payload.Sources) != 1 || payload.Sources[0].URL != "https://example.com/doc.pdf" {
			t.Errorf("unexpected sources: %+v", payload.Sources)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := newTestClient(server.URL, 5)
	mergeListItems := true
	options := &HierarchicalChunkingOptions{MergeListItems: &mergeListItems}

	chunks, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHierarchical, options)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if chunks[0] != "This is the first chunk." {
		t.Errorf("unexpected first chunk: %s", chunks[0])
	}
	if chunks[1] != "This is the second chunk." {
		t.Errorf("unexpected second chunk: %s", chunks[1])
	}
	if chunks[2] != "This is the third chunk." {
		t.Errorf("unexpected third chunk: %s", chunks[2])
	}
}

func TestChunkFile_Hybrid(t *testing.T) {
	response := DoclingChunkResponse{
		Chunks: []DoclingChunk{
			{Text: "Hybrid chunk one."},
			{Text: "Hybrid chunk two."},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chunk/hybrid/source" {
			t.Errorf("expected /v1/chunk/hybrid/source, got %s", r.URL.Path)
		}

		var payload DoclingChunkRequestPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("failed to decode request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := newTestClient(server.URL, 5)
	mergePeers := false
	options := &HybridChunkingOptions{
		Tokenizer:  "sentence-transformers/all-MiniLM-L6-v2",
		MaxTokens:  512,
		MergePeers: &mergePeers,
	}

	chunks, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHybrid, options)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	if chunks[0] != "Hybrid chunk one." {
		t.Errorf("unexpected first chunk: %s", chunks[0])
	}
}

func TestChunkFile_SemaphoreAcquireError(t *testing.T) {
	// Create a client with max concurrency of 1 and acquire the only slot
	client := newTestClient("http://localhost:9999", 1)
	client.ClientConfig.sem.TryAcquire(1)

	_, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHierarchical, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != SemaphoreAcquireError {
		t.Errorf("expected semaphore acquire error, got: %s", err.Error())
	}

	// Release the slot we acquired
	client.ClientConfig.sem.Release(1)
}

func TestChunkFile_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := newTestClient(server.URL, 5)

	_, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHierarchical, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestChunkSourceEndpoint(t *testing.T) {
	client := newTestClient("http://localhost:5001", 1)

	hierarchicalEndpoint, err := client.chunkSourceEndpoint(ChunkTypeHierarchical)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hierarchicalEndpoint != "http://localhost:5001/v1/chunk/hierarchical/source" {
		t.Errorf("unexpected endpoint: %s", hierarchicalEndpoint)
	}

	hybridEndpoint, err := client.chunkSourceEndpoint(ChunkTypeHybrid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hybridEndpoint != "http://localhost:5001/v1/chunk/hybrid/source" {
		t.Errorf("unexpected endpoint: %s", hybridEndpoint)
	}
}

func TestChunkRequestPayload_Serialization(t *testing.T) {
	mergeListItems := true
	payload := DoclingChunkRequestPayload{
		Sources: []DoclingSource{
			{URL: "https://example.com/doc.pdf", Kind: "http"},
		},
		ChunkingOptions: &HierarchicalChunkingOptions{
			MergeListItems: &mergeListItems,
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Verify top-level field names match Docling API
	if _, ok := result["sources"]; !ok {
		t.Error("expected 'sources' field in JSON")
	}
	if _, ok := result["chunking_options"]; !ok {
		t.Error("expected 'chunking_options' field in JSON")
	}

	// Verify chunking_options field names use snake_case
	chunkingOpts, ok := result["chunking_options"].(map[string]any)
	if !ok {
		t.Fatal("chunking_options is not a map")
	}
	if _, ok := chunkingOpts["merge_list_items"]; !ok {
		t.Error("expected 'merge_list_items' field in chunking_options")
	}
}

func TestChunkRequestPayload_HybridSerialization(t *testing.T) {
	mergePeers := false
	payload := DoclingChunkRequestPayload{
		Sources: []DoclingSource{
			{URL: "https://example.com/doc.pdf", Kind: "http"},
		},
		ChunkingOptions: &HybridChunkingOptions{
			Tokenizer:  "sentence-transformers/all-MiniLM-L6-v2",
			MaxTokens:  512,
			MergePeers: &mergePeers,
		},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	chunkingOpts, ok := result["chunking_options"].(map[string]any)
	if !ok {
		t.Fatal("chunking_options is not a map")
	}
	if _, ok := chunkingOpts["tokenizer"]; !ok {
		t.Error("expected 'tokenizer' field in chunking_options")
	}
	if _, ok := chunkingOpts["max_tokens"]; !ok {
		t.Error("expected 'max_tokens' field in chunking_options")
	}
	if _, ok := chunkingOpts["merge_peers"]; !ok {
		t.Error("expected 'merge_peers' field in chunking_options")
	}
}

func TestChunkFile_EmptyResponse(t *testing.T) {
	response := DoclingChunkResponse{
		Chunks: []DoclingChunk{},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := newTestClient(server.URL, 5)

	chunks, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHierarchical, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(chunks))
	}
}

func TestChunkFile_AuthHeader(t *testing.T) {
	var receivedAuthHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(DoclingChunkResponse{Chunks: []DoclingChunk{}})
	}))
	defer server.Close()

	client := &Client{
		ClientConfig: &ClientConfig{
			URL:                   server.URL,
			Key:                   "test-api-key",
			MaxConcurrentRequests: 5,
			sem:                   semaphore.NewWeighted(5),
		},
	}

	_, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHierarchical, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if receivedAuthHeader != "Bearer test-api-key" {
		t.Errorf("expected 'Bearer test-api-key', got '%s'", receivedAuthHeader)
	}
}
