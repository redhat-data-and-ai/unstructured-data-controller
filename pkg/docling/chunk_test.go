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
	taskID := "test-task-123"
	chunkResult := ChunkDocumentResponse{
		Chunks: []ChunkedDocumentResultItem{
			{Text: "This is the first chunk."},
			{Text: "This is the second chunk."},
			{Text: "This is the third chunk."},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/chunk/hierarchical/source/async":
			var payload DoclingChunkRequestPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("failed to decode request body: %v", err)
			}
			if len(payload.Sources) != 1 || payload.Sources[0].URL != "https://example.com/doc.pdf" {
				t.Errorf("unexpected sources: %+v", payload.Sources)
			}
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: TaskStatusSuccess,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status/poll/"+taskID:
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: TaskStatusSuccess,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/result/"+taskID:
			_ = json.NewEncoder(w).Encode(chunkResult)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
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
	taskID := "test-hybrid-456"
	chunkResult := ChunkDocumentResponse{
		Chunks: []ChunkedDocumentResultItem{
			{Text: "Hybrid chunk one."},
			{Text: "Hybrid chunk two."},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/chunk/hybrid/source/async":
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: TaskStatusSuccess,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status/poll/"+taskID:
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: TaskStatusSuccess,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/result/"+taskID:
			_ = json.NewEncoder(w).Encode(chunkResult)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
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

func TestChunkFile_AsyncPolling(t *testing.T) {
	taskID := "poll-task-789"
	pollCount := 0
	chunkResult := ChunkDocumentResponse{
		Chunks: []ChunkedDocumentResultItem{
			{Text: "polled chunk"},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/chunk/hierarchical/source/async":
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: TaskStatusPending,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status/poll/"+taskID:
			pollCount++
			status := TaskStatusStarted
			if pollCount >= 2 {
				status = TaskStatusSuccess
			}
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: status,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/result/"+taskID:
			_ = json.NewEncoder(w).Encode(chunkResult)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL, 5)

	chunks, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHierarchical, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 1 || chunks[0] != "polled chunk" {
		t.Errorf("unexpected chunks: %v", chunks)
	}
	if pollCount < 2 {
		t.Errorf("expected at least 2 poll calls, got %d", pollCount)
	}
}

func TestChunkFile_AsyncTaskFailure(t *testing.T) {
	taskID := "fail-task-000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/chunk/hierarchical/source/async":
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: TaskStatusPending,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status/poll/"+taskID:
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{
				TaskID:     taskID,
				TaskStatus: TaskStatusFailure,
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(server.URL, 5)

	_, err := client.ChunkFile(context.Background(), "https://example.com/doc.pdf", ChunkTypeHierarchical, nil)
	if err == nil {
		t.Fatal("expected error for failed task, got nil")
	}
}

func TestChunkSourceAsyncEndpoint(t *testing.T) {
	client := newTestClient("http://localhost:5001", 1)

	hierarchicalEndpoint, err := client.chunkSourceAsyncEndpoint(ChunkTypeHierarchical)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hierarchicalEndpoint != "http://localhost:5001/v1/chunk/hierarchical/source/async" {
		t.Errorf("unexpected endpoint: %s", hierarchicalEndpoint)
	}

	hybridEndpoint, err := client.chunkSourceAsyncEndpoint(ChunkTypeHybrid)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hybridEndpoint != "http://localhost:5001/v1/chunk/hybrid/source/async" {
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
	taskID := "empty-task"
	chunkResult := ChunkDocumentResponse{
		Chunks: []ChunkedDocumentResultItem{},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost:
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{TaskID: taskID, TaskStatus: TaskStatusSuccess})
		case r.URL.Path == "/v1/status/poll/"+taskID:
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{TaskID: taskID, TaskStatus: TaskStatusSuccess})
		case r.URL.Path == "/v1/result/"+taskID:
			_ = json.NewEncoder(w).Encode(chunkResult)
		}
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
	taskID := "auth-task"
	var receivedAuthHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			receivedAuthHeader = r.Header.Get("Authorization")
		}
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost:
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{TaskID: taskID, TaskStatus: TaskStatusSuccess})
		case r.URL.Path == "/v1/status/poll/"+taskID:
			_ = json.NewEncoder(w).Encode(TaskStatusResponse{TaskID: taskID, TaskStatus: TaskStatusSuccess})
		case r.URL.Path == "/v1/result/"+taskID:
			_ = json.NewEncoder(w).Encode(ChunkDocumentResponse{Chunks: []ChunkedDocumentResultItem{}})
		}
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
