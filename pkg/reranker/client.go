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

package reranker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// httpClientTimeout bounds the synchronous reranker request. This matches
	// the MCP tool latency budget - if the reranker does not respond within
	// this window, the caller falls back to the original search results.
	httpClientTimeout = 60 * time.Second

	// maxResponseBytes caps the reranker response body to prevent unbounded
	// memory allocation from a misbehaving or compromised endpoint.
	maxResponseBytes = 1 << 20 // 1 MiB

	// maxErrorBodyBytes limits how much of a non-200 response body is included
	// in error messages to avoid flooding logs.
	maxErrorBodyBytes = 512
)

// Reranker scores query-document pairs using a cross-encoder model
// and returns results ordered by relevance.
type Reranker interface {
	Rerank(ctx context.Context, query string, documents []string, topN int) ([]Result, error)
}

// Result holds the reranking score for a document at a given index
// in the original input slice.
type Result struct {
	Index          int     `json:"index"`
	RelevanceScore float64 `json:"relevance_score"`
}

// RerankRequest is the JSON body sent to the reranker API.
// Compatible with Cohere, Jina, and vLLM rerank endpoints, which all return an
// object with a "results" array. TEI is not supported: it returns a bare array.
type RerankRequest struct {
	Model     string   `json:"model"`
	Query     string   `json:"query"`
	Documents []string `json:"documents"`
	TopN      int      `json:"top_n,omitempty"`
	// ReturnDocuments is always sent as false: echoing every document back can
	// make the response larger than the request, which would trip maxResponseBytes.
	ReturnDocuments bool `json:"return_documents"`
}

type RerankResponse struct {
	Results []Result `json:"results"`
}

// resultWire mirrors Result with pointer fields. encoding/json zero-fills
// absent keys without error, so a response using a different schema (for
// example "score" instead of "relevance_score", or omitting "index") would
// decode cleanly into index 0 and score 0 for every entry. Decoding into
// pointers first makes "absent" distinguishable from "legitimately zero".
type resultWire struct {
	Index          *int     `json:"index"`
	RelevanceScore *float64 `json:"relevance_score"`
}

type responseWire struct {
	Results []resultWire `json:"results"`
}

type HTTPClientConfig struct {
	Endpoint   string
	AuthFormat string
	APIKey     string
	ModelName  string
}

type HTTPClient struct {
	Client *http.Client
	Config *HTTPClientConfig
}

func NewHTTPClient(config *HTTPClientConfig) *HTTPClient {
	return &HTTPClient{
		Client: &http.Client{
			Timeout: httpClientTimeout,
		},
		Config: config,
	}
}

func (c *HTTPClient) Rerank(ctx context.Context, query string, documents []string, topN int) ([]Result, error) {
	logger := log.FromContext(ctx)
	if len(documents) == 0 {
		return nil, nil
	}

	reqBody := RerankRequest{
		Model:     c.Config.ModelName,
		Query:     query,
		Documents: documents,
		TopN:      topN,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling rerank request: %w", err)
	}

	logger.Info("sending rerank request", "document_count", len(documents), "top_n", topN)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Config.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("creating rerank request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.Config.APIKey != "" && c.Config.AuthFormat != "" {
		req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.Config.AuthFormat, c.Config.APIKey))
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending rerank request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close rerank response body")
		}
	}()

	// Read one byte past the cap so an oversized body is reported as such rather
	// than silently truncated into an "unexpected end of JSON input" decode error.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading rerank response: %w", err)
	}
	if len(body) > maxResponseBytes {
		return nil, fmt.Errorf("rerank response exceeded %d bytes", maxResponseBytes)
	}

	if resp.StatusCode != http.StatusOK {
		if len(body) > maxErrorBodyBytes {
			body = body[:maxErrorBodyBytes]
		}
		return nil, fmt.Errorf("rerank API returned status %d: %s", resp.StatusCode, string(body))
	}

	var wire responseWire
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("decoding rerank response: %w", err)
	}

	results := make([]Result, 0, len(wire.Results))
	for i, result := range wire.Results {
		if result.Index == nil || result.RelevanceScore == nil {
			return nil, fmt.Errorf(
				"rerank response entry %d is missing index or relevance_score: endpoint schema is not supported", i)
		}
		results = append(results, Result{Index: *result.Index, RelevanceScore: *result.RelevanceScore})
	}

	logger.Info("rerank completed", "results_returned", len(results))
	return results, nil
}
