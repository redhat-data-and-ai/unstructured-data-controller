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
	httpClientTimeout = 60 * time.Second
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
// Compatible with Cohere, Jina, TEI, and vLLM rerank endpoints.
type RerankRequest struct {
	Model     string   `json:"model"`
	Query     string   `json:"query"`
	Documents []string `json:"documents"`
	TopN      int      `json:"top_n,omitempty"`
}

type RerankResponse struct {
	Results []Result `json:"results"`
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading rerank response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rerank API returned status %d: %s", resp.StatusCode, string(body))
	}

	var rerankResp RerankResponse
	if err := json.Unmarshal(body, &rerankResp); err != nil {
		return nil, fmt.Errorf("decoding rerank response: %w", err)
	}

	logger.Info("rerank completed", "results_returned", len(rerankResp.Results))
	return rerankResp.Results, nil
}
