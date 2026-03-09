package embedding

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

type EmbeddingGenerator interface {
	GenerateEmbeddings(ctx context.Context, inputs []string, encodingFormat string) (*EmbeddingResult, error)
}

// EmbeddingResult holds the embedding vectors and count returned by GenerateEmbeddings.
type EmbeddingResult struct {
	Embeddings [][]float64
	Count      int
}

// EmbeddingRequest is the JSON body for the embedding API (OpenAI-compatible).
type EmbeddingRequest struct {
	Model          string   `json:"model"`
	Input          []string `json:"input"`
	EncodingFormat string   `json:"encoding_format,omitempty"`
}

// EmbeddingData is a single embedding entry in the API response.
type EmbeddingData struct {
	Embedding []float64 `json:"embedding"`
}

// EmbeddingResponse is the JSON response from the embedding API.
type EmbeddingResponse struct {
	Data []EmbeddingData `json:"data"`
}

// HTTPClientConfig configures the HTTP embedding client.
type HTTPClientConfig struct {
	Endpoint   string
	AuthFormat string
	APIKey     string
}

type HTTPClient struct {
	Client *http.Client
	Config *HTTPClientConfig
}

func NewHTTPClient(config *HTTPClientConfig) *HTTPClient {
	return &HTTPClient{
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
		Config: config,
	}
}

// createHTTPRequest creates an HTTP request with auth header set from format and api key.
func (c *HTTPClient) createHTTPRequest(
	ctx context.Context, method, endpoint string, payload []byte,
) (*http.Request, error) {
	var body io.Reader
	if len(payload) > 0 {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.Config.APIKey != "" && c.Config.AuthFormat != "" {
		req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.Config.AuthFormat, c.Config.APIKey))
	}
	return req, nil
}

func (c *HTTPClient) GenerateEmbeddings(
	ctx context.Context, inputs []string, encodingFormat string,
) (*EmbeddingResult, error) {
	logger := log.FromContext(ctx)
	if len(inputs) == 0 {
		logger.Info("no inputs provided")
		return &EmbeddingResult{Embeddings: nil, Count: 0}, nil
	}

	payload, err := json.Marshal(EmbeddingRequest{
		Input:          inputs,
		EncodingFormat: encodingFormat,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	// TODO: Add a better log statement
	logger.Info("sending embedding request")
	req, err := c.createHTTPRequest(ctx, http.MethodPost, c.Config.Endpoint, payload)
	if err != nil {
		return nil, err
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send embedding request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Error(err, "failed to close response body")
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read embedding response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	var embResp EmbeddingResponse
	if err := json.Unmarshal(body, &embResp); err != nil {
		return nil, fmt.Errorf("failed to decode embedding response: %w", err)
	}

	embeddings := make([][]float64, len(embResp.Data))
	for i := range embResp.Data {
		embeddings[i] = embResp.Data[i].Embedding
	}

	return &EmbeddingResult{
		Embeddings: embeddings,
		Count:      len(embeddings),
	}, nil
}
