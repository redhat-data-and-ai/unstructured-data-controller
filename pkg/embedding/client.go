package embedding

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	commonhttp "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/http"
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
	ModelName  string
}

type HTTPClient struct {
	Client *http.Client
	Config *HTTPClientConfig
}

func NewHTTPClient(config *HTTPClientConfig) *HTTPClient {
	return &HTTPClient{
		Client: &http.Client{
			Timeout: commonhttp.HTTPClientTimeout,
		},
		Config: config,
	}
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
		Model:          c.Config.ModelName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	logger.Info("sending embedding request")
	req, err := commonhttp.CreateHTTPRequest(ctx, http.MethodPost,
		c.Config.Endpoint, payload, c.Config.AuthFormat, c.Config.APIKey)
	if err != nil {
		return nil, err
	}

	_, body, err := commonhttp.Do(ctx, c.Client, req)
	if err != nil {
		return nil, err
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
