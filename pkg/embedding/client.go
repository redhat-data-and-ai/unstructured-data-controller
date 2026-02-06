package embedding

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type EmbeddingProvider string

const (
	SelfHostedModel EmbeddingProvider = "self-hosted-model"
	GeminiModel     EmbeddingProvider = "gemini-model"
)

type EmbeddingGenerator interface {
	GenerateEmbeddings(ctx context.Context, inputs []string, apiKeySecret *corev1.Secret) (*EmbeddingResult, error)
}

type EmbeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type EmbeddingResponse struct {
	Object string          `json:"object"`
	Data   []EmbeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  EmbeddingUsage  `json:"usage"`
}

type EmbeddingData struct {
	Object    string    `json:"object"`
	Embedding []float64 `json:"embedding"`
	Index     int       `json:"index"`
}

type EmbeddingUsage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type EmbeddingResult struct {
	Embeddings [][]float64 `json:"embeddings"`
	Count      int         `json:"count"`
}

type EmbeddingGeneratorConfig struct {
	Provider              EmbeddingProvider     `json:"provider,omitempty"`
	SelfHostedModelConfig SelfHostedModelConfig `json:"selfHostedModelConfig,omitempty"`
	GeminiModelConfig     GeminiModelConfig     `json:"geminiModelConfig,omitempty"`
}

// create selfthosted model config
type SelfHostedModelConfig struct {
	Model           string `json:"model"`
	Dimensions      int    `json:"dimensions"`
	EncodingFormat  string `json:"encodingformat"`
	APIKeySecretRef string `json:"apikeysecretref"`
	Endpoint        string `json:"endpoint"`
	CertificatePath string `json:"certificatepath"`
}

// create gemini model config
type GeminiModelConfig struct {
	// will implement later
}

// create selfhostedmodel client
type SelfHostedModelClient struct {
	Config SelfHostedModelConfig
	Client *http.Client
}

// create gemini model client
type GeminiModelClient struct {
	Config GeminiModelConfig
	Client *http.Client
}

// create selfhostedmodel client from selfhosted config
func NewSelfHostedModelClient(config *SelfHostedModelConfig) (*SelfHostedModelClient, error) {
	httpClient, err := createHTTPClient(config.CertificatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}
	return &SelfHostedModelClient{
		Config: *config,
		Client: httpClient,
	}, nil
}

func NewGeminiModelClient(config *GeminiModelConfig) (*GeminiModelClient, error) {
	return nil, nil
}

func createHTTPClient(certPath string) (*http.Client, error) {
	// Load certificate
	caCert, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("failed to append certificate to pool")
	}

	tlsConfig := &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS12,
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: 60 * time.Second,
	}, nil
}

func (c *SelfHostedModelClient) GenerateEmbeddings(ctx context.Context,
	texts []string, apiKeySecret *corev1.Secret) (*EmbeddingResult, error) {
	logger := log.FromContext(ctx)
	logger.Info("generating embeddings in batch", "textCount", len(texts))

	if len(texts) == 0 {
		return &EmbeddingResult{
			Embeddings: [][]float64{},
			Count:      0,
		}, nil
	}

	// create request body
	reqBody := EmbeddingRequest{
		Model: c.Config.Model,
		Input: texts,
	}

	// Extract API key from the secret using the key name specified in APIKeySecretRef
	apiKeyBytes, exists := apiKeySecret.Data[c.Config.APIKeySecretRef]
	if !exists {
		return nil, fmt.Errorf("API key with name '%s' not found in secret", c.Config.APIKeySecretRef)
	}
	apiKey := string(apiKeyBytes)
	if apiKey == "" {
		return nil, fmt.Errorf("API key '%s' is empty in secret", c.Config.APIKeySecretRef)
	}
	logger.Info("successfully extracted API key from secret", "keyName", c.Config.APIKeySecretRef)

	// marshal request body to bytes
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	logger.Info("creating HTTP request for batch embedding generation")
	req, err := http.NewRequestWithContext(ctx, "POST", c.Config.Endpoint, bytes.NewReader(reqBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")

	logger.Info("sending batch embedding request to API")
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	logger.Info("reading response from embedding API")
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(respBody))
	}

	logger.Info("unmarshalling batch embedding response")
	var embeddingResp EmbeddingResponse
	if err := json.Unmarshal(respBody, &embeddingResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if len(embeddingResp.Data) == 0 {
		return nil, errors.New("no embeddings returned in response")
	}

	embeddings := make([][]float64, len(texts))
	for _, data := range embeddingResp.Data {
		if data.Index >= 0 && data.Index < len(embeddings) {
			embeddings[data.Index] = data.Embedding
		}
	}

	logger.Info("successfully generated batch embeddings", "count", len(embeddings))

	return &EmbeddingResult{
		Embeddings: embeddings,
		Count:      len(embeddings),
	}, nil
}

func (c *GeminiModelClient) GenerateEmbeddings(ctx context.Context,
	texts []string, apiKeySecret *corev1.Secret) (*EmbeddingResult, error) {
	logger := log.FromContext(ctx)
	logger.Info("generating embeddings in batch", "textCount", len(texts))
	return nil, nil
}
