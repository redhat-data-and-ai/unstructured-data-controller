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

package embedding

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Retryable decides whether a failed batch is retried forever or its inputs
// are dropped, so a wrong answer either wedges the stage or discards data.
func TestAPIErrorRetryable(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		wantRetryable bool
	}{
		{name: "rate limited is retryable", statusCode: http.StatusTooManyRequests, wantRetryable: true},
		{name: "server error is retryable", statusCode: http.StatusInternalServerError, wantRetryable: true},
		{name: "service unavailable is retryable", statusCode: http.StatusServiceUnavailable, wantRetryable: true},
		{name: "bad request is terminal", statusCode: http.StatusBadRequest, wantRetryable: false},
		{name: "unauthorized is terminal", statusCode: http.StatusUnauthorized, wantRetryable: false},
		{name: "not found is terminal", statusCode: http.StatusNotFound, wantRetryable: false},
		{name: "payload too large is terminal", statusCode: http.StatusRequestEntityTooLarge, wantRetryable: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			apiErr := &APIError{StatusCode: test.statusCode, Body: "{}"}
			assert.Equal(t, test.wantRetryable, apiErr.Retryable())
		})
	}
}

// The context-length rejection this guards against arrives as a 400, and the
// caller tells it apart from a 429 with errors.As rather than by reading the
// message.
func TestGenerateEmbeddingsReturnsAPIError(t *testing.T) {
	const contextLengthBody = `{"error":{"message":"This model's maximum context length is 2048 tokens.","code":400}}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, writeErr := w.Write([]byte(contextLengthBody))
		require.NoError(t, writeErr)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model"})

	result, err := client.GenerateEmbeddings(context.Background(), []string{"too long"}, "float")

	require.Error(t, err)
	assert.Nil(t, result)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
	assert.False(t, apiErr.Retryable())
	assert.Equal(t, "API returned status 400: "+contextLengthBody, err.Error())
}

func TestGenerateEmbeddingsRateLimitIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model"})

	_, err := client.GenerateEmbeddings(context.Background(), []string{"input"}, "float")

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.True(t, apiErr.Retryable())
}

func TestGenerateEmbeddingsSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, writeErr := w.Write([]byte(`{"data":[{"embedding":[0.1,0.2]},{"embedding":[0.3,0.4]}]}`))
		require.NoError(t, writeErr)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model"})

	result, err := client.GenerateEmbeddings(context.Background(), []string{"a", "b"}, "float")

	require.NoError(t, err)
	assert.Equal(t, 2, result.Count)
	assert.Equal(t, [][]float64{{0.1, 0.2}, {0.3, 0.4}}, result.Embeddings)
}

// No request should be sent for an empty batch, and the caller must still get
// a usable result rather than a nil dereference.
func TestGenerateEmbeddingsEmptyInput(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model"})

	result, err := client.GenerateEmbeddings(context.Background(), nil, "float")

	require.NoError(t, err)
	assert.Equal(t, 0, result.Count)
	assert.Zero(t, requests)
}

// A transport failure is not an APIError, and must not be mistaken for a
// terminal rejection that would silently drop the chunk.
func TestGenerateEmbeddingsTransportFailureIsNotAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model"})

	_, err := client.GenerateEmbeddings(context.Background(), []string{"input"}, "float")

	require.Error(t, err)
	var apiErr *APIError
	assert.False(t, errors.As(err, &apiErr))
}
