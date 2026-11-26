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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/httpclient"
)

// InputRejected decides whether a failed batch has one input dropped from it
// or is returned whole, so a status misread as input-specific discards every
// input the caller had.
// noRetry makes a retryable status fail on the first attempt, as these tests expect.
var noRetry = httpclient.Policy{}

func TestAPIErrorInputRejected(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantReject bool
	}{
		{name: "bad request blames the input", statusCode: http.StatusBadRequest, wantReject: true},
		{name: "payload too large blames the input", statusCode: http.StatusRequestEntityTooLarge, wantReject: true},
		{name: "unprocessable blames the input", statusCode: http.StatusUnprocessableEntity, wantReject: true},
		{name: "unauthorized blames the credential", statusCode: http.StatusUnauthorized, wantReject: false},
		{name: "forbidden blames the credential", statusCode: http.StatusForbidden, wantReject: false},
		{name: "not found blames the endpoint", statusCode: http.StatusNotFound, wantReject: false},
		{name: "rate limited is transient", statusCode: http.StatusTooManyRequests, wantReject: false},
		{name: "server error is transient", statusCode: http.StatusInternalServerError, wantReject: false},
		{name: "service unavailable is transient", statusCode: http.StatusServiceUnavailable, wantReject: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			apiErr := &APIError{StatusCode: test.statusCode, Body: "{}"}
			assert.Equal(t, test.wantReject, apiErr.InputRejected())
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

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model", RetryPolicy: &noRetry})

	result, err := client.GenerateEmbeddings(context.Background(), []string{"too long"}, "float")

	require.Error(t, err)
	assert.Nil(t, result)

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
	assert.True(t, apiErr.InputRejected())
	assert.Equal(t, "API returned status 400: "+contextLengthBody, err.Error())
}

// A rate limit must not read as a bad input: the caller drops inputs it is
// told are bad, and a 429 says nothing about any of them.
func TestGenerateEmbeddingsRateLimitIsNotInputRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model", RetryPolicy: &noRetry})

	_, err := client.GenerateEmbeddings(context.Background(), []string{"input"}, "float")

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.False(t, apiErr.InputRejected())
}

// The status that motivated the whitelist: an expired credential fails every
// request, so reading it as a bad input would drop every chunk in the file.
func TestGenerateEmbeddingsUnauthorizedIsNotInputRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model", RetryPolicy: &noRetry})

	_, err := client.GenerateEmbeddings(context.Background(), []string{"input"}, "float")

	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusUnauthorized, apiErr.StatusCode)
	assert.False(t, apiErr.InputRejected())
}

func TestGenerateEmbeddingsSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, writeErr := w.Write([]byte(`{"data":[{"embedding":[0.1,0.2]},{"embedding":[0.3,0.4]}]}`))
		require.NoError(t, writeErr)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model", RetryPolicy: &noRetry})

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

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model", RetryPolicy: &noRetry})

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

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model", RetryPolicy: &noRetry})

	_, err := client.GenerateEmbeddings(context.Background(), []string{"input"}, "float")

	require.Error(t, err)
	var apiErr *APIError
	assert.False(t, errors.As(err, &apiErr))
}

// TestGenerateEmbeddingsRetriesUnderAPolicy pins that the client really
// retries, since every other test here turns retries off.
func TestGenerateEmbeddingsRetriesUnderAPolicy(t *testing.T) {
	fastRetry := httpclient.Policy{RetryMax: 2, WaitMin: time.Millisecond, WaitMax: 2 * time.Millisecond}
	tests := []struct {
		name         string
		failures     int32 // requests answered 503 before the first success
		wantErr      bool
		wantRequests int32
	}{
		{name: "a transient 503 is retried to success", failures: 1, wantRequests: 2},
		{name: "a persistent 503 surfaces as an APIError once retries run out",
			failures: 100, wantErr: true, wantRequests: int32(fastRetry.RetryMax + 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if requests.Add(1) <= test.failures {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_, writeErr := w.Write([]byte(`{"data":[{"embedding":[0.1]}]}`))
				require.NoError(t, writeErr)
			}))
			defer server.Close()

			client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model", RetryPolicy: &fastRetry})
			_, err := client.GenerateEmbeddings(context.Background(), []string{"a"}, "float")

			assert.Equal(t, test.wantRequests, requests.Load())
			if !test.wantErr {
				require.NoError(t, err)
				return
			}
			var apiErr *APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, http.StatusServiceUnavailable, apiErr.StatusCode)
		})
	}
}

func TestUnavailable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "rate limited", err: &APIError{StatusCode: http.StatusTooManyRequests}, want: true},
		{name: "server error", err: &APIError{StatusCode: http.StatusInternalServerError}, want: true},
		{name: "service unavailable, wrapped", err: fmt.Errorf("embedding chunks: %w",
			&APIError{StatusCode: http.StatusServiceUnavailable}), want: true},
		{name: "unreachable", err: fmt.Errorf("%w: dial tcp: refused", ErrUnreachable), want: true},
		{name: "input rejected is per request", err: &APIError{StatusCode: http.StatusBadRequest}, want: false},
		{name: "bad credential is not an outage", err: &APIError{StatusCode: http.StatusUnauthorized}, want: false},
		{name: "unrelated error", err: errors.New("decoding chunks file"), want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, Unavailable(test.err))
		})
	}
}

func TestGenerateEmbeddingsMarksATransportFailureUnreachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := server.URL
	server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: closedURL, ModelName: "test-model", RetryPolicy: &noRetry})
	_, err := client.GenerateEmbeddings(context.Background(), []string{"a"}, "float")

	require.ErrorIs(t, err, ErrUnreachable)
	assert.True(t, Unavailable(err))
}

// TestGenerateEmbeddingsTimeoutIsNotAnOutage pins that a request running out its
// timeout is not reported as the API being unavailable, so the caller moves on
// to the next file instead of stopping.
func TestGenerateEmbeddingsTimeoutIsNotAnOutage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{Endpoint: server.URL, ModelName: "test-model"})
	client.Client = httpclient.New(20*time.Millisecond, noRetry)
	_, err := client.GenerateEmbeddings(context.Background(), []string{"a"}, "float")

	require.ErrorContains(t, err, "embedding request timed out")
	assert.NotErrorIs(t, err, ErrUnreachable)
	assert.False(t, Unavailable(err))
}
