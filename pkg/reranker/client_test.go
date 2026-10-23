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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func testContext() context.Context {
	log.SetLogger(zap.New(zap.UseDevMode(true)))
	return log.IntoContext(context.Background(), log.Log)
}

func TestRerank(t *testing.T) {
	tests := []struct {
		name           string
		documents      []string
		topN           int
		serverResponse RerankResponse
		serverStatus   int
		wantResults    int
		wantErr        string
	}{
		{
			name:      "successful reranking",
			documents: []string{"doc about cats", "doc about dogs", "doc about birds"},
			topN:      2,
			serverResponse: RerankResponse{
				Results: []Result{
					{Index: 2, RelevanceScore: 0.95},
					{Index: 0, RelevanceScore: 0.82},
				},
			},
			serverStatus: http.StatusOK,
			wantResults:  2,
		},
		{
			name:      "empty documents returns nil",
			documents: []string{},
			topN:      5,
		},
		{
			name:           "API error returns error",
			documents:      []string{"doc1"},
			topN:           1,
			serverResponse: RerankResponse{},
			serverStatus:   http.StatusInternalServerError,
			wantErr:        "rerank API returned status 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testContext()

			var server *httptest.Server
			if len(tt.documents) > 0 {
				server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, http.MethodPost, r.Method)
					assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
					assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))

					var reqBody RerankRequest
					err := json.NewDecoder(r.Body).Decode(&reqBody)
					require.NoError(t, err)
					assert.Equal(t, "test-query", reqBody.Query)
					assert.Equal(t, tt.documents, reqBody.Documents)
					assert.Equal(t, "test-model", reqBody.Model)

					w.WriteHeader(tt.serverStatus)
					json.NewEncoder(w).Encode(tt.serverResponse)
				}))
				defer server.Close()
			}

			endpoint := ""
			if server != nil {
				endpoint = server.URL
			}

			client := NewHTTPClient(&HTTPClientConfig{
				Endpoint:   endpoint,
				APIKey:     "test-key",
				AuthFormat: "Bearer",
				ModelName:  "test-model",
			})

			results, err := client.Rerank(ctx, "test-query", tt.documents, tt.topN)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)

			if tt.wantResults == 0 {
				assert.Nil(t, results)
			} else {
				assert.Len(t, results, tt.wantResults)
			}
		})
	}
}

func TestRerankPreservesIndexAndScores(t *testing.T) {
	ctx := testContext()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := RerankResponse{
			Results: []Result{
				{Index: 1, RelevanceScore: 0.99},
				{Index: 0, RelevanceScore: 0.45},
				{Index: 2, RelevanceScore: 0.12},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{
		Endpoint:  server.URL,
		ModelName: "test-model",
	})

	results, err := client.Rerank(ctx, "query", []string{"a", "b", "c"}, 3)
	require.NoError(t, err)
	require.Len(t, results, 3)

	assert.Equal(t, 1, results[0].Index)
	assert.InDelta(t, 0.99, results[0].RelevanceScore, 0.001)

	assert.Equal(t, 0, results[1].Index)
	assert.InDelta(t, 0.45, results[1].RelevanceScore, 0.001)

	assert.Equal(t, 2, results[2].Index)
	assert.InDelta(t, 0.12, results[2].RelevanceScore, 0.001)
}

func TestRerankNoAuth(t *testing.T) {
	ctx := testContext()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		resp := RerankResponse{Results: []Result{{Index: 0, RelevanceScore: 0.5}}}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(&HTTPClientConfig{
		Endpoint:  server.URL,
		ModelName: "test-model",
	})

	results, err := client.Rerank(ctx, "query", []string{"doc"}, 1)
	require.NoError(t, err)
	assert.Len(t, results, 1)
}
