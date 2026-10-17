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

package tools

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/reranker"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

func testContext() context.Context {
	log.SetLogger(zap.New(zap.UseDevMode(true)))
	ctx := log.IntoContext(context.Background(), log.Log)
	return ctx
}

type mockReranker struct {
	results []reranker.Result
	err     error
}

func (m *mockReranker) Rerank(_ context.Context, _ string, _ []string, _ int) ([]reranker.Result, error) {
	return m.results, m.err
}

func TestRerankChunks(t *testing.T) {
	chunks := []snowflake.ChunkResult{
		{FileID: "file1", ChunkIndex: "0", ChunkText: "about kubernetes", Score: "0.9"},
		{FileID: "file2", ChunkIndex: "1", ChunkText: "about docker", Score: "0.85"},
		{FileID: "file3", ChunkIndex: "2", ChunkText: "about golang", Score: "0.7"},
		{FileID: "file4", ChunkIndex: "3", ChunkText: "about python", Score: "0.6"},
		{FileID: "file5", ChunkIndex: "4", ChunkText: "about rust", Score: "0.5"},
	}

	tests := []struct {
		name           string
		rerankerResult []reranker.Result
		rerankerErr    error
		limit          int
		wantFileIDs    []string
		wantScores     []float64
		wantErr        bool
	}{
		{
			name: "reranker reorders chunks",
			rerankerResult: []reranker.Result{
				{Index: 2, RelevanceScore: 0.98},
				{Index: 4, RelevanceScore: 0.90},
				{Index: 0, RelevanceScore: 0.75},
				{Index: 1, RelevanceScore: 0.50},
				{Index: 3, RelevanceScore: 0.30},
			},
			limit:       3,
			wantFileIDs: []string{"file3", "file5", "file1"},
			wantScores:  []float64{0.98, 0.90, 0.75},
		},
		{
			name: "limit larger than results returns all",
			rerankerResult: []reranker.Result{
				{Index: 0, RelevanceScore: 0.8},
				{Index: 1, RelevanceScore: 0.5},
			},
			limit:       10,
			wantFileIDs: []string{"file1", "file2"},
			wantScores:  []float64{0.8, 0.5},
		},
		{
			name: "out-of-bounds index is skipped",
			rerankerResult: []reranker.Result{
				{Index: 0, RelevanceScore: 0.9},
				{Index: 99, RelevanceScore: 0.8},
				{Index: -1, RelevanceScore: 0.7},
			},
			limit:       5,
			wantFileIDs: []string{"file1"},
			wantScores:  []float64{0.9},
		},
		{
			name:        "reranker error returns original chunks",
			rerankerErr: fmt.Errorf("connection refused"),
			limit:       3,
			wantFileIDs: []string{"file1", "file2", "file3", "file4", "file5"},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testContext()
			mock := &mockReranker{results: tt.rerankerResult, err: tt.rerankerErr}

			result, err := rerankChunks(ctx, mock, "test query", chunks, tt.limit)

			if tt.wantErr {
				require.Error(t, err)
				assert.Len(t, result, len(chunks))
				return
			}

			require.NoError(t, err)
			require.Len(t, result, len(tt.wantFileIDs))

			for i, wantID := range tt.wantFileIDs {
				assert.Equal(t, wantID, result[i].FileID, "chunk %d has wrong FileID", i)
				require.NotNil(t, result[i].RerankerScore, "chunk %d should have RerankerScore", i)
				assert.InDelta(t, tt.wantScores[i], *result[i].RerankerScore, 0.001, "chunk %d has wrong RerankerScore", i)
				assert.NotEmpty(t, result[i].Score, "original cosine Score should be preserved")
			}
		})
	}
}

func TestRerankChunksPreservesCosineScore(t *testing.T) {
	ctx := testContext()
	chunks := []snowflake.ChunkResult{
		{FileID: "f1", ChunkIndex: "0", ChunkText: "hello", Score: "0.95"},
		{FileID: "f2", ChunkIndex: "1", ChunkText: "world", Score: "0.80"},
	}

	mock := &mockReranker{
		results: []reranker.Result{
			{Index: 1, RelevanceScore: 0.99},
			{Index: 0, RelevanceScore: 0.55},
		},
	}

	result, err := rerankChunks(ctx, mock, "query", chunks, 2)
	require.NoError(t, err)
	require.Len(t, result, 2)

	// Reranker promoted "world" (index 1) above "hello" (index 0)
	assert.Equal(t, "f2", result[0].FileID)
	assert.Equal(t, "0.80", result[0].Score)
	assert.InDelta(t, 0.99, *result[0].RerankerScore, 0.001)

	assert.Equal(t, "f1", result[1].FileID)
	assert.Equal(t, "0.95", result[1].Score)
	assert.InDelta(t, 0.55, *result[1].RerankerScore, 0.001)
}

func TestConvertFileGlobToSQL(t *testing.T) {
	tests := []struct {
		glob     string
		expected string
	}{
		{"", ""},
		{"*/README.md", "%/README.md"},
		{"repo-name/docs/*", "repo-name/docs/%"},
		{"file_name.txt", "file\\_name.txt"},
		{"100%_complete", "100\\%\\_complete"},
	}

	for _, tt := range tests {
		t.Run(tt.glob, func(t *testing.T) {
			assert.Equal(t, tt.expected, convertFileGlobToSQL(tt.glob))
		})
	}
}
