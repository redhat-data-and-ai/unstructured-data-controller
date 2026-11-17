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
	"errors"
	"strings"
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

// mockReranker records what it was called with so tests can assert on the
// request, not just the response.
type mockReranker struct {
	results []reranker.Result
	err     error

	gotQuery     string
	gotDocuments []string
	gotTopN      int
}

func (m *mockReranker) Rerank(_ context.Context, query string, documents []string, topN int) ([]reranker.Result, error) {
	m.gotQuery = query
	m.gotDocuments = documents
	m.gotTopN = topN
	return m.results, m.err
}

func scored(score float64) *float64 { return &score }

// wantChunk is an expected result position. A nil score means the chunk was
// backfilled from the cosine remainder rather than scored by the reranker.
type wantChunk struct {
	fileID string
	score  *float64
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
		name            string
		rerankerResult  []reranker.Result
		rerankerErr     error
		limit           int
		want            []wantChunk
		wantErrContains string
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
			limit: 3,
			want: []wantChunk{
				{"file3", scored(0.98)}, {"file5", scored(0.90)}, {"file1", scored(0.75)},
			},
		},
		{
			name: "unscored chunks are backfilled in cosine order",
			rerankerResult: []reranker.Result{
				{Index: 0, RelevanceScore: 0.8},
				{Index: 1, RelevanceScore: 0.5},
			},
			limit: 10,
			want: []wantChunk{
				{"file1", scored(0.8)}, {"file2", scored(0.5)},
				{"file3", nil}, {"file4", nil}, {"file5", nil},
			},
		},
		{
			name: "out-of-bounds indices are skipped and backfilled",
			rerankerResult: []reranker.Result{
				{Index: 0, RelevanceScore: 0.9},
				{Index: 99, RelevanceScore: 0.8},
				{Index: -1, RelevanceScore: 0.7},
			},
			limit: 5,
			want: []wantChunk{
				{"file1", scored(0.9)},
				{"file2", nil}, {"file3", nil}, {"file4", nil}, {"file5", nil},
			},
		},
		{
			// A response that omits "index" decodes to Index 0 on every entry;
			// dedup collapses it to one chunk, and backfill must cover the rest.
			name: "duplicate indices collapse but do not shrink the result",
			rerankerResult: []reranker.Result{
				{Index: 0, RelevanceScore: 0.9},
				{Index: 0, RelevanceScore: 0.8},
				{Index: 0, RelevanceScore: 0.7},
			},
			limit: 4,
			want: []wantChunk{
				{"file1", scored(0.9)},
				{"file2", nil}, {"file3", nil}, {"file4", nil},
			},
		},
		{
			name:            "reranker error returns original chunks",
			rerankerErr:     errors.New("connection refused"),
			limit:           3,
			wantErrContains: "connection refused",
		},
		{
			name:            "empty reranker response returns original chunks",
			rerankerResult:  []reranker.Result{},
			limit:           3,
			wantErrContains: "reranker returned no valid results",
		},
		{
			name: "all-invalid indices returns original chunks",
			rerankerResult: []reranker.Result{
				{Index: 99, RelevanceScore: 0.9},
				{Index: -1, RelevanceScore: 0.8},
			},
			limit:           3,
			wantErrContains: "reranker returned no valid results",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testContext()
			mock := &mockReranker{results: tt.rerankerResult, err: tt.rerankerErr}

			result, err := rerankChunks(ctx, mock, "test query", chunks, tt.limit)

			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				assert.Equal(t, chunks, result, "failed reranking must hand back the candidates untouched")
				return
			}

			require.NoError(t, err)
			require.Len(t, result, len(tt.want))

			for i, want := range tt.want {
				assert.Equal(t, want.fileID, result[i].FileID, "position %d has wrong FileID", i)
				assert.NotEmpty(t, result[i].Score, "position %d lost its cosine Score", i)
				if want.score == nil {
					assert.Nil(t, result[i].RerankerScore, "position %d was backfilled and must be un-scored", i)
					continue
				}
				require.NotNil(t, result[i].RerankerScore, "position %d should have a RerankerScore", i)
				assert.InDelta(t, *want.score, *result[i].RerankerScore, 0.001, "position %d has wrong RerankerScore", i)
			}
		})
	}
}

// Chunk text and the query are sent whole: the reranker truncates at its own
// real context limit, which is better than cutting early on a guessed one.
func TestRerankChunksSendsTextUnmodified(t *testing.T) {
	longText := strings.Repeat("d", 20000)
	longQuery := strings.Repeat("q", 5000)
	chunks := []snowflake.ChunkResult{{FileID: "f1", ChunkIndex: "0", ChunkText: longText, Score: "0.9"}}
	mock := &mockReranker{results: []reranker.Result{{Index: 0, RelevanceScore: 0.9}}}

	_, err := rerankChunks(testContext(), mock, longQuery, chunks, 1)
	require.NoError(t, err)

	require.Len(t, mock.gotDocuments, 1)
	assert.Equal(t, longText, mock.gotDocuments[0], "chunk text must reach the reranker intact")
	assert.Equal(t, longQuery, mock.gotQuery, "the query must reach the reranker intact")
	assert.Equal(t, 1, mock.gotTopN)
}

func TestResolveLimit(t *testing.T) {
	tests := []struct {
		name      string
		requested int
		want      int
	}{
		{"unset falls back to the default", 0, defaultChunkLimit},
		{"negative falls back to the default", -5, defaultChunkLimit},
		{"in-range value is kept", 25, 25},
		{"boundary value is kept", maxChunkLimit, maxChunkLimit},
		{"over-limit is clamped", 500, maxChunkLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveLimit(tt.requested))
		})
	}
}

func TestResolveFetchLimit(t *testing.T) {
	tests := []struct {
		name          string
		limit         int
		poolDepth     int
		rerankEnabled bool
		want          int
	}{
		{"no reranking fetches exactly the limit", 10, DefaultRerankerPool, false, 10},
		{"no reranking ignores the pool entirely", 3, MaxRerankerPool, false, 3},
		{"small limit still gets the full pool", 1, DefaultRerankerPool, true, DefaultRerankerPool},
		{"default limit gets the full pool", defaultChunkLimit, DefaultRerankerPool, true, DefaultRerankerPool},
		{"a deeper configured pool is honoured", defaultChunkLimit, MaxRerankerPool, true, MaxRerankerPool},
		{"limit larger than the pool wins, since we cannot return what we did not fetch",
			maxChunkLimit, DefaultRerankerPool, true, maxChunkLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveFetchLimit(tt.limit, tt.poolDepth, tt.rerankEnabled))
		})
	}
}

// The default pool must sit in the region where reranking actually helps.
// Published ablation puts recall@5 at 0.458 for a pool of 20 and 0.826 for 50,
// with reranking doing nothing below about 20, so a shallow default would ship
// a reranker configured not to work.
func TestDefaultRerankerPoolIsDeepEnoughToMatter(t *testing.T) {
	assert.GreaterOrEqual(t, DefaultRerankerPool, 20,
		"a pool below 20 is the region where reranking has no measurable effect")
	assert.LessOrEqual(t, DefaultRerankerPool, MaxRerankerPool,
		"the default must be reachable without being clamped")
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
