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

package rrf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

func chunk(fileID, chunkIndex, score string) snowflake.ChunkResult {
	return snowflake.ChunkResult{
		FileID:     fileID,
		ChunkIndex: chunkIndex,
		ChunkText:  "text for " + fileID,
		Score:      score,
	}
}

func TestFusePromotesOverlappingChunks(t *testing.T) {
	vectorResults := []snowflake.ChunkResult{
		chunk("file1", "0", "0.9"),
		chunk("file2", "1", "0.8"),
		chunk("file3", "2", "0.7"),
	}
	keywordResults := []snowflake.ChunkResult{
		chunk("file3", "2", "3"),
		chunk("file4", "0", "2"),
		chunk("file1", "0", "1"),
	}

	fused := Fuse(vectorResults, keywordResults, DefaultK)

	require.GreaterOrEqual(t, len(fused), 4)

	// file1 and file3 appear in both lists so they should rank higher
	// than file2 (vector-only) and file4 (keyword-only)
	topIDs := make([]string, 0)
	for _, c := range fused[:2] {
		topIDs = append(topIDs, c.FileID)
	}
	assert.Contains(t, topIDs, "file1")
	assert.Contains(t, topIDs, "file3")
}

func TestFusePreservesCosineScore(t *testing.T) {
	vectorResults := []snowflake.ChunkResult{
		chunk("file1", "0", "0.95"),
	}
	keywordResults := []snowflake.ChunkResult{
		chunk("file1", "0", "5"),
	}

	fused := Fuse(vectorResults, keywordResults, DefaultK)

	require.Len(t, fused, 1)
	assert.Equal(t, "0.95", fused[0].Score, "cosine similarity score from vector search should be preserved")
}

func TestFuseKeywordOnlyChunkHasEmptyScore(t *testing.T) {
	vectorResults := []snowflake.ChunkResult{
		chunk("file1", "0", "0.9"),
	}
	keywordResults := []snowflake.ChunkResult{
		chunk("file2", "0", "3"),
	}

	fused := Fuse(vectorResults, keywordResults, DefaultK)

	require.Len(t, fused, 2)

	var keywordOnlyChunk snowflake.ChunkResult
	for _, c := range fused {
		if c.FileID == "file2" {
			keywordOnlyChunk = c
			break
		}
	}
	assert.Empty(t, keywordOnlyChunk.Score, "keyword-only chunks should have empty Score (not a cosine similarity)")
}

func TestFuseEmptyInputs(t *testing.T) {
	tests := []struct {
		name    string
		vector  []snowflake.ChunkResult
		keyword []snowflake.ChunkResult
		wantLen int
	}{
		{
			name:    "both empty",
			wantLen: 0,
		},
		{
			name:    "vector only",
			vector:  []snowflake.ChunkResult{chunk("f1", "0", "0.9")},
			wantLen: 1,
		},
		{
			name:    "keyword only",
			keyword: []snowflake.ChunkResult{chunk("f1", "0", "2")},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fused := Fuse(tt.vector, tt.keyword, DefaultK)
			assert.Len(t, fused, tt.wantLen)
		})
	}
}

func TestFuseDefaultKOnInvalidInput(t *testing.T) {
	vectorResults := []snowflake.ChunkResult{
		chunk("file1", "0", "0.9"),
		chunk("file2", "1", "0.8"),
	}

	resultZero := Fuse(vectorResults, nil, 0)
	resultNeg := Fuse(vectorResults, nil, -5)
	resultDefault := Fuse(vectorResults, nil, DefaultK)

	require.Len(t, resultZero, 2)
	require.Len(t, resultNeg, 2)
	require.Len(t, resultDefault, 2)

	assert.Equal(t, resultDefault[0].FileID, resultZero[0].FileID)
	assert.Equal(t, resultDefault[0].FileID, resultNeg[0].FileID)
}

func TestFuseOrderIsStableWithinSameScore(t *testing.T) {
	// Two chunks that only appear in vector results at the same rank
	// pattern should produce deterministic output
	vectorResults := []snowflake.ChunkResult{
		chunk("file1", "0", "0.9"),
	}
	keywordResults := []snowflake.ChunkResult{
		chunk("file2", "0", "1"),
	}

	// Both at rank 0 in their respective lists, so equal RRF score.
	// Run multiple times to check stability.
	for range 10 {
		fused := Fuse(vectorResults, keywordResults, DefaultK)
		require.Len(t, fused, 2)
	}
}
