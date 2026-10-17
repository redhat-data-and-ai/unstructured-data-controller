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
	"sort"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

// DefaultK is the standard RRF smoothing constant from the original paper
// (Cormack et al., 2009). Higher values reduce the influence of rank position.
const DefaultK = 60

type fusedChunk struct {
	chunk    snowflake.ChunkResult
	rrfScore float64
}

// Fuse combines ranked results from vector search and keyword search using
// Reciprocal Rank Fusion. Each chunk's RRF score is the sum of 1/(k + rank)
// across all result lists it appears in. Chunks appearing in both lists
// get higher scores than those in only one.
//
// The cosine similarity Score from vector search is preserved on chunks that
// appeared in vector results. Keyword-only chunks have an empty Score field.
func Fuse(vectorResults, keywordResults []snowflake.ChunkResult, k int) []snowflake.ChunkResult {
	if k <= 0 {
		k = DefaultK
	}

	seen := make(map[string]*fusedChunk)

	for rank, chunk := range vectorResults {
		key := chunkKey(chunk)
		if _, ok := seen[key]; !ok {
			seen[key] = &fusedChunk{chunk: chunk}
		}
		seen[key].rrfScore += 1.0 / float64(k+rank+1)
	}

	for rank, chunk := range keywordResults {
		key := chunkKey(chunk)
		if _, ok := seen[key]; !ok {
			chunk.Score = ""
			seen[key] = &fusedChunk{chunk: chunk}
		}
		seen[key].rrfScore += 1.0 / float64(k+rank+1)
	}

	results := make([]fusedChunk, 0, len(seen))
	for _, fc := range seen {
		results = append(results, *fc)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].rrfScore > results[j].rrfScore
	})

	chunks := make([]snowflake.ChunkResult, len(results))
	for i, r := range results {
		chunks[i] = r.chunk
	}

	return chunks
}

func chunkKey(c snowflake.ChunkResult) string {
	return c.FileID + ":" + c.ChunkIndex
}
