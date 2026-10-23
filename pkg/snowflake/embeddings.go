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

package snowflake

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

type ChunkResult struct {
	FileID        string   `json:"file_id" db:"file_id"`
	ChunkIndex    string   `json:"chunk_index" db:"chunk_index"`
	ChunkText     string   `json:"chunk_text" db:"chunk_text"`
	Score         string   `json:"score" db:"score"`
	RerankerScore *float64 `json:"reranker_score,omitempty"`
}

func SearchChunks(
	ctx context.Context, oauthToken, database, schema, table, vectorLiteral string, limit int,
) ([]ChunkResult, error) {
	query := fmt.Sprintf(
		`SELECT FILE_ID, CHUNK_INDEX, CHUNK_TEXT, VECTOR_COSINE_SIMILARITY(EMBEDDING, %s::VECTOR(FLOAT,768)) AS score `+
			`FROM %s.%s.%s ORDER BY score DESC LIMIT %d`,
		vectorLiteral, database, schema, table, limit,
	)

	return queryRows[ChunkResult](ctx, oauthToken, query)
}

// SearchChunksByKeyword performs text-based search using regex term matching.
// Results are ranked by the number of query term occurrences in the chunk text.
func SearchChunksByKeyword(
	ctx context.Context, oauthToken, database, schema, table, searchQuery string, limit int,
) ([]ChunkResult, error) {
	pattern, err := buildSearchPattern(searchQuery)
	if err != nil {
		return nil, err
	}
	if pattern == "" {
		return nil, nil
	}

	query := fmt.Sprintf(
		`SELECT FILE_ID, CHUNK_INDEX, CHUNK_TEXT, `+
			`CAST(REGEXP_COUNT(CHUNK_TEXT, ?, 1, 'i') AS VARCHAR) AS score `+
			`FROM %s.%s.%s `+
			`WHERE REGEXP_COUNT(CHUNK_TEXT, ?, 1, 'i') > 0 `+
			`ORDER BY REGEXP_COUNT(CHUNK_TEXT, ?, 1, 'i') DESC `+
			`LIMIT %d`,
		database, schema, table, limit,
	)

	return queryRows[ChunkResult](ctx, oauthToken, query, pattern, pattern, pattern)
}

// SearchChunksByKeywordWithFileFilter performs text-based search scoped to files matching the given pattern.
func SearchChunksByKeywordWithFileFilter(
	ctx context.Context, oauthToken, database, schema, table, searchQuery, filePattern string, limit int,
) ([]ChunkResult, error) {
	pattern, err := buildSearchPattern(searchQuery)
	if err != nil {
		return nil, err
	}
	if pattern == "" {
		return nil, nil
	}

	query := fmt.Sprintf(
		`SELECT FILE_ID, CHUNK_INDEX, CHUNK_TEXT, `+
			`CAST(REGEXP_COUNT(CHUNK_TEXT, ?, 1, 'i') AS VARCHAR) AS score `+
			`FROM %s.%s.%s `+
			`WHERE FILE_ID LIKE ? ESCAPE '\\' AND REGEXP_COUNT(CHUNK_TEXT, ?, 1, 'i') > 0 `+
			`ORDER BY REGEXP_COUNT(CHUNK_TEXT, ?, 1, 'i') DESC `+
			`LIMIT %d`,
		database, schema, table, limit,
	)

	return queryRows[ChunkResult](ctx, oauthToken, query, pattern, filePattern, pattern, pattern)
}

// buildSearchPattern splits a query into terms, escapes regex special characters,
// and joins them into an alternation pattern for REGEXP_COUNT.
// Returns empty string if no usable terms are found.
func buildSearchPattern(query string) (string, error) {
	words := strings.Fields(query)
	terms := make([]string, 0, len(words))
	for _, w := range words {
		if len(w) < 2 {
			continue
		}
		terms = append(terms, regexp.QuoteMeta(w))
	}
	if len(terms) == 0 {
		return "", nil
	}
	return strings.Join(terms, "|"), nil
}

// SearchChunksWithFileFilter performs vector similarity search scoped to files matching the given pattern.
func SearchChunksWithFileFilter(
	ctx context.Context, oauthToken, database, schema, table, vectorLiteral, filePattern string, limit int,
) ([]ChunkResult, error) {
	query := fmt.Sprintf(
		`SELECT FILE_ID, CHUNK_INDEX, CHUNK_TEXT, VECTOR_COSINE_SIMILARITY(EMBEDDING, %s::VECTOR(FLOAT,768)) AS score `+
			`FROM %s.%s.%s WHERE FILE_ID LIKE ? ESCAPE '\\' ORDER BY score DESC LIMIT %d`,
		vectorLiteral, database, schema, table, limit,
	)

	return queryRows[ChunkResult](ctx, oauthToken, query, filePattern)
}
