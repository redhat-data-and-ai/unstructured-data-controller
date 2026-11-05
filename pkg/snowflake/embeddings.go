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
	"encoding/json"
	"fmt"
	"strings"
)

// chunkColumns are the columns both searches read, in select order. Score is
// absent because the query computes it rather than reading it.
//
// Each has to match a db tag on ChunkResult or the scanner drops it without
// complaining; TestChunkColumnsMatchTheStruct checks both directions.
var chunkColumns = []string{
	"FILE_ID", "CHUNK_INDEX", "CHUNK_TEXT", "CHUNK_ID", "RAW_TEXT", "BLOCK_IDS", "REGIONS",
}

type ChunkResult struct {
	FileID string `json:"file_id" db:"file_id"`
	// ChunkIndex is the chunk's position in the flattened view, which is not
	// ChunkID. Only ChunkID survives the file being embedded again.
	ChunkIndex string `json:"chunk_index" db:"chunk_index"`
	ChunkText  string `json:"chunk_text" db:"chunk_text"`
	Score      string `json:"score" db:"score"`

	// ChunkID identifies the chunk, so a citation can be fetched back.
	ChunkID string `json:"chunk_id,omitempty" db:"chunk_id"`
	// RawText is the body without the heading trail, for display.
	RawText string `json:"raw_text,omitempty" db:"raw_text"`

	// BlockIDsRaw and RegionsRaw receive the two VARIANT columns, which the
	// scanner renders as JSON text. promoteVariants moves them into the
	// fields below so a caller is not handed a string containing JSON.
	BlockIDsRaw string `json:"-" db:"block_ids"`
	RegionsRaw  string `json:"-" db:"regions"`

	// BlockIDs names the document elements the chunk came from, and Regions
	// is where they sit on the page, normalised to 0 to 1 from the top left.
	BlockIDs json.RawMessage `json:"block_ids,omitempty"`
	Regions  json.RawMessage `json:"regions,omitempty"`
	// RerankerScore is populated by the MCP tool layer after retrieval, never by
	// a query. It deliberately carries no db tag: scanRows renders every mapped
	// column as a string, so tagging a non-string field would make it skip this
	// one silently.
	RerankerScore *float64 `json:"reranker_score,omitempty"`
}

func SearchChunks(
	ctx context.Context, oauthToken, database, schema, table, vectorLiteral string, limit int,
) ([]ChunkResult, error) {
	query := fmt.Sprintf(
		`SELECT `+strings.Join(chunkColumns, ", ")+`, VECTOR_COSINE_SIMILARITY(EMBEDDING, %s::VECTOR(FLOAT,768)) AS score `+
			`FROM %s.%s.%s ORDER BY score DESC LIMIT %d`,
		vectorLiteral, database, schema, table, limit,
	)

	chunks, err := queryRows[ChunkResult](ctx, oauthToken, query)
	if err != nil {
		return nil, err
	}
	return promoteVariants(chunks), nil
}

// SearchChunksWithFileFilter performs vector similarity search scoped to files matching the given pattern.
func SearchChunksWithFileFilter(
	ctx context.Context, oauthToken, database, schema, table, vectorLiteral, filePattern string, limit int,
) ([]ChunkResult, error) {
	query := fmt.Sprintf(
		`SELECT `+strings.Join(chunkColumns, ", ")+`, VECTOR_COSINE_SIMILARITY(EMBEDDING, %s::VECTOR(FLOAT,768)) AS score `+
			`FROM %s.%s.%s WHERE FILE_ID LIKE ? ESCAPE '\\' ORDER BY score DESC LIMIT %d`,
		vectorLiteral, database, schema, table, limit,
	)

	chunks, err := queryRows[ChunkResult](ctx, oauthToken, query, filePattern)
	if err != nil {
		return nil, err
	}
	return promoteVariants(chunks), nil
}

// promoteVariants moves the two VARIANT columns into real JSON.
//
// The scanner renders every column as a string, so a caller would otherwise
// receive JSON wrapped in a JSON string and have to decode it twice. A value
// that does not parse is dropped rather than passed on, since a malformed
// region cannot be drawn.
func promoteVariants(chunks []ChunkResult) []ChunkResult {
	for i := range chunks {
		chunks[i].BlockIDs = asJSON(chunks[i].BlockIDsRaw)
		chunks[i].Regions = asJSON(chunks[i].RegionsRaw)
	}
	return chunks
}

func asJSON(raw string) json.RawMessage {
	if raw == "" || !json.Valid([]byte(raw)) {
		return nil
	}
	return json.RawMessage(raw)
}
