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
)

// chunkColumns are the block-level fields shared by search and single-chunk
// lookup. Every one is null for documents produced by a parser that does not
// emit blocks, so consumers must tolerate empty values.
const chunkColumns = `FILE_ID, FILE_PATH, FILE_NAME, CHUNK_INDEX, CHUNK_TEXT, ` +
	`CHUNK_ID, PAGE_NUMBER, BLOCK_INDEX, PART_INDEX, BLOCK_CLASS, ` +
	`SECTION_HEADER, BBOX, CROP_URI`

// ChunkResult is one row of the embeddings view.
//
// Every field is a string because scanRows writes results with
// SetString; numeric columns are converted by the caller.
type ChunkResult struct {
	FileID     string `json:"file_id" db:"file_id"`
	FilePath   string `json:"file_path" db:"file_path"`
	FileName   string `json:"file_name" db:"file_name"`
	ChunkIndex string `json:"chunk_index" db:"chunk_index"`
	ChunkText  string `json:"chunk_text" db:"chunk_text"`
	Score      string `json:"score,omitempty" db:"score"`

	// Block metadata. Present only for block-chunked documents.
	ChunkID       string `json:"chunk_id,omitempty" db:"chunk_id"`
	PageNumber    string `json:"page_number,omitempty" db:"page_number"`
	BlockIndex    string `json:"block_index,omitempty" db:"block_index"`
	PartIndex     string `json:"part_index,omitempty" db:"part_index"`
	BlockClass    string `json:"block_class,omitempty" db:"block_class"`
	SectionHeader string `json:"section_header,omitempty" db:"section_header"`
	BBox          string `json:"bbox,omitempty" db:"bbox"`
	// CropURI locates the block's cropped image in object storage. It is a
	// reference, never the image itself: returning bytes with every search
	// hit would put megabytes of base64 into a single response.
	CropURI string `json:"crop_uri,omitempty" db:"crop_uri"`
}

func SearchChunks(
	ctx context.Context, oauthToken, database, schema, table, vectorLiteral string, limit int,
) ([]ChunkResult, error) {
	query := fmt.Sprintf(
		`SELECT `+chunkColumns+`, `+
			`VECTOR_COSINE_SIMILARITY(EMBEDDING, %s::VECTOR(FLOAT,768)) AS score `+
			`FROM %s.%s.%s ORDER BY score DESC LIMIT %d`,
		vectorLiteral, database, schema, table, limit,
	)

	return queryRows[ChunkResult](ctx, oauthToken, query)
}

// SearchChunksWithFileFilter performs vector similarity search scoped to files matching the given path pattern.
func SearchChunksWithFileFilter(
	ctx context.Context, oauthToken, database, schema, table, vectorLiteral, filePattern string, limit int,
) ([]ChunkResult, error) {
	query := fmt.Sprintf(
		`SELECT `+chunkColumns+`, `+
			`VECTOR_COSINE_SIMILARITY(EMBEDDING, %s::VECTOR(FLOAT,768)) AS score `+
			`FROM %s.%s.%s WHERE FILE_PATH LIKE ? ESCAPE '\\' ORDER BY score DESC LIMIT %d`,
		vectorLiteral, database, schema, table, limit,
	)

	return queryRows[ChunkResult](ctx, oauthToken, query, filePattern)
}

// GetChunkByID fetches one chunk by its stable identifier.
//
// chunkID is caller-supplied and is always bound, never interpolated. The
// query runs under the caller's own OAuth token, so a row access policy on
// the view filters it: a chunk the caller may not see simply does not come
// back, and the absence is indistinguishable from a bad identifier.
func GetChunkByID(
	ctx context.Context, oauthToken, database, schema, table, chunkID string,
) (*ChunkResult, error) {
	query := fmt.Sprintf(
		`SELECT `+chunkColumns+` FROM %s.%s.%s WHERE CHUNK_ID = ? LIMIT 1`,
		database, schema, table,
	)

	results, err := queryRows[ChunkResult](ctx, oauthToken, query, chunkID)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return &results[0], nil
}

// CropReference is the minimum needed to locate a block's crop image.
type CropReference struct {
	ChunkID    string `json:"chunk_id" db:"chunk_id"`
	FileID     string `json:"file_id" db:"file_id"`
	FileName   string `json:"file_name" db:"file_name"`
	PageNumber string `json:"page_number" db:"page_number"`
	BlockClass string `json:"block_class" db:"block_class"`
	CropURI    string `json:"crop_uri" db:"crop_uri"`
}

// GetCropReference resolves a chunk to the location of its crop image.
//
// This exists so that fetching an image goes through the governed path
// first. The object in storage carries no row access policy of its own, so
// reading it straight from a caller-supplied identifier would bypass every
// permission the view enforces. No row here means no read.
func GetCropReference(
	ctx context.Context, oauthToken, database, schema, table, chunkID string,
) (*CropReference, error) {
	query := fmt.Sprintf(
		`SELECT CHUNK_ID, FILE_ID, FILE_NAME, PAGE_NUMBER, BLOCK_CLASS, CROP_URI `+
			`FROM %s.%s.%s WHERE CHUNK_ID = ? LIMIT 1`,
		database, schema, table,
	)

	results, err := queryRows[CropReference](ctx, oauthToken, query, chunkID)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return &results[0], nil
}
