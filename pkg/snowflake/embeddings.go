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

type ChunkResult struct {
	FileID     string `json:"file_id" db:"file_id"`
	ChunkIndex string `json:"chunk_index" db:"chunk_index"`
	ChunkText  string `json:"chunk_text" db:"chunk_text"`
	Score      string `json:"score" db:"score"`
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
