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
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every name here has to match a column the view defines. A wrong one fails
// the query for every caller rather than returning an empty field.
func TestChunkColumnsMatchTheView(t *testing.T) {
	assert.Equal(t,
		"FILE_ID, CHUNK_INDEX, CHUNK_TEXT, CHUNK_ID, RAW_TEXT, BLOCK_IDS, REGIONS",
		chunkColumns)
}

// The scanner maps columns onto fields by db tag, so a column with no tagged
// field is read and dropped without complaint.
func TestEveryChunkColumnHasAField(t *testing.T) {
	tagged := map[string]bool{}
	resultType := reflect.TypeOf(ChunkResult{})
	for i := range resultType.NumField() {
		if tag := resultType.Field(i).Tag.Get("db"); tag != "" {
			tagged[strings.ToLower(tag)] = true
		}
	}

	for _, column := range []string{
		"file_id", "chunk_index", "chunk_text", "chunk_id", "raw_text", "block_ids", "regions",
	} {
		assert.True(t, tagged[column], "no field is tagged db:%q", column)
	}
}

// The two VARIANT columns arrive as JSON text. Left alone they would reach a
// caller as JSON inside a JSON string, needing two decodes.
func TestPromoteVariants(t *testing.T) {
	chunks := promoteVariants([]ChunkResult{{
		BlockIDsRaw: `["#/texts/4","#/texts/5"]`,
		RegionsRaw:  `[{"pageNo":3,"bbox":[0.1,0.2,0.5,0.3]}]`,
	}})

	require.Len(t, chunks, 1)
	assert.JSONEq(t, `["#/texts/4","#/texts/5"]`, string(chunks[0].BlockIDs))
	assert.JSONEq(t, `[{"pageNo":3,"bbox":[0.1,0.2,0.5,0.3]}]`, string(chunks[0].Regions))
}

// Empty for anything embedded before the columns existed, which is every
// file today, so this is the common case rather than the edge.
func TestPromoteVariants_EmptyAndMalformed(t *testing.T) {
	chunks := promoteVariants([]ChunkResult{
		{BlockIDsRaw: "", RegionsRaw: ""},
		{BlockIDsRaw: "not json", RegionsRaw: "{"},
	})

	require.Len(t, chunks, 2)
	for _, chunk := range chunks {
		assert.Nil(t, chunk.BlockIDs)
		assert.Nil(t, chunk.Regions)
	}
}

// A row with no citation data must serialise without the new keys, so a
// caller can tell "not available" from "empty".
func TestChunkResultOmitsAbsentCitationFields(t *testing.T) {
	encoded, err := json.Marshal(ChunkResult{
		FileID: "f1", ChunkIndex: "0", ChunkText: "body", Score: "0.9",
	})
	require.NoError(t, err)

	assert.JSONEq(t,
		`{"file_id":"f1","chunk_index":"0","chunk_text":"body","score":"0.9"}`,
		string(encoded))
}

// The raw holders are scanner plumbing and must never reach a caller.
func TestChunkResultHidesTheRawVariantFields(t *testing.T) {
	encoded, err := json.Marshal(ChunkResult{
		BlockIDsRaw: `["#/texts/4"]`,
		RegionsRaw:  `[{"pageNo":1}]`,
	})
	require.NoError(t, err)

	assert.NotContains(t, string(encoded), "BlockIDsRaw")
	assert.NotContains(t, string(encoded), "RegionsRaw")
}
