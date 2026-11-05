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

package unstructured

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

func TestChunkingToolForStrategy(t *testing.T) {
	tests := []struct {
		name     string
		strategy v1alpha1.ChunkingStrategy
		want     ChunkingTool
	}{
		{
			name:     "the hybrid chunker runs inside docling",
			strategy: v1alpha1.ChunkingStrategyDoclingHybrid,
			want:     DoclingChunkingTool,
		},
		{
			name:     "recursive character splitting runs in process",
			strategy: v1alpha1.ChunkingStrategyRecursiveCharacter,
			want:     LangchainChunkingTool,
		},
		{
			name:     "markdown splitting runs in process",
			strategy: v1alpha1.ChunkingStrategyMarkdown,
			want:     LangchainChunkingTool,
		},
		{
			name:     "token splitting runs in process",
			strategy: v1alpha1.ChunkingStrategyToken,
			want:     LangchainChunkingTool,
		},
		{
			// The empty strategy is defaulted before anything reads it, and
			// the default is an in process splitter.
			name:     "the empty strategy reports the in process tool",
			strategy: "",
			want:     LangchainChunkingTool,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, ChunkingToolForStrategy(test.strategy))
		})
	}
}

// The reason the tool is derived rather than written twice. Equal compares it,
// so a writer recording one label and the freshness check assuming another
// never match, and the file is chunked again on every reconcile forever.
func TestChunksFileMetadataEqual_DiffersOnChunkingTool(t *testing.T) {
	convertedFileMetadata := &ConvertedFileMetadata{
		RawFilePath:       "raw/report.pdf",
		FileIdentifier:    "uid-1",
		DocumentConverter: DocumentConverterDocling,
	}
	config := v1alpha1.ChunksGeneratorConfig{Strategy: v1alpha1.ChunkingStrategyDoclingHybrid}

	written := &ChunksFileMetadata{
		ConvertedFileMetadata: convertedFileMetadata,
		ChunkingTool:          DoclingChunkingTool,
		ChunksGeneratorConfig: config,
	}
	sameToolAgain := &ChunksFileMetadata{
		ConvertedFileMetadata: convertedFileMetadata,
		ChunkingTool:          DoclingChunkingTool,
		ChunksGeneratorConfig: config,
	}
	assumedLangchain := &ChunksFileMetadata{
		ConvertedFileMetadata: convertedFileMetadata,
		ChunkingTool:          LangchainChunkingTool,
		ChunksGeneratorConfig: config,
	}

	assert.True(t, written.Equal(sameToolAgain), "the same chunking should compare equal")
	assert.False(t, written.Equal(assumedLangchain), "a different tool should force re-chunking")
}

// A chunker that splits a string knows nothing about where its output came
// from. Writing the geometry arrays full of empty values would change every
// existing artifact and give the warehouse columns of nothing to read.
func TestNewChunks_TextOnlyChunkerWritesOnlyText(t *testing.T) {
	stored := NewChunks(TextChunks([]string{"first", "second"}))

	assert.Equal(t, []string{"first", "second"}, stored.Text)
	assert.Nil(t, stored.RawText)
	assert.Nil(t, stored.BlockIDs)
	assert.Nil(t, stored.Regions)
	assert.Nil(t, stored.PageFirst)
	assert.Nil(t, stored.PageLast)

	// The artifact has to stay exactly what it was before any of these
	// fields existed, because every live pipeline writes this shape.
	encoded, err := json.Marshal(stored)
	require.NoError(t, err)
	assert.JSONEq(t, `{"text":["first","second"]}`, string(encoded))
}

func TestNewChunks_LaysChunksOutAsParallelArrays(t *testing.T) {
	region := docling.Region{PageNo: 1, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}}
	stored := NewChunks([]Chunk{
		{
			Text: "Heading\nfirst", RawText: "first",
			BlockIDs: []string{"#/texts/0"}, Regions: []docling.Region{region},
			PageFirst: 1, PageLast: 1,
		},
		{
			Text: "Heading\nsecond", RawText: "second",
			BlockIDs: []string{"#/texts/1", "#/texts/2"}, Regions: nil,
			PageFirst: 2, PageLast: 3,
		},
	})

	assert.Equal(t, []string{"Heading\nfirst", "Heading\nsecond"}, stored.Text)
	assert.Equal(t, []string{"first", "second"}, stored.RawText)
	assert.Equal(t, [][]string{{"#/texts/0"}, {"#/texts/1", "#/texts/2"}}, stored.BlockIDs)
	assert.Equal(t, [][]docling.Region{{region}, nil}, stored.Regions)
	assert.Equal(t, []int{1, 2}, stored.PageFirst)
	assert.Equal(t, []int{1, 3}, stored.PageLast)
}

// Every array has to be the same length as Text, because the warehouse view
// flattens Text and picks the rest out by the index that flatten produced. A
// short array silently yields NULL for the chunks past its end.
func TestNewChunks_EveryArrayIsTheSameLength(t *testing.T) {
	chunks := []Chunk{
		{Text: "a", BlockIDs: []string{"#/texts/0"}},
		{Text: "b"},
		{Text: "c", BlockIDs: []string{"#/texts/2"}},
	}

	stored := NewChunks(chunks)

	assert.Len(t, stored.Text, len(chunks))
	assert.Len(t, stored.RawText, len(chunks), "a chunk with no raw text still takes its slot")
	assert.Len(t, stored.BlockIDs, len(chunks))
	assert.Len(t, stored.Regions, len(chunks))
	assert.Len(t, stored.PageFirst, len(chunks))
	assert.Len(t, stored.PageLast, len(chunks))
}

func TestNewChunks_NoChunks(t *testing.T) {
	stored := NewChunks(nil)

	assert.Empty(t, stored.Text)
	assert.Nil(t, stored.BlockIDs)
}

func TestChunkIDRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		id   ChunkID
	}{
		{name: "an ordinary path", id: ChunkID{FilePath: "convert/report.pdf.json", Index: 7}},
		{name: "the first chunk", id: ChunkID{FilePath: "a.json", Index: 0}},
		{name: "a path containing the separator",
			id: ChunkID{FilePath: "convert/q1#final.pdf.json", Index: 3}},
		{name: "a path with spaces", id: ChunkID{FilePath: "convert/ACV Guidance.pdf.json", Index: 12}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := ParseChunkID(test.id.String())

			require.NoError(t, err)
			assert.Equal(t, test.id, parsed)
		})
	}
}

func TestChunkIDString(t *testing.T) {
	assert.Equal(t, "convert/report.pdf.json#7",
		ChunkID{FilePath: "convert/report.pdf.json", Index: 7}.String())
}

// The id is caller-supplied and names a file to read, so it is validated.
func TestParseChunkIDRejectsWhatCannotNameAChunk(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "no separator", id: "convert/report.pdf.json"},
		{name: "no index", id: "convert/report.pdf.json#"},
		{name: "a non-numeric index", id: "convert/report.pdf.json#seven"},
		{name: "a negative index", id: "convert/report.pdf.json#-1"},
		{name: "no file path", id: "#7"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseChunkID(test.id)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidChunkID)
		})
	}
}

func TestChunksAt(t *testing.T) {
	region := docling.Region{PageNo: 2, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}}
	chunks := NewChunks([]Chunk{
		{Text: "first", RawText: "one", BlockIDs: []string{"#/texts/0"},
			Regions: []docling.Region{region}, PageFirst: 2, PageLast: 2},
		{Text: "second", RawText: "two", BlockIDs: []string{"#/texts/1"}, PageFirst: 3, PageLast: 4},
	})

	assert.Equal(t, Chunk{
		Text: "first", RawText: "one", BlockIDs: []string{"#/texts/0"},
		Regions: []docling.Region{region}, PageFirst: 2, PageLast: 2,
	}, chunks.At(0))
	assert.Equal(t, 4, chunks.At(1).PageLast)
}

// A text splitter writes only the text array, as does an artifact written
// before the others existed. Reading one must not panic.
func TestChunksAt_ToleratesMissingAndShortArrays(t *testing.T) {
	textOnly := NewChunks(TextChunks([]string{"first", "second"}))

	assert.Equal(t, Chunk{Text: "second"}, textOnly.At(1))

	short := &Chunks{
		Text:     []string{"first", "second"},
		RawText:  []string{"one"},
		PageLast: []int{9},
	}
	assert.Equal(t, Chunk{Text: "second"}, short.At(1), "arrays that stop early are simply absent")
	assert.Equal(t, Chunk{Text: "first", RawText: "one", PageLast: 9}, short.At(0))
}

func TestChunksAt_OutOfRange(t *testing.T) {
	chunks := NewChunks(TextChunks([]string{"only"}))

	assert.Equal(t, Chunk{}, chunks.At(1))
	assert.Equal(t, Chunk{}, chunks.At(-1))
	assert.Equal(t, Chunk{}, (*Chunks)(nil).At(0))
}

func TestTextChunksAndChunkTextsRoundTrip(t *testing.T) {
	texts := []string{"first", "", "third"}

	chunks := TextChunks(texts)

	require.Len(t, chunks, 3)
	assert.Empty(t, chunks[0].BlockIDs, "a text splitter knows no addresses")
	assert.Equal(t, texts, ChunkTexts(chunks))
}
