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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
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
