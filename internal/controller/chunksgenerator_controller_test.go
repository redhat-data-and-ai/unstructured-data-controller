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

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

func boolPointer(value bool) *bool { return &value }

// chunkerFor is the one place a strategy becomes behaviour. Everything
// downstream asks the chunker what happened rather than asking the config
// what it is, so a wrong answer here is a wrong answer everywhere.
func TestChunkerFor(t *testing.T) {
	tests := []struct {
		name     string
		strategy operatorv1alpha1.ChunkingStrategy
		want     unstructured.Chunker
		// async records whether the chunker's work outlives the call, which
		// is what the collect path asks for with a type assertion.
		async bool
	}{
		{
			name:     "recursive character splitting",
			strategy: operatorv1alpha1.ChunkingStrategyRecursiveCharacter,
			want:     &unstructured.RecursiveCharacterSplitter{},
		},
		{
			name:     "markdown splitting",
			strategy: operatorv1alpha1.ChunkingStrategyMarkdown,
			want:     &unstructured.MarkdownSplitter{},
		},
		{
			name:     "token splitting",
			strategy: operatorv1alpha1.ChunkingStrategyToken,
			want:     &unstructured.TokenSplitter{},
		},
		{
			name:     "the hybrid chunker, which runs remotely",
			strategy: operatorv1alpha1.ChunkingStrategyDoclingHybrid,
			want:     &unstructured.DoclingChunker{},
			async:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := operatorv1alpha1.ChunksGeneratorConfig{Strategy: test.strategy}
			config.SetDefaults()

			chunker, err := chunkerFor(config)

			require.NoError(t, err)
			assert.IsType(t, test.want, chunker)

			_, isAsync := chunker.(unstructured.AsyncChunker)
			assert.Equal(t, test.async, isAsync,
				"only a chunker whose work outlives the call should offer to collect it")
		})
	}
}

// The empty strategy is defaulted before anything reads it, and 75 live
// resources set it empty rather than omitting the field.
func TestChunkerFor_TheEmptyStrategyResolvesToTheDefault(t *testing.T) {
	config := operatorv1alpha1.ChunksGeneratorConfig{Strategy: ""}
	config.SetDefaults()

	chunker, err := chunkerFor(config)

	require.NoError(t, err)
	assert.IsType(t, &unstructured.RecursiveCharacterSplitter{}, chunker)
}

func TestChunkerFor_RejectsAStrategyWithNoChunker(t *testing.T) {
	_, err := chunkerFor(operatorv1alpha1.ChunksGeneratorConfig{Strategy: "somethingElse"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "somethingElse")
}

// The hybrid chunker's settings have to survive the trip through chunkerFor,
// since nothing downstream reads the config again.
func TestChunkerFor_CarriesTheHybridSettings(t *testing.T) {
	config := operatorv1alpha1.ChunksGeneratorConfig{
		Strategy: operatorv1alpha1.ChunkingStrategyDoclingHybrid,
		HybridChunkerConfig: operatorv1alpha1.HybridChunkerConfig{
			Tokenizer: "/modelcache/tokenizer",
		},
	}
	config.SetDefaults()

	chunker, err := chunkerFor(config)
	require.NoError(t, err)

	doclingChunker, ok := chunker.(*unstructured.DoclingChunker)
	require.True(t, ok)
	assert.Equal(t, docling.ChunkingOptions{
		Tokenizer:         "/modelcache/tokenizer",
		MaxTokens:         operatorv1alpha1.DefaultHybridMaxTokens,
		IncludeRawText:    true,
		UseMarkdownTables: true,
	}, doclingChunker.Options)
}

func TestChunkingOptionsFrom(t *testing.T) {
	tests := []struct {
		name   string
		config operatorv1alpha1.HybridChunkerConfig
		want   docling.ChunkingOptions
	}{
		{
			name: "every setting is carried across",
			config: operatorv1alpha1.HybridChunkerConfig{
				Tokenizer:         "/modelcache/tokenizer",
				MaxTokens:         2048,
				IncludeRawText:    boolPointer(true),
				UseMarkdownTables: boolPointer(true),
			},
			want: docling.ChunkingOptions{
				Tokenizer:         "/modelcache/tokenizer",
				MaxTokens:         2048,
				IncludeRawText:    true,
				UseMarkdownTables: true,
			},
		},
		{
			name: "an explicit false is honoured",
			config: operatorv1alpha1.HybridChunkerConfig{
				Tokenizer:         "/modelcache/tokenizer",
				MaxTokens:         512,
				IncludeRawText:    boolPointer(false),
				UseMarkdownTables: boolPointer(false),
			},
			want: docling.ChunkingOptions{
				Tokenizer:         "/modelcache/tokenizer",
				MaxTokens:         512,
				IncludeRawText:    false,
				UseMarkdownTables: false,
			},
		},
		{
			// Both flags default to true. Reading an unset pointer as false
			// would return chunks with no text to display and tables
			// serialised as value triplets, neither of which reports an error.
			name: "unset flags fall back to their documented default of true",
			config: operatorv1alpha1.HybridChunkerConfig{
				Tokenizer: "/modelcache/tokenizer",
				MaxTokens: 2048,
			},
			want: docling.ChunkingOptions{
				Tokenizer:         "/modelcache/tokenizer",
				MaxTokens:         2048,
				IncludeRawText:    true,
				UseMarkdownTables: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := chunkingOptionsFrom(test.config)
			assert.Equal(t, test.want, options)
			// The client sets this, because it has to agree with the endpoint
			// the client picked by path.
			assert.Empty(t, options.Chunker, "the chunker name is the client's to set")
		})
	}
}

// What the defaulted spec produces, so the mapping is checked against the
// values a real resource carries rather than against hand-written ones.
func TestChunkingOptionsFrom_DefaultedConfig(t *testing.T) {
	config := operatorv1alpha1.ChunksGeneratorConfig{
		Strategy: operatorv1alpha1.ChunkingStrategyDoclingHybrid,
		HybridChunkerConfig: operatorv1alpha1.HybridChunkerConfig{
			Tokenizer: "/modelcache/tokenizer",
		},
	}
	config.SetDefaults()

	assert.Equal(t, docling.ChunkingOptions{
		Tokenizer:         "/modelcache/tokenizer",
		MaxTokens:         operatorv1alpha1.DefaultHybridMaxTokens,
		IncludeRawText:    true,
		UseMarkdownTables: true,
	}, chunkingOptionsFrom(config.HybridChunkerConfig))
}
