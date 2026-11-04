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

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

func boolPointer(value bool) *bool { return &value }

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
