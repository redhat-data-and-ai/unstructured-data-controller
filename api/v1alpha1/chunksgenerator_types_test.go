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

package v1alpha1

import (
	"testing"
)

func TestChunksGeneratorConfig_SetDefaults_EmptyConfig(t *testing.T) {
	c := ChunksGeneratorConfig{}
	c.SetDefaults()

	if c.Strategy != DefaultChunkingStrategy {
		t.Errorf("expected strategy %q, got %q", DefaultChunkingStrategy, c.Strategy)
	}
	if c.RecursiveCharacterSplitterConfig.ChunkSize != DefaultChunkSize {
		t.Errorf("expected chunkSize %d, got %d", DefaultChunkSize, c.RecursiveCharacterSplitterConfig.ChunkSize)
	}
	if c.RecursiveCharacterSplitterConfig.ChunkOverlap != DefaultChunkOverlap {
		t.Errorf("expected chunkOverlap %d, got %d", DefaultChunkOverlap, c.RecursiveCharacterSplitterConfig.ChunkOverlap)
	}
}

func TestChunksGeneratorConfig_SetDefaults_PreservesExplicitValues(t *testing.T) {
	c := ChunksGeneratorConfig{
		Strategy: ChunkingStrategyRecursiveCharacter,
		RecursiveCharacterSplitterConfig: RecursiveCharacterSplitterConfig{
			ChunkSize:    500,
			ChunkOverlap: 50,
		},
	}
	c.SetDefaults()

	if c.RecursiveCharacterSplitterConfig.ChunkSize != 500 {
		t.Errorf("expected chunkSize 500, got %d", c.RecursiveCharacterSplitterConfig.ChunkSize)
	}
	if c.RecursiveCharacterSplitterConfig.ChunkOverlap != 50 {
		t.Errorf("expected chunkOverlap 50, got %d", c.RecursiveCharacterSplitterConfig.ChunkOverlap)
	}
}

func TestChunksGeneratorConfig_SetDefaults_MarkdownStrategy(t *testing.T) {
	c := ChunksGeneratorConfig{
		Strategy: ChunkingStrategyMarkdown,
	}
	c.SetDefaults()

	if c.MarkdownSplitterConfig.ChunkSize != DefaultChunkSize {
		t.Errorf("expected chunkSize %d, got %d", DefaultChunkSize, c.MarkdownSplitterConfig.ChunkSize)
	}
	if c.MarkdownSplitterConfig.ChunkOverlap != DefaultChunkOverlap {
		t.Errorf("expected chunkOverlap %d, got %d", DefaultChunkOverlap, c.MarkdownSplitterConfig.ChunkOverlap)
	}
	if c.RecursiveCharacterSplitterConfig.ChunkSize != 0 {
		t.Errorf("expected recursive chunkSize 0, got %d", c.RecursiveCharacterSplitterConfig.ChunkSize)
	}
}

func TestChunksGeneratorConfig_SetDefaults_TokenStrategy(t *testing.T) {
	c := ChunksGeneratorConfig{
		Strategy: ChunkingStrategyToken,
	}
	c.SetDefaults()

	if c.TokenSplitterConfig.ChunkSize != DefaultChunkSize {
		t.Errorf("expected chunkSize %d, got %d", DefaultChunkSize, c.TokenSplitterConfig.ChunkSize)
	}
	if c.TokenSplitterConfig.ChunkOverlap != DefaultChunkOverlap {
		t.Errorf("expected chunkOverlap %d, got %d", DefaultChunkOverlap, c.TokenSplitterConfig.ChunkOverlap)
	}
}

// Selecting the strategy and a tokenizer has to be enough. Every one of
// docling's own defaults for the remaining settings is wrong here: max_tokens
// falls back to the tokenizer's limit, which is several times an embedding
// window, and the two flags default off, which leaves no display text and
// serialises tables as value triplets.
func TestChunksGeneratorConfig_SetDefaults_DoclingHybridStrategy(t *testing.T) {
	c := ChunksGeneratorConfig{
		Strategy:            ChunkingStrategyDoclingHybrid,
		HybridChunkerConfig: HybridChunkerConfig{Tokenizer: "/example/tokenizer"},
	}
	c.SetDefaults()

	if c.HybridChunkerConfig.MaxTokens != DefaultHybridMaxTokens {
		t.Errorf("expected maxTokens %d, got %d", DefaultHybridMaxTokens, c.HybridChunkerConfig.MaxTokens)
	}
	if c.HybridChunkerConfig.IncludeRawText == nil || !*c.HybridChunkerConfig.IncludeRawText {
		t.Error("expected includeRawText to default true, or there is nothing to display")
	}
	if c.HybridChunkerConfig.UseMarkdownTables == nil || !*c.HybridChunkerConfig.UseMarkdownTables {
		t.Error("expected useMarkdownTables to default true")
	}
	if c.HybridChunkerConfig.Tokenizer != "/example/tokenizer" {
		t.Errorf("expected tokenizer to be preserved, got %q", c.HybridChunkerConfig.Tokenizer)
	}
}

// Tokenizer has no default on purpose. There is no value that works on every
// deployment, admission rejects an empty one for this strategy, and a guess
// here would be a path that silently returns zero chunks.
func TestChunksGeneratorConfig_SetDefaults_DoclingHybridLeavesTokenizerEmpty(t *testing.T) {
	c := ChunksGeneratorConfig{Strategy: ChunkingStrategyDoclingHybrid}
	c.SetDefaults()

	if c.HybridChunkerConfig.Tokenizer != "" {
		t.Errorf("expected tokenizer to stay empty, got %q", c.HybridChunkerConfig.Tokenizer)
	}
}

func TestChunksGeneratorConfig_SetDefaults_DoclingHybridPreservesExplicitValues(t *testing.T) {
	off := false
	c := ChunksGeneratorConfig{
		Strategy: ChunkingStrategyDoclingHybrid,
		HybridChunkerConfig: HybridChunkerConfig{
			Tokenizer:         "/example/tokenizer",
			MaxTokens:         512,
			IncludeRawText:    &off,
			UseMarkdownTables: &off,
		},
	}
	c.SetDefaults()

	if c.HybridChunkerConfig.MaxTokens != 512 {
		t.Errorf("expected maxTokens 512, got %d", c.HybridChunkerConfig.MaxTokens)
	}
	if c.HybridChunkerConfig.IncludeRawText == nil || *c.HybridChunkerConfig.IncludeRawText {
		t.Error("expected includeRawText false to survive defaulting")
	}
	if c.HybridChunkerConfig.UseMarkdownTables == nil || *c.HybridChunkerConfig.UseMarkdownTables {
		t.Error("expected useMarkdownTables false to survive defaulting")
	}
}

// The other strategies must not grow this config. It travels into the
// metadata that decides whether a file needs chunking again, so populating it
// for a generator that never reads it would re-chunk that whole corpus, and
// then re-embed it.
func TestChunksGeneratorConfig_SetDefaults_OtherStrategiesLeaveHybridUnset(t *testing.T) {
	for _, strategy := range []ChunkingStrategy{
		"",
		ChunkingStrategyRecursiveCharacter,
		ChunkingStrategyMarkdown,
		ChunkingStrategyToken,
	} {
		t.Run("strategy="+string(strategy), func(t *testing.T) {
			c := ChunksGeneratorConfig{Strategy: strategy}
			c.SetDefaults()

			if c.HybridChunkerConfig != (HybridChunkerConfig{}) {
				t.Errorf("expected hybridChunkerConfig to stay unset, got %+v", c.HybridChunkerConfig)
			}
		})
	}
}

// The empty string still has to reach the existing default. 75 resources set
// it empty rather than omitting the field, because it is required in the
// generated CRD, and changing what it resolves to re-chunks all of them.
func TestChunksGeneratorConfig_SetDefaults_EmptyStrategyIsUnchanged(t *testing.T) {
	c := ChunksGeneratorConfig{}
	c.SetDefaults()

	if c.Strategy != ChunkingStrategyRecursiveCharacter {
		t.Errorf("expected the empty strategy to default to %q, got %q",
			ChunkingStrategyRecursiveCharacter, c.Strategy)
	}
}
