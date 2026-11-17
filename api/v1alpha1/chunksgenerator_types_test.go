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

// Selecting the strategy and a tokenizer has to be enough: docling's own
// defaults fall back to the tokenizer's token limit, leave no display text
// and serialise tables as value triplets.
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

// The empty string still has to reach the existing default. Resources set it
// empty rather than omitting the field, because the generated CRD makes it
// required, and changing what it resolves to re-chunks all of them.
func TestChunksGeneratorConfig_SetDefaults_EmptyStrategyIsUnchanged(t *testing.T) {
	c := ChunksGeneratorConfig{}
	c.SetDefaults()

	if c.Strategy != ChunkingStrategyRecursiveCharacter {
		t.Errorf("expected the empty strategy to default to %q, got %q",
			ChunkingStrategyRecursiveCharacter, c.Strategy)
	}
}

func TestChunksGenerator_AddOrUpdateJob_AppendsNewFiles(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.AddOrUpdateJob(ChunkJob{FilePath: "a.json", TaskID: "task-a"})
	generator.AddOrUpdateJob(ChunkJob{FilePath: "b.json", TaskID: "task-b"})

	if len(generator.Status.Jobs) != 2 {
		t.Fatalf("expected two jobs, got %d", len(generator.Status.Jobs))
	}
	if generator.Status.Jobs[0].TaskID != "task-a" || generator.Status.Jobs[1].TaskID != "task-b" {
		t.Errorf("expected the jobs to be kept in submission order, got %+v", generator.Status.Jobs)
	}
}

// A file has at most one job. A second entry for the same file would write the
// same artifact twice and hold a second slot against the chunking service for
// nothing.
func TestChunksGenerator_AddOrUpdateJob_ReplacesTheJobForTheSameFile(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.AddOrUpdateJob(ChunkJob{FilePath: "a.json", TaskID: "task-a", Attempts: 1})
	generator.AddOrUpdateJob(ChunkJob{FilePath: "a.json", TaskID: "task-a-retry", Attempts: 2})

	if len(generator.Status.Jobs) != 1 {
		t.Fatalf("expected one job, got %d", len(generator.Status.Jobs))
	}
	if generator.Status.Jobs[0].TaskID != "task-a-retry" {
		t.Errorf("expected the job to be replaced, got task id %q", generator.Status.Jobs[0].TaskID)
	}
	if generator.Status.Jobs[0].Attempts != 2 {
		t.Errorf("expected the replacement's attempt count, got %d", generator.Status.Jobs[0].Attempts)
	}
}

func TestChunksGenerator_GetJobByFilePath(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.AddOrUpdateJob(ChunkJob{FilePath: "a.json", TaskID: "task-a"})

	job := generator.GetJobByFilePath("a.json")
	if job == nil {
		t.Fatal("expected a job for a.json, got nil")
	}
	if job.TaskID != "task-a" {
		t.Errorf("expected task id %q, got %q", "task-a", job.TaskID)
	}
}

// Nil, not a zero value. The caller reads this as "already in flight", so a
// zero ChunkJob would make every file look submitted and nothing would ever be
// sent.
func TestChunksGenerator_GetJobByFilePath_UnknownFileIsNil(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.AddOrUpdateJob(ChunkJob{FilePath: "a.json", TaskID: "task-a"})

	if job := generator.GetJobByFilePath("b.json"); job != nil {
		t.Errorf("expected no job for an unsubmitted file, got %+v", job)
	}
	if job := (&ChunksGenerator{}).GetJobByFilePath("a.json"); job != nil {
		t.Errorf("expected no job when none have been submitted, got %+v", job)
	}
}

func TestChunksGenerator_DeleteJobByFilePath(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.AddOrUpdateJob(ChunkJob{FilePath: "a.json", TaskID: "task-a"})
	generator.AddOrUpdateJob(ChunkJob{FilePath: "b.json", TaskID: "task-b"})

	generator.DeleteJobByFilePath("a.json")

	if len(generator.Status.Jobs) != 1 {
		t.Fatalf("expected one job to remain, got %d", len(generator.Status.Jobs))
	}
	if generator.Status.Jobs[0].FilePath != "b.json" {
		t.Errorf("expected the other file's job to remain, got %q", generator.Status.Jobs[0].FilePath)
	}
}

// Deleting something that is not there has to be harmless. The collect path
// deletes after writing an artifact, and that can run twice if the status
// patch is retried.
func TestChunksGenerator_DeleteJobByFilePath_UnknownFileChangesNothing(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.AddOrUpdateJob(ChunkJob{FilePath: "a.json", TaskID: "task-a"})

	generator.DeleteJobByFilePath("b.json")

	if len(generator.Status.Jobs) != 1 {
		t.Errorf("expected the existing job to be untouched, got %+v", generator.Status.Jobs)
	}
}

func TestChunksGenerator_PermanentlyFailingFiles(t *testing.T) {
	generator := &ChunksGenerator{}

	if generator.IsFilePermanentlyFailing("a.json") {
		t.Error("expected a file nobody has given up on to not be failing")
	}

	generator.AddPermanentlyFailingFile("a.json")
	if !generator.IsFilePermanentlyFailing("a.json") {
		t.Error("expected the file to be recorded as permanently failing")
	}
	if generator.IsFilePermanentlyFailing("b.json") {
		t.Error("expected an unrelated file to be unaffected")
	}
}

// The file is added once per give-up and the list is never pruned, so without
// this the same path accumulates on status until the resource is edited.
func TestChunksGenerator_AddPermanentlyFailingFile_IsNotRecordedTwice(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.AddPermanentlyFailingFile("a.json")
	generator.AddPermanentlyFailingFile("a.json")

	if len(generator.Status.PermanentlyFailingFiles) != 1 {
		t.Errorf("expected one entry, got %v", generator.Status.PermanentlyFailingFiles)
	}
}

// Chunking usually fails for the whole corpus at once, on configuration
// rather than on the file. An unreachable tokenizer gives up on every file,
// so fixing it has to recover them or the only way back is editing status.
func TestChunksGenerator_ResetPermanentlyFailingFiles_OnASpecEdit(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.Generation = 4
	generator.AddPermanentlyFailingFile("a.json")

	if generator.ResetPermanentlyFailingFiles() {
		t.Error("expected no reset while the spec is unchanged")
	}

	generator.Generation = 5
	if !generator.ResetPermanentlyFailingFiles() {
		t.Error("expected the edited spec to clear the list")
	}
	if generator.Status.PermanentlyFailingFiles != nil {
		t.Errorf("expected the list to be cleared, got %v", generator.Status.PermanentlyFailingFiles)
	}
	if generator.IsFilePermanentlyFailing("a.json") {
		t.Error("expected the file to be eligible again")
	}
}

// It reports whether it changed anything, because the caller patches status
// on the answer and an empty patch on every reconcile is pure noise.
func TestChunksGenerator_ResetPermanentlyFailingFiles_NothingToReset(t *testing.T) {
	generator := &ChunksGenerator{}
	generator.Generation = 7

	if generator.ResetPermanentlyFailingFiles() {
		t.Error("expected no reset when no file has been given up on")
	}
}
