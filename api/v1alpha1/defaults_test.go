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
	// should not touch recursive config
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

func TestVectorEmbeddingsGeneratorConfig_SetDefaults_EmptyConfig(t *testing.T) {
	c := VectorEmbeddingsGeneratorConfig{}
	c.SetDefaults()

	if c.ModelName != DefaultEmbeddingModelName {
		t.Errorf("expected modelName %q, got %q", DefaultEmbeddingModelName, c.ModelName)
	}
}

func TestVectorEmbeddingsGeneratorConfig_SetDefaults_PreservesExplicitModel(t *testing.T) {
	c := VectorEmbeddingsGeneratorConfig{
		ModelName: "custom-model",
	}
	c.SetDefaults()

	if c.ModelName != "custom-model" {
		t.Errorf("expected modelName %q, got %q", "custom-model", c.ModelName)
	}
}

func TestSourceCrawlerConfig_SetDefaults_InfersS3(t *testing.T) {
	c := SourceCrawlerConfig{
		S3Config: S3Config{Bucket: "my-bucket"},
	}
	c.SetDefaults()

	if c.Type != TypeS3 {
		t.Errorf("expected type %q, got %q", TypeS3, c.Type)
	}
}

func TestSourceCrawlerConfig_SetDefaults_InfersGoogleDrive(t *testing.T) {
	c := SourceCrawlerConfig{
		GoogleDriveConfig: &GoogleDriveConfig{
			Folders: []GoogleDriveFolders{{URL: "https://drive.google.com/drive/folders/abc"}},
		},
	}
	c.SetDefaults()

	if c.Type != TypeGoogleDrive {
		t.Errorf("expected type %q, got %q", TypeGoogleDrive, c.Type)
	}
}

func TestSourceCrawlerConfig_SetDefaults_PreservesExplicitType(t *testing.T) {
	c := SourceCrawlerConfig{
		Type:     TypeS3,
		S3Config: S3Config{Bucket: "my-bucket"},
	}
	c.SetDefaults()

	if c.Type != TypeS3 {
		t.Errorf("expected type %q, got %q", TypeS3, c.Type)
	}
}

func TestSourceCrawlerConfig_SetDefaults_NoConfig(t *testing.T) {
	c := SourceCrawlerConfig{}
	c.SetDefaults()

	if c.Type != "" {
		t.Errorf("expected empty type when no config provided, got %q", c.Type)
	}
}
