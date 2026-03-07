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

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

func TestChunksFileMetadata_Equal_BothNilConvertedFileMetadata(t *testing.T) {
	a := &ChunksFileMetadata{
		ConvertedFileMetadata: nil,
		ChunkingTool:          DoclingChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyDoclingHierarchical,
		},
	}
	b := &ChunksFileMetadata{
		ConvertedFileMetadata: nil,
		ChunkingTool:          DoclingChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyDoclingHierarchical,
		},
	}

	if !a.Equal(b) {
		t.Error("expected Equal to return true when both ConvertedFileMetadata are nil and other fields match")
	}
}

func TestChunksFileMetadata_Equal_OneNilConvertedFileMetadata(t *testing.T) {
	a := &ChunksFileMetadata{
		ConvertedFileMetadata: nil,
		ChunkingTool:          DoclingChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyDoclingHierarchical,
		},
	}
	b := &ChunksFileMetadata{
		ConvertedFileMetadata: &ConvertedFileMetadata{
			RawFilePath:       "test/file.pdf",
			DocumentConverter: DocumentConverterDocling,
		},
		ChunkingTool: DoclingChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyDoclingHierarchical,
		},
	}

	if a.Equal(b) {
		t.Error("expected Equal to return false when one ConvertedFileMetadata is nil and the other is not")
	}
	if b.Equal(a) {
		t.Error("expected Equal to return false when one ConvertedFileMetadata is nil and the other is not (reversed)")
	}
}

func TestChunksFileMetadata_Equal_BothNonNilSame(t *testing.T) {
	meta := &ConvertedFileMetadata{
		RawFilePath:       "test/file.pdf",
		DocumentConverter: DocumentConverterDocling,
	}
	a := &ChunksFileMetadata{
		ConvertedFileMetadata: meta,
		ChunkingTool:          LangchainChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyRecursiveCharacter,
		},
	}
	b := &ChunksFileMetadata{
		ConvertedFileMetadata: &ConvertedFileMetadata{
			RawFilePath:       "test/file.pdf",
			DocumentConverter: DocumentConverterDocling,
		},
		ChunkingTool: LangchainChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyRecursiveCharacter,
		},
	}

	if !a.Equal(b) {
		t.Error("expected Equal to return true when both ConvertedFileMetadata are non-nil and match")
	}
}

func TestChunksFileMetadata_Equal_BothNonNilDifferent(t *testing.T) {
	a := &ChunksFileMetadata{
		ConvertedFileMetadata: &ConvertedFileMetadata{
			RawFilePath:       "test/file1.pdf",
			DocumentConverter: DocumentConverterDocling,
		},
		ChunkingTool: LangchainChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyRecursiveCharacter,
		},
	}
	b := &ChunksFileMetadata{
		ConvertedFileMetadata: &ConvertedFileMetadata{
			RawFilePath:       "test/file2.pdf",
			DocumentConverter: DocumentConverterDocling,
		},
		ChunkingTool: LangchainChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyRecursiveCharacter,
		},
	}

	if a.Equal(b) {
		t.Error("expected Equal to return false when ConvertedFileMetadata differ")
	}
}

func TestChunksFileMetadata_Equal_DifferentChunkingTool(t *testing.T) {
	a := &ChunksFileMetadata{
		ConvertedFileMetadata: nil,
		ChunkingTool:          DoclingChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyDoclingHierarchical,
		},
	}
	b := &ChunksFileMetadata{
		ConvertedFileMetadata: nil,
		ChunkingTool:          LangchainChunkingTool,
		ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyDoclingHierarchical,
		},
	}

	if a.Equal(b) {
		t.Error("expected Equal to return false when ChunkingTool differs")
	}
}
