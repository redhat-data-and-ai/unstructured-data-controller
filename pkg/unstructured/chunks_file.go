package unstructured

import (
	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

type ChunkingTool string

const (
	LangchainChunkingTool ChunkingTool = "langchain"

	// DoclingChunkingTool labels chunks produced by docling's own chunker,
	// which splits on the document's structure instead of on character counts.
	DoclingChunkingTool ChunkingTool = "docling"
)

// ChunkingToolForStrategy reports which tool runs a strategy.
//
// Both the write of a chunks file and the check for whether one is current
// have to agree on this. ChunksFileMetadata.Equal compares the tool, so a
// label written by one and guessed by the other never matches and every file
// is chunked again on every reconcile.
func ChunkingToolForStrategy(strategy v1alpha1.ChunkingStrategy) ChunkingTool {
	if strategy == v1alpha1.ChunkingStrategyDoclingHybrid {
		return DoclingChunkingTool
	}
	// The three in process splitters all run through langchain. A strategy
	// with no case at all is rejected before any of this, so there is no third
	// answer to give.
	return LangchainChunkingTool
}

type Chunks struct {
	Text []string `json:"text"`
}

type ChunksFileMetadata struct {
	ConvertedFileMetadata *ConvertedFileMetadata         `json:"convertedFileMetadata"`
	ChunkingTool          ChunkingTool                   `json:"chunkingTool"`
	ChunksGeneratorConfig v1alpha1.ChunksGeneratorConfig `json:"chunksGeneratorConfig"`
}

type ChunksDocument struct {
	Metadata *ChunksFileMetadata `json:"metadata"`
	Chunks   *Chunks             `json:"chunks"`
}

type ChunksFile struct {
	ConvertedDocument *ConvertedDocument `json:"convertedDocument"`
	ChunksDocument    *ChunksDocument    `json:"chunksDocument"`
}

func (c *ChunksFileMetadata) Equal(other *ChunksFileMetadata) bool {
	if !c.ConvertedFileMetadata.Equal(other.ConvertedFileMetadata) {
		return false
	}
	if c.ChunkingTool != other.ChunkingTool {
		return false
	}
	if !cmp.Equal(c.ChunksGeneratorConfig, other.ChunksGeneratorConfig) {
		return false
	}
	return true
}
