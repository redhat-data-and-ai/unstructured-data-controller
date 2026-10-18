package unstructured

import (
	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

type ChunkingTool string

const (
	LangchainChunkingTool ChunkingTool = "langchain"
)

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

type ChunkRow struct {
	FileID     string              `json:"fileId"`
	ChunkIndex int                 `json:"chunkIndex"`
	Text       string              `json:"text"`
	Metadata   *ChunksFileMetadata `json:"metadata"`
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
