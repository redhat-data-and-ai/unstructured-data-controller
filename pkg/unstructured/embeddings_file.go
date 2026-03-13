package unstructured

import (
	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
)

type EmbeddingDocument struct {
	Metadata   *EmbeddingFileMetadata `json:"metadata"`
	Embeddings []*Embeddings          `json:"embeddings"`
}

type EmbeddingFileMetadata struct {
	ConvertedFileMetadata    *ConvertedFileMetadata             `json:"convertedFileMetadata"`
	ChunkFileMetadata        *ChunksFileMetadata                `json:"chunkFileMetadata"`
	EmbeddingProvider        embedding.EmbeddingProvider        `json:"embeddingProvider"`
	EmbeddingGeneratorConfig embedding.EmbeddingGeneratorConfig `json:"embeddingGeneratorConfig"`
}

type Embeddings struct {
	Text      string    `json:"text"`
	Embedding []float64 `json:"embedding"`
}

type EmbeddingsFile struct {
	ConvertedDocument *ConvertedDocument `json:"convertedDocument"`
	ChunksDocument    *ChunksDocument    `json:"chunksDocument"`
	EmbeddingDocument *EmbeddingDocument `json:"embeddingDocument"`
}

func (c *EmbeddingFileMetadata) Equal(other *EmbeddingFileMetadata) bool {
	if !cmp.Equal(c.ConvertedFileMetadata, other.ConvertedFileMetadata) {
		return false
	}
	if !cmp.Equal(c.ChunkFileMetadata, other.ChunkFileMetadata) {
		return false
	}
	if c.EmbeddingProvider != other.EmbeddingProvider {
		return false
	}
	if !cmp.Equal(c.EmbeddingGeneratorConfig, other.EmbeddingGeneratorConfig) {
		return false
	}
	return true
}
