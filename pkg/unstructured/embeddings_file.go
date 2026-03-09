package unstructured

import (
	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

type EmbeddingDocument struct {
	Metadata   *EmbeddingFileMetadata `json:"metadata"`
	Embeddings []*Embeddings          `json:"embeddings"`
}

type EmbeddingFileMetadata struct {
	ConvertedFileMetadata   *ConvertedFileMetadata           `json:"convertedFileMetadata"`
	ChunkFileMetadata       *ChunksFileMetadata              `json:"chunkFileMetadata"`
	ModelName               string                           `json:"modelName"`
	NomicEmbedTextV15Config v1alpha1.NomicEmbedTextV15Config `json:"nomicEmbedTextV15Config,omitempty"`
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
	if c.ModelName != other.ModelName {
		return false
	}
	if !cmp.Equal(c.NomicEmbedTextV15Config, other.NomicEmbedTextV15Config) {
		return false
	}
	return true
}
