package unstructured

import (
	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
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
	GeminiEmbedding2Config  v1alpha1.GeminiEmbedding2Config  `json:"geminiEmbedding2Config,omitempty"`
}

// Embeddings is one vector and what it was made from. Retrieval reads this
// artifact rather than the chunks one, so the fields below the vector are
// what let a match be located as well as read.
type Embeddings struct {
	Text      string    `json:"text"`
	Embedding []float64 `json:"embedding"`

	// ChunkID names the chunk this vector was made from. See ChunkID.
	ChunkID string `json:"chunkId,omitempty"`

	// RawText is the body without the heading trail, for display.
	RawText string `json:"rawText,omitempty"`

	// BlockIDs and Regions locate the chunk in the document. Absent for a
	// chunker that splits text, which knows neither.
	BlockIDs []string         `json:"blockIds,omitempty"`
	Regions  []docling.Region `json:"regions,omitempty"`
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
	if !cmp.Equal(c.GeminiEmbedding2Config, other.GeminiEmbedding2Config) {
		return false
	}
	return true
}
