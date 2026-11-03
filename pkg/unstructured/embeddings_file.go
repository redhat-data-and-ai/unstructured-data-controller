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
	GeminiEmbedding2Config  v1alpha1.GeminiEmbedding2Config  `json:"geminiEmbedding2Config,omitempty"`
	Error                   string                           `json:"error,omitempty"`
}

type Embeddings struct {
	Text      string    `json:"text"`
	Embedding []float64 `json:"embedding"`

	// Block metadata, present only for documents chunked by block. All
	// optional so a document chunked from flat markdown serialises exactly as
	// it did before.
	ChunkID string `json:"chunkID,omitempty"`
	// EmbedText is what was actually sent to the embedding model: Text with a
	// metadata prefix. Kept so a retrieval result can be traced back to the
	// vector that matched it.
	EmbedText     string    `json:"embedText,omitempty"`
	BlockClass    string    `json:"blockClass,omitempty"`
	PageNumber    int       `json:"pageNumber,omitempty"`
	BlockIndex    int       `json:"blockIndex,omitempty"`
	PartIndex     int       `json:"partIndex,omitempty"`
	SectionHeader string    `json:"sectionHeader,omitempty"`
	BBox          []float64 `json:"bbox,omitempty"`
	CropURI       string    `json:"cropURI,omitempty"`
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
