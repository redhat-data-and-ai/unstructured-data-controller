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

// Embeddings is one vector and what it was made from.
//
// Retrieval queries this artifact and not the chunks one, so a row here is
// everything a caller gets back from a match. The fields below the vector
// are what turn that row into a citation: without them a match can be read
// but not located, and the chunks it came from cannot be fetched again.
//
// They are copied from the chunks artifact rather than joined to it. This is
// a list of objects and the warehouse view reads each field straight off the
// flattened entry, so anything not on the entry is unreachable from SQL even
// though the whole chunks document is nested in the same file.
type Embeddings struct {
	Text      string    `json:"text"`
	Embedding []float64 `json:"embedding"`

	// ChunkID names the chunk this vector was made from, so a citation can
	// be fetched back. Stored as a scalar because the warehouse keeps it in
	// a column; see ChunkID and ParseChunkID for the two halves.
	ChunkID string `json:"chunkId,omitempty"`

	// RawText is the body without the heading trail, so a citation shows
	// what the reader should see rather than what was embedded.
	RawText string `json:"rawText,omitempty"`

	// BlockIDs and Regions say which parts of the document the chunk came
	// from and where they sit. Absent for a chunker that splits text, which
	// cannot know either.
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
