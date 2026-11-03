package unstructured

import (
	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

type ChunkingTool string

const (
	LangchainChunkingTool ChunkingTool = "langchain"
	// BlockChunkingTool marks chunks derived from parser block boundaries
	// rather than from splitting flat markdown.
	BlockChunkingTool ChunkingTool = "blocks"
)

type Chunks struct {
	// Text is every chunk's body, in order. Kept populated by both the
	// markdown splitter and the block chunker so the chunks materialized view
	// and any v1 consumer keep working unchanged.
	Text []string `json:"text"`
	// Blocks is populated only by block-based chunking. Absent for documents
	// chunked from flat markdown.
	Blocks []BlockChunk `json:"blocks,omitempty"`
}

// BlockChunk is one semantic block of a document, with the metadata needed to
// cite it: where it came from, what kind of block it is, and the image crop of
// the region it occupies.
type BlockChunk struct {
	ChunkID string `json:"chunkID"`
	// Text is the indexed body: HTML for tables, markdown otherwise.
	Text string `json:"text"`
	// EmbedText is Text with a metadata prefix, and is what gets embedded.
	EmbedText     string    `json:"embedText"`
	BlockClass    string    `json:"blockClass"`
	PageNumber    int       `json:"pageNumber"`
	BlockIndex    int       `json:"blockIndex"`
	PartIndex     int       `json:"partIndex,omitempty"`
	SectionHeader string    `json:"sectionHeader,omitempty"`
	PageHeader    string    `json:"pageHeader,omitempty"`
	PageFooter    string    `json:"pageFooter,omitempty"`
	BBox          []float64 `json:"bbox,omitempty"`
	CropURI       string    `json:"cropURI,omitempty"`
}

type ChunksFileMetadata struct {
	ConvertedFileMetadata *ConvertedFileMetadata         `json:"convertedFileMetadata"`
	ChunkingTool          ChunkingTool                   `json:"chunkingTool"`
	ChunksGeneratorConfig v1alpha1.ChunksGeneratorConfig `json:"chunksGeneratorConfig"`
	Error                 string                         `json:"error,omitempty"`
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
