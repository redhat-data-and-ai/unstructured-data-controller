package unstructured

import (
	"slices"

	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
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

// Chunks is the stored form of a document's chunks.
//
// Parallel arrays aligned by index, and deliberately NOT a list of chunk
// objects. The warehouse view that reads this flattens chunks:text and picks
// the rest out with GET(array, index). Point it at a list of objects and the
// text column becomes NULL for every new file, with no error raised anywhere,
// while files written earlier keep working. A half populated view reporting
// success is worse than a loud failure.
type Chunks struct {
	// Text is the only field that has always been here, and consumers that
	// predate the rest still read it on its own.
	Text []string `json:"text"`

	// The rest describe where each chunk came from, and are present only for
	// a chunker that knows. A chunker that splits a string leaves all of
	// them out, so its artifact is unchanged from before they existed.
	RawText   []string           `json:"rawText,omitempty"`
	BlockIDs  [][]string         `json:"blockIds,omitempty"`
	Regions   [][]docling.Region `json:"regions,omitempty"`
	PageFirst []int              `json:"pageFirst,omitempty"`
	PageLast  []int              `json:"pageLast,omitempty"`
}

// NewChunks lays chunks out for storage.
//
// The geometry arrays are written only when something produced geometry.
// Emitting them full of empty values for a chunker that splits text would
// change every existing artifact and give the warehouse columns of nothing
// to read.
func NewChunks(chunks []Chunk) *Chunks {
	stored := &Chunks{Text: ChunkTexts(chunks)}

	// Any of the five, not just the addresses. A chunk can arrive with raw
	// text and a page range and no addresses at all, and gating on addresses
	// alone would drop the other four for the whole file.
	located := slices.ContainsFunc(chunks, func(chunk Chunk) bool {
		return chunk.RawText != "" || len(chunk.BlockIDs) > 0 || len(chunk.Regions) > 0 ||
			chunk.PageFirst != 0 || chunk.PageLast != 0
	})
	if !located {
		return stored
	}

	for _, chunk := range chunks {
		stored.RawText = append(stored.RawText, chunk.RawText)
		stored.BlockIDs = append(stored.BlockIDs, chunk.BlockIDs)
		stored.Regions = append(stored.Regions, chunk.Regions)
		stored.PageFirst = append(stored.PageFirst, chunk.PageFirst)
		stored.PageLast = append(stored.PageLast, chunk.PageLast)
	}

	return stored
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
