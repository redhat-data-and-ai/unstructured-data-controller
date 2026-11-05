package unstructured

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// chunkIDSeparator divides the two halves of a ChunkID. A file path may
// legally contain one, which is why parsing splits on the last occurrence
// rather than the first.
const chunkIDSeparator = "#"

// ChunkID identifies one chunk of one converted file.
//
// Derived rather than stored, so the two stages that need it cannot drift:
// the chunks artifact holds no id of its own, and adding one would not have
// helped, since it is not part of the cache key and every artifact written
// before it existed would still be considered current.
//
// It is a type because a citation hands the id back to be resolved into a
// file and a chunk again, and that is parsing rather than splitting.
type ChunkID struct {
	// FilePath is the converted file the chunk came from.
	FilePath string
	// Index is the chunk's position in that file, counted from zero.
	Index int
}

// String renders the id for storage. This is the form the warehouse keeps in
// a column, so it stays a plain scalar rather than becoming an object.
func (id ChunkID) String() string {
	return id.FilePath + chunkIDSeparator + strconv.Itoa(id.Index)
}

// ParseChunkID reads an id back.
//
// It validates rather than splits, because the id arrives from whoever is
// asking to see a citation and names a file to be read on their behalf.
func ParseChunkID(id string) (ChunkID, error) {
	separator := strings.LastIndex(id, chunkIDSeparator)
	if separator <= 0 {
		return ChunkID{}, fmt.Errorf("%w: %q has no file path and index", ErrInvalidChunkID, id)
	}

	index, err := strconv.Atoi(id[separator+len(chunkIDSeparator):])
	if err != nil {
		return ChunkID{}, fmt.Errorf("%w: %q has no numeric index", ErrInvalidChunkID, id)
	}
	if index < 0 {
		return ChunkID{}, fmt.Errorf("%w: %q has a negative index", ErrInvalidChunkID, id)
	}

	return ChunkID{FilePath: id[:separator], Index: index}, nil
}

// ErrInvalidChunkID reports an id that cannot name a chunk.
var ErrInvalidChunkID = errors.New("not a chunk id")

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

// At reads one chunk back out of the parallel arrays.
//
// Every array but Text is optional, and a file chunked by a text splitter
// has none of them, so each is read only if it reaches this index. Without
// that, reading a chunk of an older artifact panics.
func (c *Chunks) At(index int) Chunk {
	if c == nil || index < 0 || index >= len(c.Text) {
		return Chunk{}
	}

	chunk := Chunk{Text: c.Text[index]}
	if index < len(c.RawText) {
		chunk.RawText = c.RawText[index]
	}
	if index < len(c.BlockIDs) {
		chunk.BlockIDs = c.BlockIDs[index]
	}
	if index < len(c.Regions) {
		chunk.Regions = c.Regions[index]
	}
	if index < len(c.PageFirst) {
		chunk.PageFirst = c.PageFirst[index]
	}
	if index < len(c.PageLast) {
		chunk.PageLast = c.PageLast[index]
	}

	return chunk
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
