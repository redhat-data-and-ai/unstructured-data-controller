package unstructured

import (
	"context"
	"errors"
	"strings"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/tmc/langchaingo/textsplitter"
)

// ErrNoChunksProduced reports a document that carried content and chunked to
// nothing.
//
// Nothing downstream can tell that apart from a document that was genuinely
// empty. The chunks file is written, the embeddings stage finds no text to
// embed, the syncer uploads a row with no chunks, and the document is present
// in the warehouse as a title with no body. It is then never retried: the
// cache key is the converted file's metadata and the generator config, both
// unchanged, so the next reconcile sees the artifact as current. A question
// whose answer is in that document returns nothing, and no stage reports an
// error at any point.
var ErrNoChunksProduced = errors.New("document has content but produced no chunks")

// Chunk is one chunk as a chunker produced it. Only Text is filled by every
// chunker; the rest is where it came from, which a chunker that splits a
// string cannot know and which is what makes a citation possible.
type Chunk struct {
	// Text is embedded. For a structure-aware chunker it carries the
	// heading trail, so a passage stays findable under its section.
	Text string

	// RawText is the body alone, for display. Empty where the chunker cannot
	// separate the two.
	RawText string

	// BlockIDs are the addresses of the elements the chunk was built from,
	// for example "#/texts/4".
	BlockIDs []string

	// Regions is where those elements sit, normalised. A list because a
	// chunk built from several blocks has several boxes.
	Regions []docling.Region

	// PageFirst and PageLast are zero where the chunker has no pages.
	PageFirst int
	PageLast  int
}

// ChunkResult is what a chunker returns: the chunks, or a receipt for work
// still running. Both are a success. A chunker whose work outlives the call
// returns the task id and the caller collects later.
type ChunkResult struct {
	Chunks []Chunk
	TaskID string
}

// Pending reports whether the chunker started work rather than finishing it.
func (r ChunkResult) Pending() bool {
	return r.TaskID != ""
}

// Chunker turns a converted document into chunks.
type Chunker interface {
	Chunk(ctx context.Context, document *ConvertedDocument) (ChunkResult, error)
}

// AsyncChunker is a Chunker whose work outlives the call that starts it.
//
// Separate from Chunker so a chunker that finishes within the call need not
// declare a Collect it can never honour. Callers ask for it with a type
// assertion, as they do for http.Flusher.
type AsyncChunker interface {
	Chunker

	// Collect returns the chunks of a task Chunk started. The document is
	// passed again because the chunker resolves each chunk's elements
	// against it, and the starting call is long gone.
	Collect(ctx context.Context, taskID string, document *ConvertedDocument) (ChunkResult, error)
}

// TextChunks wraps plain strings as chunks carrying nothing but their text,
// which is all a chunker that splits a string can say about its output.
func TextChunks(texts []string) []Chunk {
	chunks := make([]Chunk, 0, len(texts))
	for _, text := range texts {
		chunks = append(chunks, Chunk{Text: text})
	}
	return chunks
}

// ChunkTexts returns the text of each chunk, in order. The artifact keeps it
// in its own list so older consumers still read it.
func ChunkTexts(chunks []Chunk) []string {
	texts := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		texts = append(texts, chunk.Text)
	}
	return texts
}

// ValidateChunks reports ErrNoChunksProduced when content chunked to nothing.
//
// A document that really is empty is allowed through with no chunks, because
// there is nothing to lose: the crawler accepts files that hold no extractable
// text, and failing them would wedge the stage on input that can never
// succeed. Whitespace counts as empty for the same reason.
func ValidateChunks(content string, chunks []Chunk) error {
	if len(chunks) == 0 && strings.TrimSpace(content) != "" {
		return ErrNoChunksProduced
	}
	return nil
}

type MarkdownSplitter struct {
	LangchainClient *langchain.Client
	Config          *textsplitter.Options
}

type RecursiveCharacterSplitter struct {
	LangchainClient *langchain.Client
	Config          *textsplitter.Options
}

type TokenSplitter struct {
	LangchainClient *langchain.Client
	Config          *textsplitter.Options
}

// The three splitters below work on the markdown and finish within the call,
// so none of them implements AsyncChunker. They take a context they do not
// use, because the interface is shared with a chunker that calls out over the
// network and must be cancellable.

func (c *MarkdownSplitter) Chunk(_ context.Context, document *ConvertedDocument) (ChunkResult, error) {
	texts, err := c.LangchainClient.SplitTextViaMarkdownTextSplitter(document.Content.Markdown, c.Config)
	if err != nil {
		return ChunkResult{}, err
	}
	return ChunkResult{Chunks: TextChunks(texts)}, nil
}

func (c *RecursiveCharacterSplitter) Chunk(_ context.Context, document *ConvertedDocument) (ChunkResult, error) {
	texts, err := c.LangchainClient.SplitTextViaRecursiveCharacterTextSplitter(document.Content.Markdown, c.Config)
	if err != nil {
		return ChunkResult{}, err
	}
	return ChunkResult{Chunks: TextChunks(texts)}, nil
}

func (c *TokenSplitter) Chunk(_ context.Context, document *ConvertedDocument) (ChunkResult, error) {
	texts, err := c.LangchainClient.SplitTextViaTokenTextSplitter(document.Content.Markdown, c.Config)
	if err != nil {
		return ChunkResult{}, err
	}
	return ChunkResult{Chunks: TextChunks(texts)}, nil
}
