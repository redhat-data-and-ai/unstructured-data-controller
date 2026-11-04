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

// Chunk is one chunk as a chunker produced it, before anything is stored.
//
// Only Text is filled by every chunker. The rest describes where the chunk
// came from, which a chunker that splits a string cannot know, and which is
// what makes a citation possible.
type Chunk struct {
	// Text is what gets embedded. For a structure-aware chunker it is the
	// contextualised form, with the heading trail prepended, so a passage
	// stays findable under the section it belongs to.
	Text string

	// RawText is the body alone, and is what gets displayed. Empty where the
	// chunker has no way to separate the two.
	RawText string

	// BlockIDs are the addresses of the document elements the chunk was built
	// from, for example "#/texts/4". Empty for a chunker that works on text,
	// which has no elements to name.
	BlockIDs []string

	// Regions is where those elements sit on the page, already normalised.
	// A chunk built from several blocks has several, possibly across a page
	// break, which is why this is a list and not one rectangle.
	Regions []docling.Region

	// PageFirst and PageLast are the range of pages the chunk covers, and are
	// zero where the chunker does not know about pages.
	PageFirst int
	PageLast  int
}

// ChunkResult is what a chunker returns: the chunks, or a receipt for work
// that is still running.
//
// Both cases are a success. A chunker whose work outlives the call cannot
// return chunks from the call that starts it, so it returns the task id
// instead and the caller collects later.
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
// Chunk returns a task id and a later Collect returns that task's chunks.
//
// It is separate from Chunker, rather than both methods living on one
// interface, so that a chunker which finishes within the call does not have
// to declare a Collect it can never honour. A caller that has a task to
// collect asks for this with a type assertion, the way the standard library
// asks a http.ResponseWriter whether it is also a http.Flusher.
type AsyncChunker interface {
	Chunker

	// Collect returns the chunks of a task that Chunk started. The document
	// is passed again because the chunker resolves each chunk's elements
	// against it, and the call that started the task is long gone.
	Collect(ctx context.Context, taskID string, document *ConvertedDocument) (ChunkResult, error)
}

// TextChunks wraps plain strings as chunks that carry nothing but their text.
//
// This is everything a chunker that splits a string can say about its output.
// The fields it leaves empty are the ones a citation needs, which is the
// whole difference between the two kinds of chunker.
func TextChunks(texts []string) []Chunk {
	chunks := make([]Chunk, 0, len(texts))
	for _, text := range texts {
		chunks = append(chunks, Chunk{Text: text})
	}
	return chunks
}

// ChunkTexts returns the text of each chunk, in order.
//
// The stored artifact keeps text in its own list, so it stays readable by
// consumers that predate everything else a chunk now carries.
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
