package unstructured

import (
	"context"
	"errors"
	"strings"

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

// Chunker splits the rendered text of a document into chunks.
//
// A chunk produced this way is only a string. Nothing records which part of
// the source document it came from, so the position of a chunk in the original
// file is lost at this boundary whatever the caller does with the result.
type Chunker interface {
	Chunk(text string) ([]string, error)
}

// StructuredChunker splits a converted document into chunks that still name
// the blocks of the source document they were built from.
//
// It takes the converted document rather than a string because block
// identifiers live in the document's structure, and rendering that structure
// to text throws them away. That is what cannot be expressed as a Chunker.
//
// The structure now reaches this stage, in Content.DoclingDocument, so a
// chunker that resolves those identifiers can be written against this
// interface. Nothing implements one yet.
type StructuredChunker interface {
	ChunkDocument(ctx context.Context, doc *ConvertedDocument) ([]DocumentChunk, error)
}

// DocumentChunk is one chunk together with the source document blocks it
// covers.
type DocumentChunk struct {
	// Text is the chunk itself.
	Text string

	// BlockIDs addresses the blocks of the source document that this chunk was
	// built from, in document order. It is empty when the chunker cannot
	// attribute the chunk to any particular block.
	BlockIDs []string
}

// ChunkTexts returns the text of every chunk, in order.
//
// The chunks artifact holds a flat list of strings, so this is how a
// StructuredChunker's output is written out today: the block identifiers are
// dropped and the artifact is the same as the one a Chunker produces. A nil
// slice stays nil so the artifact serialises identically either way.
func ChunkTexts(chunks []DocumentChunk) []string {
	if chunks == nil {
		return nil
	}
	texts := make([]string, 0, len(chunks))
	for i := range chunks {
		texts = append(texts, chunks[i].Text)
	}
	return texts
}

// ValidateChunks reports ErrNoChunksProduced when content chunked to nothing.
//
// A document that really is empty is allowed through with no chunks, because
// there is nothing to lose: the crawler accepts files that hold no extractable
// text, and failing them would wedge the stage on input that can never
// succeed. Whitespace counts as empty for the same reason.
func ValidateChunks(content string, chunks []string) error {
	if len(chunks) == 0 && strings.TrimSpace(content) != "" {
		return ErrNoChunksProduced
	}
	return nil
}

// TextChunkerAdapter lets a plain Chunker stand in where a StructuredChunker
// is wanted, by chunking the document's markdown.
//
// It is a passthrough, not a block-aware chunker: markdown carries no block
// identifiers, so every chunk it returns has an empty BlockIDs. It exists so
// that one call site can drive either kind of chunker, and the chunks it
// returns are exactly the ones the wrapped Chunker produced.
type TextChunkerAdapter struct {
	Chunker Chunker
}

// ChunkDocument splits the document's markdown using the wrapped Chunker.
func (a *TextChunkerAdapter) ChunkDocument(_ context.Context, doc *ConvertedDocument) ([]DocumentChunk, error) {
	// The adapter is exported and has no constructor, so check both the
	// document it was handed and the chunker it was built with.
	if a.Chunker == nil {
		return nil, errors.New("chunking a document: the adapter has no chunker")
	}
	if doc == nil || doc.Content == nil {
		return nil, errors.New("chunking a document: the converted document has no content")
	}

	texts, err := a.Chunker.Chunk(doc.Content.Markdown)
	if err != nil {
		return nil, err
	}
	// Keep a nil result nil, so the chunks artifact is byte for byte what it
	// would have been had the Chunker been called directly.
	if texts == nil {
		return nil, nil
	}

	chunks := make([]DocumentChunk, 0, len(texts))
	for _, text := range texts {
		chunks = append(chunks, DocumentChunk{Text: text})
	}
	return chunks, nil
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

var (
	_ Chunker = (*MarkdownSplitter)(nil)
	_ Chunker = (*RecursiveCharacterSplitter)(nil)
	_ Chunker = (*TokenSplitter)(nil)

	_ StructuredChunker = (*TextChunkerAdapter)(nil)
)

func (c *MarkdownSplitter) Chunk(text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaMarkdownTextSplitter(text, c.Config)
}

func (c *RecursiveCharacterSplitter) Chunk(text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaRecursiveCharacterTextSplitter(text, c.Config)
}

func (c *TokenSplitter) Chunk(text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaTokenTextSplitter(text, c.Config)
}
