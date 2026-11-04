package unstructured

import (
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

type Chunker interface {
	Chunk(text string) ([]string, error)
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

func (c *MarkdownSplitter) Chunk(text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaMarkdownTextSplitter(text, c.Config)
}

func (c *RecursiveCharacterSplitter) Chunk(text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaRecursiveCharacterTextSplitter(text, c.Config)
}

func (c *TokenSplitter) Chunk(text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaTokenTextSplitter(text, c.Config)
}
