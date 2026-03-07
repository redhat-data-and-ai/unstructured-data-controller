package unstructured

import (
	"context"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/tmc/langchaingo/textsplitter"
)

type Chunker interface {
	Chunk(ctx context.Context, input string) ([]string, error)
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

type DoclingHierarchicalChunker struct {
	DoclingClient *docling.Client
	Options       *docling.HierarchicalChunkingOptions
}

type DoclingHybridChunker struct {
	DoclingClient *docling.Client
	Options       *docling.HybridChunkingOptions
}

func (c *MarkdownSplitter) Chunk(_ context.Context, text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaMarkdownTextSplitter(text, c.Config)
}

func (c *RecursiveCharacterSplitter) Chunk(_ context.Context, text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaRecursiveCharacterTextSplitter(text, c.Config)
}

func (c *TokenSplitter) Chunk(_ context.Context, text string) ([]string, error) {
	return c.LangchainClient.SplitTextViaTokenTextSplitter(text, c.Config)
}

func (c *DoclingHierarchicalChunker) Chunk(ctx context.Context, fileURL string) ([]string, error) {
	return c.DoclingClient.ChunkFile(ctx, fileURL, docling.ChunkTypeHierarchical, c.Options)
}

func (c *DoclingHybridChunker) Chunk(ctx context.Context, fileURL string) ([]string, error) {
	return c.DoclingClient.ChunkFile(ctx, fileURL, docling.ChunkTypeHybrid, c.Options)
}
