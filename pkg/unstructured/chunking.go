package unstructured

import (
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/tmc/langchaingo/textsplitter"
)

type Chunker interface {
	Chunk(text string) ([]string, error)
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
